"""Единая точка запуска ETL-процессов.

Основные команды:
- `init-db` - применить `sql/database_schema.sql`;
- `register-sources` - зарегистрировать исходные файлы из `data/`;
- `run JOB` - безопасно выполнить один job без записи в целевую таблицу;
- `run JOB --write-db` - выполнить job с записью в целевую таблицу;
- `run-all` - безопасно выполнить весь ETL;
- `run-all --write-db --with-indicators` - полный цикл загрузки и расчетов;
- `validate` - контроль качества загруженных данных.
"""

from __future__ import annotations

import argparse
import importlib
import traceback
from pathlib import Path
from typing import Any

from config import PROJECT_ROOT, SOURCE_DEFINITIONS
from db_manager import close_engine, execute_sql_file, fetch_all, fetch_one
from etl.register_source_files import register_sources
from etl.run_registry import finish_parse_run, start_parse_run
from etl.source_registry import get_source_file_by_id, get_source_files


DEFAULT_JOB_ORDER = [
    "population",
    "regions",
    "form8_1000",
    "form33_2100",
    "form33_2200",
    "form33_2300",
    "form33_2310",
    "form33_2400",
    "form33_2500",
    "form33_2600",
    "form30_1100",
    "form30_2513",
    "form30_3100",
    "social_diseases_docx",
    "social_diseases_pdf",
]


INDICATORS_SQL_PATH = PROJECT_ROOT / "sql" / "tb_indicators.sql"


EXPECTED_SOURCE_FILES = {
    "population": (1, 2016, 2016),
    "regions": (1, None, None),
    "form8_1000": (9, 2016, 2024),
    "form33_2100": (10, 2015, 2024),
    "form33_2200": (9, 2016, 2024),
    "form33_2300": (9, 2016, 2024),
    "form33_2310": (9, 2016, 2024),
    "form33_2400": (9, 2016, 2024),
    "form33_2500": (10, 2015, 2024),
    "form33_2600": (9, 2016, 2024),
    "form30_1100": (9, 2016, 2024),
    "form30_2513": (9, 2016, 2024),
    "form30_3100": (9, 2016, 2024),
    "social_diseases_docx": (5, 2016, 2020),
    "social_diseases_pdf": (4, 2021, 2024),
}


EXPECTED_TABLE_STATS = {
    "public.tb_population": {"rows": 405, "min_year": 2016, "max_year": 2024},
    "public.tb_regions": {"rows": 85},
    "public.tb_form8_1000": {"rows": 3762, "min_year": 2016, "max_year": 2024},
    "public.tb_form33_2100": {"rows": 780, "min_year": 2015, "max_year": 2024},
    "public.tb_form33_2200": {"rows": 324, "min_year": 2016, "max_year": 2024},
    "public.tb_form33_2300": {"rows": 648, "min_year": 2016, "max_year": 2024},
    "public.tb_form33_2310": {"rows": 63, "min_year": 2016, "max_year": 2024},
    "public.tb_form33_2400": {"rows": 882, "min_year": 2016, "max_year": 2024},
    "public.tb_form33_2500": {"rows": 700, "min_year": 2015, "max_year": 2024},
    "public.tb_form33_2600": {"rows": 648, "min_year": 2016, "max_year": 2024},
    "public.tb_form30_1100": {"rows": 54, "min_year": 2016, "max_year": 2024},
    "public.tb_form30_2513": {"rows": 63, "min_year": 2016, "max_year": 2024},
    "public.tb_form30_3100": {"rows": 36, "min_year": 2016, "max_year": 2024},
    "public.tuberculosis_incidence_by_subjects_docx": {
        "rows": 1900,
        "min_year": 2016,
        "max_year": 2020,
    },
    "public.tuberculosis_incidence_by_subjects_pdf": {
        "rows": 1520,
        "min_year": 2021,
        "max_year": 2024,
    },
    "public.tb_calculated_indicators": {"rows": 1251, "min_year": 2016, "max_year": 2024},
}


def _rows_processed(result: Any) -> int | None:
    if result is None:
        return None
    if hasattr(result, "__len__"):
        try:
            return len(result)
        except TypeError:
            return None
    return None


def run_job(job_name: str, *, years: list[int] | None = None, skip_db: bool = True) -> Any:
    if job_name not in SOURCE_DEFINITIONS:
        raise KeyError(f"Unknown job: {job_name}")

    definition = SOURCE_DEFINITIONS[job_name]
    module_name = str(definition["script_module"])
    target_table = str(definition["target_table"]) if definition.get("target_table") else None
    run_id: int | None = None
    input_files: list[Path] | None

    try:
        input_files = get_source_files(job_name, years=years)
    except Exception as exc:
        if not skip_db:
            raise
        print(
            "WARNING: DB source registry is unavailable; running local skip-db fallback. "
            f"ETL run history will not be written. Details: {exc}"
        )
        input_files = None

    if input_files == []:
        raise RuntimeError(
            f"No registered source files for {job_name}. Run: python3 orchestrator.py register-sources"
        )

    if input_files is not None:
        run_id = start_parse_run(
            job_name=job_name,
            script_name=module_name,
            target_table=target_table,
        )

    try:
        module = importlib.import_module(module_name)
        if not hasattr(module, "run"):
            raise RuntimeError(
                f"{module_name} has no run(input_files=None, skip_db=False) yet. "
                "This parser is copied but not migrated."
            )

        result = module.run(input_files=input_files, skip_db=skip_db)
        if run_id is not None:
            finish_parse_run(
                run_id,
                status="success",
                rows_processed=_rows_processed(result),
            )
        return result
    except Exception as exc:
        if run_id is not None:
            finish_parse_run(
                run_id,
                status="failed",
                error_message="".join(traceback.format_exception_only(type(exc), exc)).strip(),
            )
        raise


def run_all_jobs(*, years: list[int] | None = None, skip_db: bool = True) -> dict[str, int | None]:
    results: dict[str, int | None] = {}

    for job_name in DEFAULT_JOB_ORDER:
        print(f"==> Running {job_name} ({'skip-db' if skip_db else 'write-db'})")
        result = run_job(job_name, years=years, skip_db=skip_db)
        rows_processed = _rows_processed(result)
        results[job_name] = rows_processed
        rows_label = "unknown" if rows_processed is None else str(rows_processed)
        print(f"<== Done {job_name}: rows={rows_label}")

    return results


def calculate_indicators() -> None:
    print(f"==> Calculating indicators from {INDICATORS_SQL_PATH}")
    execute_sql_file(INDICATORS_SQL_PATH)
    print("<== Done calculating indicators")


def _format_value(value: Any) -> str:
    return "NULL" if value is None else str(value)


def _print_check(name: str, ok: bool, details: str = "") -> None:
    status = "OK" if ok else "FAILED"
    suffix = f" - {details}" if details else ""
    print(f"[{status}] {name}{suffix}")


def _table_stats_sql(table_name: str, with_year: bool) -> str:
    if with_year:
        return f'''
            SELECT
                COUNT(*)::integer AS rows_count,
                MIN("Год")::integer AS min_year,
                MAX("Год")::integer AS max_year
            FROM {table_name}
        '''
    return f"SELECT COUNT(*)::integer AS rows_count FROM {table_name}"


def validate_source_files() -> bool:
    rows = fetch_all(
        """
        SELECT
            source_key,
            COUNT(*)::integer AS files_count,
            MIN(year)::integer AS min_year,
            MAX(year)::integer AS max_year
        FROM public.etl_source_files
        WHERE is_active = true
        GROUP BY source_key
        ORDER BY source_key
        """
    )
    actual = {row["source_key"]: row for row in rows}
    all_ok = True

    for source_key, expected in EXPECTED_SOURCE_FILES.items():
        expected_count, expected_min_year, expected_max_year = expected
        row = actual.get(source_key)
        ok = (
            row is not None
            and row["files_count"] == expected_count
            and row["min_year"] == expected_min_year
            and row["max_year"] == expected_max_year
        )
        all_ok = all_ok and ok
        if row is None:
            details = f"expected files={expected_count}, actual=missing"
        else:
            details = (
                f"files={row['files_count']}/{expected_count}, "
                f"years={_format_value(row['min_year'])}-{_format_value(row['max_year'])}"
            )
        _print_check(f"source_files.{source_key}", ok, details)

    extra_keys = sorted(set(actual) - set(EXPECTED_SOURCE_FILES))
    for source_key in extra_keys:
        all_ok = False
        _print_check(f"source_files.{source_key}", False, "unexpected source_key")

    return all_ok


def validate_parse_runs() -> bool:
    rows = fetch_all(
        """
        SELECT DISTINCT ON (job_name)
            job_name,
            status,
            rows_processed,
            error_message,
            started_at,
            finished_at
        FROM public.etl_parse_runs
        WHERE job_name = ANY(:job_names)
        ORDER BY job_name, started_at DESC
        """,
        {"job_names": DEFAULT_JOB_ORDER},
    )
    actual = {row["job_name"]: row for row in rows}
    all_ok = True

    for job_name in DEFAULT_JOB_ORDER:
        row = actual.get(job_name)
        ok = row is not None and row["status"] == "success"
        all_ok = all_ok and ok
        if row is None:
            details = "latest run missing"
        else:
            details = f"status={row['status']}, rows={_format_value(row['rows_processed'])}"
            if row["error_message"]:
                details = f"{details}, error={row['error_message']}"
        _print_check(f"parse_runs.{job_name}", ok, details)

    return all_ok


def validate_table_stats() -> bool:
    all_ok = True

    for table_name, expected in EXPECTED_TABLE_STATS.items():
        with_year = "min_year" in expected
        row = fetch_one(_table_stats_sql(table_name, with_year))
        if row is None:
            all_ok = False
            _print_check(f"table.{table_name}", False, "query returned no rows")
            continue

        ok = row["rows_count"] == expected["rows"]
        details = f"rows={row['rows_count']}/{expected['rows']}"

        if with_year:
            ok = (
                ok
                and row["min_year"] == expected["min_year"]
                and row["max_year"] == expected["max_year"]
            )
            details = (
                f"{details}, years={_format_value(row['min_year'])}-"
                f"{_format_value(row['max_year'])}"
            )

        all_ok = all_ok and ok
        _print_check(f"table.{table_name}", ok, details)

    return all_ok


def validate_calculated_indicators() -> bool:
    summary = fetch_one(
        """
        SELECT
            COUNT(*)::integer AS rows_count,
            COUNT(DISTINCT id)::integer AS indicators_count,
            COUNT(DISTINCT "Год")::integer AS years_count,
            MIN("Год")::integer AS min_year,
            MAX("Год")::integer AS max_year
        FROM public.tb_calculated_indicators
        """
    )
    duplicates = fetch_one(
        """
        SELECT COUNT(*)::integer AS duplicate_keys
        FROM (
            SELECT id, "Год"
            FROM public.tb_calculated_indicators
            GROUP BY id, "Год"
            HAVING COUNT(*) > 1
        ) AS duplicated
        """
    )
    if summary is None or duplicates is None:
        _print_check("calculated_indicators.summary", False, "query returned no rows")
        return False

    ok = (
        summary["rows_count"] == 1251
        and summary["indicators_count"] == 139
        and summary["years_count"] == 9
        and summary["min_year"] == 2016
        and summary["max_year"] == 2024
        and duplicates["duplicate_keys"] == 0
    )
    _print_check(
        "calculated_indicators.summary",
        ok,
        (
            f"rows={summary['rows_count']}/1251, "
            f"ids={summary['indicators_count']}/139, "
            f"years={summary['min_year']}-{summary['max_year']}, "
            f"duplicates={duplicates['duplicate_keys']}"
        ),
    )
    return ok


def validate_pipeline() -> bool:
    print("==> Validating ETL pipeline")
    checks = [
        validate_source_files(),
        validate_parse_runs(),
        validate_table_stats(),
        validate_calculated_indicators(),
    ]
    ok = all(checks)
    _print_check("pipeline", ok)
    return ok


def run_source_file(source_file_id: int, *, skip_db: bool = True) -> Any:
    row = get_source_file_by_id(source_file_id)
    if row is None:
        raise KeyError(f"Unknown source_file_id: {source_file_id}")

    job_name = row["source_key"]
    if job_name not in SOURCE_DEFINITIONS:
        raise KeyError(f"Unknown job for source file {source_file_id}: {job_name}")

    definition = SOURCE_DEFINITIONS[job_name]
    module_name = str(definition["script_module"])
    target_table = str(definition["target_table"]) if definition.get("target_table") else None
    input_file = Path(row["path"])

    run_id = start_parse_run(
        job_name=job_name,
        script_name=module_name,
        target_table=target_table,
        source_file_id=source_file_id,
    )

    try:
        module = importlib.import_module(module_name)
        if not hasattr(module, "run"):
            raise RuntimeError(
                f"{module_name} has no run(input_files=None, skip_db=False) yet. "
                "This parser is copied but not migrated."
            )

        result = module.run(input_files=[input_file], skip_db=skip_db)
        finish_parse_run(
            run_id,
            status="success",
            rows_processed=_rows_processed(result),
        )
        return result
    except Exception as exc:
        finish_parse_run(
            run_id,
            status="failed",
            error_message="".join(traceback.format_exception_only(type(exc), exc)).strip(),
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")

    register_parser = subparsers.add_parser("register-sources")
    register_parser.add_argument("--source-key")
    register_parser.add_argument("--no-checksum", action="store_true")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("job_name", choices=sorted(SOURCE_DEFINITIONS))
    run_parser.add_argument("--year", dest="years", action="append", type=int)
    run_parser.add_argument(
        "--write-db",
        action="store_true",
        help="Write parsed data to DB. By default jobs run with skip_db=True.",
    )

    run_all_parser = subparsers.add_parser("run-all")
    run_all_parser.add_argument("--year", dest="years", action="append", type=int)
    run_all_parser.add_argument(
        "--write-db",
        action="store_true",
        help="Write parsed data to DB. By default jobs run with skip_db=True.",
    )
    run_all_parser.add_argument(
        "--with-indicators",
        action="store_true",
        help="After run-all, execute sql/tb_indicators.sql. Requires --write-db.",
    )

    subparsers.add_parser(
        "calculate-indicators",
        help="Execute sql/tb_indicators.sql and rebuild public.tb_calculated_indicators.",
    )

    subparsers.add_parser(
        "validate",
        help="Run read-only ETL quality checks against source registry, parse runs, tables, and indicators.",
    )

    run_file_parser = subparsers.add_parser("run-file")
    run_file_parser.add_argument("source_file_id", type=int)
    run_file_parser.add_argument(
        "--write-db",
        action="store_true",
        help="Write parsed data to DB. By default jobs run with skip_db=True.",
    )

    subparsers.add_parser("list-jobs")
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()

        if args.command == "init-db":
            execute_sql_file(PROJECT_ROOT / "sql" / "database_schema.sql")
            print("Done. Database schema is ready.")
            return 0

        if args.command == "register-sources":
            count = register_sources(
                source_key=args.source_key,
                with_checksum=not args.no_checksum,
            )
            print(f"Done. Registered files: {count}")
            return 0

        if args.command == "run":
            run_job(args.job_name, years=args.years, skip_db=not args.write_db)
            return 0

        if args.command == "run-all":
            if args.with_indicators and not args.write_db:
                raise RuntimeError("--with-indicators requires --write-db")

            results = run_all_jobs(years=args.years, skip_db=not args.write_db)
            if args.with_indicators:
                calculate_indicators()
            print("Done. ETL summary:")
            for job_name, rows_processed in results.items():
                rows_label = "unknown" if rows_processed is None else str(rows_processed)
                print(f"{job_name}: {rows_label}")
            return 0

        if args.command == "calculate-indicators":
            calculate_indicators()
            return 0

        if args.command == "validate":
            try:
                return 0 if validate_pipeline() else 1
            except Exception as exc:
                print(f"[FAILED] validate - {type(exc).__name__}: {exc}")
                return 1

        if args.command == "run-file":
            run_source_file(args.source_file_id, skip_db=not args.write_db)
            return 0

        if args.command == "list-jobs":
            for job_name, definition in sorted(SOURCE_DEFINITIONS.items()):
                print(f"{job_name}: {definition['raw_dir']}")
            return 0

        raise RuntimeError(f"Unknown command: {args.command}")
    finally:
        close_engine()


if __name__ == "__main__":
    raise SystemExit(main())
