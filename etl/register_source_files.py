"""ETL: регистрация исходных файлов.

Назначение:
    Сканирует папку data/ по правилам из config.SOURCE_DEFINITIONS,
    фильтрует файлы по активным годам и регистрирует их в
    public.etl_source_files.

Результат:
    В БД сохраняются переносимые относительные пути, размер файла, расширение,
    год, source_key и sha256.

Запуск:
    python3 orchestrator.py register-sources

Особенности:
    Файлы, которые больше не входят в активный диапазон источника, помечаются
    как ignored, а не удаляются физически.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import SOURCE_DEFINITIONS
from db_manager import close_engine
from etl.common import parse_year_from_filename, relative_to_project, sha256_file
from etl.source_registry import deactivate_unlisted_source_files, upsert_source_file


def iter_source_files(raw_dir: Path, patterns: list[str]) -> list[Path]:
    """Возвращает все файлы, подходящие под набор glob-паттернов."""
    files: set[Path] = set()
    for pattern in patterns:
        files.update(path for path in raw_dir.glob(pattern) if path.is_file())
    return sorted(files)


def filter_files_by_year(
    files: list[Path],
    *,
    year_from: int | None = None,
    year_to: int | None = None,
) -> list[Path]:
    """Оставляет только файлы из разрешенного диапазона лет."""
    selected: list[Path] = []

    for path in files:
        year = parse_year_from_filename(path)
        if year_from is not None and (year is None or year < year_from):
            continue
        if year_to is not None and (year is None or year > year_to):
            continue
        selected.append(path)

    return selected


def register_sources(*, source_key: str | None = None, with_checksum: bool = True) -> int:
    """Регистрирует исходные файлы одного или всех ETL-источников."""
    definitions = SOURCE_DEFINITIONS
    if source_key is not None:
        if source_key not in SOURCE_DEFINITIONS:
            raise KeyError(f"Unknown source key: {source_key}")
        definitions = {source_key: SOURCE_DEFINITIONS[source_key]}

    registered = 0
    for current_key, definition in definitions.items():
        raw_dir = Path(definition["raw_dir"])
        patterns = list(definition["patterns"])
        if not raw_dir.exists():
            print(f"SKIP {current_key}: raw dir does not exist: {raw_dir}")
            continue

        year_from = definition.get("year_from")
        year_to = definition.get("year_to")
        files = filter_files_by_year(
            iter_source_files(raw_dir, patterns),
            year_from=int(year_from) if year_from is not None else None,
            year_to=int(year_to) if year_to is not None else None,
        )
        active_relative_paths: list[str] = []

        for path in files:
            relative_path = relative_to_project(path)
            active_relative_paths.append(relative_path)
            file_hash = sha256_file(path) if with_checksum else ""
            upsert_source_file(
                source_key=current_key,
                source_group=str(definition["source_group"]),
                table_code=str(definition["table_code"]) if definition["table_code"] else None,
                year=parse_year_from_filename(path),
                relative_path=relative_path,
                file_name=path.name,
                file_ext=path.suffix.lower().lstrip("."),
                file_size=path.stat().st_size,
                sha256=file_hash,
                metadata={"raw_dir": relative_to_project(raw_dir)},
            )
            registered += 1
            print(f"registered {current_key}: {relative_path}")

        deactivate_unlisted_source_files(current_key, active_relative_paths)

    return registered


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register raw source files in DB")
    parser.add_argument("--source-key", help="Register only one source key")
    parser.add_argument(
        "--no-checksum",
        action="store_true",
        help="Skip sha256 calculation for faster registration",
    )
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        count = register_sources(
            source_key=args.source_key,
            with_checksum=not args.no_checksum,
        )
        print(f"Done. Registered files: {count}")
        return 0
    finally:
        close_engine()


if __name__ == "__main__":
    raise SystemExit(main())
