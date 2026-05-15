"""ETL: журнал запусков.

Назначение:
    Создает и завершает записи в public.etl_parse_runs для контроля запусков
    ETL-задач.

Использование:
    Вызывается оркестратором перед запуском парсера и после его завершения.

Особенности:
    В безопасном режиме без --write-db журнал может пополняться, если доступно
    подключение к БД.
"""

from __future__ import annotations

from db_manager import execute_sql, fetch_one


def start_parse_run(
    *,
    job_name: str,
    script_name: str,
    target_table: str | None = None,
    source_file_id: int | None = None,
) -> int:
    """Создает запись о старте ETL-задачи и возвращает id запуска."""
    row = fetch_one(
        """
        INSERT INTO public.etl_parse_runs (
            source_file_id,
            job_name,
            script_name,
            target_table,
            status,
            started_at
        )
        VALUES (
            :source_file_id,
            :job_name,
            :script_name,
            :target_table,
            'running',
            now()
        )
        RETURNING id
        """,
        {
            "source_file_id": source_file_id,
            "job_name": job_name,
            "script_name": script_name,
            "target_table": target_table,
        },
    )
    if row is None:
        raise RuntimeError("Parse run insert did not return an id")
    return int(row["id"])


def finish_parse_run(
    run_id: int,
    *,
    status: str,
    rows_processed: int | None = None,
    error_message: str | None = None,
) -> None:
    """Закрывает запись запуска финальным статусом."""
    execute_sql(
        """
        UPDATE public.etl_parse_runs
        SET status = :status,
            rows_processed = :rows_processed,
            error_message = :error_message,
            finished_at = now()
        WHERE id = :id
        """,
        {
            "id": run_id,
            "status": status,
            "rows_processed": rows_processed,
            "error_message": error_message,
        },
    )
