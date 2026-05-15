"""ETL: реестр исходных файлов.

Назначение:
    Инкапсулирует чтение и обновление public.etl_source_files. Парсеры получают
    входные файлы через этот модуль, а не через абсолютные пути.

Использование:
    Вызывается из orchestrator.py, register_source_files.py и отдельных
    ETL-парсеров при прямом запуске.

Особенности:
    В таблице хранятся только метаданные и относительные пути к файлам из data/.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from db_manager import fetch_all, fetch_one
from etl.common import resolve_project_path


def upsert_source_file(
    *,
    source_key: str,
    source_group: str,
    table_code: str | None,
    year: int | None,
    relative_path: str,
    file_name: str,
    file_ext: str,
    file_size: int,
    sha256: str,
    metadata: dict[str, Any] | None = None,
) -> int:
    """Создает или обновляет запись об исходном файле."""
    row = fetch_one(
        """
        INSERT INTO public.etl_source_files (
            source_key,
            source_group,
            table_code,
            year,
            relative_path,
            file_name,
            file_ext,
            file_size,
            sha256,
            metadata,
            status,
            is_active,
            updated_at
        )
        VALUES (
            :source_key,
            :source_group,
            :table_code,
            :year,
            :relative_path,
            :file_name,
            :file_ext,
            :file_size,
            :sha256,
            CAST(:metadata AS jsonb),
            'registered',
            true,
            now()
        )
        ON CONFLICT (source_key, relative_path)
        DO UPDATE SET
            source_group = EXCLUDED.source_group,
            table_code = EXCLUDED.table_code,
            year = EXCLUDED.year,
            file_name = EXCLUDED.file_name,
            file_ext = EXCLUDED.file_ext,
            file_size = EXCLUDED.file_size,
            sha256 = EXCLUDED.sha256,
            metadata = EXCLUDED.metadata,
            is_active = true,
            updated_at = now()
        RETURNING id
        """,
        {
            "source_key": source_key,
            "source_group": source_group,
            "table_code": table_code,
            "year": year,
            "relative_path": relative_path,
            "file_name": file_name,
            "file_ext": file_ext,
            "file_size": file_size,
            "sha256": sha256,
            "metadata": json.dumps(metadata or {}, ensure_ascii=False),
        },
    )
    if row is None:
        raise RuntimeError("Source file upsert did not return an id")
    return int(row["id"])


def get_source_files(
    source_key: str,
    *,
    years: list[int] | tuple[int, ...] | range | None = None,
    active_only: bool = True,
) -> list[Path]:
    """Возвращает активные файлы источника как локальные пути проекта."""
    params: dict[str, Any] = {"source_key": source_key}
    filters = ["source_key = :source_key"]

    if active_only:
        filters.append("is_active = true")

    if years is not None:
        years_list = list(years)
        params["years"] = years_list
        filters.append("year = ANY(:years)")

    rows = fetch_all(
        f"""
        SELECT relative_path
        FROM public.etl_source_files
        WHERE {" AND ".join(filters)}
        ORDER BY year NULLS LAST, relative_path
        """,
        params,
    )
    return [resolve_project_path(row["relative_path"]) for row in rows]


def get_source_file_by_id(source_file_id: int) -> dict[str, Any] | None:
    """Возвращает запись реестра и готовый локальный путь `path`."""
    row = fetch_one(
        """
        SELECT *
        FROM public.etl_source_files
        WHERE id = :id
        """,
        {"id": source_file_id},
    )
    if row is not None:
        row["path"] = resolve_project_path(row["relative_path"])
    return row


def deactivate_unlisted_source_files(source_key: str, active_relative_paths: list[str]) -> None:
    """Помечает неактуальные файлы источника как `ignored`."""
    from db_manager import execute_sql

    if active_relative_paths:
        execute_sql(
            """
            UPDATE public.etl_source_files
            SET is_active = false,
                status = 'ignored',
                updated_at = now()
            WHERE source_key = :source_key
              AND NOT (relative_path = ANY(:active_relative_paths))
            """,
            {
                "source_key": source_key,
                "active_relative_paths": active_relative_paths,
            },
        )
        return

    execute_sql(
        """
        UPDATE public.etl_source_files
        SET is_active = false,
            status = 'ignored',
            updated_at = now()
        WHERE source_key = :source_key
        """,
        {"source_key": source_key},
    )


def mark_source_file_status(source_file_id: int, status: str) -> None:
    """Обновляет статус конкретного исходного файла."""
    from db_manager import execute_sql

    execute_sql(
        """
        UPDATE public.etl_source_files
        SET status = :status,
            updated_at = now()
        WHERE id = :id
        """,
        {"id": source_file_id, "status": status},
    )
