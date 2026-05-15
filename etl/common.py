"""ETL: общие утилиты.

Назначение:
    Содержит небольшие переиспользуемые функции для ETL-слоя: нормализацию
    текста, определение года из имени файла, расчет относительных путей,
    checksum и проверку существования таблиц.

Использование:
    Импортируется парсерами, реестром источников и оркестратором.

Особенности:
    Бизнес-логика конкретных форм здесь не хранится; она остается в отдельных
    модулях `etl/parse_*.py`.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from config import PROJECT_ROOT


TEXT_ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "windows-1251", "latin1"]


def normalize_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ").strip()
    text = " ".join(text.split())
    return "" if text.lower() in {"nan", "none", "null"} else text


def parse_year_from_filename(path: Path) -> int | None:
    match = re.search(r"(?:19|20)\d{2}", path.name)
    return int(match.group(0)) if match else None


def ensure_file_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")


def relative_to_project(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def resolve_project_path(relative_or_absolute_path: str | Path) -> Path:
    path = Path(relative_or_absolute_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    ensure_file_exists(path)
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def read_text_with_known_encoding(path: Path) -> tuple[str, str]:
    last_error: Exception | None = None
    for encoding in TEXT_ENCODINGS:
        try:
            return path.read_text(encoding=encoding), encoding
        except Exception as exc:
            last_error = exc
    raise ValueError(f"Could not read {path} with supported encodings") from last_error


def detect_delimiter(header_line: str) -> str:
    counts = {
        ";": header_line.count(";"),
        ",": header_line.count(","),
        "\t": header_line.count("\t"),
    }
    delimiter = max(counts, key=counts.get)
    return delimiter if counts[delimiter] > 0 else ","


def split_database_table_name(table_name: str) -> tuple[str, str]:
    parts = table_name.split(".", maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip('"'), parts[1].strip('"')
    return "public", table_name.strip('"')


def ensure_database_table_exists(connectable: Engine | Connection, table_name: str) -> None:
    schema, plain_table_name = split_database_table_name(table_name)
    sql = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_name = :table_name
        LIMIT 1
        """
    )
    params = {"schema": schema, "table_name": plain_table_name}

    if isinstance(connectable, Connection):
        exists = connectable.execute(sql, params).first() is not None
    else:
        with connectable.begin() as connection:
            exists = connection.execute(sql, params).first() is not None

    if not exists:
        raise RuntimeError(
            f"Таблица {schema}.{plain_table_name} не найдена. "
            "Сначала примените схему: python3 orchestrator.py init-db"
        )
