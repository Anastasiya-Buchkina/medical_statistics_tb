"""Централизованный слой работы с PostgreSQL.

Модуль скрывает детали подключения к БД и задает единые правила:
- строка подключения берется только из `.env` через `config.DATABASE_URL`;
- используется один SQLAlchemy engine с маленьким пулом, чтобы не выбивать
  лимит подключений Supabase;
- DDL живет в `sql/database_schema.sql`, а загрузчики только пишут данные
  в уже существующие таблицы.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine, create_engine, text

from config import DATABASE_URL


class DatabaseConfigError(RuntimeError):
    """Ошибка конфигурации подключения к БД."""


class DatabaseManager:
    """Единая точка доступа к PostgreSQL для ETL-процессов."""

    def __init__(self, database_url: str | None = DATABASE_URL) -> None:
        if not database_url:
            raise DatabaseConfigError("DATABASE_URL is not set in .env")

        self.database_url = database_url
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self.database_url,
                connect_args={
                    "connect_timeout": 10,
                    "application_name": "medical_statistics_tb_etl",
                },
                pool_pre_ping=True,
                pool_size=1,
                max_overflow=0,
                pool_recycle=300,
            )
        return self._engine

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def execute_sql(self, sql: str, params: Mapping[str, Any] | None = None) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(sql), dict(params or {}))

    def execute_sql_file(self, path: str | Path) -> None:
        sql_path = Path(path)
        self.execute_sql(sql_path.read_text(encoding="utf-8"))

    def fetch_one(
        self,
        sql: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        with self.engine.begin() as connection:
            row = connection.execute(text(sql), dict(params or {})).mappings().first()
        return dict(row) if row is not None else None

    def fetch_all(
        self,
        sql: str,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self.engine.begin() as connection:
            rows = connection.execute(text(sql), dict(params or {})).mappings().all()
        return [dict(row) for row in rows]

    def execute_many(self, sql: str, rows: Iterable[Mapping[str, Any]]) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(sql), [dict(row) for row in rows])

    def table_exists(self, table_name: str, schema: str | None = None) -> bool:
        table_schema, plain_table_name = split_table_name(table_name, schema=schema)
        row = self.fetch_one(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table_name
            LIMIT 1
            """,
            {"schema": table_schema, "table_name": plain_table_name},
        )
        return row is not None

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        schema: str | None = None,
        if_exists: str = "append",
    ) -> None:
        if if_exists != "append":
            raise ValueError(
                "load_dataframe supports only if_exists='append'. "
                "Create tables via sql/database_schema.sql."
            )

        table_schema, plain_table_name = split_table_name(table_name, schema=schema)
        if not self.table_exists(plain_table_name, schema=table_schema):
            raise RuntimeError(
                f"Table {table_schema}.{plain_table_name} does not exist. "
                "Run: python3 orchestrator.py init-db"
            )

        df.to_sql(
            name=plain_table_name,
            con=self.engine,
            schema=table_schema,
            if_exists=if_exists,
            index=False,
            method="multi",
        )


def split_table_name(table_name: str, *, schema: str | None = None) -> tuple[str, str]:
    table_schema = schema or "public"
    plain_table_name = table_name

    if schema is None and "." in table_name:
        table_schema, plain_table_name = table_name.split(".", maxsplit=1)

    return table_schema.strip('"'), plain_table_name.strip('"')


_MANAGER: DatabaseManager | None = None


def get_engine() -> Engine:
    return get_manager().engine


def get_manager() -> DatabaseManager:
    global _MANAGER

    if _MANAGER is None:
        _MANAGER = DatabaseManager()
    return _MANAGER


def close_engine() -> None:
    global _MANAGER

    if _MANAGER is not None:
        _MANAGER.close()
        _MANAGER = None


def execute_sql(
    sql: str,
    params: Mapping[str, Any] | None = None,
    *,
    database_url: str | None = None,
) -> None:
    manager = DatabaseManager(database_url) if database_url else get_manager()
    manager.execute_sql(sql, params)
    if database_url:
        manager.close()


def execute_sql_file(path: str | Path) -> None:
    get_manager().execute_sql_file(path)


def fetch_one(sql: str, params: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    return get_manager().fetch_one(sql, params)


def fetch_all(sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    return get_manager().fetch_all(sql, params)


def execute_many(
    sql: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    database_url: str | None = None,
) -> None:
    manager = DatabaseManager(database_url) if database_url else get_manager()
    manager.execute_many(sql, rows)
    if database_url:
        manager.close()


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    database_url: str | None = None,
) -> None:
    manager = DatabaseManager(database_url) if database_url else get_manager()
    manager.load_dataframe(
        df,
        table_name,
        schema=schema,
        if_exists=if_exists,
    )
    if database_url:
        manager.close()


def quote_table_name(table_name: str, *, schema: str | None = None) -> str:
    table_schema, plain_table_name = split_table_name(table_name, schema=schema)
    return f'"{table_schema}"."{plain_table_name}"'


def delete_rows_for_years(
    table_name: str,
    years: Iterable[int],
    *,
    year_column: str = "Год",
    database_url: str | None = None,
) -> None:
    years_list = [int(year) for year in years]
    if not years_list:
        return

    manager = DatabaseManager(database_url) if database_url else get_manager()
    manager.execute_sql(
        f'DELETE FROM {quote_table_name(table_name)} WHERE "{year_column}" = ANY(:years)',
        {"years": years_list},
    )
    if database_url:
        manager.close()


def delete_rows_between_years(
    table_name: str,
    *,
    start_year: int,
    end_year: int,
    year_column: str = "Год",
    database_url: str | None = None,
) -> None:
    manager = DatabaseManager(database_url) if database_url else get_manager()
    manager.execute_sql(
        f"""
        DELETE FROM {quote_table_name(table_name)}
        WHERE "{year_column}" BETWEEN :start_year AND :end_year
        """,
        {"start_year": int(start_year), "end_year": int(end_year)},
    )
    if database_url:
        manager.close()
