"""ETL: загрузка геополигонов регионов.

Назначение:
    Читает CSV-файл с полигонами регионов, нормализует название региона,
    проверяет JSON-полигон и готовит данные для public.tb_regions.

Источник:
    data/Другое/Regions.csv

Результат:
    - processed/regions.csv;
    - public.tb_regions при запуске с --write-db.

Запуск:
    python3 orchestrator.py run regions
    python3 orchestrator.py run regions --write-db

Особенности:
    Таблица регионов является справочником для BI-витрины карты DataLens.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATABASE_URL, OTHER_DATA_DIR, PROCESSED_DIR
from db_manager import execute_many, execute_sql

try:
    from etl.source_registry import get_source_files
except ImportError:
    get_source_files = None

# -----------------------------------------------------------------------------
# Логирование
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Константы
# -----------------------------------------------------------------------------

BASE_DIR: Final[Path] = Path(__file__).resolve().parent.parent
SOURCE_KEY: Final[str] = "regions"
DEFAULT_SOURCE_FILE: Final[Path] = OTHER_DATA_DIR / "Regions.csv"
DEFAULT_OUTPUT_DIR: Final[Path] = PROCESSED_DIR
DEFAULT_OUTPUT_FILE: Final[str] = "regions.csv"
DEFAULT_TABLE_NAME: Final[str] = "public.tb_regions"

SOURCE_REGION_COLUMN: Final[str] = "Регион ДТП"
REGION_COLUMN: Final[str] = "Регион"
POLYGON_COLUMN: Final[str] = "Полигон"
FINAL_COLUMNS: Final[list[str]] = [REGION_COLUMN, POLYGON_COLUMN]

# -----------------------------------------------------------------------------
# Модели и исключения
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """Параметры запуска."""

    source_file: Path
    output_dir: Path
    output_file: str
    table_name: str
    db_url: str | None
    skip_db: bool = False


class RegionsLoadError(RuntimeError):
    """Ошибка подготовки или загрузки регионов."""

# -----------------------------------------------------------------------------
# Подготовка данных
# -----------------------------------------------------------------------------

def read_source_csv(path: Path) -> pd.DataFrame:
    """Читает исходный CSV с регионами."""
    if not path.exists():
        raise RegionsLoadError(f"Файл не найден: {path}")

    return pd.read_csv(path, sep=";", encoding="cp1251")


def normalize_polygon(value: object, row_number: int) -> str:
    """Проверяет JSON полигона и возвращает компактную строку."""
    if pd.isna(value):
        raise RegionsLoadError(f"Пустой полигон в строке {row_number}")

    try:
        polygon = json.loads(str(value))
    except json.JSONDecodeError as exc:
        raise RegionsLoadError(
            f"Некорректный JSON в колонке Полигон, строка {row_number}: {exc}"
        ) from exc

    if not isinstance(polygon, list) or not polygon:
        raise RegionsLoadError(
            f"Полигон должен быть непустым JSON-массивом, строка {row_number}"
        )

    return json.dumps(polygon, ensure_ascii=False, separators=(",", ":"))


def transform_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит исходную таблицу к структуре для БД."""
    required_columns = {SOURCE_REGION_COLUMN, POLYGON_COLUMN}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise RegionsLoadError(
            f"В исходном файле не хватает колонок: {missing_columns}"
        )

    result_df = df[[SOURCE_REGION_COLUMN, POLYGON_COLUMN]].copy()
    result_df = result_df.rename(columns={SOURCE_REGION_COLUMN: REGION_COLUMN})
    result_df[REGION_COLUMN] = result_df[REGION_COLUMN].astype(str).str.strip()
    result_df[POLYGON_COLUMN] = [
        normalize_polygon(value, row_number)
        for row_number, value in enumerate(result_df[POLYGON_COLUMN], start=2)
    ]

    return result_df[FINAL_COLUMNS].reset_index(drop=True)


def validate_regions(df: pd.DataFrame) -> None:
    """Проверяет обязательные поля подготовленного датафрейма."""
    if df.empty:
        raise RegionsLoadError("После обработки не осталось строк")

    empty_regions = df[REGION_COLUMN].eq("").sum()
    if empty_regions:
        raise RegionsLoadError(f"Найдены пустые значения региона: {empty_regions}")

    if df[POLYGON_COLUMN].eq("").any():
        raise RegionsLoadError("В колонке Полигон найдены пустые значения")

    duplicate_regions = int(df.duplicated(subset=[REGION_COLUMN]).sum())
    if duplicate_regions:
        logger.warning(
            "Найдены повторяющиеся регионы: %s. Строки будут сохранены все.",
            duplicate_regions,
        )

    logger.info("Проверка данных пройдена: %s строк", len(df))


def save_control_csv(df: pd.DataFrame, output_dir: Path, output_file: str) -> Path:
    """Сохраняет контрольный CSV в папку processed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_file
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("CSV сохранен: %s", output_path)
    return output_path


def replace_table_data(table_name: str, df: pd.DataFrame, *, db_url: str | None = None) -> None:
    """Полностью перезаписывает данные регионов в таблице."""
    records = df.to_dict(orient="records")

    execute_sql(f"DELETE FROM {table_name};", database_url=db_url)
    execute_many(
        f"""
        INSERT INTO {table_name} (
            "Регион",
            "Полигон"
        )
        VALUES (
            :Регион,
            CAST(:Полигон AS jsonb)
        );
        """,
        records,
        database_url=db_url,
    )
    logger.info("В таблицу %s загружено строк: %s", table_name, len(df))

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> Config:
    """Читает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Загрузка файла Regions.csv в PostgreSQL"
    )
    parser.add_argument(
        "--source-file",
        type=Path,
        default=DEFAULT_SOURCE_FILE,
        help=f"Путь к исходному CSV. По умолчанию: {DEFAULT_SOURCE_FILE}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Папка для контрольного CSV. По умолчанию: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--output-file",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Имя контрольного CSV. По умолчанию: {DEFAULT_OUTPUT_FILE}",
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
        help=f"Целевая таблица. По умолчанию: {DEFAULT_TABLE_NAME}",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="Строка подключения к PostgreSQL. Если не указана, берется из DATABASE_URL.",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Только подготовить и сохранить CSV, без загрузки в БД.",
    )

    args = parser.parse_args()

    db_url = args.db_url or DATABASE_URL
    if not args.skip_db and not db_url:
        raise RegionsLoadError(
            "Не задана строка подключения. Передайте --db-url, "
            "задайте DATABASE_URL в .env или используйте --skip-db."
        )

    return Config(
        source_file=args.source_file,
        output_dir=args.output_dir,
        output_file=args.output_file,
        table_name=args.table_name,
        db_url=db_url,
        skip_db=args.skip_db,
    )


def _default_input_file() -> Path:
    if get_source_files is not None:
        try:
            files = get_source_files(SOURCE_KEY)
            if files:
                return files[0]
        except Exception as exc:
            logger.warning("Не удалось получить файл из реестра БД: %s", exc)
    return DEFAULT_SOURCE_FILE


def run(
    input_files: list[Path] | None = None,
    *,
    skip_db: bool = False,
    db_url: str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_file: str = DEFAULT_OUTPUT_FILE,
    table_name: str = DEFAULT_TABLE_NAME,
) -> pd.DataFrame:
    """ETL entrypoint for orchestrator and direct imports."""
    source_file = input_files[0] if input_files else _default_input_file()
    db_url = db_url or DATABASE_URL

    source_df = read_source_csv(source_file)
    result_df = transform_regions(source_df)
    validate_regions(result_df)
    save_control_csv(result_df, output_dir, output_file)

    if skip_db:
        logger.info("Загрузка в БД пропущена по флагу --skip-db")
        return result_df

    if db_url is None:
        raise RegionsLoadError("Не задана строка подключения к БД")

    replace_table_data(table_name, result_df, db_url=db_url)

    return result_df


def main() -> int:
    """Основной сценарий загрузки."""
    try:
        config = parse_args()
        run(
            input_files=[config.source_file],
            skip_db=config.skip_db,
            db_url=config.db_url,
            output_dir=config.output_dir,
            output_file=config.output_file,
            table_name=config.table_name,
        )
        return 0

    except Exception as exc:
        logger.exception("Ошибка выполнения загрузчика регионов: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
