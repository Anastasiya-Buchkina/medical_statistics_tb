"""ETL: загрузка демографии населения.

Назначение:
    Читает CSV-файл с численностью населения за 2016-2024 годы,
    преобразует широкую таблицу в long-формат и готовит данные для
    public.tb_population.

Источник:
    data/Другое/Население_2016-2024.csv

Результат:
    - processed/population_2016_2024_long.csv;
    - public.tb_population при запуске с --write-db.

Запуск:
    python3 orchestrator.py run population
    python3 orchestrator.py run population --write-db

Особенности:
    Без --write-db выполняется безопасный прогон: целевая таблица БД не
    изменяется, но при доступной БД запуск фиксируется в etl_parse_runs.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATABASE_URL, OTHER_DATA_DIR, PROCESSED_DIR
from db_manager import delete_rows_for_years, load_dataframe

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
SOURCE_KEY: Final[str] = "population"
DEFAULT_SOURCE_FILE: Final[Path] = OTHER_DATA_DIR / "Население_2016-2024.csv"
DEFAULT_OUTPUT_DIR: Final[Path] = PROCESSED_DIR
DEFAULT_OUTPUT_FILE: Final[str] = "population_2016_2024_long.csv"
DEFAULT_TABLE_NAME: Final[str] = "public.tb_population"

AGE_COLUMNS: Final[list[str]] = [
    "Всего",
    "0-4",
    "5-6",
    "7-14",
    "15-17",
    "18-24",
    "25-34",
    "35-44",
    "45-54",
    "55-64",
    "65+",
    "0-14",
    "15+",
    "0-17",
    "18+",
]

EXPECTED_SEX_VALUES: Final[set[str]] = {"всего", "мужчины", "женщины"}
EXPECTED_YEARS: Final[set[int]] = set(range(2016, 2025))

FINAL_COLUMNS: Final[list[str]] = [
    "Год",
    "Пол",
    "Возраст",
    "Численность",
]

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


class PopulationLoadError(RuntimeError):
    """Ошибка подготовки или загрузки данных населения."""

# -----------------------------------------------------------------------------
# Подготовка данных
# -----------------------------------------------------------------------------

def read_source_csv(path: Path) -> pd.DataFrame:
    """Читает исходный CSV с населением."""
    if not path.exists():
        raise PopulationLoadError(f"Файл не найден: {path}")

    return pd.read_csv(path, encoding="utf-8-sig")


def transform_population(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    """Преобразует таблицу населения из wide в long-формат."""
    required_columns = {"Год", "Пол", *AGE_COLUMNS}
    missing_columns = sorted(required_columns - set(df.columns))

    if missing_columns:
        raise PopulationLoadError(
            f"В исходном файле не хватает колонок: {missing_columns}"
        )

    work_df = df[["Год", "Пол", *AGE_COLUMNS]].copy()
    work_df["Год"] = pd.to_numeric(work_df["Год"], errors="raise").astype(int)
    work_df["Пол"] = work_df["Пол"].astype(str).str.strip()

    for column in AGE_COLUMNS:
        work_df[column] = pd.to_numeric(work_df[column], errors="raise").astype(int)

    long_df = work_df.melt(
        id_vars=["Год", "Пол"],
        value_vars=AGE_COLUMNS,
        var_name="Возраст",
        value_name="Численность",
    )

    long_df["Пол"] = pd.Categorical(
        long_df["Пол"],
        categories=["всего", "мужчины", "женщины"],
        ordered=True,
    )
    long_df["Возраст"] = pd.Categorical(
        long_df["Возраст"],
        categories=AGE_COLUMNS,
        ordered=True,
    )
    long_df = long_df[FINAL_COLUMNS].sort_values(
        ["Год", "Пол", "Возраст"]
    ).reset_index(drop=True)
    long_df["Пол"] = long_df["Пол"].astype(str)
    long_df["Возраст"] = long_df["Возраст"].astype(str)

    return long_df


def validate_population(df: pd.DataFrame) -> None:
    """Проверяет полноту и уникальность подготовленного датафрейма."""
    expected_rows = len(EXPECTED_YEARS) * len(EXPECTED_SEX_VALUES) * len(AGE_COLUMNS)

    if len(df) != expected_rows:
        raise PopulationLoadError(
            f"Некорректное число строк: {len(df)}. Ожидается: {expected_rows}"
        )

    actual_years = set(df["Год"].astype(int).unique())
    if actual_years != EXPECTED_YEARS:
        raise PopulationLoadError(
            f"Некорректный набор лет: {sorted(actual_years)}"
        )

    actual_sex_values = set(df["Пол"].astype(str).unique())
    if actual_sex_values != EXPECTED_SEX_VALUES:
        raise PopulationLoadError(
            f"Некорректные значения пола: {sorted(actual_sex_values)}"
        )

    actual_age_groups = set(df["Возраст"].astype(str).unique())
    if actual_age_groups != set(AGE_COLUMNS):
        raise PopulationLoadError(
            f"Некорректный набор возрастных групп: {sorted(actual_age_groups)}"
        )

    duplicates = df.duplicated(subset=["Год", "Пол", "Возраст"]).sum()
    if duplicates:
        raise PopulationLoadError(
            f"Найдены дубли по ключу Год + Пол + Возраст: {duplicates}"
        )

    if df["Численность"].isna().any():
        raise PopulationLoadError("В колонке Численность найдены пропуски")

    if (df["Численность"] < 0).any():
        raise PopulationLoadError("В колонке Численность найдены отрицательные значения")

    logger.info("Проверка данных пройдена: %s строк", len(df))


def save_control_csv(df: pd.DataFrame, output_dir: Path, output_file: str) -> Path:
    """Сохраняет контрольный CSV в папку processed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_file
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("CSV сохранен: %s", output_path)
    return output_path

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> Config:
    """Читает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Загрузка файла Население_2016-2024.csv в PostgreSQL"
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
        raise PopulationLoadError(
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
    result_df = transform_population(source_df, source_file)
    validate_population(result_df)
    save_control_csv(result_df, output_dir, output_file)

    if skip_db:
        logger.info("Загрузка в БД пропущена по флагу --skip-db")
        return result_df

    if db_url is None:
        raise PopulationLoadError("Не задана строка подключения к БД")

    years = sorted(result_df["Год"].astype(int).unique().tolist())
    delete_rows_for_years(table_name, years, database_url=db_url)
    load_dataframe(result_df, table_name, if_exists="append", database_url=db_url)
    logger.info("В таблицу %s загружено строк: %s", table_name, len(result_df))

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
        logger.exception("Ошибка выполнения загрузчика населения: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
