"""ETL: форма N8, таблица 1000.

Назначение:
    Парсит Excel-файлы формы N8 за 2016-2024 годы, извлекает строки 1-38
    и графы 5-15, нормализует показатели и возрастные группы.

Источник:
    data/33_8_2015-2024/*.xls(x)

Результат:
    - processed/tb_form8_1000.csv;
    - public.tb_form8_1000 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form8_1000
    python3 orchestrator.py run form8_1000 --write-db

Особенности:
    2015 год для этой таблицы не используется. Пустые значения, прочерки и
    многоточия в числовых ячейках трактуются как 0.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATABASE_URL, FORM_8_33_DIR, PROCESSED_DIR
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
# Константы проекта
# -----------------------------------------------------------------------------

EXCEL_SUFFIXES = {".xls", ".xlsx", ".xlsm"}

SOURCE_KEY = "form8_1000"
TABLE_CODE = "1000"
DEFAULT_TABLE_NAME = "public.tb_form8_1000"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = FORM_8_33_DIR
DEFAULT_OUTPUT_DIR = PROCESSED_DIR
DEFAULT_OUTPUT_FILE = "tb_form8_1000.csv"
TARGET_YEARS = set(range(2016, 2025))

EXPECTED_SOURCE_FILES = [
    "8_2016.xlsx",
    "2017.xls",
    "2018.xls",
    "2019.xls",
    "2020.xls",
    "2021.xls",
    "2022.xls",
    "2023.xls",
    "2024.xls",
]

ROW_NUMBER_TO_INDICATOR: dict[int, str] = {
    1: "Впервые выявленный активный туберкулез, всего",
    2: "Впервые выявленный активный туберкулез, всего",
    3: "Впервые выявленный активный туберкулез, МБТ+ (любой метод)",
    4: "Впервые выявленный активный туберкулез, МБТ+ (любой метод)",
    5: "Впервые выявленный туберкулез органов дыхания",
    6: "Впервые выявленный туберкулез органов дыхания",
    7: "Впервые выявленный туберкулез легких",
    8: "Впервые выявленный туберкулез легких",
    9: "Туберкулез легких, МБТ+ только культурально",
    10: "Туберкулез легких, МБТ+ только культурально",
    11: "Туберкулез легких, МБТ+ бактериоскопически",
    12: "Туберкулез легких, МБТ+ бактериоскопически",
    13: "Фиброзно-кавернозный туберкулез легких",
    14: "Фиброзно-кавернозный туберкулез легких",
    15: "Впервые выявленный туберкулез внелегочных локализаций",
    16: "Впервые выявленный туберкулез внелегочных локализаций",
    17: "Туберкулез мозговых оболочек и ЦНС",
    18: "Туберкулез мозговых оболочек и ЦНС",
    19: "Туберкулез костей и суставов",
    20: "Туберкулез костей и суставов",
    21: "Туберкулез мочеполовых органов",
    22: "Туберкулез мочеполовых органов",
    23: "Туберкулез женских половых органов",
    24: "Туберкулез периферических лимфатических узлов",
    25: "Туберкулез периферических лимфатических узлов",
    26: "Впервые выявленный активный туберкулез, сельские жители",
    27: "Впервые выявленный активный туберкулез, сельские жители",
    28: "Впервые выявленный активный туберкулез, иностранные граждане",
    29: "Впервые выявленный активный туберкулез, иностранные граждане",
    30: "Впервые выявленный активный туберкулез, контингент УИН",
    31: "Впервые выявленный активный туберкулез, контингент УИН",
    32: "Впервые выявленный активный туберкулез, лица БОМЖ",
    33: "Впервые выявленный активный туберкулез, выявлен посмертно",
    34: "Впервые выявленный активный туберкулез, выявлен посмертно",
    35: "Рецидив туберкулеза, выявленный в отчетном году",
    36: "Рецидив туберкулеза, выявленный в отчетном году",
    37: "Впервые выявленный активный туберкулез, МБТ+ (любой метод)",
    38: "Впервые выявленный активный туберкулез, МБТ+ (любой метод)",
}

COLUMN_NUMBER_TO_AGE: dict[int, str] = {
    5: "всего",
    6: "0-4",
    7: "5-6",
    8: "7-14",
    9: "15-17",
    10: "18-24",
    11: "25-34",
    12: "35-44",
    13: "45-54",
    14: "55-64",
    15: "65+",
}

FINAL_COLUMNS = [
    "Формы туберкулеза",
    "Код по МКБ-Х пересмотра",
    "Пол",
    "Возраст",
    "Год",
    "Значение",
    "Строка",
    "Графа",
    "Таблица",
]

# -----------------------------------------------------------------------------
# Модели и исключения
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """Параметры запуска парсера."""

    input_files: list[Path]
    db_url: str | None
    table_name: str = DEFAULT_TABLE_NAME
    output_dir: Path = DEFAULT_OUTPUT_DIR
    output_file: str = DEFAULT_OUTPUT_FILE
    if_exists: str = "append"
    skip_db: bool = False


class ParserError(RuntimeError):
    """Ошибка парсинга или валидации таблицы 1000."""

# -----------------------------------------------------------------------------
# Универсальные функции очистки и преобразования значений
# -----------------------------------------------------------------------------

def normalize_dash(value: str) -> str:
    """Унифицирует разные типы тире."""
    return (
        value
        .replace("—", "-")
        .replace("–", "-")
        .replace("−", "-")
    )


def clean_cell(value: Any) -> str:
    """Возвращает очищенное текстовое значение ячейки Excel."""
    if value is None or pd.isna(value):
        return ""

    return re.sub(r"\s+", " ", str(value).replace("\n", " ")).strip()


def normalize_text(value: Any) -> str:
    """Нормализует текст для поиска служебных маркеров."""
    text_value = clean_cell(value).lower().replace("ё", "е")
    text_value = normalize_dash(text_value)
    return re.sub(r"\s+", " ", text_value).strip()


def parse_year_from_filename(path: Path) -> int:
    """Извлекает отчетный год из имени файла."""
    match = re.search(r"(?:19|20)\d{2}", path.name)
    if match is None:
        raise ParserError(f"Не удалось определить год из имени файла: {path.name}")

    return int(match.group(0))


def parse_int(value: Any) -> int | None:
    """Преобразует номер строки или графы к целому числу."""
    text_value = clean_cell(value).replace(",", ".")
    if not text_value:
        return None

    match = re.fullmatch(r"(\d+)(?:\.0)?", text_value)
    if match is None:
        return None

    return int(match.group(1))


def parse_numeric(value: Any) -> float:
    """
    Преобразует значение показателя к числу.

    Пустые ячейки, прочерки и многоточия трактуются как 0.
    Текст, который нельзя привести к числу, считается ошибкой.
    """
    text_value = normalize_dash(clean_cell(value))

    if not text_value or text_value in {"-", ".", "…", "...", "x", "х", "X", "Х"}:
        return 0.0

    prepared_value = (
        text_value
        .replace(" ", "")
        .replace("\u00a0", "")
        .replace(",", ".")
    )

    try:
        return float(prepared_value)
    except ValueError as exc:
        raise ParserError(f"Не удалось преобразовать значение к числу: {value!r}") from exc

# -----------------------------------------------------------------------------
# Поиск исходных файлов и чтение Excel
# -----------------------------------------------------------------------------

def find_default_input_files(raw_dir: Path) -> list[Path]:
    """Находит стандартный набор исходных Excel-файлов формы 8."""
    if not raw_dir.exists():
        raise ParserError(f"Папка с исходными файлами не найдена: {raw_dir}")

    files = [raw_dir / file_name for file_name in EXPECTED_SOURCE_FILES]
    missing_files = [path.name for path in files if not path.exists()]

    if missing_files:
        raise ParserError(f"Не найдены исходные файлы: {', '.join(missing_files)}")

    return files


def filter_target_years(input_files: list[Path]) -> list[Path]:
    """Оставляет годы, поддерживаемые текущей реализацией парсера."""
    filtered_files = [
        path for path in input_files
        if parse_year_from_filename(path) in TARGET_YEARS
    ]
    return sorted(filtered_files, key=parse_year_from_filename)


def get_registered_input_files() -> list[Path]:
    """Получает файлы из БД-реестра или использует старый fallback."""
    if get_source_files is not None:
        try:
            files = get_source_files(SOURCE_KEY, years=sorted(TARGET_YEARS))
            if files:
                return files
        except Exception as exc:
            logger.warning("Не удалось получить файлы из реестра БД: %s", exc)

    return find_default_input_files(DEFAULT_RAW_DIR)


def validate_input_file(path: Path) -> None:
    """Проверяет существование и формат исходного Excel-файла."""
    if not path.exists():
        raise ParserError(f"Файл не найден: {path}")

    if path.suffix.lower() not in EXCEL_SUFFIXES:
        raise ParserError(f"Неподдерживаемый формат файла: {path}")


def find_sheet_names(workbook: pd.ExcelFile) -> list[str]:
    """Находит листы с таблицей 1000 или ее продолжением."""
    candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if TABLE_CODE in normalize_text(sheet_name)
    ]

    if not candidates:
        raise ParserError("Не найден лист, в названии которого есть 1000")

    return candidates


def read_excel_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    """Читает лист Excel без заголовков и сохраняет исходную структуру ячеек."""
    return pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=None,
        dtype=object,
        keep_default_na=False,
    ).replace("", None)

# -----------------------------------------------------------------------------
# Поиск границ и колонок таблицы 1000
# -----------------------------------------------------------------------------

def find_marker_row(df: pd.DataFrame) -> int:
    """Находит строку с номерами граф 1-15."""
    for row_idx in range(min(20, len(df))):
        values = [parse_int(value) for value in df.iloc[row_idx].tolist()]
        numbers = [value for value in values if value is not None]

        if set(range(1, 16)).issubset(numbers):
            return row_idx

    raise ParserError("Не найдена строка с номерами граф 1-15")


def build_graph_column_map(df: pd.DataFrame, marker_row: int) -> dict[int, int]:
    """
    Возвращает соответствие: номер графы -> индекс колонки Excel.

    Для таблицы 1000 берем первую последовательность граф 1-15 слева направо.
    Полезные для загрузки графы: 5-15.
    """
    graph_to_column: dict[int, int] = {}
    expected_graph = 1

    for col_idx, value in enumerate(df.iloc[marker_row].tolist()):
        graph_number = parse_int(value)

        if graph_number != expected_graph:
            continue

        graph_to_column[graph_number] = col_idx
        expected_graph += 1

        if expected_graph > 15:
            break

    missing_graphs = sorted(set(range(1, 16)) - set(graph_to_column))
    if missing_graphs:
        raise ParserError(f"Не найдены графы таблицы 1000: {missing_graphs}")

    return graph_to_column

# -----------------------------------------------------------------------------
# Парсинг таблицы 1000
# -----------------------------------------------------------------------------

def parse_table_1000(path: Path) -> pd.DataFrame:
    """Парсит таблицу 1000 из одного Excel-файла."""
    validate_input_file(path)

    year = parse_year_from_filename(path)
    workbook = pd.ExcelFile(path)
    sheet_names = find_sheet_names(workbook)

    records: list[dict[str, Any]] = []
    last_icd_code: str | None = None

    for sheet_name in sheet_names:
        df = read_excel_sheet(path, sheet_name)
        try:
            marker_row = find_marker_row(df)
        except ParserError:
            logger.info("%s / %s: строка граф 1-15 не найдена, лист пропущен", path.name, sheet_name)
            continue

        graph_to_column = build_graph_column_map(df, marker_row)
        sheet_records, last_icd_code = extract_records(
            df=df,
            year=year,
            marker_row=marker_row,
            graph_to_column=graph_to_column,
            initial_icd_code=last_icd_code,
        )
        records.extend(sheet_records)

    result = pd.DataFrame(records, columns=FINAL_COLUMNS)
    result = result.drop_duplicates(
        subset=["Год", "Строка", "Графа", "Таблица"],
        keep="first",
    )
    validate_single_file_result(result, path)

    logger.info("%s: найдено %s строк таблицы 1000", path.name, len(result))
    return result


def extract_records(
    df: pd.DataFrame,
    year: int,
    marker_row: int,
    graph_to_column: dict[int, int],
    initial_icd_code: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Извлекает записи таблицы 1000 из одного листа Excel."""
    records: list[dict[str, Any]] = []
    row_number_col = graph_to_column[3]
    sex_col = graph_to_column[2]
    icd_col = graph_to_column[4]
    last_icd_code = initial_icd_code

    for row_idx in range(marker_row + 1, len(df)):
        row_number = parse_int(df.iat[row_idx, row_number_col])

        if row_number is None:
            continue

        if row_number not in ROW_NUMBER_TO_INDICATOR:
            if row_number > max(ROW_NUMBER_TO_INDICATOR):
                break
            continue

        icd_code = clean_cell(df.iat[row_idx, icd_col])
        if icd_code:
            last_icd_code = icd_code

        records.extend(
            build_records_for_row(
                df=df,
                row_idx=row_idx,
                year=year,
                row_number=row_number,
                graph_to_column=graph_to_column,
                icd_code=last_icd_code,
                sex=clean_cell(df.iat[row_idx, sex_col]) or None,
            )
        )

    return records, last_icd_code


def build_records_for_row(
    df: pd.DataFrame,
    row_idx: int,
    year: int,
    row_number: int,
    graph_to_column: dict[int, int],
    icd_code: str | None,
    sex: str | None,
) -> list[dict[str, Any]]:
    """Формирует записи длинного формата для одной строки таблицы."""
    indicator = ROW_NUMBER_TO_INDICATOR[row_number]
    records: list[dict[str, Any]] = []

    for graph_number, age_group in COLUMN_NUMBER_TO_AGE.items():
        value_col = graph_to_column[graph_number]
        records.append(
            {
                "Формы туберкулеза": indicator,
                "Код по МКБ-Х пересмотра": icd_code,
                "Пол": sex,
                "Возраст": age_group,
                "Год": year,
                "Значение": parse_numeric(df.iat[row_idx, value_col]),
                "Строка": row_number,
                "Графа": graph_number,
                "Таблица": TABLE_CODE,
            }
        )

    return records


def validate_single_file_result(df: pd.DataFrame, path: Path) -> None:
    """Проверяет полноту данных, полученных из одного Excel-файла."""
    expected_rows = len(ROW_NUMBER_TO_INDICATOR) * len(COLUMN_NUMBER_TO_AGE)

    if len(df) != expected_rows:
        actual_rows_set = set(df["Строка"].dropna().astype(int)) if not df.empty else set()
        missing_rows = sorted(set(ROW_NUMBER_TO_INDICATOR) - actual_rows_set)
        raise ParserError(
            f"Для файла {path.name} ожидалось {expected_rows} строк, "
            f"получено {len(df)}. Не найдены строки: {missing_rows}"
        )

    expected_rows_set = set(ROW_NUMBER_TO_INDICATOR)
    actual_rows_set = set(df["Строка"].dropna().astype(int))
    if actual_rows_set != expected_rows_set:
        raise ParserError(
            f"В файле {path.name} некорректный набор строк: {sorted(actual_rows_set)}"
        )

    expected_graphs_set = set(COLUMN_NUMBER_TO_AGE)
    actual_graphs_set = set(df["Графа"].dropna().astype(int))
    if actual_graphs_set != expected_graphs_set:
        raise ParserError(
            f"В файле {path.name} некорректный набор граф: {sorted(actual_graphs_set)}"
        )


def collect_data(input_files: list[Path]) -> pd.DataFrame:
    """Собирает данные таблицы 1000 из всех переданных файлов."""
    frames: list[pd.DataFrame] = []

    for path in input_files:
        logger.info("Обрабатываю файл: %s", path.name)
        frames.append(parse_table_1000(path))

    if not frames:
        raise ParserError("Не удалось собрать данные: подходящие файлы не найдены")

    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["Год", "Строка", "Графа"])
        .reset_index(drop=True)
    )

# -----------------------------------------------------------------------------
# Сохранение CSV
# -----------------------------------------------------------------------------

def save_processed_csv(df: pd.DataFrame, output_dir: Path, output_file: str) -> Path:
    """Сохраняет контрольный CSV-файл в папку processed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_file
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("CSV сохранен: %s", output_path)
    return output_path


def split_table_name(table_name: str) -> tuple[str | None, str]:
    """Разделяет имя таблицы на схему и имя."""
    parts = table_name.split(".", maxsplit=1)
    return (parts[0], parts[1]) if len(parts) == 2 else (None, table_name)


def quote_table_name(table_name: str) -> str:
    """Экранирует имя таблицы с учетом схемы."""
    schema, plain_table_name = split_table_name(table_name)
    return f'"{schema}"."{plain_table_name}"' if schema else f'"{plain_table_name}"'

# -----------------------------------------------------------------------------
# Валидация итогового датафрейма
# -----------------------------------------------------------------------------

def validate_result(df: pd.DataFrame) -> None:
    """Проверяет полноту и уникальность итогового датафрейма."""
    expected_per_year = len(ROW_NUMBER_TO_INDICATOR) * len(COLUMN_NUMBER_TO_AGE)

    rows_by_year = df.groupby("Год", as_index=False).size()
    bad_years = rows_by_year.loc[rows_by_year["size"] != expected_per_year]

    if not bad_years.empty:
        raise ParserError(
            "Некорректное число строк по годам:\n"
            + bad_years.to_string(index=False)
        )

    duplicates = df.duplicated(subset=["Год", "Строка", "Графа", "Таблица"]).sum()
    if duplicates:
        raise ParserError(
            f"Найдены дубли по ключу Год + Строка + Графа + Таблица: {duplicates}"
        )

    required_columns = [
        "Формы туберкулеза",
        "Год",
        "Возраст",
        "Строка",
        "Графа",
        "Таблица",
    ]
    null_required_fields = df[required_columns].isna().sum().sum()
    if null_required_fields:
        raise ParserError("В обязательных полях найдены пропущенные значения")

    logger.info(
        "Проверка пройдена: %s годов, %s строк",
        df["Год"].nunique(),
        len(df),
    )

# -----------------------------------------------------------------------------
# CLI и точка входа
# -----------------------------------------------------------------------------

def parse_args() -> Config:
    """Читает и валидирует аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Парсер таблицы 1000 из Excel-файлов формы 8"
    )
    parser.add_argument(
        "input_files",
        nargs="*",
        type=Path,
        help=(
            "Пути к XLS/XLSX-файлам. Если не указаны, скрипт возьмет файлы "
            f"из папки {DEFAULT_RAW_DIR}"
        ),
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="Строка подключения к PostgreSQL. Если не указана, берется из DATABASE_URL.",
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
        help=f"Целевая таблица. По умолчанию: {DEFAULT_TABLE_NAME}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Папка для контрольного CSV-файла.",
    )
    parser.add_argument(
        "--output-file",
        default=DEFAULT_OUTPUT_FILE,
        help="Имя контрольного CSV-файла.",
    )
    parser.add_argument(
        "--if-exists",
        choices=["append"],
        default="append",
        help="Режим загрузки. Только append: структура БД создается через sql/database_schema.sql.",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Только подготовить и сохранить CSV, без загрузки в БД.",
    )

    args = parser.parse_args()

    db_url = args.db_url or DATABASE_URL
    if not args.skip_db and not db_url:
        raise ParserError(
            "Не задана строка подключения. Передайте --db-url, задайте DATABASE_URL "
            "в .env или используйте --skip-db"
        )

    if args.input_files:
        input_files = args.input_files
    elif args.skip_db:
        input_files = find_default_input_files(DEFAULT_RAW_DIR)
    else:
        input_files = get_registered_input_files()
    input_files = filter_target_years(input_files)

    return Config(
        input_files=input_files,
        db_url=db_url,
        table_name=args.table_name,
        output_dir=args.output_dir,
        output_file=args.output_file,
        if_exists=args.if_exists,
        skip_db=args.skip_db,
    )


def run(
    input_files: list[Path] | None = None,
    *,
    skip_db: bool = False,
    db_url: str | None = None,
    table_name: str = DEFAULT_TABLE_NAME,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_file: str = DEFAULT_OUTPUT_FILE,
    if_exists: str = "append",
) -> pd.DataFrame:
    """ETL entrypoint for orchestrator and direct imports."""
    logger.info("Целевая таблица: %s", table_name)

    if input_files is None:
        input_files = find_default_input_files(DEFAULT_RAW_DIR) if skip_db else get_registered_input_files()

    input_files = filter_target_years(input_files)
    if not input_files:
        raise ParserError("Не найдены исходные файлы формы 8 за 2016-2024 годы")

    df = collect_data(input_files)
    validate_result(df)
    save_processed_csv(df, output_dir, output_file)

    if skip_db:
        logger.info("Загрузка в БД пропущена по флагу --skip-db")
        return df

    db_url = db_url or DATABASE_URL
    if not db_url:
        raise ParserError("Не задана строка подключения к БД")

    if if_exists == "append":
        years = sorted(df["Год"].dropna().astype(int).unique().tolist())
        delete_rows_for_years(table_name, years, database_url=db_url)

    load_dataframe(df, table_name, if_exists=if_exists, database_url=db_url)
    logger.info("В таблицу %s загружено строк: %s", table_name, len(df))
    return df


def main() -> int:
    """Основная точка входа в скрипт."""
    try:
        config = parse_args()
        run(
            input_files=config.input_files,
            skip_db=config.skip_db,
            db_url=config.db_url,
            table_name=config.table_name,
            output_dir=config.output_dir,
            output_file=config.output_file,
            if_exists=config.if_exists,
        )
        return 0

    except Exception as exc:
        logger.exception("Ошибка выполнения парсера: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
