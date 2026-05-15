"""ETL: форма N33, таблица 2310.

Назначение:
    Парсит Excel-файлы формы N33 за 2016-2024 годы и извлекает дополнительный
    блок таблицы 2310.

Источник:
    data/33_8_2015-2024/*.xls(x)

Результат:
    - processed/tb_form33_2310.csv;
    - public.tb_form33_2310 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form33_2310
    python3 orchestrator.py run form33_2310 --write-db

Особенности:
    В таблице 2310 нет классического поля "Строка"; для бизнес-ключа и
    расчетов используется год, графа и код таблицы.
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
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Константы проекта
# -----------------------------------------------------------------------------

EXCEL_SUFFIXES = {".xls", ".xlsx", ".xlsm"}

SOURCE_KEY = "form33_2310"
TABLE_CODE = "2310"
DEFAULT_TABLE_NAME = "public.tb_form33_2310"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = FORM_8_33_DIR
DEFAULT_OUTPUT_DIR = PROCESSED_DIR
DEFAULT_OUTPUT_FILE = "tb_form33_2310.csv"
TARGET_YEARS = set(range(2016, 2025))

DETAIL_VALUE = "абсолютные числа"

EXPECTED_SOURCE_FILES = [
    "33_2016.xlsx",
    "2017.xls",
    "2018.xls",
    "2019.xls",
    "2020.xls",
    "2021.xls",
    "2022.xls",
    "2023.xls",
    "2024.xls",
]

# Нормализация строк таблицы 2310.
ROW_NUMBER_TO_INDICATOR: dict[int, str] = {
    1: "Из числа умерших от туберкулеза состояло на учете менее 1 года",
    2: "Из числа умерших от туберкулеза умерло больных с сочетанием ВИЧ/ТБ",
    3: (
        "Из числа умерших от туберкулеза с сочетанием ВИЧ/ТБ состояло "
        "на учете менее 1 года"
    ),
    4: (
        "Кроме того умерло от туберкулеза больных, не состоявших на учете "
        "в ПТУ, взрослые"
    ),
    5: (
        "Кроме того умерло от туберкулеза больных, не состоявших на учете "
        "в ПТУ, дети 0-14 лет"
    ),
    6: (
        "Кроме того умерло от туберкулеза больных, не состоявших на учете "
        "в ПТУ, подростки 15-17 лет"
    ),
    7: "Из числа умерших от других причин умерло больных с сочетанием ВИЧ/ТБ",
}

# Ключевые фразы для поиска строк таблицы 2310, если номера строк в Excel
# не выделены в отдельную колонку или расположены нестандартно.
ROW_NUMBER_TO_SEARCH_PHRASES: dict[int, tuple[str, ...]] = {
    1: ("умерших", "туберкулеза", "менее 1 года"),
    2: ("умерших", "туберкулеза", "вич"),
    3: ("умерших", "туберкулеза", "вич", "менее 1 года"),
    4: ("не состоявших", "пту", "взрос"),
    5: ("не состоявших", "пту", "0-14"),
    6: ("не состоявших", "пту", "15-17"),
    7: ("умерших", "других причин", "вич"),
}

FINAL_COLUMNS = ["Показатель", "Уточнение", "Год", "Значение", "Графа", "Таблица"]

# -----------------------------------------------------------------------------
# Модели и исключения
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """Параметры запуска скрипта."""

    input_files: list[Path]
    db_url: str | None
    table_name: str = DEFAULT_TABLE_NAME
    output_dir: Path = DEFAULT_OUTPUT_DIR
    output_file: str = DEFAULT_OUTPUT_FILE
    if_exists: str = "append"
    skip_db: bool = False


class ParserError(RuntimeError):
    """Ошибка парсинга или валидации данных таблицы 2310."""


def normalize_dash(value: str) -> str:
    """Унифицирует разные типы тире в текстовом значении."""
    return value.replace("—", "-").replace("–", "-")

# -----------------------------------------------------------------------------
# Универсальные функции очистки и преобразования значений
# -----------------------------------------------------------------------------

def clean_cell(value: Any) -> str:
    """Возвращает очищенное текстовое значение ячейки Excel."""
    if value is None:
        return ""

    if pd.isna(value):
        return ""

    return re.sub(r"\s+", " ", str(value).replace("\n", " ")).strip()


def normalize_text(value: Any) -> str:
    """Нормализует текст для поиска служебных маркеров."""
    text_value = clean_cell(value).lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text_value).strip()


def parse_year_from_filename(path: Path) -> int:
    """Извлекает отчетный год из имени файла."""
    match = re.search(r"(?:19|20)\d{2}", path.name)
    if not match:
        raise ParserError(f"Не удалось определить год из имени файла: {path.name}")

    return int(match.group(0))


def parse_int(value: Any) -> int | None:
    """Преобразует номер строки к целому числу."""
    text_value = clean_cell(value)
    if not text_value:
        return None

    match = re.fullmatch(r"(\d+)(?:\.0)?", text_value)
    if match is None:
        return None

    return int(match.group(1))


def normalize_for_search(value: Any) -> str:
    """Нормализует текст ячейки или строки для поиска показателей."""
    text_value = normalize_dash(normalize_text(value))
    text_value = text_value.replace("/", " ")
    text_value = re.sub(r"\s+", " ", text_value)
    return text_value.strip()


def row_text(row: pd.Series) -> str:
    """Собирает все ячейки строки Excel в одну нормализованную строку."""
    return normalize_for_search(" ".join(clean_cell(value) for value in row.tolist()))


def detect_row_number_by_text(row: pd.Series) -> int | None:
    """
    Определяет номер строки таблицы 2310 по тексту показателя.

    Это запасной сценарий для файлов, где номера строк 1–7 не вынесены
    в отдельную стабильную колонку.
    """
    text_value = row_text(row)

    # Сначала проверяем более специфичные строки, чтобы строка 3 не была
    # ошибочно принята за строку 2.
    for row_number in (3, 7, 6, 5, 4, 2, 1):
        phrases = ROW_NUMBER_TO_SEARCH_PHRASES[row_number]
        if all(phrase in text_value for phrase in phrases):
            return row_number

    return None


def parse_numeric(value: Any) -> float:
    """
    Преобразует значение показателя к числу.

    Для таблицы 2310 пустые ячейки, прочерки и многоточия трактуются как 0.
    Непонятные текстовые значения считаются ошибкой, чтобы не получить
    тихую порчу данных.
    """
    text_value = normalize_dash(clean_cell(value))

    if not text_value or text_value in {"-", "…", "..."}:
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
        raise ParserError(
            f"Не удалось преобразовать значение к числу: {value!r}"
        ) from exc


def is_numeric_like(value: Any) -> bool:
    """Проверяет, можно ли ячейку трактовать как числовое значение."""
    try:
        parse_numeric(value)
        return True
    except ParserError:
        return False

# -----------------------------------------------------------------------------
# Поиск исходных файлов и чтение Excel
# -----------------------------------------------------------------------------

def find_default_input_files(raw_dir: Path) -> list[Path]:
    """Находит стандартный набор исходных Excel-файлов формы 33 для таблицы 2310."""
    if not raw_dir.exists():
        raise ParserError(f"Папка с исходными файлами не найдена: {raw_dir}")

    files = [raw_dir / file_name for file_name in EXPECTED_SOURCE_FILES]
    missing_files = [path for path in files if not path.exists()]

    if missing_files:
        missing_names = ", ".join(path.name for path in missing_files)
        raise ParserError(f"Не найдены исходные файлы: {missing_names}")

    return files


def filter_target_years(input_files: list[Path]) -> list[Path]:
    """Оставляет годы, поддерживаемые текущей реализацией парсера."""
    filtered_files = [
        path for path in input_files
        if parse_year_from_filename(path) in TARGET_YEARS
    ]
    return sorted(filtered_files, key=parse_year_from_filename)


def get_registered_input_files() -> list[Path]:
    """Получает файлы из БД-реестра или использует локальный fallback."""
    if get_source_files is not None:
        try:
            files = get_source_files(SOURCE_KEY, years=sorted(TARGET_YEARS))
            if files:
                return files
        except Exception as exc:
            log.warning("Не удалось получить файлы из реестра БД: %s", exc)

    return find_default_input_files(DEFAULT_RAW_DIR)


def validate_input_file(path: Path) -> None:
    """Проверяет существование и формат исходного Excel-файла."""
    if not path.exists():
        raise ParserError(f"Файл не найден: {path}")

    if path.suffix.lower() not in EXCEL_SUFFIXES:
        raise ParserError(f"Неподдерживаемый формат файла: {path}")


def find_sheet_name(workbook: pd.ExcelFile) -> str:
    """
    Находит лист с таблицей 2310.

    Обычно таблица лежит на листе, где в названии есть 2300 или 2310:
    2200,2300,2310 / 2200-2300 и похожие варианты.
    """
    preferred_candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if TABLE_CODE in normalize_text(sheet_name)
    ]
    if preferred_candidates:
        return preferred_candidates[0]

    fallback_candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if "2300" in normalize_text(sheet_name)
    ]
    if fallback_candidates:
        return fallback_candidates[0]

    raise ParserError("Не найден лист, в названии которого есть 2310 или 2300")


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
# Поиск блока и значений таблицы 2310
# -----------------------------------------------------------------------------

def find_table_2310_start(df: pd.DataFrame) -> int:
    """
    Находит начало блока 2310.

    Таблица 2310 обычно идет после 2300 на том же листе. Заголовок может быть
    компактным, поэтому основной надежный маркер — код таблицы 2310.
    """
    for row_idx in range(len(df)):
        row_text = " ".join(clean_cell(value) for value in df.iloc[row_idx].tolist())
        normalized = normalize_text(row_text)

        if TABLE_CODE in normalized:
            return row_idx

    raise ParserError("Не найден заголовок таблицы 2310")


def find_row_number_column(df: pd.DataFrame, table_start: int) -> int | None:
    """
    Находит колонку, где указаны номера строк 1–7.

    Если такой колонки нет, возвращает None. Тогда строки будут определяться
    по тексту показателя.
    """
    search_end = min(table_start + 60, len(df))

    for col_idx in range(df.shape[1]):
        values = [
            parse_int(df.iat[row_idx, col_idx])
            for row_idx in range(table_start + 1, search_end)
        ]
        row_numbers = {value for value in values if value is not None}

        if set(ROW_NUMBER_TO_INDICATOR).issubset(row_numbers):
            return col_idx

    return None


def extract_value_from_row(row: pd.Series, row_number_col: int | None) -> float:
    """
    Извлекает числовое значение из строки таблицы 2310.

    В таблице 2310 нет классической строки граф. Значение обычно расположено
    справа от номера строки и текста показателя. Если колонка с номером строки
    не найдена, просматриваем всю строку.
    """
    numeric_values: list[float] = []
    start_col = row_number_col + 1 if row_number_col is not None else 0

    for value in row.iloc[start_col:].tolist():
        cell_text = clean_cell(value)

        if not cell_text:
            continue

        if is_numeric_like(value):
            numeric_values.append(parse_numeric(value))

    if not numeric_values:
        return 0.0

    return numeric_values[-1]

# -----------------------------------------------------------------------------
# Специальный парсер для строкового варианта таблицы 2310 (например, 2016 год)
# -----------------------------------------------------------------------------

def parse_inline_table_2310(df: pd.DataFrame, table_start: int, year: int) -> pd.DataFrame:
    """
    Парсит строковый вариант таблицы 2310.

    В файле 2016 таблица 2310 сверстана не вертикальной таблицей, а как
    несколько длинных строк: текст показателя, номер строки и значение идут
    последовательно слева направо. Поэтому для этого варианта собираем ячейки
    в порядке чтения и берем первое числовое значение после каждого номера
    строки 1–7.
    """
    cells: list[tuple[int, int, Any]] = []
    search_end = min(table_start + 8, len(df))

    for row_idx in range(table_start, search_end):
        normalized_row = row_text(df.iloc[row_idx])

        # Ниже таблицы 2310 может начинаться следующий блок формы.
        if row_idx > table_start and "2320" in normalized_row:
            break

        for col_idx, value in enumerate(df.iloc[row_idx].tolist()):
            if clean_cell(value):
                cells.append((row_idx, col_idx, value))

    marker_positions = find_inline_row_markers(cells)
    records: list[dict[str, Any]] = []
    seen_row_numbers: set[int] = set()

    for marker_idx, (cell_index, row_number) in enumerate(marker_positions):
        if row_number in seen_row_numbers:
            continue

        next_marker_cell_index = (
            marker_positions[marker_idx + 1][0]
            if marker_idx + 1 < len(marker_positions)
            else len(cells)
        )
        found_value = find_inline_value_between_markers(
            cells=cells,
            start_cell_index=cell_index + 1,
            end_cell_index=next_marker_cell_index,
        )

        seen_row_numbers.add(row_number)
        records.append(
            {
                "Показатель": ROW_NUMBER_TO_INDICATOR[row_number],
                "Уточнение": DETAIL_VALUE,
                "Год": year,
                "Значение": found_value,
                "Графа": row_number,
                "Таблица": TABLE_CODE,
            }
        )

        if len(seen_row_numbers) == len(ROW_NUMBER_TO_INDICATOR):
            break

    return pd.DataFrame(records, columns=FINAL_COLUMNS)


def find_inline_row_markers(cells: list[tuple[int, int, Any]]) -> list[tuple[int, int]]:
    """
    Находит позиции номеров строк 1-7 в строковом варианте таблицы 2310.

    В 2016 году номер строки может быть отдельной ячейкой или последним
    числом в текстовой ячейке, например "... сочетанием ВИЧ/ТБ 2".
    """
    marker_positions: list[tuple[int, int]] = []

    for cell_index, (_, _, value) in enumerate(cells):
        text_value = clean_cell(value)
        row_number = None

        # В строковой верстке одиночные числа чаще являются значениями,
        # поэтому номер строки принимаем только из текстовой ячейки.
        if not is_numeric_like(value):
            match = re.search(r"(?:^|\D)0?([1-7])\s*$", text_value)
            row_number = int(match.group(1)) if match else None

        if row_number in ROW_NUMBER_TO_INDICATOR:
            marker_positions.append((cell_index, row_number))

    return marker_positions


def find_inline_value_between_markers(
    cells: list[tuple[int, int, Any]],
    start_cell_index: int,
    end_cell_index: int,
) -> float:
    """Возвращает первое числовое значение между двумя маркерами строк."""
    for _, _, candidate_value in cells[start_cell_index:end_cell_index]:
        if is_numeric_like(candidate_value):
            return parse_numeric(candidate_value)

    return 0.0

# -----------------------------------------------------------------------------
# Парсинг таблицы 2310
# -----------------------------------------------------------------------------

def parse_table_2310(path: Path) -> pd.DataFrame:
    """Парсит таблицу 2310 из одного Excel-файла."""
    validate_input_file(path)

    year = parse_year_from_filename(path)
    workbook = pd.ExcelFile(path)
    sheet_name = find_sheet_name(workbook)
    df = read_excel_sheet(path, sheet_name)

    table_start = find_table_2310_start(df)
    row_number_col = find_row_number_column(df, table_start)
    if row_number_col is None:
        log.info(
            "%s: колонка с номерами строк 1-7 не найдена, использую поиск по тексту",
            path.name,
        )

    records: list[dict[str, Any]] = []
    seen_row_numbers: set[int] = set()
    search_end = min(table_start + 60, len(df))

    for row_idx in range(table_start + 1, search_end):
        if row_number_col is not None:
            row_number = parse_int(df.iat[row_idx, row_number_col])
        else:
            row_number = detect_row_number_by_text(df.iloc[row_idx])

        if row_number is None or row_number not in ROW_NUMBER_TO_INDICATOR:
            continue

        if row_number in seen_row_numbers:
            continue

        seen_row_numbers.add(row_number)
        value = extract_value_from_row(df.iloc[row_idx], row_number_col)

        records.append(
            {
                "Показатель": ROW_NUMBER_TO_INDICATOR[row_number],
                "Уточнение": DETAIL_VALUE,
                "Год": year,
                "Значение": value,
                "Графа": row_number,
                "Таблица": TABLE_CODE,
            }
        )

        if len(seen_row_numbers) == len(ROW_NUMBER_TO_INDICATOR):
            break

    result = pd.DataFrame(records, columns=FINAL_COLUMNS)

    if len(result) != len(ROW_NUMBER_TO_INDICATOR):
        log.info(
            "%s: вертикальный вариант дал %s строк, пробую строковый парсинг 2310",
            path.name,
            len(result),
        )
        result = parse_inline_table_2310(df, table_start, year)

    validate_single_file_result(result, path)

    log.info("%s: найдено %s строк таблицы 2310", path.name, len(result))
    return result


def validate_single_file_result(df: pd.DataFrame, path: Path) -> None:
    """Проверяет полноту данных, полученных из одного Excel-файла."""
    expected_rows = len(ROW_NUMBER_TO_INDICATOR)

    if len(df) != expected_rows:
        raise ParserError(
            f"Для файла {path.name} ожидалось {expected_rows} строк, получено {len(df)}"
        )

    expected_rows_set = set(ROW_NUMBER_TO_INDICATOR)
    actual_rows_set = set(df["Графа"].dropna().astype(int))
    if actual_rows_set != expected_rows_set:
        raise ParserError(
            f"В файле {path.name} некорректный набор граф: {sorted(actual_rows_set)}"
        )


def collect_data(input_files: list[Path]) -> pd.DataFrame:
    """Собирает данные таблицы 2310 из всех переданных файлов."""
    frames: list[pd.DataFrame] = []

    for path in input_files:
        log.info("Обрабатываю файл: %s", path.name)
        frames.append(parse_table_2310(path))

    if not frames:
        raise ParserError("Не удалось собрать данные: подходящие файлы не найдены")

    result = pd.concat(frames, ignore_index=True)
    return result.sort_values(["Год", "Графа"]).reset_index(drop=True)

# -----------------------------------------------------------------------------
# Сохранение CSV
# -----------------------------------------------------------------------------

def save_processed_csv(df: pd.DataFrame, output_dir: Path, output_file: str) -> Path:
    """Сохраняет контрольный CSV-файл в папку processed."""
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / output_file
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    log.info("CSV сохранен: %s", output_path)
    return output_path

# -----------------------------------------------------------------------------
# Валидация итогового датафрейма
# -----------------------------------------------------------------------------

def validate_result(df: pd.DataFrame) -> None:
    """Проверяет полноту и уникальность итогового датафрейма."""
    expected_per_year = len(ROW_NUMBER_TO_INDICATOR)

    rows_by_year = df.groupby("Год", as_index=False).size()
    bad_years = rows_by_year.loc[rows_by_year["size"] != expected_per_year]

    if not bad_years.empty:
        raise ParserError(
            "Некорректное число строк по годам:\n"
            + bad_years.to_string(index=False)
        )

    duplicates = df.duplicated(subset=["Год", "Графа", "Таблица"]).sum()
    if duplicates:
        raise ParserError(
            f"Найдены дубли по ключу Год + Графа + Таблица: {duplicates}"
        )

    required_columns = ["Показатель", "Уточнение", "Год", "Графа", "Таблица"]
    null_required_fields = df[required_columns].isna().sum().sum()
    if null_required_fields:
        raise ParserError("В обязательных полях найдены пропущенные значения")

    log.info(
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
        description="Парсер таблицы 2310 из Excel-файлов формы 33"
    )

    parser.add_argument(
        "input_files",
        nargs="*",
        type=Path,
        help=(
            "Пути к XLS/XLSX-файлам. "
            "Если не указаны, скрипт возьмет файлы из папки "
            f"{DEFAULT_RAW_DIR}"
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
        help="Безопасный режим: сохранить CSV, но не писать данные в БД.",
    )

    args = parser.parse_args()

    db_url = args.db_url or DATABASE_URL
    if not args.skip_db and not db_url:
        raise ParserError(
            "Не задана строка подключения. "
            "Передайте --db-url, задайте DATABASE_URL в .env или используйте --skip-db"
        )

    input_files = args.input_files or get_registered_input_files()

    return Config(
        input_files=filter_target_years(input_files),
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
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_file: str = DEFAULT_OUTPUT_FILE,
    table_name: str = DEFAULT_TABLE_NAME,
    if_exists: str = "append",
) -> pd.DataFrame:
    """Запускает парсер из оркестратора или напрямую из другого Python-кода."""
    resolved_input_files = input_files or get_registered_input_files()
    resolved_input_files = filter_target_years(resolved_input_files)
    resolved_db_url = db_url or DATABASE_URL

    config = Config(
        input_files=resolved_input_files,
        db_url=resolved_db_url,
        table_name=table_name,
        output_dir=output_dir,
        output_file=output_file,
        if_exists=if_exists,
        skip_db=skip_db,
    )

    log.info("Целевая таблица: %s", config.table_name)

    df = collect_data(config.input_files)
    validate_result(df)
    save_processed_csv(df, config.output_dir, config.output_file)

    if config.skip_db:
        log.info("Режим --skip-db: загрузка в PostgreSQL пропущена")
        return df

    if not config.db_url:
        raise ParserError(
            "Не задана строка подключения. "
            "Передайте db_url, задайте DATABASE_URL в .env или используйте skip_db=True"
        )

    if config.if_exists == "append":
        years = sorted(df["Год"].dropna().astype(int).unique().tolist())
        delete_rows_for_years(config.table_name, years, database_url=config.db_url)

    load_dataframe(df, config.table_name, if_exists=config.if_exists, database_url=config.db_url)
    log.info("В таблицу %s загружено строк: %s", config.table_name, len(df))
    return df


def main() -> int:
    """Основная точка входа в скрипт."""
    try:
        config = parse_args()
        run(
            input_files=config.input_files,
            skip_db=config.skip_db,
            db_url=config.db_url,
            output_dir=config.output_dir,
            output_file=config.output_file,
            table_name=config.table_name,
            if_exists=config.if_exists,
        )
        return 0

    except Exception as exc:
        log.exception("Ошибка выполнения парсера: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
