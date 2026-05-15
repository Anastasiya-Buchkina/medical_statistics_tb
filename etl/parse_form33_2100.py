"""ETL: форма N33, таблица 2100.

Назначение:
    Парсит Excel-файлы формы N33 за 2015-2024 годы, извлекает строки 1-13,
    графы 4-9 и код по МКБ из графы 3.

Источник:
    data/33_8_2015-2024/*.xls(x)

Результат:
    - processed/tb_form33_2100.csv;
    - public.tb_form33_2100 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form33_2100
    python3 orchestrator.py run form33_2100 --write-db

Особенности:
    Это одна из двух таблиц формы N33, где используется 2015 год.
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

SOURCE_KEY = "form33_2100"
TABLE_CODE = "2100"
DEFAULT_TABLE_NAME = "public.tb_form33_2100"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = FORM_8_33_DIR
DEFAULT_OUTPUT_DIR = PROCESSED_DIR
DEFAULT_OUTPUT_FILE = "tb_form33_2100.csv"
TARGET_YEARS = set(range(2015, 2025))

EXPECTED_SOURCE_FILES = [
    "33_2015.xlsx",
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

ROW_NUMBER_TO_INDICATOR: dict[int, str] = {
    1: "Туберкулез органов дыхания всего",
    2: "Туберкулез легких",
    3: "Туберкулез легких фиброзно-кавернозный",
    4: "Из общего числа больных туберкулезом легких выявлено в фазе распада",
    5: (
        "Из общего числа больных туберкулезом легких выявлено "
        "без распада и без бактериовыделения"
    ),
    6: "Другие локализации туберкулеза",
    7: "Туберкулез всего",
    8: "Имеют инвалидность в связи с туберкулезом",
    9: "Имеют инвалидность в связи с туберкулезом первой группы",
    10: "Имеют инвалидность в связи с туберкулезом второй группы",
    11: "Обследовано на АТ к ВИЧ",
    12: (
        "Обследовано на АТ к ВИЧ с положительным результатом "
        "методом иммунного блотинга"
    ),
    13: "Туберкулез в сочетании с ВИЧ",
}

ROW_NUMBER_TO_SEARCH_PHRASES: dict[int, tuple[str, ...]] = {
    1: ("туберкулез", "органов", "дыхания"),
    2: ("туберкулез", "легких"),
    3: ("фиброзно", "каверноз"),
    4: ("фазе", "распада"),
    5: ("без", "распада", "без", "бактериовыделения"),
    6: ("другие", "локализации"),
    7: ("итого",),
    8: ("инвалидность", "связи", "туберкулезом"),
    9: ("первой", "группы"),
    10: ("второй", "группы"),
    11: ("обследовано", "ат", "вич"),
    12: ("положительным", "результатом", "блотинг"),
    13: ("туберкулез", "сочетании", "вич"),
}

COLUMN_NUMBER_TO_DETAIL: dict[int, str] = {
    4: "Взято на учёт с впервые в жизни установленным диагнозом всего",
    5: "Взято на учёт с впервые в жизни установленным диагнозом детей 0-14 лет",
    6: "Взято на учёт с впервые в жизни установленным диагнозом подростков 15-17 лет",
    7: "Контингенты больных на конец отчетного года всего",
    8: "Контингенты больных на конец отчетного года дети 0-14 лет",
    9: "Контингенты больных на конец отчетного года подростки 15-17 лет",
}

FINAL_COLUMNS = [
    "Показатель",
    "Уточнение",
    "Код по МКБ",
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
    """Ошибка парсинга или валидации таблицы 2100."""

# -----------------------------------------------------------------------------
# Универсальные функции очистки и преобразования
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


def normalize_for_search(value: Any) -> str:
    """Нормализует текст ячейки или строки для поиска показателей."""
    text_value = normalize_text(value)
    text_value = text_value.replace("/", " ")
    text_value = text_value.replace(";", " ")
    text_value = text_value.replace(",", " ")
    text_value = text_value.replace(".", " ")
    text_value = text_value.replace("(", " ")
    text_value = text_value.replace(")", " ")
    return re.sub(r"\s+", " ", text_value).strip()


def parse_year_from_filename(path: Path) -> int:
    """Извлекает отчетный год из имени файла."""
    match = re.search(r"(?:19|20)\d{2}", path.name)
    if match is None:
        raise ParserError(f"Не удалось определить год из имени файла: {path.name}")

    return int(match.group(0))


def parse_int(value: Any) -> int | None:
    """Преобразует номер строки или графы к целому числу."""
    text_value = clean_cell(value)
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
        raise ParserError(f"Не удалось преобразовать значение к числу: {value!r}") from exc


def clean_icd_code(value: Any) -> str | None:
    """
    Очищает код по МКБ.

    Пустая ячейка, прочерк или многоточие трактуются как NULL.
    """
    text_value = normalize_dash(clean_cell(value))

    if not text_value or text_value in {"-", "…", "..."}:
        return None

    return re.sub(r"\s+", " ", text_value).strip()


def row_text(row: pd.Series) -> str:
    """Собирает строку Excel в один нормализованный текст."""
    return normalize_for_search(" ".join(clean_cell(value) for value in row.tolist()))

# -----------------------------------------------------------------------------
# Поиск строк таблицы 2100
# -----------------------------------------------------------------------------

def detect_row_number_by_text(row: pd.Series) -> int | None:
    """
    Определяет номер строки таблицы 2100 по тексту показателя.

    Используется как fallback, если стабильная колонка с номерами строк
    не найдена.
    """
    text_value = row_text(row)

    # Сначала проверяем более специфичные строки.
    for row_number in (13, 12, 11, 10, 9, 8, 5, 4, 3, 6, 2, 1, 7):
        phrases = ROW_NUMBER_TO_SEARCH_PHRASES[row_number]
        if all(phrase in text_value for phrase in phrases):
            return row_number

    return None


def detect_row_number_by_cells(row: pd.Series, value_start_col: int) -> int | None:
    """
    Определяет номер строки по ячейкам слева от граф со значениями.

    Используется только как осторожный fallback и дополнительно проверяется
    на совпадение со следующим ожидаемым номером строки.
    """
    candidates: list[int] = []

    for value in row.iloc[:value_start_col].tolist():
        row_number = parse_int(value)
        if row_number in ROW_NUMBER_TO_INDICATOR:
            candidates.append(row_number)

    return candidates[-1] if candidates else None

# -----------------------------------------------------------------------------
# Поиск исходных файлов и чтение Excel
# -----------------------------------------------------------------------------

def find_default_input_files(raw_dir: Path) -> list[Path]:
    """Находит стандартный набор исходных Excel-файлов формы 33."""
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


def find_sheet_name(workbook: pd.ExcelFile) -> str:
    """
    Находит лист с таблицей 2100.

    В 2016 году таблица лежит на листе «2100,2110,2120».
    В 2017–2024 годах обычно на отдельном листе «2100».
    """
    exact_candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if normalize_text(sheet_name) == TABLE_CODE
    ]

    if exact_candidates:
        return exact_candidates[0]

    candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if TABLE_CODE in normalize_text(sheet_name)
    ]

    if not candidates:
        raise ParserError("Не найден лист, в названии которого есть 2100")

    return candidates[0]


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
# Поиск границ и колонок таблицы 2100
# -----------------------------------------------------------------------------

def find_table_2100_start(df: pd.DataFrame) -> int:
    """
    Находит начало таблицы 2100.

    Ищем строку с кодом 2100 и контекстом про контингенты больных
    активным туберкулезом.
    """
    for row_idx in range(len(df)):
        normalized = row_text(df.iloc[row_idx])

        if TABLE_CODE in normalized and "контингенты" in normalized:
            return row_idx

    for row_idx in range(len(df)):
        normalized = row_text(df.iloc[row_idx])

        if TABLE_CODE in normalized:
            return row_idx

    raise ParserError("Не найден заголовок таблицы 2100")


def find_marker_row(df: pd.DataFrame, table_start: int) -> int:
    """Находит строку с номерами граф 1–9."""
    search_end = min(table_start + 30, len(df))

    for row_idx in range(table_start, search_end):
        values = [parse_int(value) for value in df.iloc[row_idx].tolist()]
        numbers = [value for value in values if value is not None]

        if set(range(1, 10)).issubset(numbers):
            return row_idx

    raise ParserError("Не найдена строка с номерами граф 1–9")


def build_graph_column_map(df: pd.DataFrame, marker_row: int) -> dict[int, int]:
    """
    Возвращает соответствие: номер графы -> индекс колонки Excel.

    Для таблицы 2100 берем первую последовательность граф 1–9 слева направо.
    Это важно, потому что справа могут быть дополнительные блоки 2110/2120.
    """
    graph_to_column: dict[int, int] = {}
    expected_graph = 1

    for col_idx, value in enumerate(df.iloc[marker_row].tolist()):
        graph_number = parse_int(value)

        if graph_number != expected_graph:
            continue

        graph_to_column[graph_number] = col_idx
        expected_graph += 1

        if expected_graph > 9:
            break

    missing_graphs = sorted(set(range(1, 10)) - set(graph_to_column))
    if missing_graphs:
        raise ParserError(f"Не найдены графы таблицы 2100: {missing_graphs}")

    return graph_to_column


def find_row_number_column(
    df: pd.DataFrame,
    marker_row: int,
    graph_to_column: dict[int, int],
) -> int | None:
    """
    Находит колонку, где указаны номера строк 1–13.

    Сначала проверяем графу 1. Если не сработало, ищем любую колонку,
    где ниже строки граф есть полный набор номеров 1–13.
    """
    search_end = min(marker_row + 50, len(df))
    graph_1_col = graph_to_column.get(1)

    if graph_1_col is not None:
        values = [
            parse_int(df.iat[row_idx, graph_1_col])
            for row_idx in range(marker_row + 1, search_end)
        ]
        row_numbers = {value for value in values if value is not None}

        if set(ROW_NUMBER_TO_INDICATOR).issubset(row_numbers):
            return graph_1_col

    for col_idx in range(df.shape[1]):
        values = [
            parse_int(df.iat[row_idx, col_idx])
            for row_idx in range(marker_row + 1, search_end)
        ]
        row_numbers = {value for value in values if value is not None}

        if set(ROW_NUMBER_TO_INDICATOR).issubset(row_numbers):
            return col_idx

    return None

# -----------------------------------------------------------------------------
# Парсинг таблицы 2100
# -----------------------------------------------------------------------------

def parse_table_2100(path: Path) -> pd.DataFrame:
    """Парсит таблицу 2100 из одного Excel-файла."""
    validate_input_file(path)

    year = parse_year_from_filename(path)
    workbook = pd.ExcelFile(path)
    sheet_name = find_sheet_name(workbook)
    df = read_excel_sheet(path, sheet_name)

    table_start = find_table_2100_start(df)
    marker_row = find_marker_row(df, table_start)
    graph_to_column = build_graph_column_map(df, marker_row)

    value_start_col = graph_to_column[4]
    row_number_col = find_row_number_column(df, marker_row, graph_to_column)

    if row_number_col is None:
        logger.info(
            "%s: колонка с номерами строк 1-13 не найдена, использую поиск по тексту",
            path.name,
        )

    records = extract_records(
        df=df,
        year=year,
        marker_row=marker_row,
        graph_to_column=graph_to_column,
        value_start_col=value_start_col,
        row_number_col=row_number_col,
    )

    result = pd.DataFrame(records, columns=FINAL_COLUMNS)
    validate_single_file_result(result, path)

    logger.info(
        "%s: лист '%s', найдено %s строк таблицы 2100",
        path.name,
        sheet_name,
        len(result),
    )
    return result


def extract_records(
    df: pd.DataFrame,
    year: int,
    marker_row: int,
    graph_to_column: dict[int, int],
    value_start_col: int,
    row_number_col: int | None,
) -> list[dict[str, Any]]:
    """Извлекает записи таблицы 2100 из листа Excel."""
    records: list[dict[str, Any]] = []
    seen_row_numbers: set[int] = set()
    search_end = min(marker_row + 60, len(df))

    for row_idx in range(marker_row + 1, search_end):
        row_number = detect_current_row_number(
            row=df.iloc[row_idx],
            row_idx=row_idx,
            df=df,
            row_number_col=row_number_col,
            value_start_col=value_start_col,
            seen_row_numbers=seen_row_numbers,
        )

        if row_number is None or row_number not in ROW_NUMBER_TO_INDICATOR:
            continue

        if row_number in seen_row_numbers:
            continue

        seen_row_numbers.add(row_number)
        records.extend(
            build_records_for_row(
                df=df,
                row_idx=row_idx,
                year=year,
                row_number=row_number,
                graph_to_column=graph_to_column,
            )
        )

        if len(seen_row_numbers) == len(ROW_NUMBER_TO_INDICATOR):
            break

    return records


def detect_current_row_number(
    row: pd.Series,
    row_idx: int,
    df: pd.DataFrame,
    row_number_col: int | None,
    value_start_col: int,
    seen_row_numbers: set[int],
) -> int | None:
    """Определяет номер текущей строки таблицы 2100."""
    if row_number_col is not None:
        return parse_int(df.iat[row_idx, row_number_col])

    row_number = detect_row_number_by_text(row)
    if row_number is not None:
        return row_number

    remaining_rows = sorted(set(ROW_NUMBER_TO_INDICATOR) - seen_row_numbers)
    expected_next_row = remaining_rows[0] if remaining_rows else None
    candidate_row = detect_row_number_by_cells(row, value_start_col)

    # Принимаем номер из ячейки только если он совпадает со следующим ожидаемым
    # номером строки. Так мы не путаем служебные числа с номером показателя.
    if candidate_row == expected_next_row:
        return candidate_row

    return None


def build_records_for_row(
    df: pd.DataFrame,
    row_idx: int,
    year: int,
    row_number: int,
    graph_to_column: dict[int, int],
) -> list[dict[str, Any]]:
    """Формирует записи длинного формата для одной строки таблицы."""
    indicator = ROW_NUMBER_TO_INDICATOR[row_number]
    icd_code = clean_icd_code(df.iat[row_idx, graph_to_column[3]])
    records: list[dict[str, Any]] = []

    for graph_number, detail in COLUMN_NUMBER_TO_DETAIL.items():
        value_col = graph_to_column[graph_number]

        records.append(
            {
                "Показатель": indicator,
                "Уточнение": detail,
                "Код по МКБ": icd_code,
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
    expected_rows = len(ROW_NUMBER_TO_INDICATOR) * len(COLUMN_NUMBER_TO_DETAIL)

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

    expected_graphs_set = set(COLUMN_NUMBER_TO_DETAIL)
    actual_graphs_set = set(df["Графа"].dropna().astype(int))
    if actual_graphs_set != expected_graphs_set:
        raise ParserError(
            f"В файле {path.name} некорректный набор граф: {sorted(actual_graphs_set)}"
        )


def collect_data(input_files: list[Path]) -> pd.DataFrame:
    """Собирает данные таблицы 2100 из всех переданных файлов."""
    frames: list[pd.DataFrame] = []

    for path in input_files:
        logger.info("Обрабатываю файл: %s", path.name)
        frames.append(parse_table_2100(path))

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
    expected_per_year = len(ROW_NUMBER_TO_INDICATOR) * len(COLUMN_NUMBER_TO_DETAIL)

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

    required_columns = ["Показатель", "Уточнение", "Год", "Строка", "Графа", "Таблица"]
    null_required_fields = df[required_columns].isna().sum().sum()
    if null_required_fields:
        raise ParserError("В обязательных полях найдены пропущенные значения")

    unexpected_graphs = sorted(set(df["Графа"].astype(int)) - set(COLUMN_NUMBER_TO_DETAIL))
    if unexpected_graphs:
        raise ParserError(f"Найдены лишние графы: {unexpected_graphs}")

    unexpected_rows = sorted(set(df["Строка"].astype(int)) - set(ROW_NUMBER_TO_INDICATOR))
    if unexpected_rows:
        raise ParserError(f"Найдены лишние строки: {unexpected_rows}")

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
        description="Парсер таблицы 2100 из Excel-файлов формы 33"
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
        raise ParserError("Не найдены исходные файлы формы 33, таблицы 2100")

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
