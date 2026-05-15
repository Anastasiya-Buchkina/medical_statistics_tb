"""ETL: форма N33, таблица 2200.

Назначение:
    Парсит Excel-файлы формы N33 за 2016-2024 годы и извлекает данные
    таблицы 2200 по выявлению больных и групп риска.

Источник:
    data/33_8_2015-2024/*.xls(x)

Результат:
    - processed/tb_form33_2200.csv;
    - public.tb_form33_2200 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form33_2200
    python3 orchestrator.py run form33_2200 --write-db

Особенности:
    Для части файлов строка 1 сверстана нестандартно, поэтому парсер
    восстанавливает ее значения из строки общего показателя.
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

SOURCE_KEY = "form33_2200"
TABLE_CODE = "2200"
DEFAULT_TABLE_NAME = "public.tb_form33_2200"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = FORM_8_33_DIR
DEFAULT_OUTPUT_DIR = PROCESSED_DIR
DEFAULT_OUTPUT_FILE = "tb_form33_2200.csv"
TARGET_YEARS = set(range(2016, 2025))

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

ROW_NUMBER_TO_INDICATOR: dict[int, str] = {
    1: "Впервые выявлено больных туберкулезом из числа осмотренных на туберкулез",
    2: "Впервые выявлено больных туберкулезом с применением туберкулинодиагностики",
    3: (
        "Впервые выявлено больных туберкулезом с применением аллергена "
        "туберкулезного рекомбинантного в стандартном разведении"
    ),
    4: "Впервые выявлено больных туберкулезом с применением флюорографии",
    5: (
        "Впервые выявлено больных туберкулезом с применением "
        "бактериологических методов"
    ),
    6: "Впервые выявлено больных туберкулезом методом бактериоскопии",
    7: "Взято на учет в III А группу диспансерного учета",
    8: "Взято на учет в V группу диспансерного учета всего",
    9: "Взято на учет в V группу диспансерного учета, в том числе в VА",
    10: "Взято на учет в V группу диспансерного учета, в том числе в VБ",
    11: (
        "Кроме того умерло больных от туберкулеза постоянных жителей, "
        "диагноз у которых установлен посмертно"
    ),
    12: (
        "Кроме того умерло больных от ВИЧ-инфекции постоянных жителей, "
        "диагноз у которых установлен посмертно"
    ),
}

ROW_NUMBER_TO_SEARCH_PHRASES: dict[int, tuple[str, ...]] = {
    1: ("впервые выявлено", "больных", "туберкулез"),
    2: ("туберкулинодиагност",),
    3: ("аллергена", "рекомбинант"),
    4: ("флюорограф",),
    5: ("бактериологических методов",),
    6: ("бактериоскоп",),
    7: ("iii", "группу", "диспансерного учета"),
    8: ("v", "группу", "диспансерного учета", "всего"),
    9: ("vа",),
    10: ("vб",),
    11: ("умерло", "туберкулеза", "посмертно"),
    12: ("умерло", "вич", "посмертно"),
}

# Исключающие фразы для строки 1, чтобы общий показатель не спутался со строками 2–6.
ROW_1_EXCLUSION_PHRASES = (
    "туберкулинодиагност",
    "аллергена",
    "рекомбинант",
    "флюорограф",
    "бактериологических методов",
    "бактериоскоп",
)

COLUMN_NUMBER_TO_DETAIL: dict[int, str] = {
    3: "Всего",
    4: "Из них дети 0-14 лет",
    5: "Из них подростки 15-17 лет",
}

FINAL_COLUMNS = [
    "Показатель",
    "Уточнение",
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
    """Ошибка парсинга или валидации таблицы 2200."""

# -----------------------------------------------------------------------------
# Универсальные функции очистки и преобразования значений
# -----------------------------------------------------------------------------

def normalize_dash(value: str) -> str:
    """Унифицирует разные типы тире."""
    return value.replace("—", "-").replace("–", "-")


def clean_cell(value: Any) -> str:
    """Возвращает очищенное текстовое значение ячейки Excel."""
    if value is None or pd.isna(value):
        return ""

    return re.sub(r"\s+", " ", str(value).replace("\n", " ")).strip()


def normalize_text(value: Any) -> str:
    """Нормализует текст для поиска служебных маркеров."""
    text_value = clean_cell(value).lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text_value).strip()


def normalize_for_search(value: Any) -> str:
    """Нормализует текст ячейки или строки для поиска показателей."""
    text_value = normalize_dash(normalize_text(value))
    text_value = text_value.replace("/", " ")
    text_value = text_value.replace("va", "vа")
    text_value = text_value.replace("v a", "vа")
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


def row_text(row: pd.Series) -> str:
    """Собирает строку Excel в один нормализованный текст."""
    return normalize_for_search(" ".join(clean_cell(value) for value in row.tolist()))

# -----------------------------------------------------------------------------
# Поиск строк таблицы 2200
# -----------------------------------------------------------------------------

def detect_row_number_by_text(row: pd.Series) -> int | None:
    """
    Определяет номер строки таблицы 2200 по тексту показателя.

    Используется для файлов, где номера строк 1–12 не вынесены в отдельную
    стабильную колонку.
    """
    text_value = row_text(row)

    # Сначала проверяем более специфичные строки, затем общий показатель строки 1.
    for row_number in (12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2):
        phrases = ROW_NUMBER_TO_SEARCH_PHRASES[row_number]
        if all(phrase in text_value for phrase in phrases):
            return row_number

    row_1_phrases = ROW_NUMBER_TO_SEARCH_PHRASES[1]
    if all(phrase in text_value for phrase in row_1_phrases):
        if not any(phrase in text_value for phrase in ROW_1_EXCLUSION_PHRASES):
            return 1

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
# Восстановление строки 1 в нестандартной разметке
# -----------------------------------------------------------------------------

def find_row_1_values_near_header(
    df: pd.DataFrame,
    marker_row: int,
    graph_to_column: dict[int, int],
) -> dict[int, float] | None:
    """
    Ищет значения строки 1 в нестандартной разметке Excel.

    В части файлов значения общего показателя строки 1 находятся в строке
    с текстом показателя, а номер строки «1» — отдельно. Из-за этого обычный
    парсер может считать строку 1 нулевой.
    """
    search_end = min(marker_row + 12, len(df))

    for row_idx in range(marker_row + 1, search_end):
        text_value = row_text(df.iloc[row_idx])

        if not all(phrase in text_value for phrase in ROW_NUMBER_TO_SEARCH_PHRASES[1]):
            continue

        if any(phrase in text_value for phrase in ROW_1_EXCLUSION_PHRASES):
            continue

        candidate_values = {
            graph_number: parse_numeric(df.iat[row_idx, value_col])
            for graph_number, value_col in graph_to_column.items()
        }

        if any(value != 0 for value in candidate_values.values()):
            return candidate_values

    return None


def repair_row_1_values_if_needed(
    df_result: pd.DataFrame,
    df_source: pd.DataFrame,
    marker_row: int,
    graph_to_column: dict[int, int],
    path: Path,
) -> pd.DataFrame:
    """
    Восстанавливает значения строки 1, если она ошибочно считалась нулевой.

    Ремонт применяется только если строка 1 равна 0, а строка 4 имеет
    ненулевые значения. Это характерный признак нестандартной разметки.
    """
    row_1_mask = df_result["Строка"] == 1
    row_4_mask = df_result["Строка"] == 4

    row_1_sum = df_result.loc[row_1_mask, "Значение"].sum()
    row_4_sum = df_result.loc[row_4_mask, "Значение"].sum()

    if row_1_sum != 0 or row_4_sum == 0:
        return df_result

    candidate_values = find_row_1_values_near_header(
        df_source,
        marker_row,
        graph_to_column,
    )

    if candidate_values is None:
        return df_result

    repaired = df_result.copy()
    for graph_number, value in candidate_values.items():
        mask = row_1_mask & (repaired["Графа"] == graph_number)
        repaired.loc[mask, "Значение"] = value

    logger.info(
        "%s: восстановлены значения строки 1 таблицы 2200 из строки общего показателя",
        path.name,
    )
    return repaired

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
    Находит лист с таблицей 2200.

    Обычно таблица лежит на листе, где в названии есть 2200:
    2200,2300,2310 / 2200-2300 и похожие варианты.
    """
    candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if TABLE_CODE in normalize_text(sheet_name)
    ]

    if not candidates:
        raise ParserError("Не найден лист, в названии которого есть 2200")

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
# Поиск границ и колонок таблицы 2200
# -----------------------------------------------------------------------------

def find_table_2200_start(df: pd.DataFrame) -> int:
    """
    Находит начало таблицы 2200.

    Таблица 2200 обычно находится в верхней части листа перед блоками 2300
    и 2310. Ищем маркер 2200 и контекст выявления больных.
    """
    for row_idx in range(len(df)):
        row_text = " ".join(clean_cell(value) for value in df.iloc[row_idx].tolist())
        normalized = normalize_text(row_text)

        if TABLE_CODE in normalized and "выявлен" in normalized:
            return row_idx

    for row_idx in range(len(df)):
        row_text = " ".join(clean_cell(value) for value in df.iloc[row_idx].tolist())
        normalized = normalize_text(row_text)

        if TABLE_CODE in normalized:
            return row_idx

    raise ParserError("Не найден заголовок таблицы 2200")


def find_marker_row(df: pd.DataFrame, table_start: int) -> int:
    """Находит строку с номерами граф 1–5."""
    search_end = min(table_start + 20, len(df))

    for row_idx in range(table_start, search_end):
        values = [parse_int(value) for value in df.iloc[row_idx].tolist()]
        numbers = [value for value in values if value is not None]

        if set(range(1, 6)).issubset(numbers):
            return row_idx

    raise ParserError("Не найдена строка с номерами граф 1–5")


def build_graph_column_map(df: pd.DataFrame, marker_row: int) -> dict[int, int]:
    """
    Возвращает соответствие: номер графы -> индекс колонки Excel.

    Для таблицы 2200 берем первую последовательность граф 1–5 слева направо.
    Полезные для загрузки графы: 3–5.
    """
    graph_to_column: dict[int, int] = {}
    expected_graph = 1

    for col_idx, value in enumerate(df.iloc[marker_row].tolist()):
        graph_number = parse_int(value)

        if graph_number != expected_graph:
            continue

        graph_to_column[graph_number] = col_idx
        expected_graph += 1

        if expected_graph > 5:
            break

    missing_graphs = sorted(set(range(1, 6)) - set(graph_to_column))
    if missing_graphs:
        raise ParserError(f"Не найдены графы таблицы 2200: {missing_graphs}")

    useful_graphs = {
        graph_number: column_index
        for graph_number, column_index in graph_to_column.items()
        if graph_number in COLUMN_NUMBER_TO_DETAIL
    }

    missing_useful_graphs = sorted(
        set(COLUMN_NUMBER_TO_DETAIL) - set(useful_graphs)
    )
    if missing_useful_graphs:
        raise ParserError(
            f"Не найдены нужные графы таблицы 2200: {missing_useful_graphs}"
        )

    return useful_graphs


def find_row_number_column(df: pd.DataFrame, marker_row: int) -> int | None:
    """
    Находит колонку, где указаны номера строк 1–12.

    Если такой колонки нет, возвращает None. Тогда строки будут определяться
    по тексту показателя.
    """
    search_end = min(marker_row + 40, len(df))

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
# Парсинг таблицы 2200
# -----------------------------------------------------------------------------

def parse_table_2200(path: Path) -> pd.DataFrame:
    """Парсит таблицу 2200 из одного Excel-файла."""
    validate_input_file(path)

    year = parse_year_from_filename(path)
    workbook = pd.ExcelFile(path)
    sheet_name = find_sheet_name(workbook)
    df = read_excel_sheet(path, sheet_name)

    table_start = find_table_2200_start(df)
    marker_row = find_marker_row(df, table_start)
    graph_to_column = build_graph_column_map(df, marker_row)
    value_start_col = min(graph_to_column.values())
    row_number_col = find_row_number_column(df, marker_row)

    if row_number_col is None:
        logger.info(
            "%s: колонка с номерами строк 1-12 не найдена, использую поиск по тексту",
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
    result = repair_row_1_values_if_needed(
        df_result=result,
        df_source=df,
        marker_row=marker_row,
        graph_to_column=graph_to_column,
        path=path,
    )
    validate_single_file_result(result, path)

    logger.info("%s: найдено %s строк таблицы 2200", path.name, len(result))
    return result


def extract_records(
    df: pd.DataFrame,
    year: int,
    marker_row: int,
    graph_to_column: dict[int, int],
    value_start_col: int,
    row_number_col: int | None,
) -> list[dict[str, Any]]:
    """Извлекает записи таблицы 2200 из листа Excel."""
    records: list[dict[str, Any]] = []
    seen_row_numbers: set[int] = set()
    search_end = min(marker_row + 45, len(df))

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
    """Определяет номер текущей строки таблицы 2200."""
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
    records: list[dict[str, Any]] = []

    for graph_number, detail in COLUMN_NUMBER_TO_DETAIL.items():
        value_col = graph_to_column[graph_number]
        records.append(
            {
                "Показатель": indicator,
                "Уточнение": detail,
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
    """Собирает данные таблицы 2200 из всех переданных файлов."""
    frames: list[pd.DataFrame] = []

    for path in input_files:
        logger.info("Обрабатываю файл: %s", path.name)
        frames.append(parse_table_2200(path))

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
        description="Парсер таблицы 2200 из Excel-файлов формы 33"
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
        raise ParserError("Не найдены исходные файлы формы 33, таблицы 2200")

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
