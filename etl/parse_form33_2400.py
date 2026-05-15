"""ETL: форма N33, таблица 2400.

Назначение:
    Парсит Excel-файлы формы N33 за 2016-2024 годы и извлекает данные
    таблицы 2400 по диспансерной работе с группами учета.

Источник:
    data/33_8_2015-2024/*.xls(x)

Результат:
    - processed/tb_form33_2400.csv;
    - public.tb_form33_2400 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form33_2400
    python3 orchestrator.py run form33_2400 --write-db

Особенности:
    Листы отличаются по периодам: для 2016 года используется блок 2320/2330/2400,
    для 2017-2024 годов - лист с таблицами 2400-2500.
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

SOURCE_KEY = "form33_2400"
TABLE_CODE = "2400"
DEFAULT_TABLE_NAME = "public.tb_form33_2400"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = FORM_8_33_DIR
DEFAULT_OUTPUT_DIR = PROCESSED_DIR
DEFAULT_OUTPUT_FILE = "tb_form33_2400.csv"
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

# Нормализация строк таблицы 2400.
ROW_NUMBER_TO_INDICATOR: dict[int, str] = {
    1: (
        "Взрослые, нуждающиеся в определении активности туберкулезного "
        "процесса, группа 0А"
    ),
    2: (
        "Взрослые, нуждающиеся в проведении дифференциально-диагностических "
        "мероприятий, группа 0Б"
    ),
    3: (
        "Взрослые с неактивным туберкулезным процессом после клинического "
        "излечения, группа III"
    ),
    4: (
        "Взрослые, состоящие в бытовом и производственном контакте с "
        "бактериовыделителем, группа IVА"
    ),
    5: (
        "Взрослые, состоящие в бытовом и производственном контакте с больным "
        "туберкулезом без бактериовыделения, группа IVА"
    ),
    6: (
        "Взрослые в профессиональном контакте с источником инфекции, "
        "группа IVБ"
    ),
    7: (
        "Дети 0-17 лет, нуждающиеся в уточнении характера туберкулиновой "
        "чувствительности, уточнении активности туберкулеза и диагностике, "
        "группа 0"
    ),
    8: (
        "Дети 0-17 лет с остаточными посттуберкулезными изменениями, "
        "группа IIIА"
    ),
    9: "Дети 0-17 лет, переведенные из I, II, IIIА групп, группа IIIБ",
    10: (
        "Дети 0-17 лет, состоящие в контакте с бактериовыделителями, "
        "группа IVА"
    ),
    11: (
        "Дети 0-17 лет из контакта с больными туберкулезом без "
        "бактериовыделения, из семей животноводов или имеющих больных "
        "туберкулезом животных, группа IVБ"
    ),
    12: (
        "Дети 0-17 лет в раннем периоде первичной туберкулезной инфекции, "
        "группа VIА"
    ),
    13: (
        "Дети 0-17 лет ранее инфицированные с гиперергической реакцией на "
        "туберкулин, из социальных групп риска с выраженными реакциями на "
        "туберкулин, группа VIБ"
    ),
    14: (
        "Дети 0-17 лет с усиливающейся туберкулиновой чувствительностью, "
        "группа VIВ"
    ),
}

# Нормализация граф таблицы 2400.
COLUMN_NUMBER_TO_DETAIL: dict[int, str] = {
    3: "Взято в текущем году",
    4: "Подлежало ХП или пробному лечению",
    5: "Прошли курс ХП, пробного лечения",
    6: "Впервые выявлено больных с активным туберкулезом",
    7: "Снято с учета",
    8: "Выбыло",
    9: "Состоит на конец года",
}

FINAL_COLUMNS = ["Показатель", "Уточнение", "Год", "Значение", "Строка", "Графа", "Таблица"]

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
    """Ошибка парсинга или валидации данных таблицы 2400."""


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

    Для таблицы 2400 пустые ячейки, прочерки и многоточия трактуются как 0.
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

# -----------------------------------------------------------------------------
# Поиск исходных файлов и чтение Excel
# -----------------------------------------------------------------------------

def find_default_input_files(raw_dir: Path) -> list[Path]:
    """Находит стандартный набор исходных Excel-файлов формы 33 для таблицы 2400."""
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
    Находит лист с таблицей 2400.

    Для 2016 года таблица лежит на листе 2320,2330,2400.
    Для 2017–2024 годов — на листе 2400-2500.
    """
    candidates = [
        sheet_name
        for sheet_name in workbook.sheet_names
        if TABLE_CODE in normalize_text(sheet_name)
    ]

    if not candidates:
        raise ParserError("Не найден лист, в названии которого есть 2400")

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
# Поиск границ и колонок таблицы 2400
# -----------------------------------------------------------------------------

def find_table_2400_start(df: pd.DataFrame) -> int:
    """
    Находит начало таблицы 2400.

    В 2017–2024 годах заголовок обычно записан одной строкой:
    «(2400) 4. Диспансерная работа с 0, IV, VI группами учета».

    В 2016 году заголовок может быть разбит на несколько строк, поэтому
    дополнительно проверяется контекст вокруг строки с маркером «2400».
    """
    for row_idx in range(len(df)):
        row_text = " ".join(clean_cell(value) for value in df.iloc[row_idx].tolist())
        normalized = normalize_text(row_text)

        if TABLE_CODE in normalized and "диспансер" in normalized:
            return row_idx

    for row_idx in range(len(df)):
        row_text = " ".join(clean_cell(value) for value in df.iloc[row_idx].tolist())
        normalized = normalize_text(row_text)

        if TABLE_CODE not in normalized:
            continue

        context_start = max(0, row_idx - 3)
        context_end = min(row_idx + 8, len(df))
        context_text = " ".join(
            " ".join(clean_cell(value) for value in df.iloc[i].tolist())
            for i in range(context_start, context_end)
        )
        context_normalized = normalize_text(context_text)

        if "диспансер" in context_normalized:
            return context_start

        # Запасной вариант: если рядом с маркером 2400 найдена строка с графами
        # 1–9, считаем этот блок таблицей 2400.
        try:
            find_marker_row(df, row_idx)
            return row_idx
        except ParserError:
            continue

    raise ParserError("Не найден заголовок таблицы 2400")


def find_marker_row(df: pd.DataFrame, table_start: int) -> int:
    """Находит строку с номерами граф 1–9."""
    search_end = min(table_start + 20, len(df))

    for row_idx in range(table_start, search_end):
        values = [parse_int(value) for value in df.iloc[row_idx].tolist()]
        numbers = [value for value in values if value is not None]

        if set(range(1, 10)).issubset(numbers):
            return row_idx

    raise ParserError("Не найдена строка с номерами граф 1–9")


def build_graph_column_map(df: pd.DataFrame, marker_row: int) -> dict[int, int]:
    """
    Возвращает соответствие: номер графы -> индекс колонки Excel.

    Для таблицы 2400 берем первую последовательность граф 1–9 слева направо.
    Если ниже или рядом есть дополнительные блоки, они игнорируются.
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
        raise ParserError(f"Не найдены графы таблицы 2400: {missing_graphs}")

    useful_graphs = {
        graph_number: column_index
        for graph_number, column_index in graph_to_column.items()
        if graph_number in COLUMN_NUMBER_TO_DETAIL
    }

    missing_useful_graphs = sorted(
        set(COLUMN_NUMBER_TO_DETAIL) - set(useful_graphs)
    )
    if missing_useful_graphs:
        raise ParserError(f"Не найдены нужные графы таблицы 2400: {missing_useful_graphs}")

    return useful_graphs


def find_row_number_column(df: pd.DataFrame, marker_row: int) -> int:
    """Находит колонку, где указаны номера строк 1–14."""
    search_end = min(marker_row + 25, len(df))

    for col_idx in range(df.shape[1]):
        values = [
            parse_int(df.iat[row_idx, col_idx])
            for row_idx in range(marker_row + 1, search_end)
        ]
        row_numbers = {value for value in values if value is not None}

        if set(ROW_NUMBER_TO_INDICATOR).issubset(row_numbers):
            return col_idx

    raise ParserError("Не найдена колонка с номерами строк 1–14")

# -----------------------------------------------------------------------------
# Парсинг таблицы 2400
# -----------------------------------------------------------------------------

def parse_table_2400(path: Path) -> pd.DataFrame:
    """Парсит таблицу 2400 из одного Excel-файла."""
    validate_input_file(path)

    year = parse_year_from_filename(path)
    workbook = pd.ExcelFile(path)
    sheet_name = find_sheet_name(workbook)
    df = read_excel_sheet(path, sheet_name)

    table_start = find_table_2400_start(df)
    marker_row = find_marker_row(df, table_start)
    graph_to_column = build_graph_column_map(df, marker_row)
    row_number_col = find_row_number_column(df, marker_row)

    records: list[dict[str, Any]] = []
    seen_row_numbers: set[int] = set()
    search_end = min(marker_row + 30, len(df))

    for row_idx in range(marker_row + 1, search_end):
        row_number = parse_int(df.iat[row_idx, row_number_col])

        if row_number is None or row_number not in ROW_NUMBER_TO_INDICATOR:
            continue

        # Берем только первую найденную строку каждого показателя.
        # Это защищает от попадания строк из соседних блоков.
        if row_number in seen_row_numbers:
            continue

        seen_row_numbers.add(row_number)
        indicator = ROW_NUMBER_TO_INDICATOR[row_number]

        for graph_number, detail in COLUMN_NUMBER_TO_DETAIL.items():
            value_col = graph_to_column[graph_number]
            value = parse_numeric(df.iat[row_idx, value_col])

            records.append(
                {
                    "Показатель": indicator,
                    "Уточнение": detail,
                    "Год": year,
                    "Значение": value,
                    "Строка": row_number,
                    "Графа": graph_number,
                    "Таблица": TABLE_CODE,
                }
            )

        if len(seen_row_numbers) == len(ROW_NUMBER_TO_INDICATOR):
            break

    result = pd.DataFrame(records, columns=FINAL_COLUMNS)
    validate_single_file_result(result, path)

    log.info("%s: найдено %s строк таблицы 2400", path.name, len(result))
    return result


def validate_single_file_result(df: pd.DataFrame, path: Path) -> None:
    """Проверяет полноту данных, полученных из одного Excel-файла."""
    expected_rows = len(ROW_NUMBER_TO_INDICATOR) * len(COLUMN_NUMBER_TO_DETAIL)

    if len(df) != expected_rows:
        raise ParserError(
            f"Для файла {path.name} ожидалось {expected_rows} строк, получено {len(df)}"
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
    """Собирает данные таблицы 2400 из всех переданных файлов."""
    frames: list[pd.DataFrame] = []

    for path in input_files:
        log.info("Обрабатываю файл: %s", path.name)
        frames.append(parse_table_2400(path))

    if not frames:
        raise ParserError("Не удалось собрать данные: подходящие файлы не найдены")

    result = pd.concat(frames, ignore_index=True)
    return result.sort_values(["Год", "Строка", "Графа"]).reset_index(drop=True)

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
        description="Парсер таблицы 2400 из Excel-файлов формы 33"
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
