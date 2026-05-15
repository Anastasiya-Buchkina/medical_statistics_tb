"""ETL: форма N30, таблица 1100.

Назначение:
    Парсит DOCX-файлы формы N30 за 2016-2024 годы и извлекает сведения
    о фтизиатрах и участковых фтизиатрах.

Источник:
    data/формы 30/*.docx

Результат:
    - processed/tb_form30_1100.csv;
    - public.tb_form30_1100 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form30_1100
    python3 orchestrator.py run form30_1100 --write-db

Особенности:
    Номера строк отличаются по годам, поэтому для расчетов используется
    нормализованное поле "Строка_норм".
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
from docx import Document

from db_manager import delete_rows_between_years, load_dataframe

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATABASE_URL, FORM_30_DIR, PROCESSED_DIR, PROJECT_ROOT

try:
    from etl.source_registry import get_source_files
except ImportError:
    get_source_files = None

# -----------------------------------------------------------------------------
# Логирование
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Настройки
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """
    Настройки путей, имени выходного файла и таблицы PostgreSQL.

    Все пути заданы относительно локального проекта medical_statistics_tb.
    При переносе проекта достаточно изменить project_dir.
    """

    project_dir: Path = PROJECT_ROOT
    raw_dir: Path = FORM_30_DIR
    processed_dir: Path = PROCESSED_DIR
    output_csv: str = "tb_form30_1100.csv"
    db_table: str = "tb_form30_1100"
    db_schema: str = "public"
    db_url: str | None = DATABASE_URL
    skip_db: bool = False
    input_files: list[Path] | None = None


CONFIG = Config()

# -----------------------------------------------------------------------------
# Справочники
# -----------------------------------------------------------------------------

# Главное правило: не брать одну и ту же строку для всех лет.
# Строки отличаются по годам.
PHTHISIOLOGIST_ROWS: dict[int, dict[str, int]] = {
    2016: {"total": 110, "district": 111},
    2017: {"total": 110, "district": 111},
    2018: {"total": 111, "district": 112},
    2019: {"total": 110, "district": 111},
    2020: {"total": 111, "district": 112},
    2021: {"total": 110, "district": 111},
    2022: {"total": 111, "district": 112},
    2023: {"total": 111, "district": 112},
    2024: {"total": 111, "district": 112},
}

# Нормализованные названия показателей
INDICATOR_NAMES: dict[str, str] = {
    "total": "Фтизиатры",
    "district": "Фтизиатры участковые",
}

# Нужные графы таблицы 1100 для расчета относительных кадровых показателей:
# гр.3 — штатные должности;
# гр.4 — занятые должности;
# гр.9 — физические лица основных работников на занятых должностях.
GRAPH_INFO: dict[int, str] = {
    3: "Число должностей в целом по организации штатных",
    4: "Число должностей в целом по организации занятых",
    9: "Число физических лиц основных работников на занятых должностях",
}

# Номер таблицы формы №30, из которой забираются данные.
TABLE_NUMBER = "1100"
SOURCE_KEY = "form30_1100"
TARGET_YEARS = set(range(2016, 2025))

# Финальный порядок колонок в CSV и при загрузке в БД.
OUTPUT_COLUMNS = [
    "Показатель",
    "Уточнение",
    "Год",
    "Значение",
    "Строка",
    "Строка_норм",
    "Графа",
    "Таблица",
]

# Нормализованные номера строк для дальнейших расчетов.
# В исходных формах номера строк могут отличаться по годам,
# но для расчетного слоя используем единый справочник:
# 111 — фтизиатры;
# 112 — фтизиатры участковые.
NORMALIZED_ROW_NUMBERS: dict[str, str] = {
    "total": "111",
    "district": "112",
}

# -----------------------------------------------------------------------------
# Вспомогательные функции
# -----------------------------------------------------------------------------

def normalize_text(value: Any) -> str:
    """
    Нормализует текст из ячеек Word-таблицы:
    - заменяет переносы строк на пробелы;
    - схлопывает повторяющиеся пробелы;
    - обрезает края.
    """
    if value is None:
        return ""

    text_value = str(value)
    text_value = text_value.replace("\n", " ")
    text_value = re.sub(r"\s+", " ", text_value)
    return text_value.strip()


def extract_year_from_filename(file_path: Path) -> int | None:
    """
    Достает год из имени файла.
    Например:
    - 30_2016.docx -> 2016
    - 30_ 2018.docx -> 2018
    """
    match = re.search(r"(20\d{2})", file_path.name)
    if not match:
        return None

    year = int(match.group(1))
    if year not in PHTHISIOLOGIST_ROWS:
        return None

    return year


def parse_decimal(value: Any) -> float | None:
    """
    Преобразует значения вида:
    - '209,00'
    - '1 234,50'
    - '119'
    в float.

    Пустые значения, X и служебные символы возвращают None.
    """
    text_value = normalize_text(value)

    if not text_value:
        return None

    if text_value.upper() in {"X", "-", "—"}:
        return None

    text_value = text_value.replace("\u00a0", " ")
    text_value = text_value.replace(" ", "")
    text_value = text_value.replace(",", ".")

    # Оставляем только числовой формат
    if not re.fullmatch(r"-?\d+(\.\d+)?", text_value):
        return None

    return float(text_value)


def extract_row_number(row_cells: list[str]) -> str | None:
    """
    Извлекает номер строки из второй ячейки таблицы.

    В таблице 1100 структура обычно такая:
    0 — наименование должности,
    1 — № строки,
    2 — графа 3,
    3 — графа 4,
    ...
    8 — графа 9.
    """
    if len(row_cells) < 2:
        return None

    row_number = normalize_text(row_cells[1])

    if not row_number:
        return None

    # Оставляем строки вида 110, 111, 112.
    if not re.fullmatch(r"\d+(\.\d+)?", row_number):
        return None

    return row_number




def is_target_row(
    row_cells: list[str],
    expected_row: int,
    indicator_type: str,
) -> bool:
    """
    Проверяет, что строка таблицы соответствует нужной строке и нужному показателю.

    Дополнительная проверка по тексту нужна, чтобы не забрать случайно строку
    с таким же номером из другой таблицы или из шапки.
    """
    if len(row_cells) < 9:
        return False

    row_number = extract_row_number(row_cells)
    if row_number != str(expected_row):
        return False

    row_name = normalize_text(row_cells[0]).lower()

    if indicator_type == "total":
        # Берем строку "фтизиатры", но не строку "из них фтизиатры участковые".
        return "фтизиат" in row_name and "участков" not in row_name

    if indicator_type == "district":
        return "фтизиат" in row_name and "участков" in row_name

    return False


def row_to_long_records(
    row_cells: list[str],
    year: int,
    row_number: int,
    indicator_type: str,
) -> list[dict[str, Any]]:
    """
    Преобразует одну строку таблицы 1100 в длинный формат:
    одна строка результата = одна графа.
    """
    records: list[dict[str, Any]] = []

    # Индексы ячеек в docx-таблице:
    # графа 3 -> index 2
    # графа 4 -> index 3
    # графа 9 -> index 8
    graph_to_cell_index = {
        3: 2,
        4: 3,
        9: 8,
    }

    for graph_number, cell_index in graph_to_cell_index.items():
        value = parse_decimal(row_cells[cell_index])

        records.append(
            {
                "Показатель": INDICATOR_NAMES[indicator_type],
                "Уточнение": GRAPH_INFO[graph_number],
                "Год": year,
                "Значение": value,
                "Строка": str(row_number),
                "Строка_норм": NORMALIZED_ROW_NUMBERS[indicator_type],
                "Графа": graph_number,
                "Таблица": TABLE_NUMBER,
            }
        )

    return records


def parse_docx_file(file_path: Path) -> list[dict[str, Any]]:
    """
    Парсит один DOCX-файл формы №30.

    Для каждого года ожидается 6 строк результата:
    2 показателя × 3 графы.

    Важно:
    Word-файлы формы №30 могут содержать повторяющиеся фрагменты и похожие
    строки в разных таблицах. Поэтому дополнительно используется набор
    found_targets: для каждого файла берем только первое корректное совпадение
    по каждому целевому показателю.
    """
    year = extract_year_from_filename(file_path)
    if year is None:
        logger.warning("Год не найден или не входит в 2016–2024: %s", file_path.name)
        return []

    logger.info("Парсим файл: %s, год: %s", file_path.name, year)

    document = Document(file_path)
    expected_rows = PHTHISIOLOGIST_ROWS[year]

    records: list[dict[str, Any]] = []
    found_targets: set[str] = set()

    for table in document.tables:
        for row in table.rows:
            row_cells = [normalize_text(cell.text) for cell in row.cells]

            for indicator_type, expected_row in expected_rows.items():
                if indicator_type in found_targets:
                    continue

                if is_target_row(
                    row_cells=row_cells,
                    expected_row=expected_row,
                    indicator_type=indicator_type,
                ):
                    records.extend(
                        row_to_long_records(
                            row_cells=row_cells,
                            year=year,
                            row_number=expected_row,
                            indicator_type=indicator_type,
                        )
                    )
                    found_targets.add(indicator_type)

                    logger.info(
                        "Найдена строка: файл=%s, год=%s, показатель=%s, строка=%s",
                        file_path.name,
                        year,
                        INDICATOR_NAMES[indicator_type],
                        expected_row,
                    )

    missing = set(expected_rows.keys()) - found_targets
    if missing:
        missing_names = [INDICATOR_NAMES[item] for item in sorted(missing)]
        logger.warning(
            "В файле %s не найдены строки: %s",
            file_path.name,
            ", ".join(missing_names),
        )

    expected_record_count = 6
    if len(records) != expected_record_count:
        logger.warning(
            "Для файла %s ожидалось %s строк результата, получено %s",
            file_path.name,
            expected_record_count,
            len(records),
        )

    return records


def collect_docx_files(raw_dir: Path) -> list[Path]:
    """
    Собирает docx-файлы формы 30 из сырой папки.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Не найдена папка с исходными файлами: {raw_dir}")

    files = sorted(raw_dir.glob("*.docx"))

    # Исключаем временные файлы Word
    files = [file for file in files if not file.name.startswith("~$")]

    if not files:
        raise FileNotFoundError(f"В папке нет docx-файлов: {raw_dir}")

    return files


def filter_target_years(input_files: list[Path]) -> list[Path]:
    """Оставляет годы, поддерживаемые текущей реализацией парсера."""
    filtered_files = [
        path for path in input_files
        if (year := extract_year_from_filename(path)) is not None and year in TARGET_YEARS
    ]
    return sorted(filtered_files, key=lambda path: extract_year_from_filename(path) or 0)


def get_registered_input_files() -> list[Path]:
    """Получает файлы из БД-реестра или использует локальный fallback."""
    if get_source_files is not None:
        try:
            files = get_source_files(SOURCE_KEY, years=sorted(TARGET_YEARS))
            if files:
                return files
        except Exception as exc:
            logger.warning("Не удалось получить файлы из реестра БД: %s", exc)

    return collect_docx_files(CONFIG.raw_dir)


def build_dataset(raw_dir: Path | None = None, input_files: list[Path] | None = None) -> pd.DataFrame:
    """
    Собирает итоговый датасет из всех docx-файлов.
    """
    if input_files is not None:
        files = input_files
    elif raw_dir is not None:
        files = collect_docx_files(raw_dir)
    else:
        files = get_registered_input_files()

    files = filter_target_years(files)

    all_records: list[dict[str, Any]] = []

    for file_path in files:
        all_records.extend(parse_docx_file(file_path))

    df = pd.DataFrame(all_records)

    if df.empty:
        raise ValueError("Итоговый датафрейм пустой. Данные не распарсились.")

    df = df.sort_values(
        by=["Год", "Показатель", "Графа"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    df = df[OUTPUT_COLUMNS]

    return df


def validate_dataset(df: pd.DataFrame) -> None:
    """
    Выполняет базовые проверки качества.
    """
    expected_years = set(range(2016, 2025))
    actual_years = set(df["Год"].unique())

    if actual_years != expected_years:
        missing_years = sorted(expected_years - actual_years)
        extra_years = sorted(actual_years - expected_years)

        raise ValueError(
            "Проблема с годами. "
            f"Отсутствуют: {missing_years}. Лишние: {extra_years}."
        )

    expected_rows_per_year = 6
    rows_by_year = df.groupby("Год").size()

    bad_years = rows_by_year[rows_by_year != expected_rows_per_year]
    if not bad_years.empty:
        raise ValueError(
            "По некоторым годам неверное количество строк. "
            f"Ожидалось {expected_rows_per_year} строк на год. "
            f"Проблемные годы: {bad_years.to_dict()}"
        )

    duplicate_mask = df.duplicated(
        subset=["Показатель", "Уточнение", "Год", "Строка_норм", "Графа", "Таблица"],
        keep=False,
    )

    if duplicate_mask.any():
        duplicates = df.loc[
            duplicate_mask,
            ["Показатель", "Уточнение", "Год", "Строка", "Строка_норм", "Графа", "Таблица"],
        ]

        raise ValueError(
            "Найдены дубли по бизнес-ключу:\n"
            f"{duplicates.to_string(index=False)}"
        )

    null_values = df[df["Значение"].isna()]
    if not null_values.empty:
        raise ValueError(
            "Найдены пустые значения в колонке 'Значение':\n"
            f"{null_values.to_string(index=False)}"
        )

    logger.info("Проверки качества пройдены успешно.")


def save_control_csv(df: pd.DataFrame, processed_dir: Path, output_csv: str) -> Path:
    """
    Сохраняет контрольный CSV в папку processed.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    output_path = processed_dir / output_csv

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info("Контрольный CSV сохранен: %s", output_path)

    return output_path


def load_to_database(df: pd.DataFrame, config: Config) -> None:
    """
    Загружает итоговый датафрейм в PostgreSQL.

    Логика загрузки идемпотентная для периода 2016–2024:
    сначала удаляем старые строки за этот период, затем вставляем свежие данные.
    Это позволяет безопасно перезапускать скрипт после исправлений парсинга.
    Колонка "Строка_норм" загружается вместе с остальными расчетными полями.
    """
    table_name = f"{config.db_schema}.{config.db_table}"
    delete_rows_between_years(
        table_name,
        start_year=2016,
        end_year=2024,
        database_url=config.db_url,
    )
    load_dataframe(df, table_name, if_exists="append", database_url=config.db_url)
    logger.info("Загрузка в БД завершена успешно.")


def parse_args() -> Config:
    """Читает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Парсер таблицы 1100 из DOCX-файлов формы 30"
    )
    parser.add_argument(
        "input_files",
        nargs="*",
        type=Path,
        help="Пути к DOCX-файлам. Если не указаны, файлы берутся из etl_source_files.",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="Строка подключения к PostgreSQL. Если не указана, берется из DATABASE_URL.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=CONFIG.processed_dir,
        help="Папка для контрольного CSV-файла.",
    )
    parser.add_argument(
        "--output-file",
        default=CONFIG.output_csv,
        help="Имя контрольного CSV-файла.",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Безопасный режим: сохранить CSV, но не писать данные в БД.",
    )

    args = parser.parse_args()
    db_url = args.db_url or DATABASE_URL
    if not args.skip_db and not db_url:
        raise EnvironmentError(
            "Не задана строка подключения. "
            "Передайте --db-url, задайте DATABASE_URL в .env или используйте --skip-db."
        )

    config = Config(
        project_dir=CONFIG.project_dir,
        raw_dir=CONFIG.raw_dir,
        processed_dir=args.output_dir,
        output_csv=args.output_file,
        db_table=CONFIG.db_table,
        db_schema=CONFIG.db_schema,
        db_url=db_url,
        skip_db=args.skip_db,
        input_files=filter_target_years(args.input_files) if args.input_files else None,
    )
    return config


def run(
    input_files: list[Path] | None = None,
    *,
    skip_db: bool = False,
    db_url: str | None = None,
    output_dir: Path = CONFIG.processed_dir,
    output_file: str = CONFIG.output_csv,
) -> pd.DataFrame:
    """Запускает парсер из оркестратора или напрямую из другого Python-кода."""
    config = Config(
        project_dir=CONFIG.project_dir,
        raw_dir=CONFIG.raw_dir,
        processed_dir=output_dir,
        output_csv=output_file,
        db_table=CONFIG.db_table,
        db_schema=CONFIG.db_schema,
        db_url=db_url or DATABASE_URL,
        skip_db=skip_db,
        input_files=input_files,
    )

    logger.info("Старт парсинга формы 30, таблицы 1100.")

    resolved_input_files = input_files or get_registered_input_files()
    df = build_dataset(input_files=resolved_input_files)

    validate_dataset(df)

    save_control_csv(
        df=df,
        processed_dir=config.processed_dir,
        output_csv=config.output_csv,
    )

    if config.skip_db:
        logger.info("Режим --skip-db: загрузка в PostgreSQL пропущена")
        return df

    load_to_database(df=df, config=config)

    logger.info("Готово. Загружено строк: %s", len(df))
    return df


def main() -> int:
    """
    Основная точка входа.
    """
    try:
        config = parse_args()
        run(
            input_files=config.input_files,
            skip_db=config.skip_db,
            db_url=config.db_url,
            output_dir=config.processed_dir,
            output_file=config.output_csv,
        )
        return 0

    except Exception as error:
        logger.exception("Ошибка выполнения скрипта: %s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
