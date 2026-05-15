"""ETL: форма N30, таблица 3100.

Назначение:
    Парсит DOCX-файлы формы N30 за 2016-2024 годы и извлекает данные
    о туберкулезных койках для взрослых и детей.

Источник:
    data/формы 30/*.docx

Результат:
    - processed/tb_form30_3100.csv;
    - public.tb_form30_3100 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form30_3100
    python3 orchestrator.py run form30_3100 --write-db

Особенности:
    Номера строк меняются по годам, поэтому для расчетов используется
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
from docx.table import Table

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
    """

    project_dir: Path = PROJECT_ROOT
    raw_dir: Path = FORM_30_DIR
    processed_dir: Path = PROCESSED_DIR
    output_csv: str = "tb_form30_3100.csv"
    db_table: str = "tb_form30_3100"
    db_schema: str = "public"
    db_url: str | None = DATABASE_URL
    skip_db: bool = False
    input_files: list[Path] | None = None


CONFIG = Config()

# -----------------------------------------------------------------------------
# Константы и справочники нормализации
# -----------------------------------------------------------------------------

TABLE_NUMBER = "3100"
SOURCE_KEY = "form30_3100"

YEAR_MIN = 2016
YEAR_MAX = 2024

ADULT_TB_BEDS = "Туберкулезные для взрослых"
CHILD_TB_BEDS = "Туберкулезные для детей"

EXPECTED_INDICATORS: set[str] = {
    ADULT_TB_BEDS,
    CHILD_TB_BEDS,
}

# Нужные графы первой части таблицы 3100.
# Графа 5 — среднегодовые койки.
# Графа 6 — поступило пациентов всего, чел.
TARGET_GRAPHS: set[int] = {5, 6}

DETAIL_NAMES: dict[int, str] = {
    5: "Средне-годовых",
    6: "Поступило пациентов всего, чел",
}

# Нормализованные номера строк для расчетного слоя.
# В исходных формах туберкулезные койки могут находиться в строках 57/58
# или 58/59, поэтому для дальнейших формул фиксируем единые номера:
# 57 — туберкулезные для взрослых;
# 58 — туберкулезные для детей.
NORMALIZED_ROW_NUMBERS: dict[str, str] = {
    ADULT_TB_BEDS: "57",
    CHILD_TB_BEDS: "58",
}

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

EXPECTED_PAIRS: set[tuple[str, int]] = {
    (ADULT_TB_BEDS, 5),
    (ADULT_TB_BEDS, 6),
    (CHILD_TB_BEDS, 5),
    (CHILD_TB_BEDS, 6),
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


def normalize_for_search(value: Any) -> str:
    """
    Нормализует текст для поиска по названию строки.

    Дополнительно приводит букву ё к е, чтобы не зависеть от варианта написания.
    """
    text_value = normalize_text(value).lower()
    text_value = text_value.replace("ё", "е")
    return text_value


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
    if year < YEAR_MIN or year > YEAR_MAX:
        return None

    return year


def parse_decimal(value: Any) -> float | None:
    """
    Преобразует значение из Word-таблицы в число.

    Поддерживает значения с пробелами и десятичной запятой:
        - '209,00'
        - '1 234,50'
        - '119'

    Пустые значения, X, прочерки и многоточия возвращает как None.
    """
    text_value = normalize_text(value)

    if not text_value:
        return None

    if text_value.upper() in {"X", "-", "—", "…"}:
        return None

    text_value = (
        text_value.replace("\u00a0", " ")
        .replace(" ", "")
        .replace(",", ".")
    )

    if not re.fullmatch(r"-?\d+(\.\d+)?", text_value):
        return None

    return float(text_value)


def extract_row_number(row_cells: list[str]) -> str | None:
    """
    Извлекает номер строки из второй ячейки Word-таблицы.
    """
    if len(row_cells) < 2:
        return None

    row_number = normalize_text(row_cells[1])

    if not row_number:
        return None

    if not re.fullmatch(r"\d+(\.\d+)?", row_number):
        return None

    return row_number


def get_bed_profile_indicator(row_name: str) -> str | None:
    """
    Определяет, является ли строка нужным профилем туберкулезных коек.

    Возвращает нормализованное название показателя.
    """
    normalized_name = normalize_for_search(row_name)

    if "туберкулез" not in normalized_name:
        return None

    if "для взрослых" in normalized_name:
        return ADULT_TB_BEDS

    if "для детей" in normalized_name:
        return CHILD_TB_BEDS

    return None


def extract_graph_mapping(table: Table) -> dict[int, int]:
    """
    Строит маппинг:
        номер графы -> индекс ячейки в Word-таблице.

    Для первой части таблицы 3100 ожидается строка с номерами граф:
        1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9

    Такой подход надежнее, чем жестко задавать индексы,
    потому что Word-таблицы в разных годах могут иметь разметочные отличия.
    """
    for row in table.rows:
        row_cells = [normalize_text(cell.text) for cell in row.cells]

        graph_mapping: dict[int, int] = {}

        for cell_index, cell_value in enumerate(row_cells):
            if not re.fullmatch(r"\d+", cell_value):
                continue

            graph_number = int(cell_value)
            graph_mapping[graph_number] = cell_index

        if TARGET_GRAPHS.issubset(graph_mapping):
            return graph_mapping

    return {}


def is_table_3100_first_part(table: Table) -> bool:
    """
    Проверяет, что Word-таблица похожа на первую часть таблицы 3100.

    Для нашей задачи важно, чтобы в таблице:
        - были графы 5 и 6;
        - были строки с туберкулезными койками для взрослых и детей.
    """
    graph_mapping = extract_graph_mapping(table)

    if not TARGET_GRAPHS.issubset(graph_mapping):
        return False

    found_indicators: set[str] = set()

    for row in table.rows:
        row_cells = [normalize_text(cell.text) for cell in row.cells]

        if not row_cells:
            continue

        indicator = get_bed_profile_indicator(row_cells[0])
        if indicator:
            found_indicators.add(indicator)

    return EXPECTED_INDICATORS.issubset(found_indicators)


def row_to_long_records(
    row_cells: list[str],
    graph_mapping: dict[int, int],
    year: int,
    row_number: str,
    indicator: str,
) -> list[dict[str, Any]]:
    """
    Преобразует одну строку таблицы 3100 в длинный формат:
    одна строка результата = одна нужная графа.
    """
    records: list[dict[str, Any]] = []

    for graph_number in sorted(TARGET_GRAPHS):
        cell_index = graph_mapping.get(graph_number)
        value = (
            parse_decimal(row_cells[cell_index])
            if cell_index is not None and len(row_cells) > cell_index
            else None
        )

        records.append(
            {
                "Показатель": indicator,
                "Уточнение": DETAIL_NAMES[graph_number],
                "Год": year,
                "Значение": value,
                "Строка": row_number,
                "Строка_норм": NORMALIZED_ROW_NUMBERS[indicator],
                "Графа": graph_number,
                "Таблица": TABLE_NUMBER,
            }
        )

    return records


def parse_docx_file(file_path: Path) -> list[dict[str, Any]]:
    """
    Парсит один DOCX-файл формы №30.

    Для каждого года ожидается 4 строки результата:
        - туберкулезные для взрослых / графа 5;
        - туберкулезные для взрослых / графа 6;
        - туберкулезные для детей / графа 5;
        - туберкулезные для детей / графа 6.
    """
    year = extract_year_from_filename(file_path)
    if year is None:
        logger.warning("Год не найден или не входит в 2016–2024: %s", file_path.name)
        return []

    logger.info("Парсим файл: %s, год: %s", file_path.name, year)

    document = Document(file_path)

    records: list[dict[str, Any]] = []
    found_pairs: set[tuple[str, int]] = set()
    table_found = False

    for table in document.tables:
        if not is_table_3100_first_part(table):
            continue

        logger.info("Найдена первая часть таблицы 3100 в файле: %s", file_path.name)
        table_found = True

        graph_mapping = extract_graph_mapping(table)

        for row in table.rows:
            row_cells = [normalize_text(cell.text) for cell in row.cells]

            if not row_cells:
                continue

            indicator = get_bed_profile_indicator(row_cells[0])
            if indicator is None:
                continue

            row_number = extract_row_number(row_cells)
            if row_number is None:
                logger.warning(
                    "Не найден номер строки для показателя '%s' в файле %s",
                    indicator,
                    file_path.name,
                )
                continue

            row_records = row_to_long_records(
                row_cells=row_cells,
                graph_mapping=graph_mapping,
                year=year,
                row_number=row_number,
                indicator=indicator,
            )

            for record in row_records:
                pair = (str(record["Показатель"]), int(record["Графа"]))

                if pair in found_pairs:
                    logger.warning(
                        "Дубль пары показатель/графа в файле %s: показатель=%s, графа=%s. "
                        "Повтор пропущен.",
                        file_path.name,
                        pair[0],
                        pair[1],
                    )
                    continue

                records.append(record)
                found_pairs.add(pair)

        # В каждом файле ожидается одна первая часть таблицы 3100.
        break

    if not table_found:
        logger.warning("Таблица 3100 не найдена в файле: %s", file_path.name)

    missing_pairs = EXPECTED_PAIRS - found_pairs
    if missing_pairs:
        logger.warning(
            "В файле %s не найдены пары показатель/графа: %s",
            file_path.name,
            sorted(missing_pairs),
        )

    expected_record_count = len(EXPECTED_PAIRS)
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
    Собирает DOCX-файлы формы 30 из сырой папки.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Не найдена папка с исходными файлами: {raw_dir}")

    files = sorted(
        file
        for file in raw_dir.glob("*.docx")
        if not file.name.startswith("~$")
    )

    if not files:
        raise FileNotFoundError(f"В папке нет DOCX-файлов: {raw_dir}")

    return files


def filter_target_years(input_files: list[Path]) -> list[Path]:
    """Оставляет годы, поддерживаемые текущей реализацией парсера."""
    filtered_files = [
        path for path in input_files
        if (year := extract_year_from_filename(path)) is not None
        and YEAR_MIN <= year <= YEAR_MAX
    ]
    return sorted(filtered_files, key=lambda path: extract_year_from_filename(path) or 0)


def get_registered_input_files() -> list[Path]:
    """Получает файлы из БД-реестра или использует локальный fallback."""
    if get_source_files is not None:
        try:
            files = get_source_files(SOURCE_KEY, years=list(range(YEAR_MIN, YEAR_MAX + 1)))
            if files:
                return files
        except Exception as exc:
            logger.warning("Не удалось получить файлы из реестра БД: %s", exc)

    return collect_docx_files(CONFIG.raw_dir)


def build_dataset(raw_dir: Path | None = None, input_files: list[Path] | None = None) -> pd.DataFrame:
    """
    Собирает итоговый датасет из всех DOCX-файлов.
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
        file_records = parse_docx_file(file_path)
        all_records.extend(file_records)

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
    if df.empty:
        raise ValueError("Нельзя валидировать пустой датафрейм.")

    expected_years = set(range(YEAR_MIN, YEAR_MAX + 1))
    actual_years = set(df["Год"].unique())

    if actual_years != expected_years:
        missing_years = sorted(expected_years - actual_years)
        extra_years = sorted(actual_years - expected_years)

        raise ValueError(
            "Проблема с годами. "
            f"Отсутствуют: {missing_years}. Лишние: {extra_years}."
        )

    expected_rows_per_year = len(EXPECTED_PAIRS)
    rows_by_year = df.groupby("Год").size()

    bad_years = rows_by_year[rows_by_year != expected_rows_per_year]
    if not bad_years.empty:
        raise ValueError(
            "По некоторым годам неверное количество строк. "
            f"Ожидалось {expected_rows_per_year} строки на год. "
            f"Проблемные годы: {bad_years.to_dict()}"
        )

    expected_pairs_df = pd.DataFrame(
        [
            {"Показатель": indicator, "Графа": graph}
            for indicator, graph in sorted(EXPECTED_PAIRS)
        ]
    )

    for year, year_df in df.groupby("Год"):
        actual_pairs_df = year_df[["Показатель", "Графа"]].drop_duplicates()

        merged = expected_pairs_df.merge(
            actual_pairs_df,
            on=["Показатель", "Графа"],
            how="left",
            indicator=True,
        )

        missing_pairs = merged[merged["_merge"] == "left_only"]
        if not missing_pairs.empty:
            raise ValueError(
                f"В году {year} отсутствуют нужные пары показатель/графа:\n"
                f"{missing_pairs[['Показатель', 'Графа']].to_string(index=False)}"
            )

    duplicate_key_columns = [
        "Показатель",
        "Уточнение",
        "Год",
        "Строка_норм",
        "Графа",
        "Таблица",
    ]
    duplicate_mask = df.duplicated(subset=duplicate_key_columns, keep=False)

    if duplicate_mask.any():
        duplicates = df.loc[
            duplicate_mask,
            [
                "Показатель",
                "Уточнение",
                "Год",
                "Строка",
                "Строка_норм",
                "Графа",
                "Таблица",
            ],
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
    """Загружает итоговый датафрейм через общий DB-слой."""
    table_name = f"{config.db_schema}.{config.db_table}"
    min_year = int(df["Год"].min())
    max_year = int(df["Год"].max())
    delete_rows_between_years(
        table_name,
        start_year=min_year,
        end_year=max_year,
        database_url=config.db_url,
    )
    load_dataframe(df, table_name, if_exists="append", database_url=config.db_url)
    logger.info("Загрузка в БД завершена успешно.")


def parse_args() -> Config:
    """Читает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Парсер таблицы 3100 из DOCX-файлов формы 30"
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

    return Config(
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

    logger.info("Старт парсинга формы 30, таблицы 3100.")

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
    Запускает полный пайплайн: парсинг, валидация, CSV и загрузка в БД.
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

    except Exception as error:  # noqa: BLE001
        logger.exception("Ошибка выполнения скрипта: %s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
