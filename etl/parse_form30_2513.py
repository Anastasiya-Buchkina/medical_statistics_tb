"""ETL: форма N30, таблица 2513.

Назначение:
    Парсит DOCX-файлы формы N30 за 2016-2024 годы и извлекает данные
    о профилактических осмотрах на туберкулез.

Источник:
    data/формы 30/*.docx

Результат:
    - processed/tb_form30_2513.csv;
    - public.tb_form30_2513 при запуске с --write-db.

Запуск:
    python3 orchestrator.py run form30_2513
    python3 orchestrator.py run form30_2513 --write-db

Особенности:
    Структура Word-таблицы немного меняется по годам, поэтому парсер
    опирается на номера строк и граф, а не только на текст заголовков.
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
    output_csv: str = "tb_form30_2513.csv"
    db_table: str = "tb_form30_2513"
    db_schema: str = "public"
    db_url: str | None = DATABASE_URL
    skip_db: bool = False
    input_files: list[Path] | None = None


CONFIG = Config()

# -----------------------------------------------------------------------------
# Константы и справочники нормализации
# -----------------------------------------------------------------------------

TABLE_NUMBER = "2513"
SOURCE_KEY = "form30_2513"
TARGET_YEARS = set(range(2016, 2025))

# Нужные строки таблицы 2513.
# Важно: используем именно номер строки из формы, а не физический порядок строк в Word.
TARGET_ROWS: set[str] = {"1", "2", "3", "4", "5"}

# Нужные комбинации строк и граф для расчета показателей.
# Строки 2 и 3 берутся по графам 3 и 5, потому что по ним считаются
# количество обследованных и выявляемость туберкулеза.
# Строки 4 и 5 берутся только по графе 3 для расчета иммунодиагностики детей.
REQUIRED_ROW_GRAPH_PAIRS: set[tuple[str, int]] = {
    ("1", 3),
    ("2", 3),
    ("2", 5),
    ("3", 3),
    ("3", 5),
    ("4", 3),
    ("5", 3),
}

# Нормализованные названия показателей для итоговой таблицы.
# Для графы 5 по строкам 2 и 3 используем одно название,
# потому что обе строки отражают число случаев выявленного туберкулеза
# разными методами обследования.
INDICATOR_NAMES: dict[tuple[str, int], str] = {
    ("1", 3): "Осмотрено пациентов всего",
    ("2", 3): "Из числа осмотренных обследовано флюорографически",
    ("2", 5): "Выявлен туберкулез",
    ("3", 3): "Из числа осмотренных обследовано бактериоскопически",
    ("3", 5): "Выявлен туберкулез",
    ("4", 3): (
        "Из числа осмотренных детей проведены иммунодиагностика с применением "
        "аллергена бактерий с 2 туберкулиновыми единицами очищенного "
        "туберкулина в стандартном разведении"
    ),
    ("5", 3): (
        "Из числа осмотренных детей проведены иммунодиагностика с применением "
        "аллергена туберкулезного рекомбинантного в стандартном разведении"
    ),
}

# Нормализованное уточнение.
# По твоему ТЗ оставляем единый вариант.
DETAIL_NAME = "Всего чел"

# Индексы ячеек в docx-таблице.
# Обычно структура таблицы 2513 такая:
# 0 — наименование;
# 1 — № строки;
# 2 — графа 3;
# 3 — графа 4;
# 4 — графа 5;
# 5 — графа 6.
GRAPH_TO_CELL_INDEX: dict[int, int] = {
    3: 2,
    5: 4,
}


OUTPUT_COLUMNS = [
    "Показатель",
    "Уточнение",
    "Год",
    "Значение",
    "Строка",
    "Графа",
    "Таблица",
]

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


def is_column_number_header_row(row_cells: list[str]) -> bool:
    """
    Определяет строку с техническими номерами граф.

    В таблице Word есть строка вида:
        1 | 2 | 3 | 4 | 5 | 6

    Это строка шапки, а не данные. Ее нельзя обрабатывать как строку №2,
    иначе номера граф ошибочно попадут в итоговый датасет как значения.
    """
    normalized_cells = [normalize_text(cell) for cell in row_cells[:6]]
    return normalized_cells == ["1", "2", "3", "4", "5", "6"]


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
    if year < 2016 or year > 2024:
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

    В таблице 2513 номер строки хранится в графе 2. Для расчета нужны
    строки 1, 2, 3, 4 и 5. Строки 1.1, 1.2, 1.3 и 6 в текущий набор
    расчетов не входят, но могут быть использованы позже для расширения.
    """
    if len(row_cells) < 2:
        return None

    row_number = normalize_text(row_cells[1])

    if not row_number:
        return None

    if not re.fullmatch(r"\d+(\.\d+)?", row_number):
        return None

    return row_number


def is_table_2513(table) -> bool:
    """
    Проверяет, что Word-таблица похожа на таблицу 2513.

    В исходных DOCX-файлах номер таблицы не всегда удобно извлечь как
    отдельное поле, поэтому используем сигнатуру по обязательным строкам
    и ключевым словам:
        - строка 1: осмотрено пациентов;
        - строки 2 и 3: флюорографическое и бактериоскопическое обследование;
        - строки 4 и 5: иммунодиагностика.
    """
    found_row_numbers: set[str] = set()
    found_keywords: set[str] = set()

    for row in table.rows:
        row_cells = [normalize_text(cell.text) for cell in row.cells]

        if is_column_number_header_row(row_cells):
            continue

        row_number = extract_row_number(row_cells)
        if row_number:
            found_row_numbers.add(row_number)

        row_text = " ".join(row_cells).lower()

        if "осмотрено пациентов" in row_text:
            found_keywords.add("patients_examined")

        if "флюорограф" in row_text:
            found_keywords.add("fluorography")

        if "бактериоскоп" in row_text:
            found_keywords.add("bacterioscopy")

        if "туберкулин" in row_text:
            found_keywords.add("tuberculin")

        if "рекомбинант" in row_text:
            found_keywords.add("recombinant")

    required_rows_present = TARGET_ROWS.issubset(found_row_numbers)
    required_keywords_present = {
        "patients_examined",
        "fluorography",
        "bacterioscopy",
        "tuberculin",
        "recombinant",
    }.issubset(found_keywords)

    return required_rows_present and required_keywords_present


def row_to_long_records(
    row_cells: list[str],
    year: int,
    row_number: str,
) -> list[dict[str, Any]]:
    """
    Преобразует одну строку таблицы 2513 в длинный формат:
    одна строка результата = одна нужная графа.
    """
    records: list[dict[str, Any]] = []

    for graph_number, cell_index in GRAPH_TO_CELL_INDEX.items():
        pair = (row_number, graph_number)

        if pair not in REQUIRED_ROW_GRAPH_PAIRS:
            continue

        value = parse_decimal(row_cells[cell_index]) if len(row_cells) > cell_index else None

        records.append(
            {
                "Показатель": INDICATOR_NAMES[pair],
                "Уточнение": DETAIL_NAME,
                "Год": year,
                "Значение": value,
                "Строка": row_number,
                "Графа": graph_number,
                "Таблица": TABLE_NUMBER,
            }
        )

    return records


def parse_docx_file(file_path: Path) -> list[dict[str, Any]]:
    """
    Парсит один DOCX-файл формы №30.

    Для каждого года ожидается 7 строк результата:
    - строка 1 / графа 3;
    - строка 2 / графы 3 и 5;
    - строка 3 / графы 3 и 5;
    - строка 4 / графа 3;
    - строка 5 / графа 3.
    """
    year = extract_year_from_filename(file_path)
    if year is None:
        logger.warning("Год не найден или не входит в 2016–2024: %s", file_path.name)
        return []

    logger.info("Парсим файл: %s, год: %s", file_path.name, year)

    document = Document(file_path)

    records: list[dict[str, Any]] = []
    found_pairs: set[tuple[str, int]] = set()

    for table in document.tables:
        if not is_table_2513(table):
            continue

        logger.info("Найдена таблица 2513 в файле: %s", file_path.name)

        for row in table.rows:
            row_cells = [normalize_text(cell.text) for cell in row.cells]

            if is_column_number_header_row(row_cells):
                continue

            row_number = extract_row_number(row_cells)

            if row_number not in TARGET_ROWS:
                continue

            row_records = row_to_long_records(
                row_cells=row_cells,
                year=year,
                row_number=row_number,
            )

            for record in row_records:
                pair = (str(record["Строка"]), int(record["Графа"]))

                if pair in found_pairs:
                    logger.warning(
                        "Дубль пары строка/графа в файле %s: строка=%s, графа=%s. "
                        "Повтор пропущен.",
                        file_path.name,
                        pair[0],
                        pair[1],
                    )
                    continue

                records.append(record)
                found_pairs.add(pair)

        # В каждом файле ожидается одна таблица 2513. После ее обработки
        # прекращаем обход остальных таблиц документа.
        break

    missing_pairs = REQUIRED_ROW_GRAPH_PAIRS - found_pairs
    if missing_pairs:
        logger.warning(
            "В файле %s не найдены пары строка/графа: %s",
            file_path.name,
            sorted(missing_pairs),
        )

    expected_record_count = len(REQUIRED_ROW_GRAPH_PAIRS)
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

    # Исключаем временные файлы Word.
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
        file_records = parse_docx_file(file_path)
        all_records.extend(file_records)

    df = pd.DataFrame(all_records)

    if df.empty:
        raise ValueError("Итоговый датафрейм пустой. Данные не распарсились.")

    df = df.sort_values(
        by=["Год", "Строка", "Графа"],
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

    expected_years = set(range(2016, 2025))
    actual_years = set(df["Год"].unique())

    if actual_years != expected_years:
        missing_years = sorted(expected_years - actual_years)
        extra_years = sorted(actual_years - expected_years)

        raise ValueError(
            "Проблема с годами. "
            f"Отсутствуют: {missing_years}. Лишние: {extra_years}."
        )

    expected_rows_per_year = len(REQUIRED_ROW_GRAPH_PAIRS)
    rows_by_year = df.groupby("Год").size()

    bad_years = rows_by_year[rows_by_year != expected_rows_per_year]
    if not bad_years.empty:
        raise ValueError(
            "По некоторым годам неверное количество строк. "
            f"Ожидалось {expected_rows_per_year} строк на год. "
            f"Проблемные годы: {bad_years.to_dict()}"
        )

    expected_pairs_df = pd.DataFrame(
        [
            {"Строка": row_number, "Графа": graph_number}
            for row_number, graph_number in sorted(REQUIRED_ROW_GRAPH_PAIRS)
        ]
    )

    for year, year_df in df.groupby("Год"):
        actual_pairs_df = year_df[["Строка", "Графа"]].drop_duplicates()

        merged = expected_pairs_df.merge(
            actual_pairs_df,
            on=["Строка", "Графа"],
            how="left",
            indicator=True,
        )

        missing_pairs = merged[merged["_merge"] == "left_only"]
        if not missing_pairs.empty:
            raise ValueError(
                f"В году {year} отсутствуют нужные пары строка/графа:\n"
                f"{missing_pairs[['Строка', 'Графа']].to_string(index=False)}"
            )

    duplicate_mask = df.duplicated(
        subset=["Показатель", "Уточнение", "Год", "Строка", "Графа", "Таблица"],
        keep=False,
    )

    if duplicate_mask.any():
        duplicates = df.loc[
            duplicate_mask,
            ["Показатель", "Уточнение", "Год", "Строка", "Графа", "Таблица"],
        ]

        raise ValueError(
            "Найдены дубли по бизнес-ключу:\n"
            f"{duplicates.to_string(index=False)}"
        )

    null_values = df[df["Значение"].isna()]
    if not null_values.empty:
        logger.warning(
            "Найдены пустые значения в колонке 'Значение'. "
            "Строки будут сохранены как NULL, потому что в исходной форме значение отсутствует:\n%s",
            null_values.to_string(index=False),
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
        description="Парсер таблицы 2513 из DOCX-файлов формы 30"
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

    logger.info("Старт парсинга формы 30, таблицы 2513.")

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

    except Exception as error:
        logger.exception("Ошибка выполнения скрипта: %s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
