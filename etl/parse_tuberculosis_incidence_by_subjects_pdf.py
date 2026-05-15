"""ETL: социально значимые заболевания, PDF-источники.

Назначение:
    Парсит таблицу 1.2 из PDF-сборников за 2021-2024 годы и преобразует
    показатели по субъектам РФ в long-формат.

Источник:
    data/Соц_заболевания/СЗ_2021.pdf ... СЗ_2024.pdf

Результат:
    - processed/tuberculosis_incidence_by_subjects_pdf_long.csv;
    - public.tuberculosis_incidence_by_subjects_pdf при запуске с --write-db.

Запуск:
    python3 orchestrator.py run social_diseases_pdf
    python3 orchestrator.py run social_diseases_pdf --write-db

Особенности:
    Поле source_file сохраняется намеренно: DOCX/PDF-таблицы являются
    промежуточным слоем перед объединением в итоговую витрину субъектов.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Final

import pdfplumber

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATABASE_URL, PROCESSED_DIR, PROJECT_ROOT, SOCIAL_DISEASES_DIR
from db_manager import delete_rows_for_years, execute_many

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
# Конфигурация
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """Настройки запуска скрипта."""

    base_dir: Path
    pdf_dir: Path
    output_csv: Path
    database_url: str | None
    target_table: str
    files_to_process: dict[str, int]
    expected_subject_rows_per_year: int = 95
    skip_db: bool = False
    input_files: list[Path] | None = None


BASE_DIR: Final[Path] = PROJECT_ROOT

CONFIG: Final[Config] = Config(
    base_dir=BASE_DIR,
    pdf_dir=SOCIAL_DISEASES_DIR,
    output_csv=PROCESSED_DIR / (
        "tuberculosis_incidence_by_subjects_pdf_long.csv"
    ),
    database_url=DATABASE_URL,
    target_table="public.tuberculosis_incidence_by_subjects_pdf",
    files_to_process={
        "СЗ_2021.pdf": 2021,
        "СЗ_2022.pdf": 2022,
        "СЗ_2023.pdf": 2023,
        "СЗ_2024.pdf": 2024,
    },
)

SOURCE_KEY: Final[str] = "social_diseases_pdf"


# -----------------------------------------------------------------------------
# КОНСТАНТЫ ДЛЯ ИТОГОВОГО LONG-FORMAT
# -----------------------------------------------------------------------------

INDICATOR_NEW_CASES: Final[str] = (
    "Пациенты с впервые в жизни установленным диагнозом активного туберкулеза"
)

INDICATOR_FOLLOWUP: Final[str] = (
    "Пациенты состоящие под диспансерным наблюдением на конец года"
)

DETAIL_ABS: Final[str] = "абсолютные числа"
DETAIL_RATE: Final[str] = "на 100000 соот.населения"

TARGET_TABLE_CONSTRAINT: Final[str] = "uq_tuberculosis_incidence_by_subjects_pdf"

VALID_VALUE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(\d+([\.,]\d+)?|\d{1,3}( \d{3})+|-|…|\.\.\.)$"
)

SERVICE_ROW_MARKERS: Final[tuple[str, ...]] = (
    "субъекты федерации",
    "число пациентов",
    "установленным диагнозом",
    "диспансерным наблюдением",
    "абс. число",
    "на 100",
    "соот.населения",
)


# -----------------------------------------------------------------------------
# МОДЕЛЬ ДАННЫХ
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ParsedRow:
    """Одна строка итоговой таблицы в длинном формате."""

    subject: str
    indicator: str
    detail: str
    year: int
    value: Decimal | None
    source_file: str

    def as_dict(self) -> dict[str, object]:
        """Преобразует строку в словарь для записи в CSV."""
        return {
            "Субъект": self.subject,
            "Показатель": self.indicator,
            "Уточнение": self.detail,
            "Год": self.year,
            "Значение": self.value,
            "source_file": self.source_file,
        }


# -----------------------------------------------------------------------------
# ОЧИСТКА И НОРМАЛИЗАЦИЯ
# -----------------------------------------------------------------------------

def clean_text(value: str | None) -> str:
    """
    Очищает текст, извлеченный из PDF.

    Что делаем:
        - заменяем неразрывные пробелы обычными;
        - убираем переносы строк внутри ячеек;
        - схлопываем множественные пробелы.
    """
    if value is None:
        return ""

    value = str(value)
    value = value.replace("\xa0", " ")
    value = value.replace("\n", " ")
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def normalize_subject(subject: str) -> str:
    """
    Нормализует технические варианты названий субъектов из PDF.

    Важно:
        здесь не приводим названия к DOCX-варианту. Это делается позже
        при сборке итоговой аналитической таблицы public.tb_incidence_subjects.
        В PDF-таблице сохраняем аккуратный вариант, полученный из PDF.
    """
    subject = clean_text(subject)

    replacements = {
        "город Санкт - Петербург": "город Санкт-Петербург",
        "Ямало-Hенецкий АО": "Ямало-Ненецкий АО",
        "Ямало-Ненецкий автономный округ": "Ямало-Ненецкий АО",
        "Ханты-Мансийский автономный округ": "Ханты-Мансийский АО",
        "Главное медицинское управление Управления делами Президента Российской Федерации": (
            "Главное медицинское управление Управления делами Президента Российской Федерации"
        ),
    }

    return replacements.get(subject, subject)


def parse_number(value: str | None) -> Decimal | None:
    """
    Преобразует значение из PDF в Decimal.

    Примеры:
        "45 420" -> Decimal("45420")
        "31,1"   -> Decimal("31.1")
        "-"      -> None
        ""       -> None
    """
    value = clean_text(value)

    if value in {"", "-", "…", "..."}:
        return None

    value = value.replace(" ", "")
    value = value.replace(",", ".")

    return Decimal(value)


def extract_year_from_filename(file_path: Path) -> int | None:
    """Достает год из имени файла."""
    match = re.search(r"(20\d{2})", file_path.name)
    return int(match.group(1)) if match else None


def is_service_row(row: list[str]) -> bool:
    """Определяет служебные строки таблицы: заголовки, подписи колонок, пустые строки."""
    row_text = " ".join(clean_text(cell) for cell in row)
    row_text_lower = row_text.lower()

    if not row_text:
        return True

    return any(marker in row_text_lower for marker in SERVICE_ROW_MARKERS)


# -----------------------------------------------------------------------------
# ИЗВЛЕЧЕНИЕ ТАБЛИЦЫ 1.2 ИЗ PDF
# -----------------------------------------------------------------------------

def extract_tables_from_pdf(pdf_path: Path) -> list[list[list[str]]]:
    """
    Извлекает все таблицы из PDF через pdfplumber.

    OCR не используется: исходные PDF содержат текстовый слой.
    """
    tables: list[list[list[str]]] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            raw_tables = page.extract_tables()

            if not raw_tables:
                continue

            for table_idx, raw_table in enumerate(raw_tables):
                if not raw_table:
                    continue

                cleaned_table = [
                    [clean_text(cell) for cell in row]
                    for row in raw_table
                    if row and any(clean_text(cell) for cell in row)
                ]

                if not cleaned_table:
                    continue

                tables.append(cleaned_table)

                log.info(
                    "[%s] стр.%s табл.%s: %s строк",
                    pdf_path.name,
                    page_num,
                    table_idx,
                    len(cleaned_table),
                )

    return tables


def looks_like_target_table(table: list[list[str]]) -> bool:
    """
    Проверяет, похожа ли таблица на таблицу 1.2.

    Нужная таблица содержит:
        - субъекты федерации;
        - пациентов с впервые установленным диагнозом;
        - пациентов под диспансерным наблюдением.
    """
    table_text = " ".join(
        clean_text(cell).lower()
        for row in table
        for cell in row
    )

    required_markers = (
        "субъекты федерации",
        "число пациентов",
        "впервые",
        "диспансерным наблюдением",
    )

    return all(marker in table_text for marker in required_markers)


def collect_target_table_rows(pdf_path: Path) -> list[list[str]]:
    """
    Собирает строки таблицы 1.2.

    Таблица в PDF обычно разбита на две страницы, поэтому собираем
    все найденные фрагменты, похожие на нужную таблицу.
    """
    all_tables = extract_tables_from_pdf(pdf_path)

    target_rows: list[list[str]] = []

    for table in all_tables:
        if looks_like_target_table(table):
            target_rows.extend(table)

    if not target_rows:
        raise ValueError(f"Не удалось найти таблицу 1.2 в файле {pdf_path.name}")

    return target_rows


# -----------------------------------------------------------------------------
# Парсинг СТРОК ТАБЛИЦЫ
# -----------------------------------------------------------------------------

def normalize_table_row(row: list[str]) -> list[str]:
    """Приводит строку таблицы к списку очищенных ячеек."""
    return [clean_text(cell) for cell in row]


def is_data_row(row: list[str]) -> bool:
    """
    Проверяет, является ли строка строкой данных.

    Специальный случай:
        строка Главного медицинского управления может содержать только прочерки.
        Ее нужно сохранить, чтобы итоговое число субъектов/агрегатов было 95.
    """
    if not row:
        return False

    first_cell = clean_text(row[0])

    if not first_cell:
        return False

    if is_service_row(row):
        return False

    if "Главное медицинское управление" in first_cell:
        return True

    value_cells = [clean_text(cell) for cell in row[1:]]

    # После названия субъекта ожидаем 8 значений:
    # 2 года × 4 метрики.
    if len(value_cells) < 8:
        return False

    valid_cells_count = sum(
        1
        for cell in value_cells
        if cell == "" or VALID_VALUE_PATTERN.match(cell)
    )

    return valid_cells_count >= 8


def fix_multiline_subject_rows(rows: list[list[str]]) -> list[list[str]]:
    """
    Склеивает строки, где название субъекта перенеслось на несколько строк.

    Примеры длинных названий:
        - Архангельская область без автономного округа;
        - Тюменская область без автономного округа;
        - Главное медицинское управление Управления делами Президента РФ.
    """
    fixed_rows: list[list[str]] = []
    pending_subject_parts: list[str] = []

    for raw_row in rows:
        row = normalize_table_row(raw_row)

        if is_service_row(row):
            continue

        first_cell = clean_text(row[0]) if row else ""
        other_cells = row[1:]

        has_numbers = any(
            re.search(r"\d", clean_text(cell))
            for cell in other_cells
        )

        has_dashes = any(
            clean_text(cell) in {"-", "…", "..."}
            for cell in other_cells
        )

        if first_cell and not has_numbers and not has_dashes:
            pending_subject_parts.append(first_cell)
            continue

        if first_cell and (has_numbers or has_dashes):
            if pending_subject_parts:
                row[0] = clean_text(" ".join(pending_subject_parts + [first_cell]))
                pending_subject_parts = []

            fixed_rows.append(row)

    if pending_subject_parts:
        log.warning(
            "Остался неиспользованный фрагмент названия субъекта: %s",
            " ".join(pending_subject_parts),
        )

    return fixed_rows


def row_to_long_format(
    row: list[str],
    target_year: int,
    source_file: str,
) -> list[ParsedRow]:
    """
    Преобразует строку широкой PDF-таблицы в 4 строки long-format.

    Структура строки таблицы 1.2:
        0  Субъект
        1  Новые случаи, абсолютное число, предыдущий год
        2  Новые случаи, абсолютное число, целевой год
        3  Новые случаи, на 100000, предыдущий год
        4  Новые случаи, на 100000, целевой год
        5  Контингенты, абсолютное число, предыдущий год
        6  Контингенты, абсолютное число, целевой год
        7  Контингенты, на 100000, предыдущий год
        8  Контингенты, на 100000, целевой год

    Для загрузки берем только целевой год: колонки 2, 4, 6 и 8.
    """
    if len(row) < 9:
        raise ValueError(f"Ожидалось минимум 9 колонок, получено {len(row)}: {row}")

    subject = normalize_subject(row[0])

    return [
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_NEW_CASES,
            detail=DETAIL_ABS,
            year=target_year,
            value=parse_number(row[2]),
            source_file=source_file,
        ),
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_NEW_CASES,
            detail=DETAIL_RATE,
            year=target_year,
            value=parse_number(row[4]),
            source_file=source_file,
        ),
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_FOLLOWUP,
            detail=DETAIL_ABS,
            year=target_year,
            value=parse_number(row[6]),
            source_file=source_file,
        ),
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_FOLLOWUP,
            detail=DETAIL_RATE,
            year=target_year,
            value=parse_number(row[8]),
            source_file=source_file,
        ),
    ]


def parse_pdf_file(pdf_path: Path, target_year: int) -> list[ParsedRow]:
    """Парсит один PDF и возвращает строки long-format только за целевой год."""
    log.info("Обрабатываю файл: %s, целевой год: %s", pdf_path.name, target_year)

    raw_rows = collect_target_table_rows(pdf_path)
    fixed_rows = fix_multiline_subject_rows(raw_rows)
    data_rows = [row for row in fixed_rows if is_data_row(row)]

    if not data_rows:
        raise ValueError(f"Не найдены строки данных в файле {pdf_path.name}")

    log.info("[%s] найдено строк субъектов/агрегатов: %s", pdf_path.name, len(data_rows))

    if len(data_rows) != CONFIG.expected_subject_rows_per_year:
        log.warning(
            "[%s] ожидалось %s строк субъектов/агрегатов, найдено %s",
            pdf_path.name,
            CONFIG.expected_subject_rows_per_year,
            len(data_rows),
        )

    result: list[ParsedRow] = []

    for row in data_rows:
        try:
            result.extend(
                row_to_long_format(
                    row=row,
                    target_year=target_year,
                    source_file=pdf_path.name,
                )
            )
        except Exception as exc:
            raise ValueError(
                f"Ошибка парсинга строки в файле {pdf_path.name}: {row}"
            ) from exc

    return result


# -----------------------------------------------------------------------------
# ДЕДУПЛИКАЦИЯ И ПРОВЕРКИ
# -----------------------------------------------------------------------------

def deduplicate_rows(rows: list[ParsedRow]) -> list[ParsedRow]:
    """Удаляет дубли по бизнес-ключу: субъект + показатель + уточнение + год."""
    seen: set[tuple[str, str, str, int]] = set()
    result: list[ParsedRow] = []

    for row in rows:
        key = (row.subject, row.indicator, row.detail, row.year)

        if key in seen:
            log.warning("Дубль пропущен: %s", key)
            continue

        seen.add(key)
        result.append(row)

    return result


def validate_result(rows: list[ParsedRow], config: Config = CONFIG) -> None:
    """Проверяет ожидаемый объем итогового набора."""
    expected_rows_per_year = config.expected_subject_rows_per_year * 4
    expected_total_rows = expected_rows_per_year * len(config.files_to_process)
    years = sorted({row.year for row in rows})

    log.info("Годы в итоговом наборе: %s", years)

    for year in years:
        year_count = sum(1 for row in rows if row.year == year)
        log.info("Год %s: %s строк", year, year_count)

        if year_count != expected_rows_per_year:
            log.warning(
                "Для года %s ожидалось %s строк, получено %s",
                year,
                expected_rows_per_year,
                year_count,
            )

    if len(rows) != expected_total_rows:
        log.warning(
            "Всего ожидалось %s строк, получено %s",
            expected_total_rows,
            len(rows),
        )
    else:
        log.info("Итоговая проверка строк пройдена: %s строк", len(rows))


def print_federal_control(rows: list[ParsedRow]) -> None:
    """Выводит контрольные значения по Российской Федерации."""
    log.info("Контрольные значения по Российской Федерации:")

    federal_rows = [row for row in rows if row.subject == "Российская Федерация"]

    for row in sorted(
        federal_rows,
        key=lambda x: (x.year, x.indicator, x.detail),
    ):
        log.info(
            "%s | %s | %s | %s",
            row.year,
            row.indicator,
            row.detail,
            row.value,
        )


# -----------------------------------------------------------------------------
# Сохранение CSV
# -----------------------------------------------------------------------------

def save_to_csv(rows: list[ParsedRow], output_path: Path) -> None:
    """Сохраняет результат в CSV с BOM для корректного открытия в Excel."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "Субъект",
        "Показатель",
        "Уточнение",
        "Год",
        "Значение",
        "source_file",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(row.as_dict())

    log.info("CSV сохранен: %s", output_path)


# -----------------------------------------------------------------------------
# ЗАГРУЗКА В POSTGRESQL
# -----------------------------------------------------------------------------


def load_to_postgres(rows: list[ParsedRow], config: Config = CONFIG) -> int:
    """
    Загружает строки в PostgreSQL.

    Используется ON CONFLICT по бизнес-ключу, чтобы повторный запуск
    не создавал дубли.
    """
    if not config.database_url:
        raise ValueError("DATABASE_URL не найден. Проверь файл .env в корне проекта.")

    years = sorted(set(config.files_to_process.values()))
    delete_rows_for_years(
        config.target_table,
        years,
        database_url=config.database_url,
    )

    insert_sql = f"""
        INSERT INTO {config.target_table}
            ("Субъект", "Показатель", "Уточнение", "Год", "Значение", source_file)
        VALUES
            (:subject, :indicator, :detail, :year, :value, :source_file)
        ON CONFLICT ("Субъект", "Показатель", "Уточнение", "Год")
        DO UPDATE SET
            "Значение" = EXCLUDED."Значение",
            source_file = EXCLUDED.source_file;
    """

    values = [
        {
            "subject": row.subject,
            "indicator": row.indicator,
            "detail": row.detail,
            "year": row.year,
            "value": row.value,
            "source_file": row.source_file,
        }
        for row in rows
    ]

    execute_many(insert_sql, values, database_url=config.database_url)

    log.info("Загружено строк в PostgreSQL: %s", len(values))

    return len(values)


# -----------------------------------------------------------------------------
# ПРОВЕРКА ОКРУЖЕНИЯ
# -----------------------------------------------------------------------------

def validate_environment(config: Config = CONFIG) -> None:
    """Проверяет наличие папки с PDF, исходных файлов и DATABASE_URL."""
    if not config.pdf_dir.exists():
        raise FileNotFoundError(f"Папка с PDF не найдена: {config.pdf_dir}")

    missing_files = []

    for file_name in config.files_to_process:
        file_path = config.pdf_dir / file_name

        if not file_path.exists():
            missing_files.append(str(file_path))

    if missing_files:
        missing_text = "\n".join(missing_files)
        raise FileNotFoundError(f"Не найдены PDF-файлы:\n{missing_text}")

    if not config.skip_db and not config.database_url:
        raise ValueError("DATABASE_URL не найден. Добавь строку подключения в .env.")

    log.info("Проверка окружения пройдена")


def filter_target_files(input_files: list[Path], config: Config = CONFIG) -> list[Path]:
    """Оставляет PDF-файлы за годы, поддерживаемые текущим парсером."""
    target_years = set(config.files_to_process.values())
    return sorted(
        [
            path for path in input_files
            if path.suffix.lower() == ".pdf"
            and (year := extract_year_from_filename(path)) in target_years
        ],
        key=lambda path: extract_year_from_filename(path) or 0,
    )


def get_registered_input_files(config: Config = CONFIG) -> list[Path]:
    """Получает файлы из БД-реестра или использует локальный fallback."""
    if get_source_files is not None:
        try:
            files = get_source_files(SOURCE_KEY, years=sorted(config.files_to_process.values()))
            if files:
                return files
        except Exception as exc:
            log.warning("Не удалось получить файлы из реестра БД: %s", exc)

    return [config.pdf_dir / file_name for file_name in config.files_to_process]


def build_files_to_process(input_files: list[Path], config: Config = CONFIG) -> dict[Path, int]:
    """Строит маппинг путь -> целевой год."""
    result: dict[Path, int] = {}

    for path in filter_target_files(input_files, config):
        year = config.files_to_process.get(path.name) or extract_year_from_filename(path)
        if year is not None:
            result[path] = year

    return dict(sorted(result.items(), key=lambda item: item[1]))


def parse_args() -> Config:
    """Читает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Парсер PDF-таблицы 1.2 по туберкулезу из соцзаболеваний"
    )
    parser.add_argument("input_files", nargs="*", type=Path)
    parser.add_argument("--db-url", default=None)
    parser.add_argument("--output-file", type=Path, default=CONFIG.output_csv)
    parser.add_argument("--skip-db", action="store_true")

    args = parser.parse_args()
    db_url = args.db_url or DATABASE_URL
    if not args.skip_db and not db_url:
        raise ValueError(
            "Не задана строка подключения. "
            "Передайте --db-url, задайте DATABASE_URL в .env или используйте --skip-db."
        )

    input_files = filter_target_files(args.input_files) if args.input_files else None
    files_to_process = (
        {path.name: extract_year_from_filename(path) for path in input_files}
        if input_files
        else CONFIG.files_to_process
    )

    return Config(
        base_dir=CONFIG.base_dir,
        pdf_dir=CONFIG.pdf_dir,
        output_csv=args.output_file,
        database_url=db_url,
        target_table=CONFIG.target_table,
        files_to_process={name: year for name, year in files_to_process.items() if year is not None},
        expected_subject_rows_per_year=CONFIG.expected_subject_rows_per_year,
        skip_db=args.skip_db,
        input_files=input_files,
    )


def run(
    input_files: list[Path] | None = None,
    *,
    skip_db: bool = False,
    db_url: str | None = None,
    output_file: Path = CONFIG.output_csv,
) -> list[ParsedRow]:
    """Запускает PDF-парсер из оркестратора или напрямую."""
    config = Config(
        base_dir=CONFIG.base_dir,
        pdf_dir=CONFIG.pdf_dir,
        output_csv=output_file,
        database_url=db_url or DATABASE_URL,
        target_table=CONFIG.target_table,
        files_to_process=CONFIG.files_to_process,
        expected_subject_rows_per_year=CONFIG.expected_subject_rows_per_year,
        skip_db=skip_db,
        input_files=input_files,
    )

    validate_environment(config)

    resolved_input_files = input_files or get_registered_input_files(config)
    files_to_process = build_files_to_process(resolved_input_files, config)
    config = Config(
        base_dir=config.base_dir,
        pdf_dir=config.pdf_dir,
        output_csv=config.output_csv,
        database_url=config.database_url,
        target_table=config.target_table,
        files_to_process={path.name: year for path, year in files_to_process.items()},
        expected_subject_rows_per_year=config.expected_subject_rows_per_year,
        skip_db=config.skip_db,
        input_files=list(files_to_process),
    )

    all_rows: list[ParsedRow] = []
    for pdf_path, target_year in files_to_process.items():
        parsed_rows = parse_pdf_file(pdf_path, target_year)
        log.info("[%s] получено строк long-format: %s", pdf_path.name, len(parsed_rows))
        all_rows.extend(parsed_rows)

    unique_rows = deduplicate_rows(all_rows)

    validate_result(unique_rows, config)
    print_federal_control(unique_rows)
    save_to_csv(unique_rows, config.output_csv)

    if config.skip_db:
        log.info("Режим --skip-db: загрузка в PostgreSQL пропущена")
        return unique_rows

    load_to_postgres(unique_rows, config)
    log.info("Готово. Скрипт успешно завершен.")
    return unique_rows


# -----------------------------------------------------------------------------
# ОСНОВНОЙ СЦЕНАРИЙ
# -----------------------------------------------------------------------------

def main() -> int:
    """
    Основной сценарий:
        1. Проверить окружение.
        2. Распарсить PDF-файлы.
        3. Удалить дубли в памяти.
        4. Проверить ожидаемый объем данных.
        5. Сохранить контрольный CSV.
        6. Загрузить данные в PostgreSQL.
    """
    try:
        config = parse_args()
        run(
            input_files=config.input_files,
            skip_db=config.skip_db,
            db_url=config.database_url,
            output_file=config.output_csv,
        )
        return 0

    except Exception as exc:
        log.exception("Скрипт завершился с ошибкой: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
