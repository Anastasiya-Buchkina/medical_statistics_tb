#!/usr/bin/env python3
"""ETL: социально значимые заболевания, DOCX-источники.

Назначение:
    Парсит таблицу 1.2 из DOCX-сборников за 2016-2020 годы и преобразует
    показатели по субъектам РФ в long-формат.

Источник:
    data/Соц_заболевания/СЗ_2016.docx ... СЗ_2020.docx

Результат:
    - processed/tuberculosis_incidence_by_subjects_docx_long.csv;
    - public.tuberculosis_incidence_by_subjects_docx при запуске с --write-db.

Запуск:
    python3 orchestrator.py run social_diseases_docx
    python3 orchestrator.py run social_diseases_docx --write-db

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
from typing import Final, Iterable

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATABASE_URL, PROCESSED_DIR, PROJECT_ROOT, SOCIAL_DISEASES_DIR
from db_manager import delete_rows_for_years, execute_many

try:
    from etl.source_registry import get_source_files
except ImportError:
    get_source_files = None

try:
    from docx import Document
except ImportError as exc:
    raise ImportError(
        "Не установлен пакет python-docx. Установи зависимости из requirements.txt."
    ) from exc

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
    raw_dir: Path
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
    raw_dir=SOCIAL_DISEASES_DIR,
    output_csv=PROCESSED_DIR / (
        "tuberculosis_incidence_by_subjects_docx_long.csv"
    ),
    database_url=DATABASE_URL,
    target_table="public.tuberculosis_incidence_by_subjects_docx",
    files_to_process={
        "СЗ_2016.docx": 2016,
        "СЗ_2017.docx": 2017,
        "СЗ_2018.docx": 2018,
        "СЗ_2019.docx": 2019,
        "СЗ_2020.docx": 2020,
    },
)

SOURCE_KEY: Final[str] = "social_diseases_docx"

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

TARGET_TABLE_CONSTRAINT: Final[str] = "uq_tuberculosis_incidence_by_subjects_docx"

VALUE_TOKEN_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:-|…|\.\.\.|[0-9]+(?:[.,][0-9]+)?)$"
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
    Очищает текст, извлеченный из DOCX.

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
    Нормализует названия субъектов из DOCX.

    Для DOCX-источников оставляем исторически используемый вариант названий,
    потому что он выбран как базовый при сборке итоговой таблицы.
    """
    subject = clean_text(subject)

    replacements = {
        "Главное медицинское управление Управления делами Президента Российской Федерации": (
            "Управление делами Президента Российской Федерации"
        ),
    }

    return replacements.get(subject, subject)


def is_value_token(value: str) -> bool:
    """Проверяет, похоже ли значение на число или допустимый пропуск."""
    return bool(VALUE_TOKEN_RE.match(clean_text(value)))


def parse_number(value: str | None) -> Decimal | None:
    """
    Преобразует значение из DOCX в Decimal.

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

# -----------------------------------------------------------------------------
# ИЗВЛЕЧЕНИЕ ТАБЛИЦЫ 1.2 ИЗ DOCX
# -----------------------------------------------------------------------------

def extract_all_tables(docx_path: Path) -> list[list[list[str]]]:
    """Извлекает все таблицы из DOCX в виде списка строк и ячеек."""
    document = Document(str(docx_path))
    tables_data: list[list[list[str]]] = []

    for table in document.tables:
        table_rows: list[list[str]] = []

        for row in table.rows:
            cells = [clean_text(cell.text) for cell in row.cells]
            table_rows.append(cells)

        tables_data.append(table_rows)

    return tables_data


def is_target_tb_table(table_rows: list[list[str]], target_year: int) -> bool:
    """
    Определяет, является ли таблица целевой таблицей 1.2 по туберкулезу.

    Проверяем не номер таблицы, а набор устойчивых маркеров внутри таблицы,
    потому что формат DOCX может отличаться между годами.
    """
    previous_year = target_year - 1
    flat_text = " ".join(" ".join(row) for row in table_rows).upper()

    required_markers = (
        "СУБЪЕКТЫ",
        "ДИСПАНСЕР",
        str(previous_year),
        str(target_year),
        "РОССИЙСКАЯ ФЕДЕРАЦИЯ",
    )

    return all(marker in flat_text for marker in required_markers)


def find_target_table(docx_path: Path, target_year: int) -> list[list[str]]:
    """Находит в документе целевую таблицу 1.2."""
    tables = extract_all_tables(docx_path)

    for table_rows in tables:
        if is_target_tb_table(table_rows, target_year=target_year):
            return table_rows

    raise ValueError(
        f"Не удалось найти таблицу 1.2 по туберкулезу в файле {docx_path.name}"
    )

# -----------------------------------------------------------------------------
# Парсинг СТРОК ТАБЛИЦЫ
# -----------------------------------------------------------------------------

def is_data_row(cells: list[str]) -> bool:
    """Проверяет, похожа ли строка таблицы на строку данных по субъекту."""
    if len(cells) < 9:
        return False

    subject = clean_text(cells[0])
    values = cells[1:9]

    if not subject:
        return False

    if subject.upper() in {"СУБЪЕКТЫ ФЕДЕРАЦИИ", "СУБЪЕКТЫ  ФЕДЕРАЦИИ"}:
        return False

    return all(is_value_token(value) for value in values)


def row_to_long_format(
    subject: str,
    values: list[str],
    target_year: int,
    source_file: str,
) -> list[ParsedRow]:
    """
    Преобразует строку широкой DOCX-таблицы в 4 строки long-format.

    Структура значений после названия субъекта:
        0  Новые случаи, абсолютное число, предыдущий год
        1  Новые случаи, абсолютное число, целевой год
        2  Новые случаи, на 100000, предыдущий год
        3  Новые случаи, на 100000, целевой год
        4  Контингенты, абсолютное число, предыдущий год
        5  Контингенты, абсолютное число, целевой год
        6  Контингенты, на 100000, предыдущий год
        7  Контингенты, на 100000, целевой год

    Для загрузки берем только целевой год: индексы 1, 3, 5 и 7.
    """
    subject = normalize_subject(subject)

    return [
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_NEW_CASES,
            detail=DETAIL_ABS,
            year=target_year,
            value=parse_number(values[1]),
            source_file=source_file,
        ),
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_NEW_CASES,
            detail=DETAIL_RATE,
            year=target_year,
            value=parse_number(values[3]),
            source_file=source_file,
        ),
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_FOLLOWUP,
            detail=DETAIL_ABS,
            year=target_year,
            value=parse_number(values[5]),
            source_file=source_file,
        ),
        ParsedRow(
            subject=subject,
            indicator=INDICATOR_FOLLOWUP,
            detail=DETAIL_RATE,
            year=target_year,
            value=parse_number(values[7]),
            source_file=source_file,
        ),
    ]


def parse_docx_file(docx_path: Path, target_year: int) -> list[ParsedRow]:
    """Парсит один DOCX и возвращает строки long-format только за целевой год."""
    log.info("Обрабатываю файл: %s, целевой год: %s", docx_path.name, target_year)

    table_rows = find_target_table(docx_path, target_year=target_year)
    source_file = docx_path.name
    result: list[ParsedRow] = []

    for row in table_rows:
        cells = row[:9]

        if not is_data_row(cells):
            continue

        result.extend(
            row_to_long_format(
                subject=cells[0],
                values=cells[1:9],
                target_year=target_year,
                source_file=source_file,
            )
        )

    if not result:
        debug_path = Path(f"debug_table_{docx_path.stem}.txt")
        debug_lines = [" | ".join(row) for row in table_rows]
        debug_path.write_text("\n".join(debug_lines), encoding="utf-8")
        raise ValueError(
            f"Не удалось извлечь строки данных из таблицы DOCX. "
            f"Сохранен диагностический файл {debug_path.name}"
        )

    subject_rows = len(result) // 4
    log.info("[%s] найдено строк субъектов/агрегатов: %s", docx_path.name, subject_rows)

    if subject_rows != CONFIG.expected_subject_rows_per_year:
        log.warning(
            "[%s] ожидалось %s строк субъектов/агрегатов, найдено %s",
            docx_path.name,
            CONFIG.expected_subject_rows_per_year,
            subject_rows,
        )

    return result

# -----------------------------------------------------------------------------
# ДЕДУПЛИКАЦИЯ И ПРОВЕРКИ
# -----------------------------------------------------------------------------

def deduplicate_rows(rows: Iterable[ParsedRow]) -> list[ParsedRow]:
    """Удаляет дубли по бизнес-ключу: субъект + показатель + уточнение + год."""
    unique_rows: list[ParsedRow] = []
    seen: set[tuple[str, str, str, int]] = set()

    for row in rows:
        key = (row.subject, row.indicator, row.detail, row.year)

        if key in seen:
            log.warning("Дубль пропущен: %s", key)
            continue

        seen.add(key)
        unique_rows.append(row)

    return unique_rows


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

def save_to_csv(rows: Iterable[ParsedRow], output_path: Path) -> None:
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

    with output_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
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
    """Проверяет наличие папки с DOCX, исходных файлов и DATABASE_URL."""
    if not config.raw_dir.exists():
        raise FileNotFoundError(f"Папка с исходными файлами не найдена: {config.raw_dir}")

    missing_files = []

    for file_name in config.files_to_process:
        file_path = config.raw_dir / file_name

        if not file_path.exists():
            missing_files.append(str(file_path))

    if missing_files:
        missing_text = "\n".join(missing_files)
        raise FileNotFoundError(f"Не найдены DOCX-файлы:\n{missing_text}")

    if not config.skip_db and not config.database_url:
        raise ValueError("DATABASE_URL не найден. Добавь строку подключения в .env.")

    log.info("Проверка окружения пройдена")


def filter_target_files(input_files: list[Path], config: Config = CONFIG) -> list[Path]:
    """Оставляет DOCX-файлы за годы, поддерживаемые текущим парсером."""
    target_years = set(config.files_to_process.values())
    return sorted(
        [
            path for path in input_files
            if path.suffix.lower() == ".docx"
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

    return [config.raw_dir / file_name for file_name in config.files_to_process]


def build_files_to_process(input_files: list[Path], config: Config = CONFIG) -> dict[Path, int]:
    """Строит маппинг путь -> целевой год."""
    configured_by_name = config.files_to_process
    result: dict[Path, int] = {}

    for path in filter_target_files(input_files, config):
        year = configured_by_name.get(path.name) or extract_year_from_filename(path)
        if year is not None:
            result[path] = year

    return dict(sorted(result.items(), key=lambda item: item[1]))


def parse_args() -> Config:
    """Читает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Парсер DOCX-таблицы 1.2 по туберкулезу из соцзаболеваний"
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
        raw_dir=CONFIG.raw_dir,
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
    """Запускает DOCX-парсер из оркестратора или напрямую."""
    config = Config(
        base_dir=CONFIG.base_dir,
        raw_dir=CONFIG.raw_dir,
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
        raw_dir=config.raw_dir,
        output_csv=config.output_csv,
        database_url=config.database_url,
        target_table=config.target_table,
        files_to_process={path.name: year for path, year in files_to_process.items()},
        expected_subject_rows_per_year=config.expected_subject_rows_per_year,
        skip_db=config.skip_db,
        input_files=list(files_to_process),
    )

    all_rows: list[ParsedRow] = []
    for docx_path, target_year in files_to_process.items():
        parsed_rows = parse_docx_file(docx_path, target_year)
        log.info("[%s] получено строк long-format: %s", docx_path.name, len(parsed_rows))
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
        2. Распарсить DOCX-файлы.
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
