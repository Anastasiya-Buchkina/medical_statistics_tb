"""Единая конфигурация проекта medical_statistics_tb.

В этом файле собраны:
- пути к данным и служебным папкам проекта;
- переменные окружения;
- описание всех ETL-источников для оркестратора.

ETL-скрипты не должны хранить собственные абсолютные пути к данным. Если
появляется новый источник, его нужно добавить в `SOURCE_DEFINITIONS`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final, TypedDict

from dotenv import load_dotenv


PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent
ENV_FILE: Final[Path] = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)

DATABASE_URL: Final[str | None] = os.getenv("DATABASE_URL")

DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
PROCESSED_DIR: Final[Path] = PROJECT_ROOT / "processed"
DASHBOARD_DIR: Final[Path] = PROJECT_ROOT / "Дашборд"

FORM_8_33_DIR: Final[Path] = DATA_DIR / "33_8_2015-2024"
FORM_30_DIR: Final[Path] = DATA_DIR / "формы 30"
SOCIAL_DISEASES_DIR: Final[Path] = DATA_DIR / "Соц_заболевания"
OTHER_DATA_DIR: Final[Path] = DATA_DIR / "Другое"


class SourceDefinition(TypedDict, total=False):
    source_group: str
    table_code: str
    raw_dir: Path
    patterns: list[str]
    year_from: int
    year_to: int
    target_table: str
    script_module: str


SOURCE_DEFINITIONS: Final[dict[str, SourceDefinition]] = {
    "population": {
        "source_group": "reference",
        "table_code": "population",
        "raw_dir": OTHER_DATA_DIR,
        "patterns": ["Население_2016-2024.csv"],
        "target_table": "public.tb_population",
        "script_module": "etl.load_population_data",
    },
    "regions": {
        "source_group": "reference",
        "table_code": "regions",
        "raw_dir": OTHER_DATA_DIR,
        "patterns": ["Regions.csv"],
        "target_table": "public.tb_regions",
        "script_module": "etl.load_regions_data",
    },
    "form8_1000": {
        "source_group": "form8",
        "table_code": "1000",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["8_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form8_1000",
        "script_module": "etl.parse_form8_1000",
    },
    "form33_2100": {
        "source_group": "form33",
        "table_code": "2100",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2015,
        "year_to": 2024,
        "target_table": "public.tb_form33_2100",
        "script_module": "etl.parse_form33_2100",
    },
    "form33_2200": {
        "source_group": "form33",
        "table_code": "2200",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form33_2200",
        "script_module": "etl.parse_form33_2200",
    },
    "form33_2300": {
        "source_group": "form33",
        "table_code": "2300",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form33_2300",
        "script_module": "etl.parse_form33_2300",
    },
    "form33_2310": {
        "source_group": "form33",
        "table_code": "2310",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form33_2310",
        "script_module": "etl.parse_form33_2310",
    },
    "form33_2400": {
        "source_group": "form33",
        "table_code": "2400",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form33_2400",
        "script_module": "etl.parse_form33_2400",
    },
    "form33_2500": {
        "source_group": "form33",
        "table_code": "2500",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2015,
        "year_to": 2024,
        "target_table": "public.tb_form33_2500",
        "script_module": "etl.parse_form33_2500",
    },
    "form33_2600": {
        "source_group": "form33",
        "table_code": "2600",
        "raw_dir": FORM_8_33_DIR,
        "patterns": ["33_*.xlsx", "20*.xls", "20*.xlsx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form33_2600",
        "script_module": "etl.parse_form33_2600",
    },
    "form30_1100": {
        "source_group": "form30",
        "table_code": "1100",
        "raw_dir": FORM_30_DIR,
        "patterns": ["*.docx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form30_1100",
        "script_module": "etl.parse_form30_1100",
    },
    "form30_2513": {
        "source_group": "form30",
        "table_code": "2513",
        "raw_dir": FORM_30_DIR,
        "patterns": ["*.docx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form30_2513",
        "script_module": "etl.parse_form30_2513",
    },
    "form30_3100": {
        "source_group": "form30",
        "table_code": "3100",
        "raw_dir": FORM_30_DIR,
        "patterns": ["*.docx"],
        "year_from": 2016,
        "year_to": 2024,
        "target_table": "public.tb_form30_3100",
        "script_module": "etl.parse_form30_3100",
    },
    "social_diseases_docx": {
        "source_group": "social_diseases",
        "table_code": "subjects_docx",
        "raw_dir": SOCIAL_DISEASES_DIR,
        "patterns": ["*.docx"],
        "year_from": 2016,
        "year_to": 2020,
        "target_table": "public.tuberculosis_incidence_by_subjects_docx",
        "script_module": "etl.parse_tuberculosis_incidence_by_subjects_docx",
    },
    "social_diseases_pdf": {
        "source_group": "social_diseases",
        "table_code": "subjects_pdf",
        "raw_dir": SOCIAL_DISEASES_DIR,
        "patterns": ["*.pdf"],
        "year_from": 2021,
        "year_to": 2024,
        "target_table": "public.tuberculosis_incidence_by_subjects_pdf",
        "script_module": "etl.parse_tuberculosis_incidence_by_subjects_pdf",
    },
}
