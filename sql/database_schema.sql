/* ============================================================================
   Проект: Медицинская статистика по туберкулезу
   Файл: database_schema.sql

   Назначение
   ----------
   Скрипт создает логическую схему базы данных для ETL-проекта:
   загрузки, нормализации, расчета и визуализации показателей по туберкулезу.

   Данные проекта
   --------------
   1. Население Иркутской области в разрезе года, пола и возраста.
   2. Данные федеральных статистических форм:
      - форма N8, таблица 1000;
      - форма N30, таблицы 1100, 2513, 3100;
      - форма N33, таблицы 2100, 2200, 2300, 2310, 2400, 2500, 2600.
   3. Заболеваемость и контингенты пациентов активным туберкулезом
      по субъектам Российской Федерации.
   4. Расчетные эпидемиологические и организационные показатели.
   5. Геоданные регионов для BI-визуализации.
   6. Служебные таблицы ETL: реестр исходных файлов и история запусков.

   Принципы проектирования
   -----------------------
   - Сырые формы хранятся в нормализованном long-формате.
   - Бизнес-ключи защищены UNIQUE-ограничениями там, где структура источника
     однозначно задает строку.
   - Справочники вынесены отдельно от фактов.
   - Служебная ETL-метаинформация отделена от медицинских данных.
   - Скрипт содержит DDL и комментарии.

   СУБД: PostgreSQL / Supabase PostgreSQL.
   ============================================================================ */


/* ============================================================================
   1. Справочники
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.dim_subject_federal_district (
    "Субъект" text NOT NULL,
    "Федеральный округ" text NULL,
    year_from integer NOT NULL,
    year_to integer NOT NULL,

    CONSTRAINT uq_dim_subject_federal_district
        UNIQUE ("Субъект", year_from, year_to),

    CONSTRAINT chk_dim_subject_federal_district_years
        CHECK (year_from <= year_to)
);

COMMENT ON TABLE public.dim_subject_federal_district IS
'Справочник принадлежности субъекта РФ федеральному округу с учетом периода действия. Нужен из-за перехода отдельных регионов между округами.';

COMMENT ON COLUMN public.dim_subject_federal_district."Субъект" IS
'Название субъекта или агрегированной строки из статистического источника.';

COMMENT ON COLUMN public.dim_subject_federal_district."Федеральный округ" IS
'Федеральный округ. Для агрегатов верхнего уровня может быть NULL.';

COMMENT ON COLUMN public.dim_subject_federal_district.year_from IS
'Первый год действия соответствия.';

COMMENT ON COLUMN public.dim_subject_federal_district.year_to IS
'Последний год действия соответствия.';


CREATE TABLE IF NOT EXISTS public.dim_tb_subject_region_map (
    subject_name text NOT NULL,
    region_name text NULL,
    map_type text NOT NULL,
    comment text NULL,

    CONSTRAINT dim_tb_subject_region_map_pkey
        PRIMARY KEY (subject_name),

    CONSTRAINT chk_dim_tb_subject_region_map_type
        CHECK (map_type IN ('manual', 'exclude')),

    CONSTRAINT chk_dim_tb_subject_region_map_region_name
        CHECK (
            (map_type = 'manual' AND region_name IS NOT NULL)
            OR
            (map_type = 'exclude' AND region_name IS NULL)
        )
);

COMMENT ON TABLE public.dim_tb_subject_region_map IS
'Справочник ручного сопоставления названий субъектов из медицинской статистики с названиями регионов в геотаблице.';

COMMENT ON COLUMN public.dim_tb_subject_region_map.subject_name IS
'Название субъекта в статистическом источнике.';

COMMENT ON COLUMN public.dim_tb_subject_region_map.region_name IS
'Название региона в таблице public.tb_regions. Заполняется только для ручного сопоставления.';

COMMENT ON COLUMN public.dim_tb_subject_region_map.map_type IS
'Тип сопоставления: manual - ручное соответствие, exclude - исключить из картографической витрины.';

COMMENT ON COLUMN public.dim_tb_subject_region_map.comment IS
'Пояснение, почему требуется ручное сопоставление или исключение.';


/* ============================================================================
   2. Геоданные
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tb_regions (
    id bigserial PRIMARY KEY,
    "Регион" text NOT NULL,
    "Полигон" jsonb NOT NULL,

    CONSTRAINT uq_tb_regions_region_polygon
        UNIQUE ("Регион", "Полигон")
);

COMMENT ON TABLE public.tb_regions IS
'Геополигоны регионов РФ для построения карт в BI-инструментах.';

COMMENT ON COLUMN public.tb_regions."Регион" IS
'Название региона в географическом справочнике.';

COMMENT ON COLUMN public.tb_regions."Полигон" IS
'JSON-массив координат полигона или мультиполигона региона.';

CREATE INDEX IF NOT EXISTS idx_tb_regions_region
    ON public.tb_regions ("Регион");


/* ============================================================================
   3. Демография
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tb_population (
    id bigserial PRIMARY KEY,
    "Год" integer NOT NULL,
    "Пол" text NOT NULL,
    "Возраст" text NOT NULL,
    "Численность" integer NOT NULL,

    CONSTRAINT uq_tb_population_year_sex_age
        UNIQUE ("Год", "Пол", "Возраст"),

    CONSTRAINT chk_tb_population_year
        CHECK ("Год" BETWEEN 1900 AND 2100),

    CONSTRAINT chk_tb_population_value
        CHECK ("Численность" >= 0)
);

COMMENT ON TABLE public.tb_population IS
'Численность населения в длинном формате: год, пол, возрастная группа, численность.';

COMMENT ON COLUMN public.tb_population."Год" IS
'Отчетный год.';

COMMENT ON COLUMN public.tb_population."Пол" IS
'Пол: всего, мужчины, женщины.';

COMMENT ON COLUMN public.tb_population."Возраст" IS
'Возрастная группа.';

COMMENT ON COLUMN public.tb_population."Численность" IS
'Численность населения в указанном разрезе.';

CREATE INDEX IF NOT EXISTS idx_tb_population_year
    ON public.tb_population ("Год");


/* ============================================================================
   4. Форма N8
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tb_form8_1000 (
    "Формы туберкулеза" text NOT NULL,
    "Код по МКБ - Х пересмотра" text NULL,
    "Пол" text NULL,
    "Возраст" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NULL,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '1000',

    CONSTRAINT uq_tb_form8_1000_year_row_graph_sex_age_table
        UNIQUE ("Год", "Строка", "Графа", "Пол", "Возраст", "Таблица")
);

COMMENT ON TABLE public.tb_form8_1000 IS
'Форма N8, таблица 1000: впервые выявленные больные туберкулезом в разрезах формы, пола и возраста.';

CREATE INDEX IF NOT EXISTS idx_tb_form8_1000_year
    ON public.tb_form8_1000 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form8_1000_row_graph
    ON public.tb_form8_1000 ("Строка", "Графа");


/* ============================================================================
   5. Форма N30
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tb_form30_1100 (
    id bigserial PRIMARY KEY,
    "Показатель" text NULL,
    "Уточнение" text NULL,
    "Год" integer NULL,
    "Значение" numeric NULL,
    "Строка" text NULL,
    "Графа" integer NULL,
    "Таблица" text NULL DEFAULT '1100',
    "Строка_норм" integer NULL
);

COMMENT ON TABLE public.tb_form30_1100 IS
'Форма N30, таблица 1100: сведения о фтизиатрах и участковых фтизиатрах.';

COMMENT ON COLUMN public.tb_form30_1100."Строка_норм" IS
'Нормализованный номер строки для расчетов, так как исходные номера строк меняются по годам.';

CREATE INDEX IF NOT EXISTS idx_tb_form30_1100_year
    ON public.tb_form30_1100 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form30_1100_norm_row_graph
    ON public.tb_form30_1100 ("Строка_норм", "Графа");


CREATE TABLE IF NOT EXISTS public.tb_form30_2513 (
    id bigserial PRIMARY KEY,
    "Показатель" text NULL,
    "Уточнение" text NULL,
    "Год" integer NULL,
    "Значение" numeric NULL,
    "Строка" integer NULL,
    "Графа" integer NULL,
    "Таблица" text NULL DEFAULT '2513'
);

COMMENT ON TABLE public.tb_form30_2513 IS
'Форма N30, таблица 2513: профилактические осмотры на туберкулез и выявление туберкулеза при осмотрах.';

CREATE INDEX IF NOT EXISTS idx_tb_form30_2513_year
    ON public.tb_form30_2513 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form30_2513_row_graph
    ON public.tb_form30_2513 ("Строка", "Графа");


CREATE TABLE IF NOT EXISTS public.tb_form30_3100 (
    id bigserial PRIMARY KEY,
    "Показатель" text NULL,
    "Уточнение" text NULL,
    "Год" integer NULL,
    "Значение" numeric NULL,
    "Строка" text NULL,
    "Графа" integer NULL,
    "Таблица" text NULL DEFAULT '3100',
    "Строка_норм" integer NULL
);

COMMENT ON TABLE public.tb_form30_3100 IS
'Форма N30, таблица 3100: туберкулезные койки для взрослых и детей.';

COMMENT ON COLUMN public.tb_form30_3100."Строка_норм" IS
'Нормализованный номер строки для расчетов, так как исходные номера строк меняются по годам.';

CREATE INDEX IF NOT EXISTS idx_tb_form30_3100_year
    ON public.tb_form30_3100 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form30_3100_norm_row_graph
    ON public.tb_form30_3100 ("Строка_норм", "Графа");


/* ============================================================================
   6. Форма N33
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tb_form33_2100 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Код по МКБ" text NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NULL,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2100',

    CONSTRAINT uq_tb_form33_2100_year_row_graph_table
        UNIQUE ("Год", "Строка", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2100 IS
'Форма N33, таблица 2100: контингенты больных активным туберкулезом, состоящие на учете.';


CREATE TABLE IF NOT EXISTS public.tb_form33_2200 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NOT NULL DEFAULT 0,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2200',

    CONSTRAINT uq_tb_form33_2200_year_row_graph_table
        UNIQUE ("Год", "Строка", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2200 IS
'Форма N33, таблица 2200: выявление больных и отдельных групп риска.';


CREATE TABLE IF NOT EXISTS public.tb_form33_2300 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NOT NULL DEFAULT 0,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2300',

    CONSTRAINT uq_tb_form33_2300_year_row_graph_table
        UNIQUE ("Год", "Строка", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2300 IS
'Форма N33, таблица 2300: движение контингентов больных туберкулезом.';


CREATE TABLE IF NOT EXISTS public.tb_form33_2310 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NOT NULL DEFAULT 0,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2310',

    CONSTRAINT uq_tb_form33_2310_year_graph_table
        UNIQUE ("Год", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2310 IS
'Форма N33, таблица 2310: дополнительный блок по смертности.';


CREATE TABLE IF NOT EXISTS public.tb_form33_2400 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NOT NULL DEFAULT 0,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2400',

    CONSTRAINT uq_tb_form33_2400_year_row_graph_table
        UNIQUE ("Год", "Строка", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2400 IS
'Форма N33, таблица 2400: диспансерная работа с группами учета.';


CREATE TABLE IF NOT EXISTS public.tb_form33_2500 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NOT NULL DEFAULT 0,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2500',

    CONSTRAINT uq_tb_form33_2500_year_row_graph_table
        UNIQUE ("Год", "Строка", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2500 IS
'Форма N33, таблица 2500: бактериовыделители, состоящие на учете.';


CREATE TABLE IF NOT EXISTS public.tb_form33_2600 (
    id bigserial PRIMARY KEY,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NULL,
    "Строка" integer NOT NULL,
    "Графа" integer NOT NULL,
    "Таблица" text NOT NULL DEFAULT '2600',

    CONSTRAINT uq_tb_form33_2600_year_row_graph_table
        UNIQUE ("Год", "Строка", "Графа", "Таблица")
);

COMMENT ON TABLE public.tb_form33_2600 IS
'Форма N33, таблица 2600: больничная и санаторная помощь.';

CREATE INDEX IF NOT EXISTS idx_tb_form33_2100_year
    ON public.tb_form33_2100 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2200_year
    ON public.tb_form33_2200 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2300_year
    ON public.tb_form33_2300 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2310_year
    ON public.tb_form33_2310 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2400_year
    ON public.tb_form33_2400 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2500_year
    ON public.tb_form33_2500 ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2600_year
    ON public.tb_form33_2600 ("Год");


/* ============================================================================
   7. Заболеваемость и контингенты по субъектам РФ
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tuberculosis_incidence_by_subjects_docx (
    "Субъект" text NOT NULL,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NULL,
    source_file text NOT NULL,

    CONSTRAINT uq_tuberculosis_incidence_by_subjects_docx
        UNIQUE ("Субъект", "Показатель", "Уточнение", "Год")
);

COMMENT ON TABLE public.tuberculosis_incidence_by_subjects_docx IS
'Промежуточная таблица: данные таблицы 1.2 из DOCX-сборников социально значимых заболеваний за 2016-2020 годы.';

COMMENT ON COLUMN public.tuberculosis_incidence_by_subjects_docx.source_file IS
'Имя исходного DOCX-файла. Поле сохранено здесь намеренно, потому что DOCX/PDF-источники являются промежуточным слоем перед объединением в итоговую витрину.';


CREATE TABLE IF NOT EXISTS public.tuberculosis_incidence_by_subjects_pdf (
    "Субъект" text NOT NULL,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NULL,
    source_file text NOT NULL,

    CONSTRAINT uq_tuberculosis_incidence_by_subjects_pdf
        UNIQUE ("Субъект", "Показатель", "Уточнение", "Год")
);

COMMENT ON TABLE public.tuberculosis_incidence_by_subjects_pdf IS
'Промежуточная таблица: данные таблицы 1.2 из PDF-сборников социально значимых заболеваний за 2021-2024 годы.';

COMMENT ON COLUMN public.tuberculosis_incidence_by_subjects_pdf.source_file IS
'Имя исходного PDF-файла. Поле сохранено здесь намеренно, потому что DOCX/PDF-источники являются промежуточным слоем перед объединением в итоговую витрину.';


CREATE TABLE IF NOT EXISTS public.tb_incidence_subjects (
    "Субъект" text NOT NULL,
    "Показатель" text NOT NULL,
    "Уточнение" text NOT NULL,
    "Год" integer NOT NULL,
    "Значение" numeric NULL,
    "Федеральный округ" text NULL,

    CONSTRAINT uq_tb_incidence_subjects
        UNIQUE ("Субъект", "Показатель", "Уточнение", "Год")
);

COMMENT ON TABLE public.tb_incidence_subjects IS
'Итоговая аналитическая таблица по заболеваемости и контингентам активного туберкулеза по субъектам РФ.';

COMMENT ON COLUMN public.tb_incidence_subjects."Федеральный округ" IS
'Федеральный округ субъекта на соответствующий год. Для агрегатов верхнего уровня может быть NULL.';

CREATE INDEX IF NOT EXISTS idx_tb_incidence_subjects_year
    ON public.tb_incidence_subjects ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_incidence_subjects_subject
    ON public.tb_incidence_subjects ("Субъект");

CREATE INDEX IF NOT EXISTS idx_tb_incidence_subjects_indicator
    ON public.tb_incidence_subjects ("Показатель", "Уточнение");


/* ============================================================================
   8. Расчетные показатели
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.tb_calculated_indicators (
    id text NOT NULL,
    "Показатель" text NOT NULL,
    "Год" integer NOT NULL,
    "Числитель" numeric NULL,
    "Знаменатель" numeric NULL,
    "Значение" numeric NULL,
    "Алиас" text NULL,
    "Источник" text NULL,
    "Формула" text NULL,
    "Единица" text NULL,
    calculated_at timestamp without time zone NOT NULL DEFAULT now(),

    CONSTRAINT uq_tb_calculated_indicators
        UNIQUE (id, "Год")
);

COMMENT ON TABLE public.tb_calculated_indicators IS
'Итоговая витрина расчетных показателей по туберкулезу: числитель, знаменатель, значение, формула и единица измерения.';

COMMENT ON COLUMN public.tb_calculated_indicators.id IS
'Идентификатор показателя из алгоритма расчета.';

COMMENT ON COLUMN public.tb_calculated_indicators."Показатель" IS
'Человекочитаемое название расчетного показателя.';

COMMENT ON COLUMN public.tb_calculated_indicators."Числитель" IS
'Значение числителя, использованное при расчете показателя.';

COMMENT ON COLUMN public.tb_calculated_indicators."Знаменатель" IS
'Значение знаменателя, использованное при расчете показателя. Может быть NULL для абсолютных показателей.';

COMMENT ON COLUMN public.tb_calculated_indicators."Значение" IS
'Итоговое значение расчетного показателя. Если знаменатель отсутствует или равен нулю, значение может быть NULL.';

COMMENT ON COLUMN public.tb_calculated_indicators."Источник" IS
'Код исходной формы или набора данных, из которого берется базовое значение.';

COMMENT ON COLUMN public.tb_calculated_indicators."Формула" IS
'Техническое описание источников числителя и знаменателя.';

COMMENT ON COLUMN public.tb_calculated_indicators.calculated_at IS
'Дата и время записи строки в расчетную витрину.';

CREATE INDEX IF NOT EXISTS idx_tb_calculated_indicators_year
    ON public.tb_calculated_indicators ("Год");

CREATE INDEX IF NOT EXISTS idx_tb_calculated_indicators_source
    ON public.tb_calculated_indicators ("Источник");


/* ============================================================================
   9. Составные индексы для расчетов

   Эти индексы ускоряют типовые запросы расчетного SQL, где значения
   выбираются по году, строке и графе формы. Для таблиц с нормализованными
   строками формы N30 используется поле "Строка_норм".
   ============================================================================ */

CREATE INDEX IF NOT EXISTS idx_tb_form8_1000_calc
    ON public.tb_form8_1000 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form30_1100_calc
    ON public.tb_form30_1100 ("Год", "Строка_норм", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form30_2513_calc
    ON public.tb_form30_2513 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form30_3100_calc
    ON public.tb_form30_3100 ("Год", "Строка_норм", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2100_calc
    ON public.tb_form33_2100 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2200_calc
    ON public.tb_form33_2200 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2300_calc
    ON public.tb_form33_2300 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2310_calc
    ON public.tb_form33_2310 ("Год", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2400_calc
    ON public.tb_form33_2400 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2500_calc
    ON public.tb_form33_2500 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_form33_2600_calc
    ON public.tb_form33_2600 ("Год", "Строка", "Графа");

CREATE INDEX IF NOT EXISTS idx_tb_calculated_indicators_year_id
    ON public.tb_calculated_indicators ("Год", id);

CREATE INDEX IF NOT EXISTS idx_tb_incidence_subjects_dashboard
    ON public.tb_incidence_subjects (
        "Год",
        "Субъект",
        "Показатель",
        "Уточнение"
    );

/* ============================================================================
   10. Служебные таблицы ETL
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.etl_source_files (
    id bigserial PRIMARY KEY,
    source_key text NOT NULL,
    source_group text NOT NULL,
    table_code text NULL,
    year integer NULL,
    relative_path text NOT NULL,
    file_name text NOT NULL,
    file_ext text NOT NULL,
    file_size bigint NULL,
    sha256 text NULL,
    is_active boolean NOT NULL DEFAULT true,
    status text NOT NULL DEFAULT 'registered',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamp without time zone NOT NULL DEFAULT now(),

    CONSTRAINT uq_etl_source_files
        UNIQUE (source_key, relative_path),

    CONSTRAINT chk_etl_source_files_status
        CHECK (status IN ('registered', 'processing', 'processed', 'failed', 'ignored')),

    CONSTRAINT chk_etl_source_files_file_size
        CHECK (file_size IS NULL OR file_size >= 0)
);

COMMENT ON TABLE public.etl_source_files IS
'Реестр исходных файлов проекта. Хранит только файлы из папки data и используется парсерами как источник путей.';

COMMENT ON COLUMN public.etl_source_files.source_key IS
'Ключ ETL-задачи, например form33_2100, form30_2513, population.';

COMMENT ON COLUMN public.etl_source_files.source_group IS
'Группа источника: form8, form30, form33, social_diseases, reference.';

COMMENT ON COLUMN public.etl_source_files.table_code IS
'Код таблицы формы или логический код источника.';

COMMENT ON COLUMN public.etl_source_files.relative_path IS
'Относительный путь к файлу от корня проекта. Используется вместо абсолютного пути для переносимости.';

COMMENT ON COLUMN public.etl_source_files.sha256 IS
'Контрольная сумма файла для отслеживания изменений источника.';

COMMENT ON COLUMN public.etl_source_files.status IS
'Статус файла в ETL-реестре: registered, processing, processed, failed или ignored.';

COMMENT ON COLUMN public.etl_source_files.metadata IS
'Дополнительные технические атрибуты источника в JSON-формате.';

COMMENT ON COLUMN public.etl_source_files.updated_at IS
'Дата и время последнего обновления записи в реестре источников.';

CREATE INDEX IF NOT EXISTS idx_etl_source_files_source_key
    ON public.etl_source_files (source_key);

CREATE INDEX IF NOT EXISTS idx_etl_source_files_year
    ON public.etl_source_files (year);

CREATE INDEX IF NOT EXISTS idx_etl_source_files_active
    ON public.etl_source_files (is_active);

CREATE INDEX IF NOT EXISTS idx_etl_source_files_lookup
    ON public.etl_source_files (source_key, is_active, year);


CREATE TABLE IF NOT EXISTS public.etl_parse_runs (
    id bigserial PRIMARY KEY,
    source_file_id bigint NULL REFERENCES public.etl_source_files(id),
    job_name text NOT NULL,
    script_name text NOT NULL,
    target_table text NULL,
    status text NOT NULL,
    rows_processed integer NULL,
    error_message text NULL,
    started_at timestamp without time zone NOT NULL DEFAULT now(),
    finished_at timestamp without time zone NULL,

    CONSTRAINT chk_etl_parse_runs_status
        CHECK (status IN ('running', 'success', 'failed')),

    CONSTRAINT chk_etl_parse_runs_rows_processed
        CHECK (rows_processed IS NULL OR rows_processed >= 0),

    CONSTRAINT chk_etl_parse_runs_finished_at
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

COMMENT ON TABLE public.etl_parse_runs IS
'История запусков ETL-задач: какой скрипт запускался, когда, с каким статусом и сколько строк обработал.';

COMMENT ON COLUMN public.etl_parse_runs.source_file_id IS
'Опциональная ссылка на конкретный исходный файл, если запуск был точечным.';

COMMENT ON COLUMN public.etl_parse_runs.job_name IS
'Имя ETL-задачи из config.py.';

COMMENT ON COLUMN public.etl_parse_runs.status IS
'Статус запуска: running, success, failed.';

CREATE INDEX IF NOT EXISTS idx_etl_parse_runs_job_name
    ON public.etl_parse_runs (job_name);

CREATE INDEX IF NOT EXISTS idx_etl_parse_runs_status
    ON public.etl_parse_runs (status);

CREATE INDEX IF NOT EXISTS idx_etl_parse_runs_started_at
    ON public.etl_parse_runs (started_at);

CREATE INDEX IF NOT EXISTS idx_etl_parse_runs_lookup
    ON public.etl_parse_runs (job_name, status, started_at DESC);


/* ============================================================================
   11. BI-витрина для карты DataLens
   ============================================================================ */

-- View пересоздается явно, потому что ее набор колонок менялся в ходе развития
-- проекта: техническое поле source_file было удалено из итоговой витрины.
DROP VIEW IF EXISTS public.v_tb_incidence_subjects_datalens_map;

CREATE VIEW public.v_tb_incidence_subjects_datalens_map AS
WITH regions_dedup AS (
    SELECT DISTINCT ON ("Регион")
        "Регион",
        "Полигон"
    FROM public.tb_regions
    ORDER BY
        "Регион",
        id
)
SELECT
    i."Субъект",
    COALESCE(m.region_name, TRIM(i."Субъект")) AS "Регион_для_полигона",
    COALESCE(m.map_type, 'direct') AS map_type,
    i."Федеральный округ",
    i."Показатель",
    i."Уточнение",
    i."Год",
    i."Значение",
    r."Полигон"::text AS "Полигон"
FROM public.tb_incidence_subjects AS i
LEFT JOIN public.dim_tb_subject_region_map AS m
    ON TRIM(i."Субъект") = m.subject_name
LEFT JOIN regions_dedup AS r
    ON COALESCE(m.region_name, TRIM(i."Субъект")) = r."Регион"
WHERE COALESCE(m.map_type, 'direct') <> 'exclude';

COMMENT ON VIEW public.v_tb_incidence_subjects_datalens_map IS
'Витрина для карты DataLens: показатели по субъектам РФ с названием региона для полигона и полигоном в текстовом формате.';


/* ============================================================================
   12. Полезные контрольные запросы

   Эти запросы не меняют данные. Их можно запускать после загрузки ETL,
   чтобы проверить полноту и качество витрин.
   ============================================================================ */

/*
-- Количество строк по годам в итоговой таблице субъектов.
SELECT
    "Год",
    COUNT(*) AS rows_count,
    COUNT(DISTINCT "Субъект") AS subjects_count
FROM public.tb_incidence_subjects
GROUP BY "Год"
ORDER BY "Год";

-- Дубли по бизнес-ключу в итоговой таблице субъектов.
SELECT
    "Субъект",
    "Показатель",
    "Уточнение",
    "Год",
    COUNT(*) AS rows_count
FROM public.tb_incidence_subjects
GROUP BY
    "Субъект",
    "Показатель",
    "Уточнение",
    "Год"
HAVING COUNT(*) > 1;

-- Исходные файлы, зарегистрированные в ETL-реестре.
SELECT
    source_key,
    COUNT(*) AS files_count,
    MIN(year) AS min_year,
    MAX(year) AS max_year
FROM public.etl_source_files
WHERE is_active = true
GROUP BY source_key
ORDER BY source_key;
*/
