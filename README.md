# Анализ медицинской статистики по туберкулезу

## 🎯 Цель проекта

Цель проекта - создать воспроизводимый ETL-пайплайн для подготовки медицинской
статистики по туберкулезу: зарегистрировать исходные файлы, распарсить Excel,
DOCX, PDF и CSV-источники, нормализовать данные и привести в long-формат, загрузить их в
PostgreSQL/Supabase и подготовить расчетную витрину для аналитики и дашборда в
Yandex DataLens.

Проект построен как портфолио-ETL: конфигурация, подключение к базе данных,
оркестрация, SQL-схема, парсеры и контроль качества разнесены по отдельным
слоям.

---

<a id="contents"></a>
## 📚 Содержание

- [🧭 Обзор проекта](#overview)
- [🛠️ Стек](#stack)
- [📁 Структура проекта](#structure)
- [⚙️ Настройка окружения](#setup)
- [📥 Этап 1. Регистрация исходных файлов](#stage1)
- [🗄️ Этап 2. Создание структуры базы данных](#stage2)
- [🔄 Этап 3. Парсинг и нормализация данных](#stage3)
  - [Этап 3.1. Справочники населения и геоданных](#stage3-1)
  - [Этап 3.2. Формы 8 и 33](#stage3-2)
  - [Этап 3.3. Формы 30](#stage3-3)
  - [Этап 3.4. Сборники DOCX/PDF по субъектам РФ](#stage3-4)
- [🧮 Этап 4. Расчет аналитических показателей](#stage4)
- [✅ Этап 5. Контроль качества ETL](#stage5)
- [📊 Этап 6. Подготовка данных для DataLens](#stage6)
- [🚀 Основные команды запуска](#commands)
- [📌 Правила проекта](#rules)

---

<a id="overview"></a>
## 🧭 Обзор проекта

Проект включает этапы:

- регистрация исходных файлов из локальной папки `data/` в служебной таблице
  `public.etl_source_files`;
- ведение журнала запусков ETL в таблице `public.etl_parse_runs`;
- парсинг статистических форм по туберкулезу:
  - форма 8, таблица 1000;
  - форма 30, таблицы 1100, 2513, 3100;
  - форма 33, таблицы 2100, 2200, 2300, 2310, 2400, 2500, 2600;
- загрузка справочников:
  - численность населения;
  - геоданные регионов РФ для карт DataLens;
- парсинг DOCX/PDF-сборников по социально значимым заболеваниям;
- создание нормализованных таблиц PostgreSQL;
- пересчет витрины `public.tb_calculated_indicators`;
- подготовка BI-витрины карты `public.v_tb_incidence_subjects_datalens_map`;
- валидация полноты источников, запусков и целевых таблиц.

Итог:

- подготовлен управляемый ETL-пайплайн с единым оркестратором;
- все парсеры приведены к единому интерфейсу `run(input_files=None, skip_db=False)`;
- схема базы данных описана в одном SQL-файле;
- безопасный режим запуска включен по умолчанию и не перезаписывает целевые таблицы;
- данные готовы для аналитических расчетов и визуализации в Yandex DataLens.

---

<a id="stack"></a>
## 🛠️ Стек

- **Python:** pandas, SQLAlchemy, python-dotenv
- **Excel:** openpyxl, xlrd
- **DOCX/PDF:** python-docx, pdfplumber
- **PostgreSQL / Supabase**
- **SQL:** DDL-схема, constraints, индексы, view, расчетная DML-логика
- **BI:** Yandex DataLens

---

<a id="structure"></a>
## 📁 Структура проекта

```text
medical_statistics_tb/
├── .env.example                 # пример переменных окружения
├── config.py                    # единая конфигурация путей, источников и .env
├── db_manager.py                # централизованная работа с PostgreSQL
├── orchestrator.py              # единая точка запуска ETL
├── requirements.txt             # зависимости проекта
├── etl/                         # рабочие загрузчики, парсеры и реестры ETL
│   ├── common.py
│   ├── source_registry.py
│   ├── run_registry.py
│   ├── register_source_files.py
│   ├── load_population_data.py
│   ├── load_regions_data.py
│   ├── parse_form8_1000.py
│   ├── parse_form30_1100.py
│   ├── parse_form30_2513.py
│   ├── parse_form30_3100.py
│   ├── parse_form33_2100.py
│   ├── parse_form33_2200.py
│   ├── parse_form33_2300.py
│   ├── parse_form33_2310.py
│   ├── parse_form33_2400.py
│   ├── parse_form33_2500.py
│   ├── parse_form33_2600.py
│   ├── parse_tuberculosis_incidence_by_subjects_docx.py
│   └── parse_tuberculosis_incidence_by_subjects_pdf.py
├── sql/
│   ├── database_schema.sql      # таблицы, индексы, constraints, comments, view
│   └── tb_indicators.sql        # пересборка расчетной витрины
├── data/                        # локальные исходные данные, не публикуются
├── processed/                   # контрольные CSV, не публикуются
└── images                       # скриншоты BI-дашборда и схема etl
```

---

<a id="setup"></a>
## ⚙️ Настройка окружения

Проект использует файл `.env` для хранения параметров подключения к
PostgreSQL/Supabase.

Пример: [`.env.example`](.env.example)


#### Зависимости

```bash
pip install -r requirements.txt
```

#### Применение схемы БД

```bash
python3 orchestrator.py init-db
```

#### Регистрация исходных файлов

```bash
python3 orchestrator.py register-sources
```

---

<a id="stage1"></a>
### 📥 Этап 1. Регистрация исходных файлов

**Python-скрипт:** [`etl/register_source_files.py`](etl/register_source_files.py)

**Конфигурация источников:** [`config.py`](config.py)

**Что делает этап:**

- сканирует папку `data/` по правилам из `SOURCE_DEFINITIONS`;
- находит файлы нужных форматов: `.xlsx`, `.xls`, `.docx`, `.pdf`, `.csv`;
- определяет `source_key`, группу источника, код таблицы формы и год;
- сохраняет относительный путь к файлу в `public.etl_source_files`;
- рассчитывает размер файла и контрольную сумму `sha256`;
- использует UPSERT по ключу `(source_key, relative_path)`.

**Служебная таблица:**

- `public.etl_source_files`

**Основные поля:**

- `source_key` - имя ETL-задачи;
- `source_group` - группа источника;
- `table_code` - код таблицы формы или логический код справочника;
- `year` - год исходного файла;
- `relative_path` - переносимый путь от корня проекта;
- `sha256` - контрольная сумма файла;
- `status` - статус файла в ETL-реестре.

**Результат этапа:**

- все исходные файлы зарегистрированы в БД;
- парсеры получают список файлов из `etl_source_files`;
- проект становится переносимым между локальной средой и другим окружением.

---

<a id="stage2"></a>
### 🗄️ Этап 2. Создание структуры базы данных

**SQL-файл:** [`sql/database_schema.sql`](sql/database_schema.sql)

База данных PostgreSQL содержит:

- нормализованные таблицы статистических форм;
- справочники;
- промежуточные таблицы DOCX/PDF;
- итоговые аналитические таблицы;
- служебные таблицы ETL;
- индексы для расчетов и фильтрации;
- ограничения целостности;
- комментарии к таблицам и колонкам;
- BI-view для карты регионов.

**Служебные таблицы ETL:**

- `public.etl_source_files` - реестр исходных файлов;
- `public.etl_parse_runs` - журнал запусков ETL-задач.

**Таблицы справочников:**

- `public.tb_population` - численность населения по полу, возрасту и году;
- `public.tb_regions` - геоданные регионов РФ для DataLens;
- `public.dim_subject_federal_district` - соответствие субъектов федеральным округам;
- `public.dim_tb_subject_region_map` - маппинг названий субъектов на регионы карты.

**Таблицы форм:**

- `public.tb_form8_1000`;
- `public.tb_form30_1100`;
- `public.tb_form30_2513`;
- `public.tb_form30_3100`;
- `public.tb_form33_2100`;
- `public.tb_form33_2200`;
- `public.tb_form33_2300`;
- `public.tb_form33_2310`;
- `public.tb_form33_2400`;
- `public.tb_form33_2500`;
- `public.tb_form33_2600`.

**Региональные таблицы:**

- `public.tuberculosis_incidence_by_subjects_docx`;
- `public.tuberculosis_incidence_by_subjects_pdf`;
- `public.tb_incidence_subjects`.

**Расчетные и BI-объекты:**

- `public.tb_calculated_indicators`;
- `public.v_tb_incidence_subjects_datalens_map`.

**Результат этапа:**

- структура БД создается централизованно;
- ETL-скрипты не создают таблицы и не меняют DDL;
- индексы и ограничения описаны в коде проекта.

---

<a id="stage3"></a>
### 🔄 Этап 3. Парсинг и нормализация данных

Все рабочие ETL-модули запускаются через [`orchestrator.py`](orchestrator.py).
Оркестратор:

- берет описание источника из `config.SOURCE_DEFINITIONS`;
- получает список файлов из `public.etl_source_files`;
- импортирует нужный ETL-модуль;
- передает в него `input_files`;
- управляет безопасным режимом `skip_db`;
- пишет историю запуска в `public.etl_parse_runs`.

---

<a id="stage3-1"></a>
#### Этап 3.1. Справочники населения и геоданных

**Python-скрипты:**

- [`etl/load_population_data.py`](etl/load_population_data.py)
- [`etl/load_regions_data.py`](etl/load_regions_data.py)

**Источники данных:**

- Росстат, официальный бюллетень  
  [«Численность населения Российской Федерации по полу и возрасту»](https://rosstat.gov.ru/folder/12781)
- документация Yandex DataLens по типам данных и геоданным: готовые наборы
  партнера Геоинтеллект, включая регионы РФ  
  [DataLens: Типы данных](https://yandex.cloud/ru/docs/datalens/dataset/data-types)

**Локальные файлы:**

- `data/Другое/Население_2016-2024.csv`
- `data/Другое/Regions.csv`

**Что делают скрипты:**

- приводят данные населения к long-формату;
- формируют поля `Год`, `Пол`, `Возраст`, `Численность`;
- загружают геоданные регионов РФ для построения карт;
- сохраняют контрольные CSV в папку `processed/`;
- при `--write-db` записывают результат в целевые таблицы.

**Целевые таблицы:**

- `public.tb_population`;
- `public.tb_regions`.

**Результат этапа:**

- подготовлен справочник населения для расчета относительных показателей;
- подготовлены полигоны регионов РФ для картографической витрины в DataLens.

---

<a id="stage3-2"></a>
#### Этап 3.2. Формы 8 и 33

**Python-скрипты:**

- [`etl/parse_form8_1000.py`](etl/parse_form8_1000.py)
- [`etl/parse_form33_2100.py`](etl/parse_form33_2100.py)
- [`etl/parse_form33_2200.py`](etl/parse_form33_2200.py)
- [`etl/parse_form33_2300.py`](etl/parse_form33_2300.py)
- [`etl/parse_form33_2310.py`](etl/parse_form33_2310.py)
- [`etl/parse_form33_2400.py`](etl/parse_form33_2400.py)
- [`etl/parse_form33_2500.py`](etl/parse_form33_2500.py)
- [`etl/parse_form33_2600.py`](etl/parse_form33_2600.py)

**Локальный источник:**

- `data/33_8_2015-2024/`

**Что делают скрипты:**

- читают Excel-файлы форм 8 и 33;
- извлекают нужные таблицы и диапазоны строк/граф;
- нормализуют данные в long-формат;
- приводят годы, строки, графы и числовые значения к единому виду;
- сохраняют результат в `processed/*.csv`;
- при `--write-db` загружают данные в PostgreSQL.

**Целевые таблицы:**

- `public.tb_form8_1000`;
- `public.tb_form33_2100`;
- `public.tb_form33_2200`;
- `public.tb_form33_2300`;
- `public.tb_form33_2310`;
- `public.tb_form33_2400`;
- `public.tb_form33_2500`;
- `public.tb_form33_2600`.

**Особенности периода:**

- 2015 год используется только для `form33_2100` и `form33_2500`;
- остальные таблицы форм 8/33 используются за 2016-2024 годы.

**Результат этапа:**

- показатели форм 8 и 33 приведены к единому табличному виду;
- данные готовы для расчетов в `sql/tb_indicators.sql`.

---

<a id="stage3-3"></a>
#### Этап 3.3. Формы 30

**Python-скрипты:**

- [`etl/parse_form30_1100.py`](etl/parse_form30_1100.py)
- [`etl/parse_form30_2513.py`](etl/parse_form30_2513.py)
- [`etl/parse_form30_3100.py`](etl/parse_form30_3100.py)

**Локальный источник:**

- `data/формы 30/*.docx`

**Что делают скрипты:**

- читают DOCX-файлы формы 30;
- находят таблицы 1100, 2513 и 3100;
- извлекают значения по строкам и графам;
- нормализуют номера строк, включая поле `Строка_норм` там, где это нужно для
  расчетов;
- сохраняют контрольные CSV;
- при `--write-db` загружают данные в целевые таблицы.

**Целевые таблицы:**

- `public.tb_form30_1100`;
- `public.tb_form30_2513`;
- `public.tb_form30_3100`.

**Результат этапа:**

- данные формы 30 загружены в структуру, совместимую с расчетной SQL-витриной;
- таблицы индексированы по году, строке и графе для ускорения расчетов.

---

<a id="stage3-4"></a>
#### Этап 3.4. Сборники DOCX/PDF по субъектам РФ

**Python-скрипты:**

- [`etl/parse_tuberculosis_incidence_by_subjects_docx.py`](etl/parse_tuberculosis_incidence_by_subjects_docx.py)
- [`etl/parse_tuberculosis_incidence_by_subjects_pdf.py`](etl/parse_tuberculosis_incidence_by_subjects_pdf.py)

**Локальный источник:**

- `data/Соц_заболевания/*.docx`
- `data/Соц_заболевания/*.pdf`

**Что делают скрипты:**

- извлекают показатели по субъектам РФ из сборников по социально значимым
  заболеваниям;
- обрабатывают DOCX-источники за 2016-2020 годы;
- обрабатывают PDF-источники за 2021-2024 годы;
- приводят показатели к единому long-формату;
- сохраняют имя исходного файла в поле `source_file`;
- сохраняют контрольные CSV;
- при `--write-db` загружают данные в промежуточные таблицы.

**Целевые таблицы:**

- `public.tuberculosis_incidence_by_subjects_docx`;
- `public.tuberculosis_incidence_by_subjects_pdf`.

**Особенность хранения `source_file`:**

Поле `source_file` оставлено только в DOCX/PDF-таблицах. Для таблиц форм оно
удалено, потому что происхождение файлов контролируется через
`public.etl_source_files` и `public.etl_parse_runs`.

**Результат этапа:**

- региональные показатели из DOCX/PDF приведены к единому виду;
- данные подготовлены для объединения в итоговую таблицу субъектов РФ и
  построения картографической витрины.

---

<a id="stage4"></a>
### 🧮 Этап 4. Расчет аналитических показателей

**SQL-файл:** [`sql/tb_indicators.sql`](sql/tb_indicators.sql)

**Целевая таблица:**

- `public.tb_calculated_indicators`

**Что делает SQL-скрипт:**

- очищает расчетную витрину;
- собирает показатели из нормализованных таблиц форм;
- использует численность населения как знаменатель для относительных показателей;
- рассчитывает числитель, знаменатель, значение, формулу и единицу измерения;
- записывает результат в `public.tb_calculated_indicators`;
- выполняет контрольные SELECT-запросы после пересборки.

**Ожидаемый результат:**

- 139 расчетных показателей;
- 9 лет наблюдений: 2016-2024;
- 1251 строка в расчетной витрине.

**Запуск:**

```bash
python3 orchestrator.py calculate-indicators
```

---

<a id="stage5"></a>
### ✅ Этап 5. Контроль качества ETL

**Команда:**

```bash
python3 orchestrator.py validate
```

**Что проверяется:**

- полнота регистрации файлов в `public.etl_source_files`;
- наличие успешных запусков в `public.etl_parse_runs`;
- количество строк в целевых таблицах;
- минимальный и максимальный год в таблицах;
- полнота расчетной витрины;
- отсутствие дублей в `public.tb_calculated_indicators`.

**Ожидаемые объемы данных:**

```text
tb_population: 405
tb_regions: 85
tb_form8_1000: 3762
tb_form33_2100: 780
tb_form33_2200: 324
tb_form33_2300: 648
tb_form33_2310: 63
tb_form33_2400: 882
tb_form33_2500: 700
tb_form33_2600: 648
tb_form30_1100: 54
tb_form30_2513: 63
tb_form30_3100: 36
tuberculosis_incidence_by_subjects_docx: 1900
tuberculosis_incidence_by_subjects_pdf: 1520
tb_calculated_indicators: 1251
```

**Результат этапа:**

- ETL можно проверить одной командой;
- контроль качества отделен от ручных SQL-запросов;
- ошибки загрузки, пропущенные файлы и расхождения в строках видны сразу.

---

<a id="stage6"></a>
### 📊 Этап 6. Подготовка данных для DataLens

**BI-объекты:**

- `public.tb_calculated_indicators` - KPI и графики;
- `public.v_tb_incidence_subjects_datalens_map` - витрина для карты регионов РФ.

**Что используется для карты:**

- показатели по субъектам РФ из `public.tb_incidence_subjects`;
- маппинг названий субъектов из `public.dim_tb_subject_region_map`;
- полигоны регионов из `public.tb_regions`.

**Результат этапа:**

- расчетные показатели доступны для графиков и KPI;
- региональные данные соединены с полигонами;
- данные подготовлены для построения дашборда в Yandex DataLens.

---

<a id="commands"></a>
## 🚀 Основные команды запуска

#### Создать или обновить структуру БД

```bash
python3 orchestrator.py init-db
```

#### Зарегистрировать исходные файлы

```bash
python3 orchestrator.py register-sources
```

#### Посмотреть доступные ETL-задачи

```bash
python3 orchestrator.py list-jobs
```

#### Безопасно прогнать весь ETL без записи в целевые таблицы

```bash
python3 orchestrator.py run-all
```

При доступной БД такой запуск записывает только процесс в
`public.etl_parse_runs`. Целевые таблицы не перезаписываются.

#### Полностью загрузить данные в БД

```bash
python3 orchestrator.py run-all --write-db
```

#### Загрузить данные и пересчитать расчетную витрину

```bash
python3 orchestrator.py run-all --write-db --with-indicators
```

#### Запустить один ETL-job

```bash
python3 orchestrator.py run form33_2100
python3 orchestrator.py run form33_2100 --write-db
```

#### Пересчитать только показатели

```bash
python3 orchestrator.py calculate-indicators
```

#### Проверить качество данных

```bash
python3 orchestrator.py validate
```

---

<a id="rules"></a>
## 📌 Правила проекта

- Схема БД создается только через `sql/database_schema.sql`.
- Расчетные показатели изменяются только в `sql/tb_indicators.sql`.
- `config.py` хранит пути, шаблоны файлов, годы, целевые таблицы и модули парсеров.
- `db_manager.py` является единственным слоем подключения к PostgreSQL/Supabase.
- `orchestrator.py` является единой точкой запуска ETL.
- Парсеры не создают таблицы и не используют `if_exists='replace'`.
- По умолчанию ETL работает безопасно: без `--write-db` целевые таблицы не меняются.
- `source_file` хранится только в DOCX/PDF-таблицах по социально значимым заболеваниям.
- 2015 год используется только для `form33_2100` и `form33_2500`.
- Локальные данные `data/`, результаты `processed/`, файл `.env` и резервная папка
  `scr/` не предназначены для публикации на GitHub.

---

## 📚 Источники данных

- Росстат:  
  [«Численность населения Российской Федерации по полу и возрасту»](https://rosstat.gov.ru/folder/12781)
- Yandex DataLens:  
  [документация по типам данных и готовым геоданным партнера Геоинтеллект](https://yandex.cloud/ru/docs/datalens/dataset/data-types)
- Локальные исходные файлы статистических форм и сборников размещаются в папке
  `data/` и не входят в публичный репозиторий.
