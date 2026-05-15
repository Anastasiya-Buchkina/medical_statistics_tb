/* ============================================================================
   Проект: Медицинская статистика по туберкулезу
   Файл: tb_indicators.sql

   Назначение
   ----------
   Скрипт полностью пересобирает расчетную витрину
   public.tb_calculated_indicators на основе уже загруженных ETL-таблиц.

   Входные таблицы
   ---------------
   - public.tb_population;
   - public.tb_form8_1000;
   - public.tb_form30_1100, public.tb_form30_2513, public.tb_form30_3100;
   - public.tb_form33_2100, public.tb_form33_2200, public.tb_form33_2300,
     public.tb_form33_2310, public.tb_form33_2400, public.tb_form33_2500,
     public.tb_form33_2600.

   Результат
   ---------
   - расчетный период: 2016-2024;
   - ожидаемый объем: 139 показателей x 9 лет = 1251 строка;
   - один показатель представлен стабильным id и рассчитывается по годам;
   - округление не выполняется, чтобы не терять точность;
   - если знаменатель отсутствует или равен 0, значение показателя = NULL.

   Архитектурное правило
   ---------------------
   DDL для public.tb_calculated_indicators находится в sql/database_schema.sql.
   Этот файл не создает структуру БД, а только очищает и пересобирает витрину.

   Важные особенности источников
   -----------------------------
   - public.tb_form30_1100: номер строки используется из поля "Строка_норм";
   - public.tb_form30_3100: номер строки используется из поля "Строка_норм";
   - public.tb_form33_2310: вместо поля "Строка" используется поле "Графа";
   - показатель 2200:4/01 из эталона соответствует "Строка" = 1.
   ============================================================================ */

BEGIN;

SET LOCAL search_path = public;


/* ============================================================================
   1. Очистка расчетной витрины
   ============================================================================ */

TRUNCATE TABLE public.tb_calculated_indicators;


/* ============================================================================
   2. Расчет показателей
   ============================================================================ */

WITH years AS (
    SELECT DISTINCT "Год"
    FROM public.tb_population
    WHERE "Год" BETWEEN 2016 AND 2024
      AND "Пол" = 'всего'
      AND "Возраст" = 'Всего'
),
metrics AS (
    SELECT
        'id_1.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом всего (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб тер'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:5) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:5) / (population:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.3'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:5) / (population:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.4'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 0-4 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 0-4'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:6) / (population:age_0_4) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-4') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.4.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 0-4 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 0-4 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:6) / (population:age_0_4:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '0-4') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.4.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 0-4 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 0-4 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:6) / (population:age_0_4:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '0-4') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.5'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 5-6 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 5-6'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:7) / (population:age_5_6) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '5-6') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.5.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 5-6 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 5-6 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:7) / (population:age_5_6:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '5-6') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.5.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 5-6 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 5-6 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:7) / (population:age_5_6:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '5-6') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.6'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 7-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 7-14'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:8) / (population:age_7_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '7-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.6.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 7-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 7-14 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:8) / (population:age_7_14:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '7-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.6.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 7-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 7-14 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:8) / (population:age_7_14:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '7-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.7'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 15-17'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.7.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 15-17 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:9) / (population:age_15_17:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.7.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 15-17 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:9) / (population:age_15_17:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.8'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 18-24 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 18-24'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:10) / (population:age_18_24) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (10)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '18-24') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.8.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 18-24 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 18-24 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:10) / (population:age_18_24:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (10)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '18-24') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.8.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 18-24 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 18-24 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:10) / (population:age_18_24:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (10)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '18-24') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.9'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 25-34 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 25-34'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:11) / (population:age_25_34) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (11)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '25-34') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.9.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 25-34 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 25-34 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:11) / (population:age_25_34:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (11)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '25-34') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.9.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 25-34 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 25-34 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:11) / (population:age_25_34:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (11)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '25-34') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.10'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 35-44 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 35-44'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:12) / (population:age_35_44) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (12)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '35-44') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.10.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 35-44 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 35-44 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:12) / (population:age_35_44:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (12)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '35-44') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.10.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 35-44 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 35-44 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:12) / (population:age_35_44:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (12)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '35-44') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.11'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 45-54 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 45-54'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:13) / (population:age_45_54) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (13)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '45-54') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.11.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 45-54 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 45-54 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:13) / (population:age_45_54:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (13)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '45-54') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.11.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 45-54 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 45-54 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:13) / (population:age_45_54:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (13)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '45-54') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.12'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 55-64 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 55-64'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:14) / (population:age_55_64) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (14)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '55-64') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.12.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 55-64 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 55-64 муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:14) / (population:age_55_64:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (14)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '55-64') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.12.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 55-64 года (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 55-64 жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:14) / (population:age_55_64:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (14)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '55-64') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.13'::text AS id,
        'Показатель первичной заболеваемости туберкулезом населения 65 лет и более (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 65+'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1+2:15) / (population:age_65_plus) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 2) AND "Графа" IN (15)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '65+') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.13.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом мужского населения 65 лет и более (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 65+ муж'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:1:15) / (population:age_65_plus:male) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (15)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'мужчины'
               AND "Возраст" = '65+') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_1.13.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом женского населения 65 лет и более (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 65+ жен'::text AS "Алиас",
        '1000'::text AS "Источник",
        '(1000:2:15) / (population:age_65_plus:female) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form8_1000 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (15)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'женщины'
               AND "Возраст" = '65+') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.1.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом обслуживаемой территории (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб МЗ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:7:4 + 2200:11+12:3) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            ((SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (4)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2200 WHERE "Год" = yrs."Год" AND "Строка" IN (11, 12) AND "Графа" IN (3))) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.1.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:7:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.1.3'::text AS id,
        'Показатель первичной заболеваемости туберкулезом подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:7:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.1.4'::text AS id,
        'Показатель общей заболеваемости туберкулезом всего (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:7:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.1.5'::text AS id,
        'Показатель общей заболеваемости туберкулезом детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:7:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.1.6'::text AS id,
        'Показатель общей заболеваемости туберкулезом подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:7:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.2.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом органов дыхания (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ОД туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:1:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.2.2'::text AS id,
        'Показатель первичной заболеваемости ТОД детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ОД туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:1:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.2.3'::text AS id,
        'Показатель первичной заболеваемости ТОД подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ОД туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:1:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.2.4'::text AS id,
        'Показатель общей заболеваемости туберкулезом органов дыхания (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ОД туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:1:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.2.5'::text AS id,
        'Показатель общей заболеваемости ТОД детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ОД туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:1:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.2.6'::text AS id,
        'Показатель общей заболеваемости ТОД подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ОД туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:1:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.3.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом легких всего (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ Л туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:2:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.3.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом легких детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ Л туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:2:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.3.3'::text AS id,
        'Показатель первичной заболеваемости туберкулезом легких подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ Л туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:2:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.3.4'::text AS id,
        'Показатель общей заболеваемости туберкулезом легких всего (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ Л туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:2:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.3.5'::text AS id,
        'Показатель общей заболеваемости туберкулезом легких детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ Л туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:2:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.3.6'::text AS id,
        'Показатель общей заболеваемости туберкулезом легких подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ Л туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:2:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.1'::text AS id,
        'Показатель первичной заболеваемости фиброзно-кавернозным туберкулезом (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ФКТ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.2'::text AS id,
        'Показатель первичной заболеваемости ФКТ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ФКТ 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.3'::text AS id,
        'Показатель первичной заболеваемости ФКТ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ФКТ 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.4'::text AS id,
        'Доля ФКТ среди впервые выявленных больных (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля ФКТ ВВБ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:4) / (2100:7:4) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (4)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.5'::text AS id,
        'Показатель общей заболеваемости фиброзно-кавернозным туберкулезом (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ФКТ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.6'::text AS id,
        'Показатель общей заболеваемости ФКТ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ФКТ 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.7'::text AS id,
        'Показатель общей заболеваемости ФКТ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ФКТ 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.4.8'::text AS id,
        'Доля ФКТ среди контингентов больных (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля ФКТ контингент'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:3:7) / (2100:7:7) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (7)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом в фазе распада (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ CV+'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.2'::text AS id,
        'Показатель первичной заболеваемости ТБ в фазе распада детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ CV+ 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.3'::text AS id,
        'Показатель первичной заболеваемости ТБ в фазе распада подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ CV+ 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.4'::text AS id,
        'Доля туберкулеза с распадом легочной ткани среди ВВБ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля CV+ ВВБ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:4) / (2100:7:4) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (4)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.5'::text AS id,
        'Показатель общей заболеваемости туберкулезом в фазе распада (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ CV+'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.6'::text AS id,
        'Показатель общей заболеваемости ТБ в фазе распада детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ CV+ 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.7'::text AS id,
        'Показатель общей заболеваемости ТБ в фазе распада подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ CV+ 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.5.8'::text AS id,
        'Доля туберкулеза с распадом легочной ткани среди контингентов больных (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля CV+ контингент'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:4:7) / (2100:7:7) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (7)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.6.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом с бактериовыделением ОД (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ БК+'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2500:1:3) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.6.2'::text AS id,
        'Доля туберкулеза с бактериовыделением среди ВВБ ОД (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля БК+ ВВБ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2500:1:3) / (2100:1:4) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (4)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.7.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом внелегочной локализации (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ВНЛ туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:6:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.7.2'::text AS id,
        'Показатель первичной заболеваемости ТБ ВНЛ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ВНЛ туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:6:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.7.3'::text AS id,
        'Показатель первичной заболеваемости ТБ ВНЛ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ВНЛ туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:6:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.7.4'::text AS id,
        'Показатель общей заболеваемости туберкулезом внелегочной локализации (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ВНЛ туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:6:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.7.5'::text AS id,
        'Показатель общей заболеваемости ТБ ВНЛ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ВНЛ туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:6:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.7.6'::text AS id,
        'Показатель общей заболеваемости ТБ ВНЛ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ВНЛ туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:6:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.8.1'::text AS id,
        'Показатель первичной инвалидности в связи с туберкулезом (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПИ туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:8:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.8.2'::text AS id,
        'Показатель первичной инвалидности ТБ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПИ туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:8:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.8.3'::text AS id,
        'Показатель первичной инвалидности ТБ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПИ туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:8:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.8.4'::text AS id,
        'Показатель общей инвалидности в связи с туберкулезом (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОИ туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:8:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.8.5'::text AS id,
        'Показатель общей инвалидности ТБ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОИ туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:8:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.8.6'::text AS id,
        'Показатель общей инвалидности ТБ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОИ туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:8:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.9.1'::text AS id,
        'Охват обследованием на АТ к ВИЧ ВВБ туберкулезом (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват АТ ВВ ВИЧ'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:11:4) / (2100:7:4) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (4)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.9.2'::text AS id,
        'Охват обследованием на АТ к ВИЧ ВВБ ТБ детей 0-14 лет (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват АТ ВВ ВИЧ 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:11:5) / (2100:7:5) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (5)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.9.3'::text AS id,
        'Охват обследованием на АТ к ВИЧ ВВБ ТБ подростков 15-17 лет (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват АТ ВВ ВИЧ 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:11:6) / (2100:7:6) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (6)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.9.4'::text AS id,
        'Охват обследованием на АТ к ВИЧ контингентов ТБ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват АТ ВИЧ контингент'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:11:7) / (2100:7:7) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (7)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.9.5'::text AS id,
        'Охват обследованием на АТ к ВИЧ контингентов ТБ детей 0-14 лет (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват АТ ВИЧ 0-14 контингент'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:11:8) / (2100:7:8) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (8)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.9.6'::text AS id,
        'Охват обследованием на АТ к ВИЧ контингентов ТБ подростков 15-17 лет (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват АТ ВИЧ 15-17 контингент'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:11:9) / (2100:7:9) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (9)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.1'::text AS id,
        'Показатель первичной заболеваемости ТБ в сочетании с ВИЧ (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ВИЧ+туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:13:4) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (13) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.2'::text AS id,
        'Показатель первичной заболеваемости ТБ+ВИЧ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ВИЧ+туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:13:5) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (13) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.3'::text AS id,
        'Показатель первичной заболеваемости ТБ+ВИЧ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ ВИЧ+туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:13:6) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (13) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.4'::text AS id,
        'Показатель общей заболеваемости ТБ в сочетании с ВИЧ (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ВИЧ+туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:13:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (13) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.5'::text AS id,
        'Показатель общей заболеваемости ТБ+ВИЧ детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ВИЧ+туб 0-14'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:13:8) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (13) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.6'::text AS id,
        'Показатель общей заболеваемости ТБ+ВИЧ подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ ВИЧ+туб 15-17'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2100:13:9) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (13) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.7'::text AS id,
        'Показатель смертности больных ТБ+ВИЧ от других причин (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПС ВИЧ+туб другие'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2310:7) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2310 WHERE "Год" = yrs."Год" AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_2.10.8'::text AS id,
        'Показатель смертности больных ТБ+ВИЧ от туберкулеза (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПС ВИЧ+туб от туб'::text AS "Алиас",
        '2100'::text AS "Источник",
        '(2310:2) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2310 WHERE "Год" = yrs."Год" AND "Графа" IN (2)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.1'::text AS id,
        'Показатель первичной заболеваемости туберкулезом по данным медицинских осмотров и туберкулинодиагностики всего (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ мед.осм'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:4/01:3) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric
             FROM public.tb_form33_2200
             WHERE "Год" = yrs."Год"
               AND "Строка" IN (1)
               AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.2'::text AS id,
        'Показатель первичной заболеваемости туберкулезом по данным медицинских осмотров и туберкулинодиагностики детей 0-14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ мед.осм 0-14'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:4/01:4) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric
             FROM public.tb_form33_2200
             WHERE "Год" = yrs."Год"
               AND "Строка" IN (1)
               AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.3'::text AS id,
        'Показатель первичной заболеваемости туберкулезом по данным медицинских осмотров и туберкулинодиагностики подростков 15-17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ мед.осм 15-17'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:4/01:5) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric
             FROM public.tb_form33_2200
             WHERE "Год" = yrs."Год"
               AND "Строка" IN (1)
               AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.4'::text AS id,
        'Доля впервые выявленных больных туберкулезом методом туберкулинодиагностики (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля ВВ иммунол'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:2:3) / (2200:4/01:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2200 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Значение")::numeric
             FROM public.tb_form33_2200
             WHERE "Год" = yrs."Год"
               AND "Строка" IN (1)
               AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.5'::text AS id,
        'Доля впервые выявленных больных туберкулезом методом флюорографии (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля ВВ ФЛГ'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:4:3) / (2200:4/01:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2200 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Значение")::numeric
             FROM public.tb_form33_2200
             WHERE "Год" = yrs."Год"
               AND "Строка" IN (1)
               AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.6'::text AS id,
        'Доля впервые выявленных больных туберкулезом бактериологическими методами (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля ВВ бак'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:6:3) / (2200:4/01:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2200 WHERE "Год" = yrs."Год" AND "Строка" IN (6) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Значение")::numeric
             FROM public.tb_form33_2200
             WHERE "Год" = yrs."Год"
               AND "Строка" IN (1)
               AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.7'::text AS id,
        'Показатель посмертной диагностики туберкулеза (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПС посмерт'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:11:3) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2200 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.8'::text AS id,
        'Показатель смертности от туберкулеза (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПС туб'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2200:11:3 + 2300:7:3 + 2300:7:9) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            ((SELECT SUM("Значение")::numeric FROM public.tb_form33_2200 WHERE "Год" = yrs."Год" AND "Строка" IN (11) AND "Графа" IN (3)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (3)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (9))) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.9'::text AS id,
        'Показатель смертности больных туберкулезом от других причин (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПС больных др. причин'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2300:8:3 + 2300:8:9) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            ((SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (3)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (8) AND "Графа" IN (9))) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_3.10'::text AS id,
        'Доля умерших от туберкулеза больных, состоявших на учете менее 1 года (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'ПС до года'::text AS "Алиас",
        '2200'::text AS "Источник",
        '(2310:1) / (2300:7:3 + 2300:7:9) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2310 WHERE "Год" = yrs."Год" AND "Графа" IN (1)) AS "Числитель",
            ((SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (3)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (9))) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_4.1'::text AS id,
        'Охват больных туберкулезом органов дыхания с МБТ+ исследованиями на МЛУ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват МБТ+ МЛУ'::text AS "Алиас",
        '2300'::text AS "Источник",
        '(2500:2:3+6+8+9+10) / (2500:1:3+6+8+9+10) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3, 6, 8, 9, 10)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3, 6, 8, 9, 10)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_4.2'::text AS id,
        'Показатель частоты впервые выявленных больных ТОД с МЛУ'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ПЗ МЛУ'::text AS "Алиас",
        '2300'::text AS "Источник",
        '(2500:3:3) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_4.3'::text AS id,
        'Показатель распространенности туберкулеза органов дыхания с МЛУ'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ОЗ МЛУ'::text AS "Алиас",
        '2300'::text AS "Источник",
        '(2500:3:16) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (16)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_4.4'::text AS id,
        'Доля ТОД с первичной МЛУ МБТ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля перв. МЛУ'::text AS "Алиас",
        '2300'::text AS "Источник",
        '(2500:3:3) / (2500:2:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_4.5'::text AS id,
        'Доля ТОД с вторичной МЛУ МБТ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Доля втор. МЛУ'::text AS "Алиас",
        '2300'::text AS "Источник",
        '(2500:3:6+8+9+10+11) / (2500:1:3+6+8+9+10) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (6, 8, 9, 10, 11)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3, 6, 8, 9, 10)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_5.1'::text AS id,
        'Показатель частоты рецидивов всего'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'Рецидивы'::text AS "Алиас",
        'effectiveness'::text AS "Источник",
        '(2300:1:3+9) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3, 9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_5.2'::text AS id,
        'Частота рецидивов из III группы'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'Рецидивы III гр.'::text AS "Алиас",
        'effectiveness'::text AS "Источник",
        '(2300:2:3+9) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3, 9)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_5.3'::text AS id,
        'Охват ХП взрослых'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват ХП взр.'::text AS "Алиас",
        'effectiveness'::text AS "Источник",
        '(2400:4:5) / (2400:4:4) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2400 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2400 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (4)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_5.4'::text AS id,
        'Клиническое излечение ТОД'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Клин.излеч.'::text AS "Алиас",
        'effectiveness'::text AS "Источник",
        '(2300:4:3) / (avg(2100[prev]:1:7; 2100:1:7)) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2300 WHERE "Год" = yrs."Год" AND "Строка" IN (4) AND "Графа" IN (3)) AS "Числитель",
            (((SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" - 1 AND "Строка" IN (1) AND "Графа" IN (7)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (7))) / 2.0) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_5.5'::text AS id,
        'Абацилирование'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Абацил.'::text AS "Алиас",
        'effectiveness'::text AS "Источник",
        '(2500:1+4:14) / (avg(2500[prev]:1+4:16; 2500:1+4:16)) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 4) AND "Графа" IN (14)) AS "Числитель",
            (((SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" - 1 AND "Строка" IN (1, 4) AND "Графа" IN (16)) + (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 4) AND "Графа" IN (16))) / 2.0) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.1'::text AS id,
        'Охват госпитализацией впервые выявленных больных туберкулезом (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват госпит. ВВ'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:1:6) / (2100:7:4) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2100 WHERE "Год" = yrs."Год" AND "Строка" IN (7) AND "Графа" IN (4)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.2'::text AS id,
        'Охват госпитализацией впервые выявленных больных туберкулезом с МБТ+ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват госпит. ВВ+'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:2:6) / (2500:1+4:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 4) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.3'::text AS id,
        'Охват госпитализацией контингентов больных туберкулезом с МБТ+ (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват госпит. контингент.+'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:2:3) / (2500:1+4:16) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2500 WHERE "Год" = yrs."Год" AND "Строка" IN (1, 4) AND "Графа" IN (16)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.4'::text AS id,
        'Госпитализированная заболеваемость впервые выявленных больных туберкулезом (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ГЗ ВВ'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:1:6) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.5'::text AS id,
        'Госпитализированная заболеваемость контингентов больных туберкулезом (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ГЗ контингент'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:1:3) / (population:all) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.6'::text AS id,
        'Госпитализированная заболеваемость впервые выявленных больных туберкулезом детей до 14 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ГЗ ВВ 0-14'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:1:7) / (population:age_0_14) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (7)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_6.7'::text AS id,
        'Госпитализированная заболеваемость впервые выявленных больных туберкулезом подростков 15–17 лет (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ГЗ ВВ 15-17'::text AS "Алиас",
        '2600'::text AS "Источник",
        '(2600:1:8) / (population:age_15_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form33_2600 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (8)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_7.1'::text AS id,
        'Охват населения всеми видами осмотров (в %)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват проф.осмотр'::text AS "Алиас",
        '2513'::text AS "Источник",
        '(2513:1:3) / (population:all) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (1) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_7.2'::text AS id,
        'Охват иммунодиагностикой детей 0-14 лет (в %)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват иммунол. 0-14'::text AS "Алиас",
        '2513'::text AS "Источник",
        '(2513:4+5:3) / (population:age_0_14) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (4, 5) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-14') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_7.3'::text AS id,
        'Охват флюорографическими осмотрами населения старше 15 лет (в %)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'Охват ФЛГ'::text AS "Алиас",
        '2513'::text AS "Источник",
        '(2513:2:3) / (population:age_over_15) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '15+') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_7.4'::text AS id,
        'Выявляемость туберкулеза методом флюорографии (1000 осмотренных)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 1000
        END AS "Значение",
        'Выявляемость ФЛГ'::text AS "Алиас",
        '2513'::text AS "Источник",
        '(2513:2:5) / (2513:2:3) * 1000'::text AS "Формула",
        'на 1000 осмотренных'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (2) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_7.5'::text AS id,
        'Выявляемость туберкулеза бактериоскопически (1000 осмотренных)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 1000
        END AS "Значение",
        'Выявляемость бак'::text AS "Алиас",
        '2513'::text AS "Источник",
        '(2513:3:5) / (2513:3:3) * 1000'::text AS "Формула",
        'на 1000 осмотренных'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_2513 WHERE "Год" = yrs."Год" AND "Строка" IN (3) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.1'::text AS id,
        'Обеспеченность населения фтизиатрами (на 10 000 населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 10000
        END AS "Значение",
        'Врачи туб'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:111:3) / (population:all) * 10000'::text AS "Формула",
        'на 10 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.2'::text AS id,
        'Укомплектованность кадрами (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'УК врачи туб'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:111:4) / (1100:111:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.3'::text AS id,
        'Укомплектованность физическими лицами (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'УФ врачи туб'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:111:9) / (1100:111:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.4'::text AS id,
        'Коэффициент совместительства'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 1
        END AS "Значение",
        'КС врачи туб'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:111:4) / (1100:111:9) * 1'::text AS "Формула",
        'коэффициент'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (111) AND "Графа" IN (9)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.5'::text AS id,
        'Обеспеченность населения фтизиатрами участковыми (на 10 000 населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 10000
        END AS "Значение",
        'Врачи туб участ'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:112:3) / (population:all) * 10000'::text AS "Формула",
        'на 10 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (3)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = 'Всего') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.6'::text AS id,
        'Укомплектованность кадрами  фтизиатрами участковыми (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'УК врачи туб участ'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:112:4) / (1100:112:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.7'::text AS id,
        'Укомплектованность  фтизиатрами участковыми физическими лицами (%)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100
        END AS "Значение",
        'УФ врачи туб участ'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:112:9) / (1100:112:3) * 100'::text AS "Формула",
        '%'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (9)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (3)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_8.8'::text AS id,
        'Коэффициент совместительства  фтизиатрами участковыми'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 1
        END AS "Значение",
        'КС врачи туб участ'::text AS "Алиас",
        '1100'::text AS "Источник",
        '(1100:112:4) / (1100:112:9) * 1'::text AS "Формула",
        'коэффициент'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (4)) AS "Числитель",
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_1100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (112) AND "Графа" IN (9)) AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_9.1'::text AS id,
        'Обеспеченность населения туберкулезными койками для взрослых (на 10 000 населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 10000
        END AS "Значение",
        'Койки туб взр'::text AS "Алиас",
        '3100'::text AS "Источник",
        '(3100:57:5) / (population:age_18_plus) * 10000'::text AS "Формула",
        'на 10 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_3100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (57) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '18+') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_9.2'::text AS id,
        'Обеспеченность населения туберкулезными койками для детей (на 10 000 населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 10000
        END AS "Значение",
        'Койки туб дет'::text AS "Алиас",
        '3100'::text AS "Источник",
        '(3100:58:5) / (population:age_0_17) * 10000'::text AS "Формула",
        'на 10 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_3100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (58) AND "Графа" IN (5)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-17') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_9.3'::text AS id,
        'Госпитализированная заболеваемость взрослого населения (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ГЗ взр'::text AS "Алиас",
        '3100'::text AS "Источник",
        '(3100:57:6) / (population:age_18_plus) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_3100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (57) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '18+') AS "Знаменатель"
    ) calc

    UNION ALL

    SELECT
        'id_9.4'::text AS id,
        'Госпитализированная заболеваемость детского населения 0-17 (на 100 тыс. населения)'::text AS "Показатель",
        yrs."Год"::integer AS "Год",
        calc."Числитель",
        calc."Знаменатель",
        CASE
            WHEN calc."Знаменатель" IS NULL OR calc."Знаменатель" = 0 THEN NULL
            ELSE calc."Числитель" / NULLIF(calc."Знаменатель", 0) * 100000
        END AS "Значение",
        'ГЗ дети 0-17'::text AS "Алиас",
        '3100'::text AS "Источник",
        '(3100:58:6) / (population:age_0_17) * 100000'::text AS "Формула",
        'на 100 000 населения'::text AS "Единица"
    FROM years yrs
    CROSS JOIN LATERAL (
        SELECT
            (SELECT SUM("Значение")::numeric FROM public.tb_form30_3100 WHERE "Год" = yrs."Год" AND "Строка_норм" IN (58) AND "Графа" IN (6)) AS "Числитель",
            (SELECT SUM("Численность")::numeric
             FROM public.tb_population
             WHERE "Год" = yrs."Год"
               AND "Пол" = 'всего'
               AND "Возраст" = '0-17') AS "Знаменатель"
    ) calc
)

INSERT INTO public.tb_calculated_indicators (
    id,
    "Показатель",
    "Год",
    "Числитель",
    "Знаменатель",
    "Значение",
    "Алиас",
    "Источник",
    "Формула",
    "Единица"
)
SELECT
    id,
    "Показатель",
    "Год",
    "Числитель",
    "Знаменатель",
    "Значение",
    "Алиас",
    "Источник",
    "Формула",
    "Единица"
FROM metrics
ORDER BY id, "Год";

COMMIT;


/* ============================================================================
   3. Контроль результата

   Эти запросы не изменяют данные. Их удобно запускать после пересборки витрины,
   чтобы быстро проверить объем, покрытие по годам и возможные проблемы качества.
   ============================================================================ */

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT id) AS indicators_count,
    COUNT(DISTINCT "Год") AS years_count,
    MIN("Год") AS min_year,
    MAX("Год") AS max_year
FROM public.tb_calculated_indicators;

SELECT
    "Год",
    COUNT(*) AS rows_count
FROM public.tb_calculated_indicators
GROUP BY "Год"
ORDER BY "Год";

SELECT
    id,
    "Год",
    COUNT(*) AS duplicates_count
FROM public.tb_calculated_indicators
GROUP BY id, "Год"
HAVING COUNT(*) > 1
ORDER BY id, "Год";

SELECT
    id,
    "Показатель",
    "Год",
    "Числитель",
    "Знаменатель",
    "Значение",
    "Формула"
FROM public.tb_calculated_indicators
WHERE "Знаменатель" IS NULL
   OR "Знаменатель" = 0
ORDER BY id, "Год";

SELECT
    id,
    "Показатель",
    "Год",
    "Числитель",
    "Знаменатель",
    "Значение",
    "Алиас",
    "Источник",
    "Формула",
    "Единица"
FROM public.tb_calculated_indicators
ORDER BY id, "Год";
