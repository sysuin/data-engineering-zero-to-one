-- dim_date: one row per calendar day, 2021-01-01 to 2026-12-31, a year
-- either side of Meridian's records. No source sends it; the runner's
-- project filled it from Python (groundwork/dates.py), and here it is
-- one SELECT. A working day is Monday to Friday; a fiscal year starts
-- in var('fiscal_start_month') and is named for the year it ends in.
{% set fs = var('fiscal_start_month') %}
WITH days AS (
    SELECT CAST(d AS DATE) AS day
    FROM range(DATE '2021-01-01', DATE '2027-01-01',
               INTERVAL 1 DAY) AS r(d)
)
SELECT
    CAST(strftime(day, '%Y%m%d') AS INTEGER)        AS date_key,
    day                                             AS date,
    CAST(year(day) AS SMALLINT)                     AS year,
    CAST(quarter(day) AS TINYINT)                   AS quarter,
    CAST(month(day) AS TINYINT)                     AS month,
    monthname(day)                                  AS month_name,
    CAST(day(day) AS TINYINT)                       AS day_of_month,
    CAST(dayofyear(day) AS SMALLINT)                AS day_of_year,
    CAST(isodow(day) AS TINYINT)                    AS day_of_week,
    dayname(day)                                    AS day_name,
    isodow(day) >= 6                                AS is_weekend,
    isodow(day) <= 5                                AS is_working_day,
    CAST(CASE WHEN isodow(day) <= 5 THEN
        count(*) FILTER (isodow(day) <= 5) OVER (
            PARTITION BY year(day), month(day) ORDER BY day)
    END AS TINYINT)                         AS working_day_of_month,
    CAST(isoyear(day) AS SMALLINT)                  AS iso_year,
    CAST(weekofyear(day) AS TINYINT)                AS iso_week,
    CAST(date_trunc('week', day) AS DATE)           AS week_start,
    CAST(date_trunc('month', day) AS DATE)          AS month_start,
    CAST(last_day(day) AS DATE)                     AS month_end,
    day = last_day(day)                             AS is_month_end,
    CAST(year(day) + CASE WHEN {{ fs }} > 1
                           AND month(day) >= {{ fs }} THEN 1 ELSE 0 END
         AS SMALLINT)                               AS fiscal_year,
    CAST((month(day) - {{ fs }} + 12) % 12 // 3 + 1 AS TINYINT)
                                                    AS fiscal_quarter,
    CAST((month(day) - {{ fs }} + 12) % 12 + 1 AS TINYINT)
                                                    AS fiscal_month
FROM days
ORDER BY day
