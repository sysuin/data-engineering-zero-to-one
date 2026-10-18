-- fct_order_lines: one row per line of a placed, not-cancelled order.
-- The order's day is its depot's day: placed_at, a UTC moment, read on
-- the calendar of the account's region. Every key is the version valid
-- on that day: the account as it stood, and the product at the cost
-- in force. A key the dimensions cannot give yet is the unknown
-- member's, so the line is counted now and re-pointed later.
--
-- Incremental: a run restates whole orders, for Chapter 13's reasons.
-- Changed: the order or a line has updated_at after var('since'), when
-- the last run read Tradeline, less the lookback. Unknown: a line
-- points at an unknown member. Stale: a line's version no longer
-- covers its day. Gone: the order is no longer placed (post_hook).
-- With no 'since', every order is taken again. The two refs below
-- are read only on the incremental path, which dbt does not see when
-- it parses the project; the hints put them in the graph.
-- depends_on: {{ ref('stg_tradeline__orders') }}
-- depends_on: {{ ref('stg_tradeline__order_lines') }}
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} WHERE order_id IN (
                   SELECT order_id FROM {{ this }}
                   ANTI JOIN {{ ref('int_order_lines') }}
                   USING (order_id))"
) }}

WITH
{% if is_incremental() %}
redo AS (
  {% if var('since') %}
    {% set start %}(TIMESTAMP '{{ var("since") }}'
        - INTERVAL {{ var('lookback_hours') }} HOUR){% endset %}
    SELECT order_id FROM {{ ref('stg_tradeline__orders') }}
    WHERE updated_at >= {{ start }}
    UNION
    SELECT order_id FROM {{ ref('stg_tradeline__order_lines') }}
    WHERE updated_at >= {{ start }}
    UNION
    SELECT f.order_id FROM {{ this }} AS f
    JOIN {{ ref('dim_account') }} AS a USING (account_key)
    JOIN {{ ref('dim_product') }} AS p USING (product_key)
    WHERE a.account_id = -1 OR p.sku = 'UNKNOWN'
       OR NOT (f.order_date >= a.valid_from AND f.order_date
               < coalesce(a.valid_to, DATE '9999-12-31'))
       OR NOT (f.order_date >= p.valid_from AND f.order_date
               < coalesce(p.valid_to, DATE '9999-12-31'))
    UNION
    SELECT order_id FROM {{ this }}
    ANTI JOIN {{ ref('dim_account') }} USING (account_key)
    UNION
    SELECT order_id FROM {{ this }}
    ANTI JOIN {{ ref('dim_product') }} USING (product_key)
  {% else %}
    SELECT order_id FROM {{ ref('int_order_lines') }}
  {% endif %}
),
lines AS (
    SELECT * FROM {{ ref('int_order_lines') }}
    SEMI JOIN redo USING (order_id)
),
{% else %}
lines AS (
    SELECT * FROM {{ ref('int_order_lines') }}
),
{% endif %}
home AS (                            -- each account's depot calendar
    SELECT a.account_id, a.region_key, r.timezone
    FROM {{ ref('dim_account') }} AS a
    JOIN {{ ref('dim_region') }} AS r USING (region_key)
    WHERE a.is_current
),
orders AS (                         -- each order once
    SELECT DISTINCT order_id, coalesce(account_id, -1) AS account_id,
           placed_at
    FROM lines
),
days AS (                    -- its day, on its depot's calendar
    SELECT o.order_id,
           coalesce(h.region_key, u.region_key)     AS region_key,
           CAST(timezone(coalesce(h.timezone, u.timezone),
                         timezone('UTC', o.placed_at))
                AS DATE)                            AS order_date
    FROM orders AS o
    LEFT JOIN home AS h USING (account_id)
    JOIN home AS u ON u.account_id = -1     -- the unknown's: UTC
),
dated AS (
    SELECT *
    FROM lines JOIN days USING (order_id)
),
unknown AS (
    SELECT (SELECT account_key FROM {{ ref('dim_account') }}
            WHERE account_id = -1)                  AS account_key,
           (SELECT product_key FROM {{ ref('dim_product') }}
            WHERE sku = 'UNKNOWN')                  AS product_key,
           (SELECT supplier_key FROM {{ ref('dim_supplier') }}
            WHERE supplier_id = -1)                 AS supplier_key
)
SELECT
    CAST(d.order_id AS BIGINT)                      AS order_id,
    CAST(d.line_no AS SMALLINT)                     AS line_no,
    CAST(d.order_number AS BIGINT)                  AS order_number,
    CAST(strftime(d.order_date, '%Y%m%d') AS INTEGER) AS date_key,
    coalesce(a.account_key, u.account_key)          AS account_key,
    coalesce(p.product_key, u.product_key)          AS product_key,
    coalesce(p.supplier_key, u.supplier_key)        AS supplier_key,
    d.region_key,
    d.channel,
    d.placed_at,
    d.order_date,
    CAST(d.qty AS INTEGER)                          AS qty,
    CAST(d.discount_pct AS SMALLINT)                AS discount_pct,
    CAST(d.unit_price_cents * 0.01 AS DECIMAL(18, 2)) AS unit_price,
    p.unit_cost,
    CAST(d.qty * d.unit_price_cents * 0.01 AS DECIMAL(18, 2))
                                                    AS revenue,
    CAST(d.qty * p.unit_cost AS DECIMAL(18, 2))     AS cost,
    CAST(d.qty * d.unit_price_cents * 0.01 - d.qty * p.unit_cost
         AS DECIMAL(18, 2))                         AS gross_margin
FROM dated AS d
CROSS JOIN unknown AS u
LEFT JOIN {{ ref('dim_account') }} AS a
    ON a.account_id = d.account_id
   AND d.order_date >= a.valid_from
   AND d.order_date < coalesce(a.valid_to, DATE '9999-12-31')
LEFT JOIN {{ ref('dim_product') }} AS p
    ON p.sku = d.sku
   AND d.order_date >= p.valid_from
   AND d.order_date < coalesce(p.valid_to, DATE '9999-12-31')
