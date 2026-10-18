-- Every placed order and line in Tradeline's staging reaches the
-- facts, and nothing else does: counts and revenue in cents, side by
-- side. The cancelled are left out on both sides by the same rule.
{{ config(tags=['quality']) }}
WITH staged AS (
    SELECT count(DISTINCT o.order_id) AS orders, count(*) AS lines,
           sum(l.qty * l.unit_price_cents) AS cents
    FROM {{ ref('stg_tradeline__order_lines') }} AS l
    JOIN {{ ref('stg_tradeline__orders') }} AS o USING (order_id)
    WHERE o.status = 'placed'
),
facts AS (
    SELECT (SELECT count(*) FROM {{ ref('fct_orders') }}) AS orders,
           count(*) AS lines,
           CAST(sum(revenue) * 100 AS BIGINT) AS cents
    FROM {{ ref('fct_order_lines') }}
)
SELECT s.*, f.orders AS fact_orders, f.lines AS fact_lines,
       f.cents AS fact_cents
FROM staged AS s, facts AS f
WHERE (s.orders, s.lines, s.cents) <> (f.orders, f.lines, f.cents)
