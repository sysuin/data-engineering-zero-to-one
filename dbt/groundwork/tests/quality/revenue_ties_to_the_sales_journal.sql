-- Revenue by month and region, as the warehouse builds it, against
-- the ledger's sales journal, for every month the ledger has closed:
-- the cells where the two differ by as much as a cent. Journals
-- finance posts by hand are not in it; ledger_journals_are_documented
-- deals with those.
{{ config(tags=['quality']) }}
WITH ours AS (
    SELECT date_trunc('month', f.order_date) AS period_month,
           r.region_code, sum(f.revenue) AS revenue
    FROM {{ ref('fct_order_lines') }} AS f
    JOIN {{ ref('dim_region') }} AS r USING (region_key)
    GROUP BY ALL
),
ledger AS (
    SELECT period_month, region_code,
           sum(amount_cents) * 0.01 AS revenue
    FROM {{ ref('stg_ledger__gl_revenue') }}
    WHERE description = 'Sales journal'
    GROUP BY ALL
)
SELECT period_month, region_code,
       o.revenue AS warehouse, l.revenue AS ledger
FROM ledger AS l
FULL JOIN ours AS o USING (period_month, region_code)
WHERE period_month IN (SELECT period_month FROM ledger)
  AND o.revenue IS DISTINCT FROM l.revenue
