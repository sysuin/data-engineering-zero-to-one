-- Every line finance posted to sales revenue by hand is in finance's
-- register, seeds/finance/ledger_journals.csv, with the same month,
-- region, amount and words; and every line in the register is still
-- in the ledger. A warning, for finance: the warehouse is not wrong,
-- but the reconciliation cannot close until the register is.
{{ config(tags=['quality'], severity='warn') }}
WITH posted AS (
    SELECT strftime(period_month, '%Y-%m') AS period,
           region_code AS region, amount_cents, description
    FROM {{ ref('stg_ledger__gl_revenue') }}
    WHERE description <> 'Sales journal'
),
documented AS (
    SELECT period, region, CAST(amount * 100 AS BIGINT) AS amount_cents,
           description
    FROM {{ ref('ledger_journals') }}
)
(SELECT 'posted, not documented' AS problem, *
 FROM (SELECT * FROM posted EXCEPT SELECT * FROM documented))
UNION ALL
(SELECT 'documented, not posted', *
 FROM (SELECT * FROM documented EXCEPT SELECT * FROM posted))
