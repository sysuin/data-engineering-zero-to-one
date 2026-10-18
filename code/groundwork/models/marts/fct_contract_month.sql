-- fct_contract_month: one row per contract per month end it is in
-- force (a periodic snapshot). A contract is in force on a month end
-- that falls between its first and last days, both included, which is
-- how Relay writes them. The month ends run from January 2022 to the
-- last one before today's export. contracts_in_force is 1 on every
-- row: sum it across contracts on one month end, never across months.
-- materialised: table
WITH c AS (
    SELECT c.*, coalesce(i.account_id, -1) AS acct
    FROM {{ ref('stg_relay__contracts') }} AS c
    LEFT JOIN {{ ref('int_account_identity') }} AS i
        ON i.crm_id = c.account_id
    WHERE c.snapshot_date = (SELECT max(snapshot_date)
                             FROM {{ ref('stg_relay__contracts') }})
),
months AS (
    SELECT CAST(last_day(m) AS DATE) AS month_end
    FROM range(DATE '2022-01-01',
               (SELECT max(snapshot_date)
                FROM {{ ref('stg_relay__contracts') }}),
               INTERVAL 1 MONTH) AS r(m)
    WHERE last_day(m) < (SELECT max(snapshot_date)
                         FROM {{ ref('stg_relay__contracts') }})
),
rows AS (
    SELECT c.contract_id, c.acct, c.discount_pct, m.month_end,
           (year(m.month_end) * 12 + month(m.month_end))
           - (year(c.start_date) * 12 + month(c.start_date)) + 1
                                                    AS months_in_force,
           (year(c.end_date) * 12 + month(c.end_date))
           - (year(m.month_end) * 12 + month(m.month_end))
                                                    AS months_remaining
    FROM c
    JOIN months AS m
        ON m.month_end BETWEEN c.start_date AND c.end_date
)
SELECT
    CAST(r.contract_id AS INTEGER)                  AS contract_id,
    CAST(strftime(r.month_end, '%Y%m%d') AS INTEGER)
                                            AS month_end_date_key,
    coalesce(a.account_key, u.account_key)          AS account_key,
    coalesce(cur.region_key, u.region_key)          AS region_key,
    r.month_end,
    CAST(r.months_in_force AS SMALLINT)             AS months_in_force,
    CAST(r.months_remaining AS SMALLINT)            AS months_remaining,
    CAST(r.discount_pct AS SMALLINT)                AS discount_pct,
    CAST(1 AS TINYINT)                          AS contracts_in_force
FROM rows AS r
CROSS JOIN (SELECT account_key, region_key FROM {{ ref('dim_account') }}
            WHERE account_id = -1) AS u
LEFT JOIN {{ ref('dim_account') }} AS a
    ON a.account_id = r.acct
   AND r.month_end >= a.valid_from
   AND r.month_end < coalesce(a.valid_to, DATE '9999-12-31')
LEFT JOIN {{ ref('dim_account') }} AS cur
    ON cur.account_id = r.acct AND cur.is_current
