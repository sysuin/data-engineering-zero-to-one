-- fct_contracts: one row per contract, as today's export holds it. The
-- account is the version valid on the contract's first day; a contract
-- older than the account's history (which begins on 1 January 2022 at
-- the earliest) takes the account's first version, the nearest
-- evidence there is. Dates are Relay's calendar dates, and end_date is
-- the contract's last day, not the day after.
WITH c AS (
    SELECT c.*, coalesce(i.account_id, -1) AS acct
    FROM {{ ref('stg_relay__contracts') }} AS c
    LEFT JOIN {{ ref('int_account_identity') }} AS i
        ON i.crm_id = c.account_id
    WHERE c.snapshot_date = (SELECT max(snapshot_date)
                             FROM {{ ref('stg_relay__contracts') }})
),
first AS (
    SELECT account_id, min(valid_from) AS first_from
    FROM {{ ref('dim_account') }}
    GROUP BY account_id
)
SELECT
    CAST(c.contract_id AS INTEGER)                  AS contract_id,
    coalesce(a.account_key, u.account_key)          AS account_key,
    coalesce(cur.region_key, u.region_key)          AS region_key,
    CAST(strftime(c.start_date, '%Y%m%d') AS INTEGER) AS start_date_key,
    CAST(strftime(c.end_date, '%Y%m%d') AS INTEGER) AS end_date_key,
    CAST(strftime(c.notice_date, '%Y%m%d') AS INTEGER)
                                                    AS notice_date_key,
    c.start_date,
    c.end_date,
    CAST(c.term_months AS SMALLINT)                 AS term_months,
    c.is_legacy_terms                               AS legacy_terms,
    CAST(c.discount_pct AS SMALLINT)                AS discount_pct,
    c.outcome,
    c.cancellation_reason
FROM c
CROSS JOIN (SELECT account_key, region_key FROM {{ ref('dim_account') }}
            WHERE account_id = -1) AS u
LEFT JOIN first AS f ON f.account_id = c.acct
LEFT JOIN {{ ref('dim_account') }} AS a
    ON a.account_id = c.acct
   AND greatest(c.start_date, f.first_from) >= a.valid_from
   AND greatest(c.start_date, f.first_from)
       < coalesce(a.valid_to, DATE '9999-12-31')
LEFT JOIN {{ ref('dim_account') }} AS cur
    ON cur.account_id = c.acct AND cur.is_current
