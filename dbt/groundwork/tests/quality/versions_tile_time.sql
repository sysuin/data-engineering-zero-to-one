-- Chapter 12's promise for every SCD2 dimension: a key's versions
-- follow one another with no gap and no overlap, each ending the day
-- the next begins, and only the last is open and current.
{{ config(tags=['quality']) }}
WITH versions AS (
    SELECT 'dim_account' AS dim, account_id::VARCHAR AS id,
           valid_from, valid_to, is_current
    FROM {{ ref('dim_account') }}
    UNION ALL
    SELECT 'dim_product', sku, valid_from, valid_to, is_current
    FROM {{ ref('dim_product') }}
),
ordered AS (
    SELECT *, lead(valid_from) OVER (PARTITION BY dim, id
                                     ORDER BY valid_from) AS next_from
    FROM versions
)
SELECT * FROM ordered
WHERE valid_to IS DISTINCT FROM next_from
   OR is_current <> (valid_to IS NULL)
   OR valid_from >= coalesce(valid_to, DATE '9999-12-31')
