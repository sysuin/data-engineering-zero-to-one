-- Each staging model's key, found in Chapter 11, holds: the rows of
-- any key that appears twice. Tested here, at the boundary, a
-- duplicate is caught before a join downstream multiplies it.
{{ config(tags=['quality']) }}
SELECT 'stg_tradeline__orders' AS model,
       order_id::VARCHAR AS key,
       count(*) AS n
FROM {{ ref('stg_tradeline__orders') }} GROUP BY ALL HAVING n > 1
UNION ALL
SELECT 'stg_tradeline__order_lines', order_id || '-' || line_no,
       count(*)
FROM {{ ref('stg_tradeline__order_lines') }} GROUP BY ALL
HAVING count(*) > 1
UNION ALL
SELECT 'stg_relay__accounts', account_id || '@' || snapshot_date,
       count(*)
FROM {{ ref('stg_relay__accounts') }} GROUP BY ALL HAVING count(*) > 1
UNION ALL
SELECT 'stg_relay__contracts', contract_id || '@' || snapshot_date,
       count(*)
FROM {{ ref('stg_relay__contracts') }} GROUP BY ALL HAVING count(*) > 1
UNION ALL
SELECT 'stg_frontline__tickets', ticket_id, count(*)
FROM {{ ref('stg_frontline__tickets') }} GROUP BY ALL HAVING count(*) > 1
