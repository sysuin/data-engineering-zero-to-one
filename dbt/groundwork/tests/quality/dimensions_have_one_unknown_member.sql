-- Chapter 12's promise: each dimension a fact can miss has exactly
-- one unknown member, for a fact to point at until the dimension
-- catches up. The dimensions that break it, and how many they have.
{{ config(tags=['quality']) }}
SELECT * FROM (
    SELECT 'dim_account' AS dim, count(*) AS unknown
    FROM {{ ref('dim_account') }} WHERE account_id = -1
    UNION ALL
    SELECT 'dim_product', count(*)
    FROM {{ ref('dim_product') }} WHERE sku = 'UNKNOWN'
    UNION ALL
    SELECT 'dim_region', count(*)
    FROM {{ ref('dim_region') }} WHERE region_id = -1
    UNION ALL
    SELECT 'dim_supplier', count(*)
    FROM {{ ref('dim_supplier') }} WHERE supplier_id = -1
)
WHERE unknown <> 1
