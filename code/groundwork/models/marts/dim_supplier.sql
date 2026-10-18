-- dim_supplier: one row per supplier on procurement's master list,
-- under procurement's spelling, and the unknown member.
-- materialised: table
SELECT
    substr(md5(CAST(supplier_id AS VARCHAR)), 1, 16) AS supplier_key,
    CAST(supplier_id AS SMALLINT)                   AS supplier_id,
    supplier_name,
    country_code                                    AS country,
    tier
FROM {{ ref('stg_reference__suppliers') }}
UNION ALL
SELECT substr(md5('-1'), 1, 16), CAST(-1 AS SMALLINT),
       'Unknown supplier', '--', 'Unknown'
