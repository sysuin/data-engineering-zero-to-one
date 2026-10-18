-- dim_supplier: one row per supplier on procurement's master list,
-- under procurement's spelling, and the unknown member.
SELECT
    {{ md5_key('supplier_id') }}                    AS supplier_key,
    CAST(supplier_id AS SMALLINT)                   AS supplier_id,
    supplier_name,
    country_code                                    AS country,
    tier
FROM {{ ref('stg_reference__suppliers') }}
UNION ALL
SELECT {{ md5_key("'-1'") }}, CAST(-1 AS SMALLINT),
       'Unknown supplier', '--', 'Unknown'
