-- dim_region: one row per region, with the ledger's code for it and
-- the calendar its depot keeps, and one row for the unknown member:
-- the region of an account the warehouse has not met yet.
SELECT
    {{ md5_key('r.region_id') }}                    AS region_key,
    CAST(r.region_id AS SMALLINT)                   AS region_id,
    r.region_name,
    c.region_code,
    r.depot,
    r.timezone
FROM {{ ref('stg_reference__regions') }} AS r
JOIN {{ ref('region_codes') }} AS c
    ON CAST(c.region_id AS INTEGER) = r.region_id
UNION ALL
SELECT {{ md5_key("'-1'") }}, CAST(-1 AS SMALLINT), 'Unknown', '--',
       'Unknown', 'UTC'
