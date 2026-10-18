-- stg_reference__regions: one row per sales region, its depot and the
-- time zone the depot keeps, from the file operations maintains.
-- materialised: view
SELECT
    CAST(r.region_id AS INTEGER)                    AS region_id,
    trim(r.name)                                    AS region_name,
    trim(r.depot)                                   AS depot,
    r.timezone,
    r._load_id
FROM {{ source('raw', 'reference_regions') }} AS r
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'reference' AND f.name = r.file
   AND f.sha256 = r.file_sha256
