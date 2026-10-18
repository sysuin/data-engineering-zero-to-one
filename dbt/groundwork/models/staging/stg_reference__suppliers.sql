-- stg_reference__suppliers: one row per supplier on procurement's
-- master list, which owns supplier_id.
SELECT
    CAST(s.supplier_id AS INTEGER)                  AS supplier_id,
    trim(s.name)                                    AS supplier_name,
    s.country                                       AS country_code,
    s.tier,
    s._load_id
FROM {{ source('raw', 'reference_suppliers') }} AS s
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'reference' AND f.name = s.file
   AND f.sha256 = s.file_sha256
