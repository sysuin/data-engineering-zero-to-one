-- stg_reference__price_events: one row per price change the commercial
-- team recorded, as they wrote it.
SELECT
    CAST(strptime(e.effective_on, '%Y-%m-%d') AS DATE)
                                                    AS effective_date,
    trim(e.scope)                                   AS scope,
    CAST(e.change_pct AS DECIMAL(4, 1))             AS change_pct,
    nullif(trim(e.note), '')                        AS note,
    e._load_id
FROM {{ source('raw', 'reference_price_events') }} AS e
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'reference' AND f.name = e.file
   AND f.sha256 = e.file_sha256
