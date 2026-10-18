-- stg_relay__contracts: one row per contract per night's export, from
-- the files in force, both CRMs' values read into one vocabulary.
SELECT
    c.load_date                                     AS snapshot_date,
    CAST(c.contract_id AS INTEGER)                  AS contract_id,
    CAST(c.account_id AS INTEGER)                   AS account_id,
    CAST(CASE c.source_format
        WHEN 'legacy' THEN strptime(c.start_date, '%m/%d/%Y')
        ELSE strptime(c.start_date, '%Y-%m-%d')
    END AS DATE)                                    AS start_date,
    CAST(CASE c.source_format
        WHEN 'legacy' THEN strptime(c.end_date, '%m/%d/%Y')
        ELSE strptime(c.end_date, '%Y-%m-%d')
    END AS DATE)                                    AS end_date,
    CAST(c.term_months AS INTEGER)                  AS term_months,
    CASE c.legacy_terms
        WHEN 'true' THEN true WHEN 'Y' THEN true
        WHEN 'false' THEN false WHEN 'N' THEN false
    END                                             AS is_legacy_terms,
    CAST(nullif(c.discount_pct, '') AS INTEGER)     AS discount_pct,
    replace(lower(nullif(c.outcome, '')), ' ', '_') AS outcome,
    CAST(CASE c.source_format
        WHEN 'legacy'
            THEN strptime(nullif(c.notice_date, ''), '%m/%d/%Y')
        ELSE strptime(nullif(c.notice_date, ''), '%Y-%m-%d')
    END AS DATE)                                    AS notice_date,
    nullif(trim(c.cancellation_reason), '')   AS cancellation_reason,
    strptime(rtrim(nullif(c.updated_at, ''), 'Z'),
             '%Y-%m-%dT%H:%M:%S')                   AS updated_at,
    c.source_format,
    c._load_id
FROM {{ source('raw', 'relay_contracts') }} AS c
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'relay' AND f.name = c.file
   AND f.sha256 = c.file_sha256
