-- stg_relay__migration_map: one row per old CRM id that IT matched to
-- a Relay account on 1 March 2024, as IT's file says.
SELECT
    CAST(m.old_crm_id AS INTEGER)                   AS old_crm_id,
    CAST(m.relay_id AS INTEGER)                     AS account_id,
    m.matched_by,
    m._load_id
FROM {{ source('raw', 'relay_migration_map') }} AS m
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'relay' AND f.name = m.file
   AND f.sha256 = m.file_sha256
