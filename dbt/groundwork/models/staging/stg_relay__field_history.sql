-- stg_relay__field_history: one row per change Relay recorded to an
-- account or contract field, from the audit files in force.
SELECT
    strptime(rtrim(h.changed_at, 'Z'), '%Y-%m-%dT%H:%M:%S')
                                                    AS changed_at,
    h.object                                        AS record_type,
    CAST(h.record_id AS INTEGER)                    AS record_id,
    h.field                                         AS field_name,
    nullif(h.old_value, '')                         AS old_value,
    nullif(h.new_value, '')                         AS new_value,
    trim(h.changed_by)                              AS changed_by,
    h._load_id
FROM {{ source('raw', 'relay_field_history') }} AS h
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'relay' AND f.name = h.file
   AND f.sha256 = h.file_sha256
