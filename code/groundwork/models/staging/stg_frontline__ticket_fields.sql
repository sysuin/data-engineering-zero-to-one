-- stg_frontline__ticket_fields: one row per custom field of each
-- ticket's latest version. The semi join keeps only rows from the pull
-- that holds that version; it adds no column and no row.
-- materialised: view
WITH latest AS (
    SELECT ticket_id, updated_at, load_date
    FROM {{ source('raw', 'frontline_tickets') }}
    QUALIFY row_number() OVER (
        PARTITION BY ticket_id
        ORDER BY updated_at DESC, load_date DESC) = 1
)
SELECT
    f.ticket_id,
    strptime(rtrim(f.updated_at, 'Z'), '%Y-%m-%dT%H:%M:%S')
                                                    AS updated_at,
    f.field_id,
    f.name                                          AS field_name,
    nullif(trim(f.value), '')                       AS value,
    f._load_id
FROM {{ source('raw', 'frontline_ticket_fields') }} AS f
SEMI JOIN latest USING (ticket_id, updated_at, load_date)
