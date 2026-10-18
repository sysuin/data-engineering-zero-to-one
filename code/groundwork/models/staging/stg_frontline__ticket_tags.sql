-- stg_frontline__ticket_tags: one row per tag of each ticket's latest
-- version, in the order the API listed them. Filtered as the fields
-- are, by a semi join to the pull that holds the latest version.
-- materialised: view
WITH latest AS (
    SELECT ticket_id, updated_at, load_date
    FROM {{ source('raw', 'frontline_tickets') }}
    QUALIFY row_number() OVER (
        PARTITION BY ticket_id
        ORDER BY updated_at DESC, load_date DESC) = 1
)
SELECT
    t.ticket_id,
    strptime(rtrim(t.updated_at, 'Z'), '%Y-%m-%dT%H:%M:%S')
                                                    AS updated_at,
    t.position,
    t.tag,
    t._load_id
FROM {{ source('raw', 'frontline_ticket_tags') }} AS t
SEMI JOIN latest USING (ticket_id, updated_at, load_date)
