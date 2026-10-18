-- stg_frontline__tickets: one row per ticket, its latest version
-- across every pull. Each pull holds the tickets that changed since
-- the last one, so the latest version is the one with the latest
-- updated_at (on a tie, the later pull). The JSON payload stays in raw.
WITH versions AS (
    SELECT *
    FROM {{ source('raw', 'frontline_tickets') }}
    QUALIFY row_number() OVER (
        PARTITION BY ticket_id
        ORDER BY updated_at DESC, load_date DESC) = 1
)
SELECT
    ticket_id,
    strptime(rtrim(created_at, 'Z'), '%Y-%m-%dT%H:%M:%S') AS created_at,
    strptime(rtrim(updated_at, 'Z'), '%Y-%m-%dT%H:%M:%S') AS updated_at,
    status,
    priority,
    channel,
    language,
    requester_account_id                            AS account_id,
    requester_name,
    requester_email,
    nullif(trim(assignee), '')                      AS assignee,
    nullif(trim(description), '')                   AS description,
    upper(trim(sku))                                AS sku,
    load_date                                       AS pulled_on,
    _load_id
FROM versions
