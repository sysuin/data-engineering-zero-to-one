-- stg_frontline__ticket_versions: one row per version of a ticket that
-- some pull saw. Each night's pull holds the tickets that changed, as
-- they stood then, so raw keeps a ticket once for every night it
-- changed; a version two overlapping pulls both saw is kept once,
-- from the first. What happened between two pulls is not here.
-- materialised: view
SELECT
    ticket_id,
    strptime(rtrim(updated_at, 'Z'), '%Y-%m-%dT%H:%M:%S') AS updated_at,
    status,
    priority,
    nullif(trim(assignee), '')                      AS assignee,
    load_date                                       AS pulled_on,
    _load_id
FROM {{ source('raw', 'frontline_tickets') }}
QUALIFY row_number() OVER (
    PARTITION BY ticket_id, updated_at ORDER BY load_date) = 1
