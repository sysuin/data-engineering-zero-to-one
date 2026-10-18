-- stg_frontline__ticket_comments: one row per comment on a ticket, the
-- latest copy any pull fetched. Bodies are customers' words and some
-- hold contact details; they stay as sent (Chapter 24).
SELECT
    comment_id,
    ticket_id,
    strptime(rtrim(created_at, 'Z'), '%Y-%m-%dT%H:%M:%S') AS created_at,
    author,
    public                                          AS is_public,
    body,
    load_date                                       AS pulled_on,
    _load_id
FROM {{ source('raw', 'frontline_ticket_comments') }}
QUALIFY row_number() OVER (
    PARTITION BY comment_id ORDER BY load_date DESC) = 1
