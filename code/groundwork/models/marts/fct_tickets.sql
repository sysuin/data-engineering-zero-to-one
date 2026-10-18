-- fct_tickets: one row per ticket, its life from opened to closed (an
-- accumulating snapshot). Each milestone is the first moment a pull
-- saw the ticket in that state, from every version raw holds:
--   opened_at       when the ticket was created
--   first_reply_at  when an agent took it, status new to open.
--                   Frontline records no agent replies as comments
--                   (every comment is the customer's), so this is the
--                   desk's first response as Frontline can show it.
--   solved_at       first seen solved
--   closed_at       first seen closed
-- A state no pull saw, because the ticket passed through it between
-- two pulls or before the first, stays NULL: never guessed. Dates are
-- the depot's; the account is the version of the day it opened.
-- materialised: table
WITH seen AS (
    SELECT ticket_id,
           min(updated_at) FILTER (status = 'open')   AS first_reply_at,
           min(updated_at) FILTER (status = 'solved') AS solved_at,
           min(updated_at) FILTER (status = 'closed') AS closed_at
    FROM {{ ref('stg_frontline__ticket_versions') }}
    GROUP BY ticket_id
),
fields AS (
    SELECT ticket_id,
           any_value(value) FILTER (field_name = 'desk')     AS desk,
           any_value(value) FILTER (field_name = 'category') AS category
    FROM {{ ref('stg_frontline__ticket_fields') }}
    GROUP BY ticket_id
),
comments AS (
    SELECT ticket_id, count(*) AS n
    FROM {{ ref('stg_frontline__ticket_comments') }}
    GROUP BY ticket_id
),
home AS (
    SELECT a.account_id, a.region_key, r.timezone
    FROM {{ ref('dim_account') }} AS a
    JOIN {{ ref('dim_region') }} AS r USING (region_key)
    WHERE a.is_current
),
t AS (
    SELECT t.ticket_id, t.created_at AS opened_at, t.status,
           t.priority, t.channel, t.sku, s.first_reply_at,
           s.solved_at, s.closed_at,
           CASE WHEN t.account_id IS NOT NULL
                THEN coalesce(i.account_id, -1) END AS account_id,
           CASE WHEN t.account_id IS NOT NULL
                THEN coalesce(h.region_key, u.region_key) END
                                                    AS region_key,
           coalesce(h.timezone, 'UTC')              AS tz
    FROM {{ ref('stg_frontline__tickets') }} AS t
    JOIN seen AS s USING (ticket_id)
    LEFT JOIN {{ ref('int_account_identity') }} AS i
        ON i.crm_id = t.account_id
    LEFT JOIN home AS h ON h.account_id = i.account_id
    JOIN home AS u ON u.account_id = -1
),
d AS (                       -- each moment on the depot's calendar
    SELECT *,
        CAST(timezone(tz, timezone('UTC', opened_at)) AS DATE) AS od,
        CAST(timezone(tz, timezone('UTC', first_reply_at)) AS DATE)
                                                    AS fd,
        CAST(timezone(tz, timezone('UTC', solved_at)) AS DATE) AS sd,
        CAST(timezone(tz, timezone('UTC', closed_at)) AS DATE) AS cd
    FROM t
)
SELECT
    d.ticket_id,
    CASE WHEN d.account_id IS NOT NULL
         THEN coalesce(a.account_key, u.account_key) END AS account_key,
    d.region_key,
    CASE WHEN d.sku IS NOT NULL
         THEN coalesce(p.product_key, u.product_key) END AS product_key,
    CAST(strftime(d.od, '%Y%m%d') AS INTEGER)       AS opened_date_key,
    CAST(strftime(d.fd, '%Y%m%d') AS INTEGER)  AS first_reply_date_key,
    CAST(strftime(d.sd, '%Y%m%d') AS INTEGER)       AS solved_date_key,
    CAST(strftime(d.cd, '%Y%m%d') AS INTEGER)       AS closed_date_key,
    d.opened_at,
    d.first_reply_at,
    d.solved_at,
    d.closed_at,
    d.channel,
    f.category,
    d.priority,
    f.desk,
    d.status,
    CAST(coalesce(c.n, 0) AS SMALLINT)              AS comment_count,
    CAST(date_diff('second', d.opened_at, d.first_reply_at) / 3600
         AS DECIMAL(10, 2))                     AS hours_to_first_reply,
    CAST(date_diff('second', d.opened_at, d.solved_at) / 3600
         AS DECIMAL(10, 2))                         AS hours_to_solve
FROM d
CROSS JOIN (
    SELECT (SELECT account_key FROM {{ ref('dim_account') }}
            WHERE account_id = -1) AS account_key,
           (SELECT product_key FROM {{ ref('dim_product') }}
            WHERE sku = 'UNKNOWN') AS product_key) AS u
LEFT JOIN fields AS f USING (ticket_id)
LEFT JOIN comments AS c USING (ticket_id)
LEFT JOIN {{ ref('dim_account') }} AS a
    ON a.account_id = d.account_id
   AND d.od >= a.valid_from
   AND d.od < coalesce(a.valid_to, DATE '9999-12-31')
LEFT JOIN {{ ref('dim_product') }} AS p
    ON p.sku = d.sku
   AND d.od >= p.valid_from
   AND d.od < coalesce(p.valid_to, DATE '9999-12-31')
