-- int_account_versions: one row per account per version of its
-- segment, owner and status, rebuilt from Relay's audit. Today's export
-- says where each account stands; the audit, read backwards, says what
-- it was before each change. A version begins on the depot's date of a
-- change; two changes on one day leave the day's last value. The first
-- version begins when the account was created, or on 1 January 2022,
-- the first night anyone kept, for accounts older than that.
-- materialised: table
WITH today AS (
    SELECT account_id, segment, owner_name AS owner, status,
           CAST(closed_date AS VARCHAR) AS closed_on
    FROM {{ ref('stg_relay__accounts') }}
    WHERE snapshot_date = (SELECT max(snapshot_date)
                           FROM {{ ref('stg_relay__accounts') }})
),
days AS (                            -- one value per field per day
    SELECT account_id, field_name, change_date,
           arg_min(old_value, changed_at) AS old_value,
           arg_max(new_value, changed_at) AS new_value
    FROM {{ ref('int_account_changes') }}
    WHERE field_name <> 'created'
    GROUP BY ALL
),
starts AS (
    SELECT t.account_id,
           coalesce(min(c.change_date), DATE '2022-01-01') AS valid_from
    FROM today AS t
    LEFT JOIN {{ ref('int_account_changes') }} AS c
        ON c.account_id = t.account_id AND c.field_name = 'created'
    GROUP BY t.account_id
    UNION
    SELECT account_id, change_date FROM days
    WHERE field_name IN ('segment', 'owner', 'status')
),
valued AS (       -- each field as it stood that day: the last change
    SELECT s.account_id, s.valid_from, f.field_name,   -- on or before
           coalesce(                       -- it, else what the next
               (SELECT arg_max({'v': d.new_value}, d.change_date)
                FROM days d                -- change replaced, else
                WHERE d.account_id = s.account_id     -- today's value
                  AND d.field_name = f.field_name
                  AND d.change_date <= s.valid_from),
               (SELECT arg_min({'v': d.old_value}, d.change_date)
                FROM days d
                WHERE d.account_id = s.account_id
                  AND d.field_name = f.field_name
                  AND d.change_date > s.valid_from),
               {'v': CASE f.field_name
                   WHEN 'segment' THEN t.segment
                   WHEN 'owner' THEN t.owner
                   WHEN 'status' THEN t.status
                   ELSE t.closed_on END}).v           AS value
    FROM starts AS s
    JOIN today AS t USING (account_id)
    CROSS JOIN (VALUES ('segment'), ('owner'), ('status'),
                       ('closed_on')) AS f(field_name)
),
wide AS (
    SELECT account_id, valid_from,
           any_value(value) FILTER (field_name = 'segment') AS segment,
           any_value(value) FILTER (field_name = 'owner') AS owner,
           any_value(value) FILTER (field_name = 'status') AS status,
           any_value(value) FILTER (field_name = 'closed_on')
                                                    AS closed_on
    FROM valued
    GROUP BY ALL
),
marked AS (                          -- a version that changes nothing
    SELECT *,                        -- is not a version
           (segment, owner, status) IS DISTINCT FROM
           lag((segment, owner, status)) OVER (
               PARTITION BY account_id ORDER BY valid_from) AS is_new
    FROM wide
)
SELECT account_id, valid_from,
       lead(valid_from) OVER (PARTITION BY account_id
                              ORDER BY valid_from) AS valid_to,
       segment, owner, status,
       CAST(closed_on AS DATE) AS closed_on
FROM marked
WHERE is_new
