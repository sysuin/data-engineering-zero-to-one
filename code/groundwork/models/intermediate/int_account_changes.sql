-- int_account_changes: one row per change Relay's audit recorded to an
-- account's segment, owner, status or closing date, or its creation,
-- dated by the calendar of the account's depot.
-- materialised: view
WITH home AS (                       -- each account's depot calendar
    SELECT a.account_id, r.timezone
    FROM {{ ref('stg_relay__accounts') }} AS a
    JOIN {{ ref('stg_reference__regions') }} AS r USING (region_name)
    QUALIFY a.snapshot_date = max(a.snapshot_date)
            OVER (PARTITION BY a.account_id)
)
SELECT
    h.record_id                                     AS account_id,
    h.changed_at,
    CAST(timezone(home.timezone, timezone('UTC', h.changed_at))
         AS DATE)                                   AS change_date,
    h.field_name,
    h.old_value,
    h.new_value,
    h.changed_by
FROM {{ ref('stg_relay__field_history') }} AS h
JOIN home ON home.account_id = h.record_id
WHERE h.record_type = 'account'
  AND h.field_name IN ('created', 'segment', 'owner', 'status',
                       'closed_on')
