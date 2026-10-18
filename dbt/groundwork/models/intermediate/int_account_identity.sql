-- int_account_identity: one row per account id the CRM ever exported,
-- and the Relay account it belongs to. Relay's own ids map to
-- themselves; the old CRM's second records map through IT's migration
-- map or, where IT's map has no row, the matches sales operations
-- confirmed (seeds.account_matches). An id that resolves to nothing
-- keeps a NULL account_id, so a test can count it.
WITH seen AS (
    SELECT account_id AS crm_id, max(snapshot_date) AS last_seen
    FROM {{ ref('stg_relay__accounts') }}
    GROUP BY account_id
),
latest AS (
    SELECT max(snapshot_date) AS night
    FROM {{ ref('stg_relay__accounts') }}
),
mapped AS (
    SELECT old_crm_id AS crm_id, account_id, 'it_map' AS matched_by
    FROM {{ ref('stg_relay__migration_map') }}
    UNION ALL
    SELECT CAST(old_crm_id AS INTEGER), CAST(account_id AS INTEGER),
           'rule'
    FROM {{ ref('account_matches') }}
)
SELECT
    s.crm_id,
    CASE WHEN s.last_seen = l.night THEN s.crm_id
         ELSE m.account_id END                      AS account_id,
    CASE WHEN s.last_seen = l.night THEN 'relay'
         ELSE coalesce(m.matched_by, 'unresolved') END AS matched_by,
    s.last_seen
FROM seen AS s
CROSS JOIN latest AS l
LEFT JOIN mapped AS m ON m.crm_id = s.crm_id
