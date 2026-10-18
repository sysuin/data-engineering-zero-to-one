-- dim_account: one row per account per version (SCD Type 2 on segment,
-- owner and status), keyed by Relay's account id. The versions are the
-- audit's; name, region, key-account flag, customer-since date and
-- postcode are today's export's on every version (Type 1). Old CRM ids
-- are not rows here: int_account_identity resolves them to these. The
-- last row is the unknown member, account -1, valid on every date, for
-- a fact whose account the CRM has not exported yet.
-- materialised: table
WITH today AS (
    SELECT * FROM {{ ref('stg_relay__accounts') }}
    WHERE snapshot_date = (SELECT max(snapshot_date)
                           FROM {{ ref('stg_relay__accounts') }})
)
SELECT
    substr(md5(CAST(v.account_id AS VARCHAR) || '|'
               || strftime(v.valid_from, '%Y-%m-%d')), 1, 16)
                                                    AS account_key,
    CAST(v.account_id AS INTEGER)                   AS account_id,
    t.account_name,
    substr(md5(CAST(r.region_id AS VARCHAR)), 1, 16) AS region_key,
    v.segment,
    v.owner,
    v.status,
    t.is_key_account                                AS key_account,
    t.customer_since_date                           AS customer_since,
    v.closed_on,
    t.postcode,
    v.valid_from,
    v.valid_to,
    v.valid_to IS NULL                              AS is_current
FROM {{ ref('int_account_versions') }} AS v
JOIN today AS t USING (account_id)
JOIN {{ ref('stg_reference__regions') }} AS r USING (region_name)
UNION ALL
SELECT substr(md5('-1|2021-01-01'), 1, 16), -1, 'Unknown account',
       substr(md5('-1'), 1, 16), 'Unknown', NULL, 'unknown', false,
       NULL, NULL, NULL, DATE '2021-01-01', NULL, true
