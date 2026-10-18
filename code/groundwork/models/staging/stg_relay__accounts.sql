-- stg_relay__accounts: one row per account per night's export, from
-- the files in force. Raw keeps both CRMs' values as text; each era is
-- read its own way here, so a row means the same whichever wrote it.
-- materialised: view
SELECT
    a.load_date                                     AS snapshot_date,
    CAST(a.account_id AS INTEGER)                   AS account_id,
    trim(a.account_name)                            AS account_name,
    a.region                                        AS region_name,
    a.segment,
    trim(a.owner)                                   AS owner_name,
    CAST(CASE a.source_format
        WHEN 'legacy' THEN strptime(a.customer_since, '%m/%d/%Y')
        ELSE strptime(a.customer_since, '%Y-%m-%d')
    END AS DATE)                              AS customer_since_date,
    CASE a.source_format
        WHEN 'legacy' THEN lpad(a.postcode, 5, '0')
        ELSE a.postcode
    END                                             AS postcode,
    CASE a.key_account
        WHEN 'true' THEN true WHEN 'Y' THEN true
        WHEN 'false' THEN false WHEN 'N' THEN false
    END                                             AS is_key_account,
    lower(a.status)                                 AS status,
    CAST(CASE a.source_format
        WHEN 'legacy' THEN strptime(nullif(a.closed_on, ''), '%m/%d/%Y')
        ELSE strptime(nullif(a.closed_on, ''), '%Y-%m-%d')
    END AS DATE)                                    AS closed_date,
    CASE a.source_format
        WHEN 'legacy' THEN regexp_replace(trim(a.contact_name),
                                          '^(.+), (.+)$', '\2 \1')
        ELSE trim(a.contact_name)
    END                                             AS contact_name,
    trim(a.contact_email)                           AS contact_email,
    a.contact_phone,
    strptime(rtrim(nullif(a.updated_at, ''), 'Z'),
             '%Y-%m-%dT%H:%M:%S')                   AS updated_at,
    a.source_format,
    a._load_id
FROM {{ source('raw', 'relay_accounts') }} AS a
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'relay' AND f.name = a.file
   AND f.sha256 = a.file_sha256
