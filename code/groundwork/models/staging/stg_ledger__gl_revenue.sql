-- stg_ledger__gl_revenue: one row per journal line posted to sales
-- revenue in a closed month, from the month files in force, the
-- amount in integer cents.
-- materialised: view
SELECT
    CAST(strptime(g.period || '-01', '%Y-%m-%d') AS DATE)
                                                    AS period_month,
    g.entity,
    g.region                                        AS region_code,
    g.gl_account,
    g.gl_name,
    g.description,
    CAST(CAST(g.amount AS DECIMAL(14, 2)) * 100 AS BIGINT)
                                                    AS amount_cents,
    g._load_id
FROM {{ source('raw', 'ledger_gl_revenue') }} AS g
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'ledger' AND f.name = g.file
   AND f.sha256 = g.file_sha256
