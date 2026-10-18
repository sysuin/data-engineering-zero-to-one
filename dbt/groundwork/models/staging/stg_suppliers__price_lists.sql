-- stg_suppliers__price_lists: one row per product on each supplier's
-- price list in force, its cost in integer cents whatever unit the
-- supplier wrote, and procurement's supplier_id for the sender.
SELECT
    n.supplier_id::INTEGER                          AS supplier_id,
    p.supplier                                      AS supplier_slug,
    upper(trim(p.sku))                              AS sku,
    nullif(trim(p.description), '')                 AS description,
    CASE p.cost_unit
        WHEN 'cents' THEN CAST(p.cost AS BIGINT)
        WHEN 'dollars' THEN CAST(CAST(p.cost AS DECIMAL(12, 2)) * 100
                                 AS BIGINT)
    END                                             AS unit_cost_cents,
    CAST(CASE p.layout
        WHEN 'pemberton' THEN strptime(p.effective_from, '%d/%m/%Y')
        WHEN 'aldridge' THEN strptime(p.effective_from, '%d %B %Y')
        ELSE strptime(p.effective_from, '%Y-%m-%d')
    END AS DATE)                                    AS effective_date,
    p.layout,
    p.file                                          AS source_file,
    p.file_sha256,
    p._load_id
FROM {{ source('raw', 'suppliers_price_lists') }} AS p
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'suppliers' AND f.name = p.file
   AND f.sha256 = p.file_sha256
LEFT JOIN {{ ref('supplier_names') }} AS n
    ON n.source = 'suppliers' AND n.spelling = trim(p.supplier)
