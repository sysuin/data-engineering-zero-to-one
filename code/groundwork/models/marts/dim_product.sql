-- dim_product: one row per SKU per cost version (SCD Type 2 on
-- unit_cost). Name, category, supplier and list price are the
-- catalogue's today, on every version; the cost is the price lists'.
-- One more row, the unknown member, for a SKU no source has described.
-- materialised: table
SELECT
    substr(md5(v.sku || '|' || strftime(v.valid_from, '%Y-%m-%d')),
           1, 16)                                   AS product_key,
    v.sku,
    p.product_name,
    p.category,
    substr(md5(CAST(p.supplier_id AS VARCHAR)), 1, 16) AS supplier_key,
    CAST(p.list_price_cents * 0.01 AS DECIMAL(18, 2)) AS list_price,
    CAST(v.unit_cost_cents * 0.01 AS DECIMAL(18, 2)) AS unit_cost,
    v.valid_from,
    v.valid_to,
    v.valid_to IS NULL                              AS is_current
FROM {{ ref('int_product_costs') }} AS v
JOIN {{ ref('stg_catalogue__products') }} AS p USING (sku)
UNION ALL
SELECT substr(md5('UNKNOWN|2021-01-01'), 1, 16), 'UNKNOWN',
       'Unknown product', 'Unknown', substr(md5('-1'), 1, 16),
       NULL, NULL, DATE '2021-01-01', NULL, true
