-- pack_supplier: sales by supplier and month, section 3 of the pack,
-- joined to procurement's list by name, as the old job joined it.
-- materialised: table
SELECT l.month, s.supplier_name AS supplier, s.tier, count(*) AS lines,
       sum(l.value_cents) AS value_cents
FROM {{ ref('int_pack__lines') }} AS l
JOIN {{ ref('stg_catalogue__products') }} AS p USING (sku)
JOIN {{ ref('stg_reference__suppliers') }} AS s
    ON s.supplier_name = p.supplier_name
GROUP BY l.month, s.supplier_name, s.tier
