-- pack_category: sales by category and month, section 2 of the pack.
-- materialised: table
SELECT l.month, p.category, count(*) AS lines,
       sum(l.value_cents) AS value_cents
FROM {{ ref('int_pack__lines') }} AS l
JOIN {{ ref('stg_catalogue__products') }} AS p USING (sku)
GROUP BY l.month, p.category
