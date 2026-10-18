-- pack_realisation: list price against price charged, by category and
-- month, section 4 of the pack. Both sides in integer cents.
-- materialised: table
SELECT l.month, p.category, sum(l.value_cents) AS value_cents,
       sum(l.qty * p.list_price_cents) AS list_value_cents
FROM {{ ref('int_pack__lines') }} AS l
JOIN {{ ref('stg_catalogue__products') }} AS p USING (sku)
GROUP BY l.month, p.category
