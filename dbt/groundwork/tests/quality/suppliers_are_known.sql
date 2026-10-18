-- Every supplier named in the catalogue and the price lists is one of
-- procurement's, through the seed supplier_names (Chapter 11). A new
-- spelling arrives as a NULL supplier_id. A warning, for procurement:
-- add the spelling to the seed.
{{ config(tags=['quality'], severity='warn') }}
SELECT 'stg_catalogue__products' AS model, supplier_name AS spelling,
       count(*) AS rows
FROM {{ ref('stg_catalogue__products') }}
WHERE supplier_id IS NULL GROUP BY ALL
UNION ALL
SELECT 'stg_suppliers__price_lists', supplier_slug, count(*)
FROM {{ ref('stg_suppliers__price_lists') }}
WHERE supplier_id IS NULL GROUP BY ALL
