-- int_product_costs: one row per SKU per cost version. A version
-- begins on the effective date of the first price list that changed
-- the SKU's cost, and ends when the next one begins; a list that
-- repeats the cost, or a month with no list, changes nothing.
-- materialised: table
WITH listed AS (
    SELECT sku, effective_date, unit_cost_cents,
           unit_cost_cents IS DISTINCT FROM lag(unit_cost_cents) OVER (
               PARTITION BY sku ORDER BY effective_date) AS is_new
    FROM {{ ref('stg_suppliers__price_lists') }}
)
SELECT sku,
       effective_date                               AS valid_from,
       lead(effective_date) OVER (PARTITION BY sku
                                  ORDER BY effective_date) AS valid_to,
       unit_cost_cents
FROM listed
WHERE is_new
