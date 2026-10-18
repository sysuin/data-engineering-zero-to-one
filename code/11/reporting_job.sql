-- monthly_sales.sql: the reporting job's sales pack.
-- Runs on the 2nd of each month. Reads Tradeline, the catalogue and
-- procurement's supplier list where they live.
-- 2023-02  first version (sections 1-2)
-- 2023-06  section 3 for procurement, copied from section 2
-- 2023-11  section 4, list price vs price charged
-- 2024-05  section 2: SKUs in the catalogue trimmed and upper-cased
--          (merchandising typed some by hand)
-- DO NOT EDIT WITHOUT TELLING THE COMMERCIAL TEAM

-- 1. Sales by channel and month ------------------------------------
CREATE OR REPLACE TEMP TABLE pack_channel AS
WITH lines AS (
    SELECT l.order_id,
           l.sku,
           l.qty,
           l.unit_price_cents,
           l.qty * l.unit_price_cents AS value_cents
    FROM tradeline.order_lines l
),
orders AS (
    SELECT o.id,
           o.channel,
           strftime(CAST(o.placed_at AS TIMESTAMP), '%Y-%m') AS month
    FROM tradeline.orders o
),
channels AS (
    SELECT code, label AS name FROM tradeline.channels
)
SELECT o.month,
       c.name AS channel,
       count(*) AS lines,
       sum(l.value_cents) AS value_cents
FROM lines l
JOIN orders o ON o.id = l.order_id
JOIN channels c ON c.code = o.channel
GROUP BY 1, 2;

-- 2. Sales by category and month -----------------------------------
CREATE OR REPLACE TEMP TABLE pack_category AS
WITH lines AS (
    SELECT l.order_id,
           l.sku,
           l.qty,
           l.unit_price_cents,
           l.qty * l.unit_price_cents AS value_cents
    FROM tradeline.order_lines l
),
orders AS (
    SELECT o.id,
           strftime(CAST(o.placed_at AS TIMESTAMP), '%Y-%m') AS month
    FROM tradeline.orders o
),
catalogue AS (
    SELECT upper(trim("SKU")) AS sku,
           "Category" AS category,
           "Supplier" AS supplier,
           CAST(replace("List Price", '$', '') AS DECIMAL(10, 2))
               AS list_price
    FROM read_csv('$CATALOGUE', all_varchar = true)
)
SELECT o.month,
       p.category,
       count(*) AS lines,
       sum(l.value_cents) AS value_cents
FROM lines l
JOIN orders o ON o.id = l.order_id
JOIN catalogue p ON p.sku = l.sku
GROUP BY 1, 2;

-- 3. Sales by supplier and month (for procurement) -----------------
CREATE OR REPLACE TEMP TABLE pack_supplier AS
WITH lines AS (
    SELECT l.order_id,
           l.sku,
           l.qty,
           l.unit_price_cents,
           l.qty * l.unit_price_cents AS value_cents
    FROM tradeline.order_lines l
),
orders AS (
    SELECT o.id,
           strftime(CAST(o.placed_at AS TIMESTAMP), '%Y-%m') AS month
    FROM tradeline.orders o
),
catalogue AS (
    SELECT "SKU" AS sku,
           "Category" AS category,
           "Supplier" AS supplier,
           CAST(replace("List Price", '$', '') AS DECIMAL(10, 2))
               AS list_price
    FROM read_csv('$CATALOGUE', all_varchar = true)
),
suppliers AS (
    SELECT supplier_id, name, tier
    FROM read_csv('$SUPPLIERS', all_varchar = true)
)
SELECT o.month,
       s.name AS supplier,
       s.tier,
       count(*) AS lines,
       sum(l.value_cents) AS value_cents
FROM lines l
JOIN orders o ON o.id = l.order_id
JOIN catalogue p ON p.sku = l.sku
JOIN suppliers s ON s.name = p.supplier
GROUP BY 1, 2, 3;

-- 4. List price against price charged, by category -----------------
CREATE OR REPLACE TEMP TABLE pack_realisation AS
WITH lines AS (
    SELECT l.order_id,
           l.sku,
           l.qty,
           l.unit_price_cents,
           l.qty * l.unit_price_cents AS value_cents
    FROM tradeline.order_lines l
),
orders AS (
    SELECT o.id,
           strftime(CAST(o.placed_at AS TIMESTAMP), '%Y-%m') AS month
    FROM tradeline.orders o
),
catalogue AS (
    SELECT "SKU" AS sku,
           "Category" AS category,
           CAST(replace("List Price", '$', '') AS DOUBLE) AS list_price
    FROM read_csv('$CATALOGUE', all_varchar = true)
)
SELECT o.month,
       p.category,
       sum(l.value_cents) AS value_cents,
       sum(l.qty * p.list_price * 100) AS list_value_cents
FROM lines l
JOIN orders o ON o.id = l.order_id
JOIN catalogue p ON p.sku = l.sku
GROUP BY 1, 2;
