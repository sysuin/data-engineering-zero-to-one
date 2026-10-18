-- The compat schema: the tables Books 1 and 2 were handed, under their
-- names and with their columns, as views over Groundwork.
--
-- Book 2's meridian_ml.db is a SQLite file, so its dates are TEXT and
-- its money is REAL. Each view returns the same: dates as 'YYYY-MM-DD'
-- strings, money as DOUBLE cast from Groundwork's DECIMAL at the last
-- step, booleans as 0 and 1. A query written for Book 2 runs here
-- unchanged and gets Book 2's answer.
--
-- Most columns come from the marts. A few come from staging, because
-- the marts made a decision Book 2 did not: they resolve the old CRM's
-- ids (an order is credited to the account, not to the record it was
-- placed under), and they keep a ticket's product as a key, not as the
-- SKU the desk typed. Each such column says so.
--
-- Running this file twice is safe.

CREATE SCHEMA IF NOT EXISTS compat;

CREATE OR REPLACE VIEW compat.regions AS
SELECT CAST(region_id AS INTEGER) AS region_id, region_name AS name,
       depot
FROM marts.dim_region
WHERE region_id > 0;

CREATE OR REPLACE VIEW compat.suppliers AS
SELECT CAST(supplier_id AS INTEGER) AS supplier_id,
       supplier_name AS name, country, tier
FROM marts.dim_supplier
WHERE supplier_id > 0;

-- Book 2 keeps one cost a product: the first one its price lists gave.
-- launched_on: the catalogue's own column, which the marts left out.
CREATE OR REPLACE VIEW compat.products AS
SELECT p.sku, p.product_name AS name, p.category,
       CAST(s.supplier_id AS INTEGER) AS supplier_id,
       CAST(p.unit_cost AS DOUBLE) AS unit_cost,
       CAST(p.list_price AS DOUBLE) AS list_price,
       strftime(c.launched_date, '%Y-%m-%d') AS launched_on
FROM marts.dim_product AS p
JOIN marts.dim_supplier AS s USING (supplier_key)
LEFT JOIN staging.stg_catalogue__products AS c USING (sku)
WHERE p.sku <> 'UNKNOWN'
QUALIFY row_number() OVER (PARTITION BY p.sku
                          ORDER BY p.valid_from) = 1;

-- Every account as Relay last exported it, and the old CRM's 212
-- second records as its last export (29 February 2024) had them. The
-- marts resolve those records to their Relay accounts; Book 2 kept
-- both, and its orders and tickets point at whichever was used.
CREATE OR REPLACE VIEW compat.accounts AS
SELECT a.account_id, a.account_name AS name,
       CAST(r.region_id AS INTEGER) AS region_id, a.segment,
       a.owner AS account_manager,
       strftime(a.customer_since, '%Y-%m-%d') AS since, a.postcode,
       CAST(a.key_account AS INTEGER) AS is_key_account,
       'Meridian CRM' AS crm_source,
       strftime(a.closed_on, '%Y-%m-%d') AS closed_on
FROM marts.dim_account AS a
JOIN marts.dim_region AS r USING (region_key)
WHERE a.is_current AND a.account_id > 0
UNION ALL
SELECT o.account_id, o.account_name, CAST(r.region_id AS INTEGER),
       o.segment, o.owner_name,
       strftime(o.customer_since_date, '%Y-%m-%d'), o.postcode,
       0, 'Legacy CRM', NULL
FROM staging.stg_relay__accounts AS o
JOIN intermediate.int_account_identity AS i ON i.crm_id = o.account_id
JOIN marts.dim_region AS r ON r.region_name = o.region_name
WHERE o.source_format = 'legacy'
  AND o.snapshot_date = DATE '2024-02-29'
  AND i.matched_by <> 'relay';

-- Book 2's grain: a new row when the segment or the manager changes.
-- dim_account also versions on status, so its versions are folded,
-- and its half-open valid_to becomes Book 2's last day in force.
CREATE OR REPLACE VIEW compat.account_history AS
WITH changes AS (
    SELECT *, (segment, owner) IS DISTINCT FROM lag((segment, owner))
              OVER (PARTITION BY account_id ORDER BY valid_from) AS new
    FROM marts.dim_account
    WHERE account_id > 0
)
SELECT account_id, strftime(valid_from, '%Y-%m-%d') AS valid_from,
       strftime(lead(valid_from) OVER (PARTITION BY account_id
                ORDER BY valid_from) - 1, '%Y-%m-%d') AS valid_to,
       segment, owner AS account_manager
FROM changes
WHERE new;

CREATE OR REPLACE VIEW compat.contracts AS
SELECT c.contract_id, a.account_id,
       strftime(c.start_date, '%Y-%m-%d') AS start_date,
       strftime(c.end_date, '%Y-%m-%d') AS end_date,
       CAST(c.term_months AS INTEGER) AS term_months,
       CAST(c.legacy_terms AS INTEGER) AS legacy_terms,
       CAST(c.discount_pct AS INTEGER) AS discount_pct, c.outcome,
       strftime(strptime(CAST(c.notice_date_key AS VARCHAR), '%Y%m%d'),
                '%Y-%m-%d') AS notice_date,
       c.cancellation_reason
FROM marts.fct_contracts AS c
JOIN marts.dim_account AS a USING (account_key);

-- order_id is Book 2's order number; account_id is the record the
-- order was placed under, legacy or not, from staging.
CREATE OR REPLACE VIEW compat.orders AS
SELECT o.order_number AS order_id, CAST(t.account_id AS INTEGER)
       AS account_id, strftime(o.order_date, '%Y-%m-%d') AS order_date,
       o.channel
FROM marts.fct_orders AS o
JOIN staging.stg_tradeline__orders AS t USING (order_id);

CREATE OR REPLACE VIEW compat.order_lines AS
SELECT l.order_number AS order_id,
       CAST(l.line_no AS INTEGER) AS line_no, p.sku, l.qty,
       CAST(l.unit_price AS DOUBLE) AS unit_price,
       CAST(l.unit_cost AS DOUBLE) AS unit_cost,
       CAST(l.discount_pct AS INTEGER) AS discount_pct
FROM marts.fct_order_lines AS l
JOIN marts.dim_product AS p USING (product_key);

-- The desk's labels, Book 2's spelling of them, the opening moment on
-- the depot's clock. From staging: the account the ticket was raised
-- under, the SKU as the desk recorded it, the language and the body.
CREATE OR REPLACE VIEW compat.tickets AS
SELECT f.ticket_id, CAST(t.account_id AS INTEGER) AS account_id,
       strftime((f.opened_at AT TIME ZONE 'UTC')
                AT TIME ZONE r.timezone, '%Y-%m-%d %H:%M:%S')
           AS opened_at,
       CASE f.channel WHEN 'email' THEN 'Email'
                      WHEN 'web' THEN 'Web form'
                      WHEN 'voice' THEN 'Phone' END AS channel,
       t.sku, t.language, t.description AS body,
       upper(left(f.desk, 1)) || replace(substr(f.desk, 2), '_', ' ')
           AS desk,
       upper(left(f.category, 1)) || substr(f.category, 2) AS category,
       upper(left(f.priority, 1)) || substr(f.priority, 2) AS priority
FROM marts.fct_tickets AS f
JOIN staging.stg_frontline__tickets AS t USING (ticket_id)
JOIN marts.dim_region AS r USING (region_key);

CREATE OR REPLACE VIEW compat.price_events AS
SELECT strftime(effective_date, '%Y-%m-%d') AS effective_on, scope,
       CAST(change_pct AS DOUBLE) AS change_pct, note
FROM staging.stg_reference__price_events;

-- Book 2's view, word for word but for the schema and the line breaks.
CREATE OR REPLACE VIEW compat.v_sales AS
SELECT o.order_id, o.order_date, o.channel, a.account_id,
       a.name AS account, a.segment, r.name AS region, p.sku,
       p.category, s.name AS supplier, l.qty, l.unit_price,
       l.unit_cost, l.discount_pct,
       ROUND(l.qty * l.unit_price, 2) AS revenue,
       ROUND(l.qty * (l.unit_price - l.unit_cost), 2) AS gross_profit
FROM compat.order_lines l
JOIN compat.orders    o ON o.order_id = l.order_id
JOIN compat.accounts  a ON a.account_id = o.account_id
JOIN compat.regions   r ON r.region_id = a.region_id
JOIN compat.products  p ON p.sku = l.sku
JOIN compat.suppliers s ON s.supplier_id = p.supplier_id;
