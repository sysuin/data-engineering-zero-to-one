-- fct_orders: one row per placed, not-cancelled order, its lines
-- rolled up. Built from fct_order_lines, never beside it, so the two
-- cannot disagree: an order's revenue is its lines' revenue. Its cost
-- is known only when every line's cost is.
SELECT
    order_id,
    any_value(order_number)                         AS order_number,
    any_value(date_key)                             AS date_key,
    any_value(account_key)                          AS account_key,
    any_value(region_key)                           AS region_key,
    any_value(channel)                              AS channel,
    any_value(placed_at)                            AS placed_at,
    any_value(order_date)                           AS order_date,
    CAST(count(*) AS SMALLINT)                      AS line_count,
    CAST(sum(qty) AS INTEGER)                       AS units,
    CAST(sum(revenue) AS DECIMAL(18, 2))            AS revenue,
    CAST(CASE WHEN count(cost) = count(*) THEN sum(cost) END
         AS DECIMAL(18, 2))                         AS cost,
    CAST(CASE WHEN count(cost) = count(*) THEN sum(gross_margin) END
         AS DECIMAL(18, 2))                         AS gross_margin
FROM {{ ref('fct_order_lines') }}
GROUP BY order_id
