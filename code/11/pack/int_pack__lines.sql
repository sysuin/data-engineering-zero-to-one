-- int_pack__lines: one row per order line with its order's month and
-- its value in cents. The four copies of the old job's first CTE,
-- written once. The month is the old job's: placed_at's UTC month.
-- materialised: view
SELECT
    l.order_id,
    l.line_no,
    l.sku,
    o.channel,
    strftime(o.placed_at, '%Y-%m')                  AS month,
    l.qty,
    l.qty * l.unit_price_cents                      AS value_cents
FROM {{ ref('stg_tradeline__order_lines') }} AS l
JOIN {{ ref('stg_tradeline__orders') }} AS o USING (order_id)
