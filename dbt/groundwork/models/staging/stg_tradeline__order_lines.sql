-- stg_tradeline__order_lines: one row per order line as it stands
-- after the last load. The price is integer cents, after the line's
-- discount, as Tradeline stores it.
SELECT
    order_id,
    line_no,
    upper(trim(sku))                                AS sku,
    qty,
    unit_price_cents,
    discount_pct,
    strptime(created_at, '%Y-%m-%d %H:%M:%S')       AS created_at,
    strptime(updated_at, '%Y-%m-%d %H:%M:%S')       AS updated_at,
    _load_id
FROM {{ source('raw', 'tradeline_order_lines_current') }}
