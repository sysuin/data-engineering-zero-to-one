-- int_order_lines: one row per line of an order that was placed and
-- not cancelled, with its order's number, moment and channel, and its
-- account resolved to Relay's id. A cancelled order did not happen,
-- so its lines are not here; a test order was deleted in Tradeline and
-- never reaches raw's current view. updated_at is the later of the
-- order's and the line's, for loads that take only what changed.
-- materialised: view
SELECT
    l.order_id,
    l.line_no,
    o.order_number,
    o.account_id                                    AS crm_id,
    i.account_id,               -- NULL: an id the CRM has not exported
    o.placed_at,
    c.channel,
    l.sku,
    l.qty,
    l.discount_pct,
    l.unit_price_cents,
    greatest(o.updated_at, l.updated_at)            AS updated_at
FROM {{ ref('stg_tradeline__order_lines') }} AS l
JOIN {{ ref('stg_tradeline__orders') }} AS o USING (order_id)
LEFT JOIN {{ ref('int_account_identity') }} AS i
    ON i.crm_id = o.account_id
LEFT JOIN {{ ref('channel_codes') }} AS c ON c.code = o.channel
WHERE o.status = 'placed'
