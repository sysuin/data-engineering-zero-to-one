-- stg_tradeline__orders: one row per order in Tradeline as it stands
-- after the last load, deleted orders gone. Status and placed_at are
-- kept exactly as the source has them.
-- materialised: view
SELECT
    id                                              AS order_id,
    order_number,
    account_id,
    strptime(placed_at, '%Y-%m-%d %H:%M:%S')        AS placed_at,
    channel,
    status,
    strptime(created_at, '%Y-%m-%d %H:%M:%S')       AS created_at,
    strptime(updated_at, '%Y-%m-%d %H:%M:%S')       AS updated_at,
    _load_id
FROM {{ source('raw', 'tradeline_orders_current') }}
