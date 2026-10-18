-- The values on a line, each held to what it can be: a quantity of at
-- least one, prices that are not negative, a discount between 0 and
-- 100 per cent, revenue that is quantity times price, and a day that
-- is the UTC day of placed_at or the day before (every depot is west
-- of Greenwich), with a date key that spells that day.
{{ config(tags=['quality']) }}
SELECT order_id, line_no, qty, unit_price, discount_pct, revenue,
       placed_at, order_date, date_key
FROM {{ ref('fct_order_lines') }}
WHERE qty < 1
   OR unit_price < 0 OR unit_cost < 0
   OR discount_pct NOT BETWEEN 0 AND 100
   OR revenue <> qty * unit_price
   OR order_date NOT BETWEEN CAST(placed_at AS DATE) - 1
                         AND CAST(placed_at AS DATE)
   OR date_key <> CAST(strftime(order_date, '%Y%m%d') AS INTEGER)
