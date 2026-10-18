-- A singular test: a query that returns the rows that break a promise,
-- and passes when it returns none. The promise is fct_orders' header:
-- an order's revenue and units are its lines', exactly.
SELECT o.order_id, o.revenue, l.revenue AS lines_revenue
FROM {{ ref('fct_orders') }} AS o
JOIN (SELECT order_id, sum(revenue) AS revenue, sum(qty) AS units
      FROM {{ ref('fct_order_lines') }}
      GROUP BY order_id) AS l USING (order_id)
WHERE o.revenue <> l.revenue OR o.units <> l.units
