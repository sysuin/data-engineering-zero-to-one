-- December 2025's revenue by the segment each account was in when it
-- ordered: the tile on sales operations' dashboard.
SELECT a.segment, sum(f.revenue) AS revenue
FROM marts.fct_order_lines AS f
JOIN marts.dim_account AS a ON a.account_key = f.account_key
WHERE f.order_date >= DATE '2025-12-01'
  AND f.order_date < DATE '2026-01-01'
GROUP BY a.segment
ORDER BY revenue DESC
