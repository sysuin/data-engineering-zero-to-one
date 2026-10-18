-- Each placed order joined to the version of its account valid on the
-- order's day: from valid_from, up to but not including valid_to.
SELECT o.order_id, o.order_date, v.account_key, v.owner
FROM orders AS o
JOIN marts.dim_account AS v
  ON v.account_id = o.account_id
 AND o.order_date >= v.valid_from
 AND (o.order_date < v.valid_to OR v.valid_to IS NULL)
