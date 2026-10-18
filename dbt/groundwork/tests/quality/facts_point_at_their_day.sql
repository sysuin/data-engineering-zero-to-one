-- Chapter 13's promise: a line points at the account and product as
-- they stood on the order's day, the depot's day. The lines whose
-- versions do not cover it.
{{ config(tags=['quality']) }}
SELECT f.order_id, f.line_no, f.order_date,
       a.valid_from AS account_from, a.valid_to AS account_to,
       p.valid_from AS product_from, p.valid_to AS product_to
FROM {{ ref('fct_order_lines') }} AS f
JOIN {{ ref('dim_account') }} AS a USING (account_key)
JOIN {{ ref('dim_product') }} AS p USING (product_key)
WHERE NOT (f.order_date >= a.valid_from
           AND f.order_date < coalesce(a.valid_to, DATE '9999-12-31'))
   OR NOT (f.order_date >= p.valid_from
           AND f.order_date < coalesce(p.valid_to, DATE '9999-12-31'))
