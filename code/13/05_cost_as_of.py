# Costs as of the order's day: the as-of join to dim_product against
# today's cost, and the result against Book 2's unit_cost.
# timeout: 600
from groundwork.config import BOOK2_WAREHOUSE

from _facts import built

con = built()
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 (TYPE sqlite, "
            "READ_ONLY)")
print(f"{'sku':<13}{'valid_from':<12}{'valid_to':<12}{'unit_cost':>10}"
      f"{'lines':>9}")
for sku, vf, vt, cost, n in con.execute("""
        SELECT p.sku, p.valid_from, p.valid_to, p.unit_cost,
               count(f.product_key)
        FROM marts.dim_product AS p
        LEFT JOIN marts.fct_order_lines AS f USING (product_key)
        WHERE p.sku IN (SELECT sku FROM marts.dim_product
                        GROUP BY sku HAVING count(*) > 1)
        GROUP BY ALL ORDER BY 1, 2""").fetchall():
    print(f"{sku:<13}{vf!s:<12}{str(vt or ''):<12}{cost:>10}{n:>9,}")

asof, today, n = con.execute("""
    SELECT sum(f.gross_margin), sum(f.revenue - f.qty * now.unit_cost),
           count(*) FILTER (f.unit_cost <> now.unit_cost)
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_product AS p USING (product_key)
    JOIN marts.dim_product AS now ON now.sku = p.sku AND now.is_current
    """).fetchone()
print(f"\nmargin, cost as of the order's day {asof:>16,.2f}")
print(f"margin, today's cost on every line {today:>16,.2f}")
print(f"lines whose cost differs {n:,}; margin moved "
      f"{today - asof:,.2f}")

(differ,) = con.execute("""
    SELECT count(*) FROM marts.fct_order_lines AS f
    JOIN b2.order_lines AS b
      ON b.order_id = f.order_number AND b.line_no = f.line_no
    WHERE f.unit_cost <> CAST(round(b.unit_cost, 2)
                              AS DECIMAL(18, 2))""").fetchone()
print(f"lines whose unit_cost differs from Book 2's: {differ}")
