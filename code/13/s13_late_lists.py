# Exercise 2: every price list that arrived after the day it took
# effect, and which of them changed a cost while it was late.
# timeout: 600
from _facts import built

con = built()
rows = con.execute("""
    WITH lists AS (
        SELECT DISTINCT p.supplier_slug, p.effective_date,
               p.source_file, f.arrived_at
        FROM staging.stg_suppliers__price_lists AS p
        JOIN audit.file_ledger AS f ON f.name = p.source_file),
    changed AS (                 -- a new cost, not a product's first
        SELECT valid_from, sku FROM intermediate.int_product_costs
        QUALIFY row_number() OVER (PARTITION BY sku
                                   ORDER BY valid_from) > 1)
    SELECT l.supplier_slug, l.effective_date, l.arrived_at,
           CAST(l.arrived_at AS DATE) - l.effective_date AS days_late,
           (SELECT count(*) FROM staging.stg_suppliers__price_lists AS p
            JOIN changed AS c ON c.sku = p.sku
             AND c.valid_from = p.effective_date
            WHERE p.source_file = l.source_file) AS costs_moved
    FROM lists AS l ORDER BY days_late DESC, 1""").fetchall()
late = [r for r in rows if r[3] > 0]
print(f"price lists {len(rows):,}; arrived after their effective date "
      f"{len(late):,}")
print(f"{'days late':<10}{'lists':>6}")
for days in sorted({r[3] for r in late}, reverse=True):
    print(f"{days:<10}{sum(r[3] == days for r in late):>6}")
print("late lists that changed a cost:")
for slug, eff, arrived, days, moved in late:
    if moved:
        print(f"  {slug:<19}{eff!s:<12}{arrived:%Y-%m-%d %H:%M}  "
              f"{moved} SKUs")
