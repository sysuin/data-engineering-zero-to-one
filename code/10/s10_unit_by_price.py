# Exercise 10.2: a unit check that needs no history and no declared
# unit: every list's costs against Tradeline's selling prices.
import duckdb
import pyarrow as pa

from groundwork.config import LANDING, TRADELINE_DB
from groundwork.ingest import files, suppliers

rows = []
for a in files.discover(LANDING, "suppliers"):
    for r in suppliers.read(a.path.read_bytes(), a).rows:
        rows.append(dict(r, file=a.name))
con = duckdb.connect()
con.register("lists", pa.Table.from_pylist(rows))
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
con.execute("""
    CREATE TEMP TABLE price AS          -- cents a unit, per SKU
    SELECT sku, sum(qty * unit_price_cents) / sum(qty) AS cents
    FROM t.order_lines GROUP BY sku""")

# Cost over price, with the cost read as dollars whatever the file
# says. A list whose median is above 1 sells at a loss on paper.
CHECK = """
    SELECT file, any_value(layout),
           median(cost::DECIMAL(12, 2) * 100 / p.cents) AS ratio
    FROM lists JOIN price p USING (sku)
    WHERE cost IS NOT NULL GROUP BY file"""
by_layout = con.execute(f"""
    SELECT layout, count(*), count(*) FILTER (WHERE ratio > 1),
           round(min(ratio), 2), round(max(ratio), 2)
    FROM ({CHECK}) AS x(file, layout, ratio)
    GROUP BY layout ORDER BY layout""").fetchall()
print(f"{'layout':<10}{'lists':>6}{'fail':>6}  cost/price, min to max")
for layout, n, fail, lo, hi in by_layout:
    print(f"{layout:<10}{n:>6}{fail:>6}  {lo} to {hi}")

# The same check with each row's declared unit applied.
honest = con.execute(CHECK.replace(
    "cost::DECIMAL(12, 2) * 100", "CASE cost_unit WHEN 'cents' THEN "
    "cost::DECIMAL(12, 2) ELSE cost::DECIMAL(12, 2) * 100 END")
).fetchall()
print(f"\nwith the declared unit: {len(honest)} lists, "
      f"{sum(r > 1 for _, _, r in honest)} above 1:")
for f, _, r in sorted(honest):
    if r > 1:
        print(f"  {f.split('/')[-1]}  {r:.2f}")
