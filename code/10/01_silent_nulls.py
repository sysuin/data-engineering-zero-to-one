# Chapter 9's supplier loader over every price list: it succeeds, it
# reconciles, and Pemberton's costs from July 2024 are empty.
import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb

from groundwork.config import LANDING, NOW, SANDBOX, TRADELINE_DB
from groundwork.ingest import suppliers

here = SANDBOX / "ch10" / "silent"
shutil.rmtree(here, ignore_errors=True)
x = suppliers.load(NOW.date().replace(day=4), run_at=NOW,
                   landing=LANDING, root=here / "raw",
                   warehouse=here / "warehouse.duckdb",
                   archive=here / "landed")
print(f"{x.load_id}  {x.status}")
print(f"  rows read {x.rows_read:,}, the files hold "
      f"{x.source_count:,}, written {x.rows_written:,}")

con = duckdb.connect(str(here / "warehouse.duckdb"))
print("\nPemberton's lists in raw:")
for first, last, n, rows, empty in con.execute("""
    SELECT min(strptime(effective_from, '%d/%m/%Y'))::DATE,
           max(strptime(effective_from, '%d/%m/%Y'))::DATE,
           count(DISTINCT file), count(*), count(*) - count(cost)
    FROM raw.suppliers_price_lists WHERE supplier = 'pemberton-mills'
    GROUP BY cost IS NULL ORDER BY 1""").fetchall():
    print(f"  {first:%b %Y} to {last:%b %Y}  {n:>2} lists "
          f"{rows:>4} rows {empty:>4} with no cost")

# The margin report: each order line at its month's cost. A line
# whose cost is empty adds its sales and no cost.
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
con.execute("""
    CREATE TEMP TABLE cost AS
    SELECT r.supplier, r.sku, date_trunc('month', CASE r.layout
             WHEN 'pemberton' THEN strptime(effective_from, '%d/%m/%Y')
             WHEN 'aldridge' THEN strptime(effective_from, '%d %B %Y')
             ELSE strptime(effective_from, '%Y-%m-%d') END)::DATE AS m,
           CASE cost_unit WHEN 'cents' THEN cost::BIGINT
                ELSE round(cost::DECIMAL(12, 2) * 100)::BIGINT
           END AS cents
    FROM raw.suppliers_price_lists r
    JOIN audit.current_files f ON f.sha256 = r.file_sha256""")
# What Pemberton's files say, read by DuckDB with both headers.
con.execute(f"""
    CREATE TEMP TABLE pemberton AS
    SELECT "Item Code" AS sku,
           date_trunc('month', strptime("Valid From", '%d/%m/%Y'))::DATE
             AS m,
           round(coalesce("Cost (USD)", "Unit Cost USD")::DECIMAL(12, 2)
                 * 100)::BIGINT AS cents
    FROM read_csv('{LANDING}/suppliers/pemberton-mills/*.csv',
                  union_by_name = true, all_varchar = true)""")
months = con.execute("""
    WITH s AS (
      SELECT date_trunc('month', o.placed_at::TIMESTAMP)::DATE AS m,
             l.sku, sum(l.qty) AS qty,
             sum(l.qty * l.unit_price_cents) AS sales
      FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
      WHERE o.status <> 'cancelled' GROUP BY ALL),
    priced AS (
      SELECT s.*, c.cents, c.supplier,
             coalesce(c.cents, p.cents) AS known
      FROM s LEFT JOIN cost c USING (sku, m)
             LEFT JOIN pemberton p USING (sku, m))
    SELECT m, sum(sales),
           sum(sales) - sum(qty * cents) AS reported,
           sum(sales) - sum(qty * known) AS actual,
           sum(sales) FILTER (WHERE cents IS NULL) AS uncosted
    FROM priced WHERE m BETWEEN '2024-05-01' AND '2025-12-01'
    GROUP BY m ORDER BY m""").fetchall()


def usd(c):
    return f"${c / 100:,.0f}"


print(f"\n{'month':<9}{'sales':>13}{'margin, report':>17}"
      f"{'margin, true':>16}")
for m, sales, rep, act, _ in months[:8]:
    print(f"{m:%Y-%m}  {usd(sales):>12}{usd(rep):>11} "
          f"{rep / sales:>5.1%}{usd(act):>11} {act / sales:>5.1%}")
after = [r for r in months if r[0] >= datetime(2024, 7, 1).date()]
half = sum(r[2] - r[3] for r in after[:6])
print(f"Jul to Dec 2024: margin overstated by {usd(half)}")
rep, act = sum(r[2] for r in after), sum(r[3] for r in after)
print(f"Jul 2024 to Dec 2025: report {usd(rep)}, true {usd(act)}")
print(f"  overstated by {usd(rep - act)} in {len(after)} months")
print(f"  {usd(sum(r[4] for r in after))} of sales had no cost")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "months": [[str(m), s, r, a] for m, s, r, a, _ in months]},
    indent=1))
con.close()
shutil.rmtree(here)
