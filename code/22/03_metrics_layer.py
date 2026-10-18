# The semantic layer: a definition compiled to SQL, reconciled with
# Book 2, and the two kinds of metric that must not be summed.
# timeout: 300
import textwrap

import duckdb

from groundwork.config import BOOK2_WAREHOUSE
from groundwork.serve import metrics
from groundwork.serve.metrics import MetricError, period

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()), read_only=True)
start, end = period(2025, 11)
sql, params = metrics.compile("revenue", by=["region"], start=start,
                              end=end)
print(sql, params, sep="\n")
rows = metrics.query(con, "revenue", by=["region"], start=start,
                     end=end)
for r in rows:
    print(f"  {r['region']:<12}{r['revenue']:>16,}")
print(f"  {'all':<12}{sum(r['revenue'] for r in rows):>16,}")

# Every month and region of four years, against Book 2's v_sales.
ours = {(str(r["month"]), r["region"]): r["revenue"] for r in
        metrics.query(con, "revenue", by=["region"], grain="month")}
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 (TYPE sqlite, "
            "READ_ONLY)")
theirs = {(m, reg): cents for m, reg, cents in con.execute("""
    SELECT substr(order_date, 1, 7) || '-01', region,
           sum(CAST(round(revenue * 100) AS BIGINT))
    FROM b2.v_sales GROUP BY ALL""").fetchall()}
same = sum(int(v * 100) == theirs.get(k) for k, v in ours.items())
print(f"month x region cells: ours {len(ours)}, Book 2's "
      f"{len(theirs)}, equal to the cent {same}")

year = period(2025)
parts = metrics.query(con, "not_renewed_rate", by=["segment"],
                      start=year[0], end=year[1])
(whole,) = metrics.query(con, "not_renewed_rate", start=year[0],
                         end=year[1])
mean = sum(r["not_renewed_rate"] for r in parts) / len(parts)
print(f"2025 not renewed: the layer {whole['not_renewed_rate']:.2f}%,"
      f" mean of {len(parts)} segments' rates {mean:.2f}%")
by_month = metrics.query(con, "active_accounts", grain="month",
                         start=year[0], end=year[1])
(y,) = metrics.query(con, "active_accounts", grain="year",
                     start=year[0], end=year[1])
summed = sum(r["active_accounts"] for r in by_month)
print(f"2025 active accounts: the layer {y['active_accounts']:,}; "
      f"12 month ends summed {summed:,}")
try:
    metrics.compile("active_accounts", by=["channel"])
except MetricError as e:
    print(textwrap.fill(f"refused: {e}", 68))
con.close()
tidy("warehouse.duckdb")
