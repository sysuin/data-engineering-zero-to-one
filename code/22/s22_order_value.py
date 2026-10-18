# Exercise 2: average order value, defined on orders and, wrongly, on
# order lines, for 2025 by channel.
# timeout: 300
import duckdb
import yaml

from groundwork.serve import metrics
from groundwork.serve.metrics import period

from _serve import HERE, tidy, warehouse

doc = yaml.safe_load(metrics.YAML.read_text())
doc["metrics"]["average_order_value"] = {
    "label": "Average order value", "owner": "finance",
    "description": "Revenue per order placed and not cancelled.",
    "table": "marts.fct_orders", "time": "f.order_date",
    "measure": "round(sum(f.revenue) / count(*), 2)", "kind": "ratio",
    "dimensions": ["region", "segment", "channel"], "loads": []}
copy = HERE / "metrics_s22.yaml"
copy.write_text(yaml.safe_dump(doc))
cat = metrics.catalogue(copy)
con = duckdb.connect(str(warehouse()), read_only=True)
start, end = period(2025)
rows = metrics.query(con, "average_order_value", by=["channel"],
                     start=start, end=end, cat=cat)
lines = dict(con.execute("""
    SELECT channel, round(sum(revenue) / count(*), 2)
    FROM marts.fct_order_lines
    WHERE order_date >= ? AND order_date < ? GROUP BY 1""",
    [start, end]).fetchall())
print(f"{'channel':<14}{'per order':>12}{'per line':>12}")
for r in rows:
    print(f"{r['channel']:<14}{r['average_order_value']:>12,.2f}"
          f"{lines[r['channel']]:>12,.2f}")
con.close()
copy.unlink()
tidy("warehouse.duckdb")
