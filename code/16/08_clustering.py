# Clustering: the fact as dbt built it, and written in order of its day.
import json
from pathlib import Path

import duckdb
from _plans import HERE, analyze, connect, scans, tidy, warehouse

con = connect()
path = HERE / "clustered.duckdb"
tidy("clustered.duckdb")
db = duckdb.connect(str(path))
db.execute(f"ATTACH '{warehouse()}' AS w (READ_ONLY)")
db.execute("CREATE TABLE by_day AS FROM w.marts.fct_order_lines "
           "ORDER BY order_date, order_id, line_no")
print(f"{'fct_order_lines':<16}{'groups':>7}{'first group spans':>26}"
      f"{'December reads':>16}")
clustering = {}
for label, cx, table in (("as built", con, "marts.fct_order_lines"),
                         ("by day", db, "by_day")):
    groups, first = cx.execute(f"""
        SELECT count(DISTINCT row_group_id),
               min(stats) FILTER (WHERE row_group_id = 0)
        FROM pragma_storage_info('{table}')
        WHERE column_name = 'order_date' AND segment_type = 'DATE'
        """).fetchone()
    span = first[len("[Min: "):first.index("]")]
    span = span.replace(", Max:", " to")
    (scan,) = scans(analyze(cx, f"""
        SELECT sum(revenue) FROM {table}
        WHERE order_date >= DATE '2025-12-01'
          AND order_date < DATE '2026-01-01'"""))
    clustering[label] = {"read": scan["operator_rows_scanned"],
                         "kept": scan["operator_cardinality"]}
    print(f"{label:<16}{groups:>7}{span:>26}"
          f"{scan['operator_rows_scanned']:>16,}")
print(f"rows of December in either: {scan['operator_cardinality']:,}")
db.close()
tidy("clustered.duckdb")
(Path(__file__).parent / "08_clustering.json").write_text(
    json.dumps(clustering, indent=1) + "\n")
