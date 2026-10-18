# Run the query under EXPLAIN ANALYZE and read what each step did.
import json
from pathlib import Path

from _plans import analyze, connect, name

HERE = Path(__file__).parent
SQL = (HERE / "segment_revenue.sql").read_text()
con = connect()
profile = analyze(con, SQL)
steps = []


def walk(node, depth=0):
    if name(node) != "PROJECTION":
        info = node["extra_info"]
        steps.append({
            "depth": depth, "operator": name(node),
            "rows": node["operator_cardinality"],
            "scanned": node.get("operator_rows_scanned", 0),
            "estimate": int(info.get("Estimated Cardinality", 0)),
            "info": {k: v for k, v in info.items()
                     if k in ("Table", "Projections", "Filters",
                              "Conditions", "Join Type")}})
        depth += 1
    for child in node["children"]:
        walk(child, depth)


walk(profile["children"][0]["children"][0])  # under EXPLAIN_ANALYZE
print(f"{'operator':<32}{'rows out':>10}{'estimated':>11}"
      f"{'scanned':>11}")
for s in steps:
    table = s["info"].get("Table", "").split(".")[-1]
    label = "  " * s["depth"] + f"{s['operator']} {table}"
    scanned = f"{s['scanned']:,}" if s["scanned"] else ""
    estimate = f"{s['estimate']:,}" if s["estimate"] else ""
    print(f"{label:<32}{s["rows"]:>10,}{estimate:>11}{scanned:>11}"
          .rstrip())
print()
for segment, revenue in con.execute(SQL).fetchall():
    print(f"{segment:<16}{revenue:>14,}")
(HERE / "02_explain_analyze.json").write_text(
    json.dumps(steps, indent=1) + "\n")
