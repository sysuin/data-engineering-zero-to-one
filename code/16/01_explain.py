# One query's plans: as DuckDB read it, and as it will run it.
from pathlib import Path

from _plans import connect, explain, name, operators

SQL = (Path(__file__).parent / "segment_revenue.sql").read_text()
con = connect()
plans = explain(con, SQL)
width = dict(con.execute("SELECT table_name, column_count "
                         "FROM duckdb_tables()").fetchall())


def detail(node):
    info = node.get("extra_info", {})
    if "Table" in info:                      # a scan: what it reads
        table = info["Table"].split(".")[-1]
        cols = len(info.get("Projections", [])) or width[table]
        text = f"{table}: {cols} of {width[table]} columns"
        return text + (", filtered" if info.get("Filters") else "")
    if "Conditions" in info:
        return f"{info['Join Type'].lower()}, on account_key"
    if "Groups" in info:
        return "one row per segment"
    return {"FILTER": "order_date in December",
            "ORDER_BY": "revenue, largest first"}.get(name(node), "")


def show(node, depth=0, skip=()):
    if name(node) in skip:                   # step over, keep children
        depth -= 1
    else:
        print(f"  {'  ' * depth}{name(node)}  {detail(node)}".rstrip())
    for child in node["children"]:
        show(child, depth + 1, skip)


print("as written: the logical plan")
show(plans["logical_plan"])
print("as it will run: the physical plan, projections not shown")
show(plans["physical_plan"], skip={"PROJECTION"})
print("pushed into the scans, and the planner's estimates:")
for _, node in operators(plans["physical_plan"]):
    if name(node) == "SEQ_SCAN":
        info = node["extra_info"]
        table = info["Table"].split(".")[-1]
        rows, = con.execute(f"SELECT count(*) FROM marts.{table}"
                            ).fetchone()
        print(f"  {table}: {', '.join(info['Projections'])}")
        if info.get("Filters"):
            print(f"    where {info['Filters'].replace('::DATE', '')}")
        print(f"    expects {int(info['Estimated Cardinality']):,}"
              f" of {rows:,} rows")
