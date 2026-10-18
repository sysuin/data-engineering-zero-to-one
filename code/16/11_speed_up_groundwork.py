# nondeterministic: build timings, which differ by machine and run
# timeout: 900
# Three of Groundwork's models and queries, before and after.
import time
from pathlib import Path

import duckdb
from _plans import HERE, analyze, faster, model, name, operators, scans
from _plans import tidy, warehouse

tidy("speed.duckdb")
db = duckdb.connect(str(HERE / "speed.duckdb"))
db.execute(f"ATTACH '{warehouse()}' AS warehouse (READ_ONLY)")
db.execute("USE warehouse")


def build(table, sql):
    """CREATE TABLE in the scratch file; the profile, with seconds."""
    start = time.perf_counter()
    profile = analyze(db, f"CREATE TABLE speed.{table} AS {sql}")
    profile["seconds"] = time.perf_counter() - start
    return profile


# 1. fct_order_lines: the build.
before = build("before", model("fct_order_lines"))
after = build("after", faster("fct_order_lines"))
steps = [n for _, n in operators(before) if "operator_timing" in n]
hot = max(steps, key=lambda n: n["operator_timing"])
share = (hot["operator_timing"]
         / sum(n["operator_timing"] for n in steps))
print("1. fct_order_lines, built")
print(f"   hottest step before: {name(hot)} of "
      f"{hot['extra_info']['Projections'][-1]}, "
      f"{share:.0%} of the work")
for label, p in (("before", before), ("after", after)):
    print(f"   {label:<7}{p['seconds']:6.1f} s on the clock, "
          f"{p['cpu_time']:6.1f} s of CPU")
ratio = before["cpu_time"] / after["cpu_time"]
print(f"   less CPU after: {ratio > 1}, about {ratio:.1f} times less")
differ, = db.execute("""SELECT count(*) FROM (
    (FROM speed.before EXCEPT ALL FROM speed.after) UNION ALL
    (FROM speed.after EXCEPT ALL FROM speed.before))""").fetchone()
print(f"   rows in one and not the other: {differ}")

# 2. December by segment, read from each table.
tile = (Path(__file__).parent / "segment_revenue.sql").read_text()
print("2. sales operations' tile, December by segment")
for table in ("before", "after"):
    sql = tile.replace("marts.fct_order_lines", f"speed.{table}")
    read = [s["operator_rows_scanned"] for s in scans(analyze(db, sql))
            if table in s["extra_info"].get("Table", "")]
    print(f"   reads {read[0]:>9,} rows of fct_order_lines {table}")

# 3. dim_account: the latest export.
print("3. dim_account, built")
for label, sql in (("before", model("dim_account")),
                   ("after", faster("dim_account"))):
    profile = build(f"dim_{label}", sql)
    opened = sum(int(s["extra_info"].get("Total Files Read", 0) or
                     s["extra_info"].get("Scanning Files", "0")
                     .split("/")[0])
                 for s in scans(profile) if name(s) == "READ_PARQUET"
                 and "relay/accounts" in str(s["extra_info"]))
    parsed = sum(s["operator_cardinality"] for s in scans(profile)
                 if "relay/accounts" in str(s["extra_info"]))
    print(f"   {label:<7}account files read {opened:>2}, rows parsed "
          f"{parsed:>6,}, {profile['seconds']:4.2f} s")
differ, = db.execute("""SELECT count(*) FROM (
    (FROM speed.dim_before EXCEPT ALL FROM speed.dim_after) UNION ALL
    (FROM speed.dim_after EXCEPT ALL FROM speed.dim_before))"""
    ).fetchone()
print(f"   rows in one and not the other: {differ}")
db.close()
tidy("speed.duckdb")
