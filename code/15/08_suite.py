# Groundwork v0.6's checks in the order a night runs them, on a copy:
# the loads, then the tables, then the warehouse against the world.
# timeout: 1200
from collections import Counter
from datetime import datetime

from groundwork import quality, reconcile

from _sandbox import connect, dbt, save, tidy, work

AT = datetime(2026, 1, 5, 7, 0)
path = work("suite")

con = connect(path, read_only=False)
checks = quality.check(con, AT)
quality.record(con, checks, AT)
con.close()
tally = Counter(c.status for c in checks)
print(f"1. quality.check at {AT:%H:%M}: {len(checks)} checks, "
      + ", ".join(f"{n} {s}" for s, n in sorted(tally.items())))
for c in checks:
    if c.status == "skip":
        print(f"     skip: {c.name} {c.detail}")

res = dbt("build", "--select", "tag:quality", path=path)
tests = sum(r.node.resource_type == "test" for r in res.result.results)
print("2. dbt build --select tag:quality: " + ", ".join(
    f"{n} {s}" for s, n in sorted(Counter(
        str(r.status) for r in res.result.results).items())))

print("3. python -m groundwork.reconcile")
code = reconcile.main(["--warehouse", str(path), "--at",
                       AT.isoformat()])
con = connect(path)
rows = {}
for table in ("quality_checks", "reconciliations"):
    # this run's rows: the copy keeps every earlier night's too
    (n,) = con.execute(f"SELECT count(*) FROM audit.{table} "
                       "WHERE run_at = ?", [AT]).fetchone()
    print(f"audit.{table}: {n} rows")
    rows[table] = n
print("exit code", code)
cells = len(reconcile.revenue(con))
con.close()
save("08_suite", {"checks": len(checks), "tally": tally,
                  "tests": tests, "rows": rows, "cells": cells,
                  "exit": code})
tidy("suite")
