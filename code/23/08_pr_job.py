# timeout: 3600
# The pull-request job, rehearsed in a sandbox from nothing: land the
# raw layer, build main's project and the branch's, and diff them.
import shutil

import duckdb
from _sandbox import CHANGE, HERE, built, project

from groundwork import diff, identity
from groundwork.config import NOW
from groundwork.ingest import frontline_comments
from groundwork.ingest.__main__ import run
from meridian.sources import Clock, frontline

ci = HERE / "pr-job"
shutil.rmtree(ci, ignore_errors=True)
base = ci / "base" / "warehouse.duckdb"
kw = dict(root=ci / "raw", warehouse=base)
at = NOW.replace(tzinfo=None)

lines = run("2026-01-04", archive=ci / "landed", **kw)
print("land the raw layer, as make ingest does:")
print(f"  {len(lines)} tables, {sum(x.loaded for x in lines):,} rows, "
      f"{sum(x.parked for x in lines)} parked, "
      f"{sum(x.status not in ('passed', 'parked') for x in lines)} "
      f"failed")
n = identity.restore(archive=ci / "landed", **kw)
print(f"  the old CRM's last night, restored: {n:,} accounts")
x = frontline_comments.load("2026-01-04", run_at=at,
                            base=frontline.serve(Clock(at)), **kw)
print(f"  ticket comments: {x.rows_written:,}")

print("build each project from it, with dbt build --full-refresh:")
a, b = ci / "a" / "warehouse.duckdb", ci / "b" / "warehouse.duckdb"
for who, folder, path in (("main", project("pr-main"), a),
                          ("branch", project("pr-branch", CHANGE), b)):
    got = diff.build(folder, path, base)
    print(f"  {who:<7} {got.get('success', 0)} built, "
          f"{got.get('pass', 0)} tests passed, "
          f"{got.get('fail', 0) + got.get('error', 0)} failed")

print("\nmake diff:")
print(diff.report(diff.compare(a, b)))

prod, _ = built("main")      # listing 01's, on production's raw layer
same = diff.compare(prod, a, schemas=("intermediate", "marts"))
print(f"\nmain here against main on production's raw layer:\n"
      f"  {sum(d.same for d in same)} of {len(same)} intermediate and "
      f"mart tables identical")
for d in same:
    if not d.same:
        side, n = (("production's", d.rows_a) if d.rows_b is None
                   else ("this build's", d.rows_b))
        print(f"  {d.name}: only in {side}, {n} rows")
con = duckdb.connect(str(a), read_only=True)
(rows,) = con.execute("SELECT count(*) FROM marts.fct_order_lines"
                      ).fetchone()
con.close()
print(f"  fct_order_lines: {rows:,} rows")
shutil.rmtree(ci)
for name in ("pr-main", "pr-branch"):
    shutil.rmtree(HERE / name)
