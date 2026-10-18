# Roles as schemas of views, checked by classifying what each role can
# read; then a well-meant change that breaks the rule; then the export.
# timeout: 300
import json
from pathlib import Path

import duckdb

from groundwork.govern import classify, masking, roles

from _govern import HERE, base

con = base().connect()
masking.build(con)
made = roles.create(con)
for name, role in roles.ROLES.items():
    print(f"{name:<18}{made[name]:>3} views, {len(role.allow)} "
          "personal columns allowed")
print(f"problems: {roles.check(con) or 'none'}")

# A colleague needs the requester's email "for one report" and adds it
# to the analyst's tickets view.
con.execute("""
    CREATE OR REPLACE VIEW role_analyst.stg_frontline__tickets AS
    SELECT m.*, s.requester_email
    FROM masked.stg_frontline__tickets AS m
    JOIN staging.stg_frontline__tickets AS s USING (ticket_id)""")
for problem in roles.check(con):
    where, why = problem.split(": ", 1)
    who, what = where.split(" can read ")
    print(f"problem: {who} can read\n  {what}\n  {why}")
roles.create(con)

# Rows: what the Southwest manager sees of the orders.
for rel in ("fct_orders", "fct_tickets"):
    (everyone,) = con.execute(
        f"SELECT count(*) FROM marts.{rel}").fetchone()
    (theirs,) = con.execute(
        f"SELECT count(*) FROM role_manager_southwest.{rel}").fetchone()
    print(f"{rel:<12} all {everyone:>9,}   Southwest's {theirs:>8,}")

# The only boundary DuckDB itself keeps: a file of the role's own.
path = HERE / "analyst.duckdb"
rows = roles.export(con, "analyst", path)
con.close()
with duckdb.connect(str(path), read_only=True) as f:
    cols = classify.classify(f, ("main",))
print(f"\nanalyst.duckdb: {len(rows)} tables, "
      f"{sum(rows.values()):,} rows,\n"
      f"  {sum(c.hits for c in cols)} holding an email or phone")
left = sorted({c.column for c in cols if c.cls == classify.PERSONAL})
print(f"personal columns in it: {', '.join(left[:3])},\n"
      f"  {', '.join(left[3:])}")
path.unlink()
Path(__file__).with_suffix(".json").write_text(json.dumps({
    name: {"views": made[name], "reads": sorted(role.reads),
           "hide": role.hide, "region": role.region}
    for name, role in roles.ROLES.items()}, indent=1) + "\n")
