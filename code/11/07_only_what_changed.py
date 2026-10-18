# Run, change one thing, run again: the runner rebuilds the model that
# changed and everything downstream of it, and skips the rest.
import textwrap
from datetime import date, datetime, timedelta

import duckdb

from groundwork import loadlog
from groundwork.transform import runner

from _pack import sandbox, tidy

warehouse, folder = sandbox("changed")
at = datetime(2026, 1, 5, 6, 30)


def step(what, full=False):
    global at
    at += timedelta(minutes=10)
    results = runner.run(warehouse, folder=folder, run_at=at, full=full)
    built = [r.model for r in results if r.status == "built"]
    print(f"{what}\n  {len(built)} built, "
          f"{len(results) - len(built)} skipped")
    if 0 < len(built) < 6:
        print(textwrap.fill("built: " + ", ".join(built), 66,
                            initial_indent="  ",
                            subsequent_indent="         "))


step("1. The first run")
step("2. The same run again, nothing changed")

path = folder / "pack" / "pack_supplier.sql"
sql = path.read_text()
path.write_text(sql.replace(
    "AS s\n    ON s.supplier_name = p.supplier_name", "AS s\n"
    "    USING (supplier_id)"))
step("3. pack_supplier joins on supplier_id, not on the name")

path = folder / "staging" / "stg_catalogue__products.sql"
path.write_text(path.read_text().replace(
    "-- force. SKUs are", "-- force.  SKUs are"))
step("4. One space added to a comment in stg_catalogue__products")

con = duckdb.connect(str(warehouse))
load_id = loadlog.start(con, "tradeline", "orders", date(2026, 1, 5),
                        started_at=datetime(2026, 1, 6, 6, 0))
loadlog.finish(con, load_id, finished_at=datetime(2026, 1, 6, 6, 1))
con.close()
step(f"5. Tonight's Tradeline load, logged: {load_id}")
step("6. --full", full=True)

con = duckdb.connect(str(warehouse))
print("\naudit.model_runs, by run:")
for run_id, built, skipped in con.execute("""
        SELECT run_id, count(*) FILTER (WHERE status = 'built'),
               count(*) FILTER (WHERE status = 'skipped')
        FROM audit.model_runs GROUP BY run_id ORDER BY run_id"""
        ).fetchall():
    print(f"  run {run_id}: {built:>2} built, {skipped:>2} skipped")
(cents,) = con.execute(
    "SELECT sum(value_cents) FROM pack.pack_supplier").fetchone()
print(f"pack_supplier now totals ${cents // 100:,}.{cents % 100:02d}")
con.close()
tidy("changed")
