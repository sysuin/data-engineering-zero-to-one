# The runner's first run over a copy of the warehouse: Groundwork's
# staging, its seed and the sales pack, built in order and logged.
from datetime import datetime

import duckdb

from groundwork.transform import runner

from _pack import sandbox, tidy

warehouse, folder = sandbox("runner")
results = runner.run(warehouse, folder=folder,
                     run_at=datetime(2026, 1, 5, 6, 30))
for r in results:
    print(f"{r.status:<7} {r.relation:<37} {r.materialised:<6}"
          f"{r.rows:>11,}")

con = duckdb.connect(str(warehouse))
print("\nThe pack's grand totals, from the models:")
print(f"  {'section':<14}{'lines':>12}{'sales':>20}")
for name in ("channel", "category", "supplier", "realisation"):
    n, cents = con.execute(
        f"SELECT sum(lines), sum(value_cents) FROM pack.pack_{name}"
        if name != "realisation" else
        "SELECT NULL, sum(value_cents) FROM pack.pack_realisation"
    ).fetchone()
    shown = f"{n:,}" if n is not None else "-"
    usd = f"${cents // 100:,}.{cents % 100:02d}"
    print(f"  {name:<14}{shown:>12}{usd:>20}")

print("\naudit.model_runs:")
for row in con.execute("""
        SELECT run_id, status, count(*), min(started_at),
               count(*) FILTER (WHERE error IS NOT NULL)
        FROM audit.model_runs GROUP BY ALL ORDER BY ALL""").fetchall():
    print(f"  run {row[0]}: {row[2]} {row[1]}, started {row[3]:%H:%M}, "
          f"{row[4]} errors")
con.close()
tidy("runner")
