# What the real night of 4 January 2026 left behind, task by task,
# and what its record cannot say: which loads were its own.
from datetime import timedelta

import duckdb
from _sandbox import base, compiled

from groundwork.observe import metrics

RUN = "nightly.2026-01-04.1"
con = duckdb.connect(str(base() / "warehouse.duckdb"), read_only=True)
record = metrics.run_record(con, RUN)
print(f"{RUN}: {len(record)} tasks, "
      f"{sum(t.state == 'succeeded' for t in record)} succeeded, "
      f"{sum(t.tries for t in record)} tries")
print(f"{'task':<17}{'start':<9}{'secs':>5}  what it said")
for t in record:
    said = t.detail if len(t.detail) <= 31 else t.detail[:28] + "..."
    print(f"{t.task:<17}{t.started:%H:%M:%S}{t.seconds:>6,.0f}  {said}")

manifest, _ = compiled()
nodes = sum(n["resource_type"] in ("model", "test", "seed", "snapshot")
            for n in manifest["nodes"].values())
print(f"\ndbt_build ran {nodes} nodes; its record is one line.")

t = next(t for t in record if t.task == "ingest_tradeline")
end = t.started + timedelta(seconds=t.seconds)
ids = metrics.loads_during(con, "tradeline", t.started, end,
                           "2026-01-04")
print(f"loads for 2026-01-04 that started during ingest_tradeline: "
      f"{len(ids)}")
for x in ids:
    print(f"  {x}")
(n,) = con.execute("SELECT count(*) FROM audit.load_log WHERE "
                   "load_date = DATE '2026-01-04'").fetchone()
print(f"every load for 2026-01-04: {n}, none naming the run that "
      "made it")
con.close()
