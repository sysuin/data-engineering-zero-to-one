# nondeterministic: load times, which differ by machine and by run
# timeout: 300
# What a nightly full refresh of Tradeline costs as the business grows:
# the same loader, run on nine nights four years apart.
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

from groundwork.config import SANDBOX
from groundwork.ingest import tradeline_full
from meridian.sources import tradeline

HERE = SANDBOX / "ch06" / "cost"
DB = SANDBOX / "ch06" / "tl.db"
shutil.rmtree(HERE, ignore_errors=True)
DB.unlink(missing_ok=True)

nights = ["2022-04-01", "2022-10-01", "2023-04-01", "2023-10-01",
          "2024-04-01", "2024-10-01", "2025-04-01", "2025-10-01",
          "2026-01-01"]
print("run at              rows     MB   seconds   rows a second")
points = []
for night in nights:
    run_at = datetime.fromisoformat(f"{night} 02:05")
    tradeline.at(run_at, DB)                # moves forward only
    loads = tradeline_full.load(run_at.date(), run_at=run_at, source=DB,
                                root=HERE / "raw",
                                warehouse=HERE / "warehouse.duckdb")
    rows = sum(x.rows_written for x in loads)    # orders + lines
    secs = sum(x.seconds for x in loads)
    mb = sum(x.extra["bytes"] for x in loads) / 1e6
    points.append({"night": night, "rows": rows, "seconds": secs,
                   "bytes": sum(x.extra["bytes"] for x in loads)})
    print(f"{night} 02:05 {rows:>9,} {mb:>6.1f} {secs:>9.1f}"
          f" {rows / secs:>15,.0f}")
    shutil.rmtree(HERE / "raw")             # keep the sandbox small

first, last = points[0], points[-1]
grew = last["rows"] / first["rows"]
slower = last["seconds"] / first["seconds"]
print(f"rows copied grew {grew:.1f} times; "
      f"time grew {slower:.1f} times")
longest = max(points, key=lambda p: p["seconds"])["night"]
print(f"the longest load: {longest}")

con = sqlite3.connect(DB)
(new,) = con.execute("SELECT count(*) FROM order_lines WHERE "
                     "created_at >= '2025-12-31 02:05:00'").fetchone()
(total,) = con.execute("SELECT count(*) FROM order_lines").fetchone()
con.close()
print(f"on {nights[-1]}: {total:,} order lines copied,")
print(f"  {new:,} of them new since the night before "
      f"({new / total:.2%})")
DB.unlink()
shutil.rmtree(HERE)
Path(__file__).with_suffix(".json").write_text(
    json.dumps({"points": points, "new": new, "lines": total},
               indent=1) + "\n")
