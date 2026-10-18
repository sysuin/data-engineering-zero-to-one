# timeout: 600
# The race: a year of nightly updated_at loads at 02:05 UTC, and the
# rows the 02:00 job stamped before the extract but committed after it.
import json
from datetime import datetime, timedelta
from pathlib import Path

from _watermark import Loader, compare, nights

JOB = """SELECT count(*), max(committed_at) FROM replication_log
         WHERE table_name = ? AND committed_at > ?
           AND json_extract(row_json, '$.updated_at') = ?"""
loader, marks, seen = Loader("updated_at", ">"), {}, []
for night, moment, src in nights(datetime(2025, 1, 1, 2, 5), 365):
    stamp = str(moment - timedelta(days=1, minutes=5))   # last 02:00
    if night:           # the log says when last night's job committed
        rows, lost = 0, 0
        for table in ("orders", "order_lines"):
            n, at = src.execute(JOB, (table, stamp, stamp)).fetchone()
            rows += n
            if at[11:] > "02:05:00" and marks[table] > stamp:
                lost += n          # not read then, and never since
        seen.append({"job": stamp, "committed": at, "rows": rows,
                     "lost": lost, "marks": marks})
    loader.run(night, src)
    marks = dict(loader.mark)

late = [x for x in seen if x["committed"][11:] > "02:05:00"]
bit = [x for x in late if x["lost"]]
print(f"{len(seen)} nights of the 02:00 job, "
      f"{seen[0]['job'][:10]} to {seen[-1]['job'][:10]}")
print(f"  committed after the 02:05 extract  {len(late):>6,} nights")
print(f"  ... with the watermark past 02:00  {len(bit):>6,} nights")
latest = max(x["committed"][11:] for x in seen)
print(f"  its latest commit                {latest:>8}")
print(f"  rows it wrote                      "
      f"{sum(x['rows'] for x in seen):>6,}")
print(f"  rows the loader never read         "
      f"{sum(x['lost'] for x in seen):>6,}")

wrong, kinds = compare(loader)
(orders,) = loader.db.execute("SELECT count(*) FROM src.orders"
                              ).fetchone()
bad = kinds["lines amended at 02:00"]
print(f"orders with wrong lines on {moment:%Y-%m-%d}: {bad:,} of "
      f"{orders:,} ({bad / orders:.2%})")
for kind, n in kinds.items():
    print(f"  {kind:<26} {n:>5,}")

# One night in detail, for the figure: the first the race cost rows
x = bit[0]
seen_then = loader.db.execute(
    "SELECT DISTINCT id, updated_at FROM got_orders WHERE updated_at "
    "> ? AND updated_at <= ? ORDER BY 2", [x["job"], x["job"][:11]
                                          + "02:05:00"]).fetchall()
print(f"night of {x['job'][:10]}: {x['rows']} rows stamped "
      f"{x['job'][11:]}, committed {x['committed'][11:]}")
print(f"  other orders stamped by 02:05: {len(seen_then)}, the last at "
      f"{seen_then[-1][1][11:]}")
print(f"  the watermark after the extract: "
      f"{x['marks']['order_lines'][11:]}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"nights": [[y["job"][:10], y["committed"][11:], y["lost"]]
                for y in seen], "night": x, "placed": seen_then},
    indent=1) + "\n")
