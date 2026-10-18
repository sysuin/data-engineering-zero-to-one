# Groundwork's Tradeline loader in both modes, thirty nights at 02:05
# UTC: reconciled by the loader each morning, then checked row by row.
import shutil
from datetime import timedelta

import duckdb

from _watermark import DB, KEYS, nights
from groundwork import loadlog
from groundwork.config import SANDBOX
from groundwork.ingest import tradeline as groundwork

HERE = SANDBOX / "ch07" / "groundwork"
shutil.rmtree(HERE, ignore_errors=True)
MODES = ("incremental", "cdc")
where = {m: {"root": HERE / m / "raw",
             "warehouse": HERE / m / "warehouse.duckdb"} for m in MODES}


def run(day, moment, mode):
    return groundwork.load(day, run_at=moment, mode=mode, source=DB,
                           **where[mode])


weeks = {}                  # rows read, loads, loads that agreed, days
for night, moment, src in nights():
    day = moment.date() - timedelta(days=1)
    group = weeks.setdefault((night + 6) // 7, {})
    for mode in MODES:
        loads = run(day, moment, mode)
        g = group.setdefault(mode, [0, 0, 0, []])
        g[0] += sum(x.rows_read for x in loads)
        g[1] += 1
        g[2] += all(x.extra["check"]["agrees"] for x in loads)
        g[3].append(day)

print(f"{'load dates':<16}{'incremental: read':>19}{'agreed':>8}"
      f"{'cdc: read':>12}{'agreed':>8}")
for group in weeks.values():
    days = group["cdc"][3]
    span = (f"{days[0]:%d %b} snapshot" if len(days) == 1 else
            f"{days[0]:%d %b}-{days[-1]:%d %b}")
    print(f"{span:<16}", *(f"{n:>18,} {ok:>2} of {k}"
                           if m == "incremental" else
                           f"{n:>11,} {ok:>2} of {k}"
                           for m, (n, k, ok, _) in group.items()))

# The morning after the last night: every row, both ways
print(f"row by row against Tradeline at {moment:%Y-%m-%d %H:%M}:")
for mode in MODES:
    con = duckdb.connect(str(where[mode]["warehouse"]))
    con.execute(f"ATTACH '{DB}' AS src (TYPE sqlite, READ_ONLY)")
    n = {}
    for t in KEYS:
        ours = (f"(SELECT * EXCLUDE (_load_id, _loaded_at) FROM "
                f"raw.tradeline_{t}_current)")
        n[t] = [con.execute(f"SELECT count(*) FROM (FROM {a} EXCEPT "
                            f"FROM {b})").fetchone()[0]
                for a, b in ((ours, f"src.{t}"), (f"src.{t}", ours))]
    con.close()
    print(f"  {mode:<12} extra: {n['orders'][0]} orders, "
          f"{n['order_lines'][0]} lines; missing: {n['orders'][1]} "
          f"orders, {n['order_lines'][1]} lines")

# The last load date again, as an operator's rerun would: the same
# copy, one more attempt in the log, and the bookmark each mode keeps
print(f"load date {day} run again:")
for mode in MODES:
    con = duckdb.connect(str(where[mode]["warehouse"]))
    before = [x.extra["check"]["copy"] for x in
              (loadlog.last(con, "tradeline", t) for t in KEYS)]
    con.close()
    loads = run(day, moment, mode)
    after = [x.extra["check"]["copy"] for x in loads]
    extra = loads[1].extra
    mark = (f"lsn {extra['position']:,}" if mode == "cdc"
            else extra["watermark"])
    print(f"  {mode:<12} attempt {loads[1].load_id[-1]}, copy "
          f"{'unchanged' if after == before else 'CHANGED'}; "
          f"next {mark}")
shutil.rmtree(HERE)                          # keep the sandbox small
