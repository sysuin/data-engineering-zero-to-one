# timeout: 3600
# Groundwork v0.8's night for 4 January 2026 in a sandbox copy: the
# nightly DAG with its monitor, then the watch at 07:00.
import textwrap
from datetime import datetime

import duckdb
from _sandbox import env, tidy, work

from groundwork.observe import alerts, nightly
from groundwork.orchestrate import runner

if __name__ == "__main__":
    copy = work("09")
    r = runner.run(nightly(), "2026-01-04", env=env(copy))
    print(r.report())

    con = duckdb.connect(str(copy / "warehouse.duckdb"))
    print("\naudit.metrics, recorded by the monitor:")
    for signal, n, alarms in con.execute("""
            SELECT signal, count(*), count(*) FILTER (status = 'alarm')
            FROM audit.metrics GROUP BY 1 ORDER BY 1""").fetchall():
        print(f"  {signal:<13}{n:>3} measured, {alarms} in alarm")

    at = datetime(2026, 1, 5, 7)
    w = alerts.watch(con, at, copy / "raw")
    print(f"\nthe watch at {at:%H:%M} on {at.day} {at:%B}: "
          f"{len(w.opened)} opened")
    for when, kind, to, texts in alerts.messages(w.sent):
        print(f"  {when:%H:%M} {kind} to {to}: "
              f"{textwrap.shorten(texts[0], 34, placeholder=' ...')}")
    again = alerts.watch(con, at, copy / "raw")
    print(f"the watch again, same moment: {len(again.opened)} opened, "
          f"{len(again.repeated)} still open, {len(again.sent)} sent")
    b = alerts.budget(con, at)
    print(f"SLO since {b.first}: {b.good} on time, {b.bad} late, "
          f"budget left {b.left:g}")
    con.close()
    tidy("09")
