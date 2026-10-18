# timeout: 7200
# nondeterministic: it prints how long the nights took
# Ninety-one nights, 16 December 2024 to 16 March 2025, backfilled into
# an empty Groundwork. The machine goes down in the middle of one night;
# the same command, run again, carries on.
import json
import statistics
from dataclasses import replace
from datetime import date

import duckdb
from _sandbox import (FIRST, LAST, OLD_CRM, finished, replay,
                      start_quarter)

from groundwork import reconcile as rc
from groundwork.config import LANDING, NOW, TRADELINE_DB
from groundwork.orchestrate import backfill
from groundwork.orchestrate.dag import DAG

CRASH = date(2025, 2, 14)


def down_after(fn, ctx):
    """The real task, then the machine goes down before the run can
    write down that it finished."""
    out = fn(ctx)
    if ctx.day == CRASH and ctx.try_number == 1 and "rerun" not in seen:
        seen.add("rerun")
        raise KeyboardInterrupt("the machine went down")
    return out


seen = set()

if __name__ == "__main__":
    env, seeded = start_quarter()      # an empty Groundwork, and what
    for x in seeded[:2]:               # it held before its first night
        print(f"before: relay.{x.table} {OLD_CRM}, {x.loaded:,} rows, "
              f"{x.parked} parked")
    dag = backfill.backfill_dag()
    t = dag.tasks["ingest_tradeline"]
    crashing = DAG("backfill", [
        replace(t, fn=lambda ctx, f=t.fn: down_after(f, ctx))
        if t.name == "ingest_tradeline" else t
        for t in dag.tasks.values()])
    rp, log = replay(parallel=1), []
    try:
        backfill.run(FIRST, LAST, env=env, replay=rp, dag=crashing,
                     log=log)
    except KeyboardInterrupt as e:
        print(f"first command: {len(log)} nights, then {e}\n  during "
              f"{CRASH}'s ingest_tradeline")
    b = backfill.run(FIRST, LAST, env=env, replay=rp, log=log)
    print("second command, the same:")
    print(b.report())
    gaps = [str(n.day) for n in log if "gap" in n.detail]
    print(f"no export in the archive, a gap: {', '.join(gaps)}")

    con = duckdb.connect(str(env.warehouse), read_only=True)
    print(f"\n{CRASH}, the night the machine went down:")
    crash = {"tries": [], "tables": {}}
    for run_id, n, state in con.execute("""
        SELECT run_id, try_number, state FROM audit.task_runs
        WHERE logical_date = ? AND task = 'ingest_tradeline'
        ORDER BY run_id, try_number""", [CRASH]).fetchall():
        print(f"  {run_id:<26} try {n}  {state}")
        crash["tries"].append([run_id, n, state])
    for table in ("orders", "order_lines"):
        loads, held = con.execute(f"""
            SELECT (SELECT count(*) FROM audit.load_log WHERE source =
                    'tradeline' AND table_name = ? AND load_date = ?),
                   (SELECT count(*) FROM raw.tradeline_{table}_log
                    WHERE load_date = ?)""",
                                  [table, CRASH, CRASH]).fetchone()
        (read,) = con.execute("""
            SELECT rows_read FROM audit.load_log WHERE source =
            'tradeline' AND table_name = ? AND load_date = ?
            AND status = 'succeeded'""", [table, CRASH]).fetchone()
        print(f"  {table:<12} {loads} loads; raw holds {held:,} rows; "
              f"the last read {read:,}")
        crash["tables"][table] = [loads, held, read]

    print("\nwhat the backfill read, nights 2 to 91:")
    for source, table, rows in con.execute("""
        SELECT source, table_name, sum(rows_written)
        FROM audit.load_log WHERE status = 'succeeded'
          AND load_date > ? AND table_name IN ('orders', 'order_lines',
              'tickets', 'ticket_comments', 'accounts', 'contracts')
        GROUP BY ALL ORDER BY 1, 2""", [FIRST]).fetchall():
        print(f"  {source + '.' + table:<26}{rows:>12,}")
    con.close()
    asked = [n.requests for n in log[1:]]
    print(f"desk requests: night 1 {log[0].requests:,}; after it, "
          f"a median of {statistics.median(asked):,.0f} a night")

    print("\nthe six counts, reconciled as of 06:00 on 17 March and "
          "today:")
    print(f"  {'count':<19}{'marts':>9}{'as of':>14}{'today':>15}")
    p = rp.prepare(LAST, 0)
    con = duckdb.connect(str(env.warehouse), read_only=True)
    asked = []
    for src, land, at in ((p.tradeline_db, p.landing, p.at),
                          (TRADELINE_DB, LANDING, NOW)):
        desk = backfill.desk_total(rp.desk(at.replace(tzinfo=None)))
        asked.append([replace(c, source=desk)    # the desk, asked
                      if c.name == "Frontline tickets" else c
                      for c in rc.counts(con, src, land)])
    con.close()
    for a, t in zip(*asked):
        mark = lambda c: "ties" if c.ties else "no"   # noqa: E731
        print(f"  {a.name:<19}{a.warehouse:>9,}{a.source:>9,} "
              f"{mark(a):<4}{t.source:>10,} {mark(t)}")
    print("  (Tradeline's own counts include its cancelled orders: "
          f"{asked[0][0].explained[0][1]:,}\n  as of 17 March, "
          f"{asked[1][0].explained[0][1]:,} today)")

    per = statistics.median(n.seconds for n in log[1:])
    print(f"\nnight 1 took {log[0].seconds / 60:.1f} min; the rest a "
          f"median of {per:.1f} s")
    print(f"at that rate, 1,465 nights: about "
          f"{1465 * per / 3600:.1f} h")
    zero = log[0].replayed[0]
    with open("code/18/03_backfill.json", "w") as f:
        json.dump({"crash": crash, "day": str(CRASH), "nights": [
            {"day": str(n.day), "state": n.state,
             "seconds": round(n.seconds, 2), "replayed": [
                 round(t - zero, 2) for t in n.replayed or ()],
             "loaded": [round(t - zero, 2) for t in n.loaded],
             "requests": n.requests, "detail": n.detail}
            for n in log]}, f, indent=1)
    finished(b)
