# timeout: 1200
# The night of 29 February 2024, the old CRM's last, loaded into empty
# Groundworks: twice from the sources as they stood when its run was
# due, once from the sources as they stand today; then its Relay
# accounts against the partition Chapter 9 loaded.
from dataclasses import replace
from datetime import date

import duckdb
from _sandbox import HERE, tidy

from groundwork.config import LANDING, NOW, RAW, TRADELINE_DB
from groundwork.ingest.__main__ import run
from groundwork.orchestrate import backfill, drill
from groundwork.storage import raw_path
from meridian.sources import Clock, frontline

DAY = date(2024, 2, 29)
ONLY = ["tradeline", "frontline", "relay"]


def night(name, today=False):
    """Load the night into an empty Groundwork, as the nightly run's
    ingest would."""
    tidy(name)                          # an empty Groundwork
    env = backfill.sandbox_env(HERE / "work" / name)
    at = backfill.due(DAY)              # 06:00 on 1 March, both ways
    if today:                    # the right date, today's sources
        env = replace(env, landing=LANDING, tradeline_db=TRADELINE_DB,
                      frontline_base=frontline.serve(Clock(NOW)))
    else:                               # the sources as they stood
        rp = backfill.Replay(HERE / "work" / name / "replay", 0)
        env = rp.env(env, rp.prepare(DAY))
    lines = run(DAY, run_at=at, only=ONLY, landing=env.landing,
                tradeline_db=env.tradeline_db,
                frontline_base=env.frontline_base, root=env.root,
                warehouse=env.warehouse, archive=env.archive)
    return env, {f"{x.source}.{x.table}": x for x in lines}


if __name__ == "__main__":
    (first, got), (again, _), (now, _) = (night("02a"), night("02b"),
                                          night("02c", today=True))
    same = {c.what: c for c in drill.compare_raw(first.root, again.root,
                                                 DAY)}
    late = {c.what: c for c in drill.compare_raw(first.root, now.root,
                                                 DAY)}
    print(f"night {DAY}, its run due "
          f"{backfill.due(DAY):%Y-%m-%d %H:%M}")
    print(f"{'raw table':<26}{'rows':>10}{'again':>8}{'today':>11}")
    for name in sorted(same):
        if name.endswith("pages"):
            continue
        a, b = same[name], late.get(name)
        today = "missing" if b is None or b.note else f"{b.differ:,}"
        print(f"{name:<26}{a.rows:>10,}{a.differ:>8,}{today:>11}")
    print("(rows in the first load; rows that differ from it)")
    con = duckdb.connect(str(now.warehouse), read_only=True)
    ahead, last = con.execute(
        "SELECT count(*), max(placed_at) FROM raw.tradeline_orders "
        "WHERE load_date = ? AND placed_at > ?",
        [DAY, f"{backfill.due(DAY)}"]).fetchone()
    con.close()
    print(f"today's sources put {ahead:,} orders placed after the run "
          f"was due\ninto the night's partition, the last at {last}")

    print(f"\nrelay.accounts for {DAY}, against Chapter 9's load:")
    ch9 = raw_path("relay", "accounts", DAY, RAW)
    x = got["relay.accounts"]
    c = drill.same_rows(ch9, raw_path("relay", "accounts", DAY,
                                      first.root))
    con = duckdb.connect(str(first.warehouse), read_only=True)
    (why,) = con.execute("SELECT any_value(reason) FROM "
                         "audit.quarantine WHERE table_name = "
                         "'accounts'").fetchone()
    con.close()
    print(f"  today's ingest  {x.loaded:,} rows, {x.parked} parked, "
          f"{c.differ} rows differ\n    parked because {why}")
    tidy("02d")
    env = backfill.sandbox_env(HERE / "work" / "02d")
    env = replace(env, landing=first.landing)
    (y, _) = backfill.load_relay(env, DAY, backfill.due(DAY))[:2]
    c = drill.same_rows(ch9, raw_path("relay", "accounts", DAY,
                                      env.root))
    print(f"  the backfill's  {y.loaded:,} rows, {y.parked} parked, "
          f"{c.differ} rows differ")
    for name in ("02a", "02b", "02c", "02d"):
        tidy(name)
