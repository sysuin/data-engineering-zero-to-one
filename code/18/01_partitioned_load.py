# timeout: 600
# One partition rebuilt, in raw and in the marts, and every other
# partition left exactly as it was.
import json
import os
from datetime import date, datetime

import duckdb
from _sandbox import tidy, today

from groundwork.config import LANDING
from groundwork.ingest.__main__ import run
from groundwork.orchestrate import backfill

NIGHT = date(2025, 12, 28)          # a Relay night to load again
DAY = date(2025, 11, 28)            # a day of orders to rebuild


def relay_files(env):
    """Each partition of raw.relay_accounts: its file's inode."""
    folder = env.root / "relay" / "accounts"
    return {p.name[10:]: os.stat(p / "part-0.parquet").st_ino
            for p in sorted(folder.iterdir())}


def fingerprints(con, first, last):
    """One hash per day of fct_order_lines: rows, in key order."""
    rows = con.execute("""
        SELECT order_date, md5(string_agg(f::VARCHAR, '|'
                                ORDER BY order_id, line_no)), count(*)
        FROM marts.fct_order_lines AS f
        WHERE order_date BETWEEN ? AND ? GROUP BY 1 ORDER BY 1""",
                       [first, last]).fetchall()
    return {d: (h, n) for d, h, n in rows}


if __name__ == "__main__":
    env = today("01")
    con = duckdb.connect(str(env.warehouse))
    print("raw.relay_accounts, one partition a night:")
    before = relay_files(env)
    for table in ("load_log", "file_ledger"):    # forget the night ...
        con.execute(f"DELETE FROM audit.{table} WHERE source = "
                    "'relay' AND load_date = ?", [NIGHT])
    con.close()                                  # ... and load it again
    run(NIGHT, run_at=datetime(2025, 12, 29, 6), only=["relay"],
        landing=LANDING, root=env.root, warehouse=env.warehouse,
        archive=env.archive)
    after = relay_files(env)
    same = [d for d in before if before[d] == after[d]]
    new = sorted(set(before) - set(same))
    print(f"  {len(before)} partitions; {len(same)} are the same files "
          f"as before;\n  written again: {', '.join(new)}")
    con = duckdb.connect(str(env.warehouse))
    (n,) = con.execute("SELECT count(*) FROM raw.relay_accounts WHERE "
                       "load_date = ?", [NIGHT]).fetchone()
    print(f"  {NIGHT}: {n:,} rows, as before")

    print("\nmarts.fct_order_lines, one partition a day:")
    (days, rows) = con.execute("SELECT count(DISTINCT order_date), "
                               "count(*) FROM marts.fct_order_lines"
                               ).fetchone()
    print(f"  {days:,} days, {rows:,} lines")
    first, last = date(2025, 11, 1), date(2025, 11, 30)
    was = fingerprints(con, first, last)
    con.close()
    sql = backfill.compiled("fct_order_lines", env)
    con = duckdb.connect(str(env.warehouse))
    gone, put = backfill.replace_days(con, "fct_order_lines", [DAY],
                                      sql)
    now = fingerprints(con, first, last)
    (total,) = con.execute("SELECT count(*) FROM marts.fct_order_lines"
                           ).fetchone()
    con.close()
    print(f"  {DAY}: {gone:,} lines taken out, {put:,} put back")
    changed = [d for d in was if was[d] != now[d]]
    print(f"  November's {len(was)} days hashed before and after: "
          f"{len(changed)} differ")
    print(f"  the table: {total:,} lines")
    for d in (date(2025, 11, 27), DAY, date(2025, 11, 29)):
        print(f"    {d}  {was[d][0][:12]}  {now[d][0][:12]}"
              f"  {now[d][1]:>5,} lines")
    with open("code/18/01_partitioned_load.json", "w") as f:
        json.dump({"night": str(NIGHT), "relay": sorted(before),
                   "rewritten": new,
                   "day": str(DAY), "days": {
                       str(d): now[d][1] for d in sorted(now)},
                   "changed": [str(d) for d in changed]}, f, indent=1)
    tidy("01")
