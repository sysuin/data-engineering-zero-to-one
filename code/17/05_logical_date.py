# timeout: 900
# The run for Friday 14 March 2025 happens at 06:00 UTC on Saturday the
# 15th: what each part of it reads, and what "yesterday" would read.
import json
import os
import sqlite3
import subprocess
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import duckdb
from _sandbox import tidy, work

from groundwork.orchestrate.dag import DBT, PROJECT
from meridian.sources import relay

day = date(2025, 3, 14)
due = datetime.combine(day + timedelta(days=1), datetime.min.time())
due = due.replace(hour=6)
crm = sqlite3.connect(f"file:{relay.STORE}?mode=ro", uri=True)
written = dict(crm.execute("SELECT night, exported_at FROM nights"))
print(f"logical date   {day:%a %d %b %Y}")
print(f"run due        {due:%a %d %b %Y %H:%M} UTC")
w = datetime.fromisoformat(written[str(day)])
print(f"Relay export   relay/{day}/, written {w:%d %b %H:%M} UTC")
print(f"Tradeline      as it stands at {due:%H:%M} on the {due:%d}th")
print(f"dbt vars       night={day}, since=<the last read a build used>")

# The date a script works out for itself, on four servers' clocks.
print("\nthe day a script picks for itself, and the export for it:")
utc = due.replace(tzinfo=timezone.utc)
asks = [("today", "UTC", utc), ("yesterday", "UTC", utc),
        ("yesterday", "New York", utc),
        ("yesterday", "Los Angeles", utc),
        ("yesterday", "UTC, rerun", datetime(2025, 3, 20, 9,
                                              tzinfo=timezone.utc))]
zones = {"New York": "America/New_York",
         "Los Angeles": "America/Los_Angeles"}
picked = []
for how, where, at in asks:
    local = at.astimezone(ZoneInfo(zones.get(where, "UTC")))
    got = local.date() - timedelta(days=how == "yesterday")
    came = written.get(str(got))
    ready = came is not None and came <= f"{at:%Y-%m-%d %H:%M}"
    picked.append([how, where, f"{local:%d %b %H:%M}", str(got)])
    print(f"  {how:<9} {where:<11} {local:%d %b %H:%M}  {got}  "
          f"{'right' if got == day else 'WRONG':<5}  "
          f"{'there' if ready else 'not yet'}")

# Orders whose depot day is D but which were placed after the run for D.
HERE = work("05")
con = duckdb.connect(str(HERE / "warehouse.duckdb"), read_only=True)
(n, days, total) = con.execute("""
    SELECT count(*) FILTER (WHERE placed_at >= order_date + 1
                                   + INTERVAL 6 HOUR),
           count(DISTINCT order_date) FILTER (
               WHERE placed_at >= order_date + 1 + INTERVAL 6 HOUR),
           count(*) FROM marts.fct_orders""").fetchone()
con.close()
print(f"\norders placed after 06:00 UTC on the day after their depot "
      f"day:\n  {n:,} of {total:,}, on {days:,} logical dates")

# The snapshot told the run date instead of the logical date.
def snapshot(night):
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               GROUNDWORK_DBT_PATH=str(HERE / "warehouse.duckdb"))
    subprocess.run([str(DBT), "snapshot", "--target", "dev",
                    "--profiles-dir", ".", "--quiet",
                    "--vars", json.dumps({"night": night}),
                    "--target-path", str(HERE / "target"),
                    "--log-path", str(HERE / "logs")],
                   cwd=PROJECT, env=env, check=True,
                   capture_output=True)
    w = duckdb.connect(str(HERE / "warehouse.duckdb"), read_only=True)
    got = w.execute("""
        SELECT count(*) FILTER (WHERE dbt_valid_to IS NULL),
               count(*) FILTER (WHERE dbt_valid_to IS NOT NULL)
        FROM snapshots.snp_relay__accounts""").fetchone()
    w.close()
    return got


print("\nsnapshots.snp_relay__accounts after dbt snapshot, night=")
for night, why in (("2026-01-04", "the logical date"),
                   ("2026-01-05", "the run date")):
    open_, closed = snapshot(night)
    print(f"  {night}  {why:<17} {open_:>6,} current {closed:>6,} "
          "closed")
tidy("05")
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "day": str(day), "due": due.isoformat(), "export": str(w),
    "picked": picked, "late_orders": n, "late_days": days}))
