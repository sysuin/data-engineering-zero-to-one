# The Pemberton incident replayed three ways: with Chapter 9's loader,
# with the contract and no watch, and with the watch.
import json
import textwrap
from datetime import date
from pathlib import Path

import duckdb
from _sandbox import copy_of, morning, replay, tidy

from groundwork.observe import alerts, metrics

FILE = "PEMBERTON-PRICES-202407.csv"


def runs(con):
    return con.execute("""
        SELECT logical_date, run_id, kind, state, run_at, finished_at
        FROM audit.dag_runs ORDER BY run_at, run_id""").fetchall()


def day(d):
    return f"{d.day} {d:%b}"


def wrap(s):
    return textwrap.fill(s, 66, initial_indent="  ",
                         subsequent_indent="    ")


folder = replay("unwatched")
con = duckdb.connect(str(folder / "warehouse.duckdb"), read_only=True)
arrived, rows = con.execute("""
    SELECT arrived_at, (SELECT rows FROM audit.contract_runs
                        WHERE name = f.name LIMIT 1)
    FROM audit.file_ledger f WHERE name LIKE ?""",
                            [f"%{FILE}"]).fetchone()
print(f"{FILE} arrived {arrived:%Y-%m-%d %H:%M} UTC, {rows} rows")
timeline = {"arrived": str(arrived), "rows": rows}

for name in ("unwatched", "watched"):
    replay(name)
    con = duckdb.connect(str(copy_of(name, "08")))
    every = runs(con)
    failed = [r for r in every if r[3] == "failed"]
    first = failed[0]
    (task, detail), = con.execute(
        "SELECT task, detail FROM audit.task_runs WHERE run_id = ? AND "
        "state = 'failed'", [first[1]]).fetchall()
    (never,) = con.execute(
        "SELECT count(*) FROM audit.task_runs WHERE run_id = ? AND "
        "state = 'upstream_failed'", [first[1]]).fetchone()
    print(f"\n{name}:")
    if name == "unwatched":
        print(f"  {first[1]}, run at {first[4]:%H:%M} on "
              f"{day(first[4])}:")
        why = detail.split("csv: ")[-1]
        print(wrap(f"{task} failed, fatal: {why}"))
        print(f"  {never} tasks never ran; nothing was published")
    sensor = con.execute("""
        SELECT logical_date, detail FROM audit.task_runs
        WHERE task = 'wait_relay' AND state = 'failed'""").fetchall()
    for d, why in sensor:
        print(f"  {day(d)} also: wait_relay failed, {why}")
    print(f"  nights that failed: {len(failed)}, " + (
        f"{day(failed[0][0])} to {day(failed[-1][0])}"
        if len(failed) > 2 else ", ".join(day(r[0]) for r in failed)))
    pages = []
    if name == "watched":
        for i in range(3):
            w = alerts.watch(con, morning(date(2024, 7, 1 + i)),
                             replay(name) / "raw")
            pages += [s for s in w.sent if s.kind == "page"]
        print(f"  first page reached data engineering "
              f"{pages[0].at:%H:%M} UTC on {day(pages[0].at)}")
    behind = max(metrics.freshness(con, d, morning(d))[-1].value or 0
                 for d, *_ in every if d.month == 7)
    then = [d for d, *_ in every if d.month == 7 and metrics.freshness(
        con, d, morning(d))[-1].value == behind][0]
    print(f"  furthest behind: {behind} day{'s' * (behind != 1)}, the "
          f"morning after {day(then)}")
    fix = next(r for r in every if r[2] == "rerun")
    print(f"  rerun {fix[1]} at {fix[4]:%H:%M} on {day(fix[4])}: "
          f"{fix[3]}")
    timeline[name] = {
        "failed": [str(r[0]) for r in failed],
        "first_run": str(first[4]), "fix": str(fix[4]),
        "fixed_day": str(fix[0]), "behind": behind,
        "sensor": [str(d) for d, _ in sensor],
        "page": str(pages[0].at) if pages else None,
        "worst": str(then)}
    con.close()
tidy("08")

folder = replay("chapter9")
con = duckdb.connect(str(folder / "warehouse.duckdb"), read_only=True)
failed = sum(r[3] == "failed" for r in runs(con))
(empty,) = con.execute("""
    SELECT count(*) FROM raw.suppliers_price_lists
    WHERE file LIKE '%PEMBERTON-PRICES-2024%'
      AND nullif(trim(cost), '') IS NULL""").fetchone()
print(f"\nChapter 9's loader, no contract:\n  nights that failed: "
      f"{failed}; Pemberton rows with no cost by 31 Jul: {empty}")
for s in metrics.distribution(date(2024, 7, 2), folder / "raw"):
    if s.subject == "suppliers.price_lists":
        print(f"  2 Jul, {s.measure}: {s.value:.0%} (limit "
              f"{s.high:g}): {s.status}, severity {s.severity}")
        timeline["chapter9"] = {"failed": failed, "empty": empty,
                                "share": s.value}
con.close()
Path(__file__).with_suffix(".json").write_text(json.dumps(timeline,
                                                          indent=1))
