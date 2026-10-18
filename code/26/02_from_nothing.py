# timeout: 7200
# nondeterministic: it prints how long make data and the night took
# From nothing, twice: make data into a folder of its own, checked
# byte for byte against data/; then Groundwork built on an empty folder
# by one command, the v1.0 night for 4 January 2026.
import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
from _sandbox import FRESH, HERE

from groundwork import nightly
from groundwork.config import DATA, ROOT

WORLD = HERE / "world"
MAKE_DATA = [  # what `make data` runs, pointed at WORLD
    ("generate.py", "--out", WORLD / "meridian"),
    ("generate_ml.py", "--book1", WORLD / "meridian", "--out",
     WORLD / "meridian-ml"),
    ("verify_ml.py", "--data", WORLD / "meridian-ml", "--book1",
     WORLD / "meridian"),
    ("generate_sources.py", "--ml", WORLD / "meridian-ml", "--out",
     WORLD / "meridian-sources"),
    ("verify_sources.py", "--sources", WORLD / "meridian-sources",
     "--ml", WORLD / "meridian-ml", "--book1", WORLD / "meridian"),
]
PARTS = {"meridian": "Book 1's Meridian", "meridian-ml": "Book 2's",
         "meridian-sources": "the seven sources"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_data() -> dict:
    """Every script `make data` runs, into WORLD; its last words."""
    shutil.rmtree(WORLD, ignore_errors=True)
    said = {}
    for script, *args in MAKE_DATA:
        path = ROOT / "code" / "meridian" / script
        done = subprocess.run([sys.executable, str(path),
                               *map(str, args)],
                              capture_output=True, text=True)
        said[script] = (done.returncode,
                        done.stdout.strip().splitlines()[-1])
    return said


def same_as_data() -> None:
    """Each generated file against its twin under data/, by SHA-256;
    and every landed file's arrival time."""
    same = arrived = files = landed = 0
    for part, label in PARTS.items():
        ours = sorted(p for p in (WORLD / part).rglob("*")
                      if p.is_file())
        for p in ours:
            twin = DATA / part / p.relative_to(WORLD / part)
            same += twin.exists() and sha(p) == sha(twin)
            if "landing" in p.parts:
                landed += 1
                arrived += (int(p.stat().st_mtime)
                            == int(twin.stat().st_mtime))
        files += len(ours)
        size = sum(p.stat().st_size for p in ours) / 1e6
        print(f"  {label:<20}{len(ours):>7,} files{size:>9.1f} MB")
    print(f"  identical to data/: {same:,} of {files:,} files;")
    print(f"  landing zone arrival times the same: {arrived:,} of "
          f"{landed:,}")


def first_night():
    shutil.rmtree(FRESH, ignore_errors=True)
    began = time.monotonic()
    run = nightly.build(FRESH)           # make warehouse FOLDER=...
    return run, (time.monotonic() - began) / 60


def main():
    began = time.monotonic()
    said = make_data()
    data_minutes = (time.monotonic() - began) / 60
    print("make data, into data/sandbox/ch26/world")
    same_as_data()
    for script in ("verify_ml.py", "verify_sources.py"):
        code, last = said[script]
        print(f"  {script:<20}exit {code}: {last}")
    print(f"  about {data_minutes:.0f} minutes")
    shutil.rmtree(WORLD)

    run, minutes = first_night()
    con = duckdb.connect(str(FRESH / "warehouse.duckdb"),
                         read_only=True)
    tasks = con.execute("""
        SELECT task, try_number, epoch(finished_at - started_at), detail
        FROM audit.task_runs WHERE run_id = ? AND state = 'succeeded'
        ORDER BY started_at, task""", [run.run_id]).fetchall()
    print(f"\nmake warehouse, on an empty folder: {run.run_id}, "
          f"{run.state}")
    print(f"{'task':<17}{'try':>4}{'sec':>6}  detail")
    for task, tries, secs, detail in tasks:
        d = detail if len(detail) <= 38 else detail[:35] + "..."
        print(f"{task:<17}{tries:>4}{secs:>6.0f}  {d}")
    results = json.loads((FRESH / "dbt" / "run_results.json")
                         .read_text())["results"]
    slowest = max(results, key=lambda r: r["execution_time"])
    print(f"slowest dbt node: {slowest['unique_id'].split('.')[-1]}, "
          f"{slowest['execution_time']:.0f} s")
    rows = {t: con.execute(f"SELECT count(*) FROM marts.{t}")
            .fetchone()[0] for t in ("fct_order_lines", "dim_account",
                                     "fct_portal_sessions")}
    con.close()
    size = sum(p.stat().st_size for p in FRESH.rglob("*")
               if p.is_file())
    for t, n in rows.items():
        print(f"  {t:<22}{n:>10,} rows")
    print(f"from nothing to published: about {minutes:.0f} minutes, "
          f"{size / 1e6:.0f} MB on disk")
    Path(__file__).with_suffix(".json").write_text(json.dumps(
        {"make_data_minutes": data_minutes, "night_minutes": minutes,
         "tasks": [[t, s] for t, _, s, _ in tasks]}))


if __name__ == "__main__":       # the night's tasks start processes
    main()
