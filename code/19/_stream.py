"""The shared pieces of Chapter 19's listings.

The portal's log is built once, from every hourly file in the order the
files arrived, in the chapter's sandbox; 03_a_log.py builds it afresh
and the later listings reuse it. Each listing starts its own consumer
groups from nothing, so it prints the same thing however often it runs.
"""
import csv
import shutil
from datetime import datetime

from groundwork.config import LANDING, REFERENCE, SANDBOX
from groundwork.stream.consumer import parse
from groundwork.stream.log import Log, append_files
from meridian.sources import landing

HERE = SANDBOX / "ch19"
LOG = HERE / "portal-log"
SEGMENT = 100_000


def portal_log(fresh: bool = False) -> Log:
    """The portal's events as a log, every file replayed in arrival
    order (built the first time it is asked for)."""
    if fresh:
        shutil.rmtree(LOG, ignore_errors=True)
    log = Log(LOG, segment_records=SEGMENT)
    append_files(log, landing.arrived(under="portal"))
    return log


def fresh_group(log: Log, group: str) -> None:
    """Forget a consumer group's committed offset and state."""
    for p in (log.path / "offsets").glob(f"{group}.*"):
        p.unlink()


def offset_at(log: Log, moment: datetime) -> int:
    """The first offset received at or after `moment`. The log is in
    the order the portal received events, so bisection finds it."""
    lo, hi = log.start_offset, log.end_offset
    while lo < hi:
        mid = (lo + hi) // 2
        (_, value), = log.read(mid, 1)
        if parse(value)["received_at"] < moment:
            lo = mid + 1
        else:
            hi = mid
    return lo


def region_of() -> dict[int, int]:
    """Each account's region, from Relay's latest export."""
    with open(REFERENCE / "regions.csv", encoding="utf-8") as f:
        ids = {r["name"]: int(r["region_id"])
               for r in csv.DictReader(f)}
    latest = sorted((LANDING / "relay").glob("20*/accounts.csv"))[-1]
    with open(latest, encoding="utf-8-sig") as f:
        return {int(r["account_id"]): ids[r["region"]]
                for r in csv.DictReader(f)}
