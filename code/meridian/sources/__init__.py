"""
Meridian's operational sources, as they stood at any moment.

The files under data/meridian-sources/ hold each system's full history. These modules
replay it, so a listing can ask what a pipeline would have seen had it run at 02:05 UTC
on 14 March 2025, and get the same answer on every machine:

    tradeline   the orders application's database, replayed to a moment
    relay       the CRM's nightly export for any night, and its field audit
    frontline   the support desk's API, served locally, answering as of a clock
    landing     the files that had arrived by a moment

None of this is the reader's code. It stands in for systems other people run, and the
book says so each time a listing uses it. Appendix F describes how it works.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[3]
SOURCES = Path(os.getenv("MERIDIAN_SOURCES", ROOT / "data" / "meridian-sources"))
LANDING = SOURCES / "landing"

TODAY = date(2026, 1, 5)
NOW = datetime(2026, 1, 5, 6, 0, tzinfo=UTC)


def parse_moment(when) -> datetime:
    """A moment from a datetime, a date (midnight UTC) or a string, always aware and UTC."""
    if isinstance(when, datetime):
        return when.astimezone(UTC) if when.tzinfo else when.replace(tzinfo=UTC)
    if isinstance(when, date):
        return datetime(when.year, when.month, when.day, tzinfo=UTC)
    s = str(when).strip().replace("T", " ").rstrip("Z")
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


class Clock:
    """
    The simulated time a source answers to. Listings set it; nothing reads the wall clock.

        clock = Clock("2025-03-14 02:05")
        clock.advance(minutes=10)
    """

    def __init__(self, now=NOW):
        self.now = parse_moment(now)

    def advance(self, **kw) -> datetime:
        self.now += timedelta(**kw)
        return self.now

    def set(self, when) -> datetime:
        self.now = parse_moment(when)
        return self.now

    def __repr__(self) -> str:
        return f"Clock({self.now:%Y-%m-%d %H:%M:%S} UTC)"
