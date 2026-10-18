"""
Every setting in one place: where the sources are, where Groundwork writes, and the dates
the whole book agrees on.

Two rules govern this file:

1.  **No listing hard-codes a path or a date.** Listings import `WAREHOUSE`, `LANDING`,
    `TODAY` and the rest from here, so the whole book moves together if one changes.

2.  **Nothing reads the wall clock.** A pipeline asks what day it is from the run it
    belongs to, and a listing that simulates a run says which day. `TODAY` is the book's
    today, Monday 5 January 2026, when every source has caught up with the end of 2025.

No secret appears here or in any file git can see. Frontline's token comes from the
environment (`FRONTLINE_TOKEN`), loaded from `.env`; the demo token in `.env.example`
is the one the local server accepts.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

# The project root: two levels up from this file (code/groundwork/config.py).
ROOT = Path(__file__).resolve().parents[2]

try:
    from dotenv import load_dotenv
except ImportError:                                           # pragma: no cover
    pass
else:
    load_dotenv(ROOT / ".env", override=False)


# --------------------------------------------------------------------------- dates
UTC = timezone.utc
TODAY = date(2026, 1, 5)
"""The book's today: a Monday. Every source has caught up with the last day of 2025."""

NOW = datetime(2026, 1, 5, 6, 0, tzinfo=UTC)
"""The moment the sources stand at: 06:00 UTC on TODAY, after the last nightly export."""

FIRST_DAY = date(2022, 1, 1)
"""The first day Meridian's records cover."""

LAST_DAY = date(2025, 12, 31)
"""The last day of trading on record."""

DEPOT_TZ = {1: "America/New_York", 2: "America/New_York", 3: "America/New_York",
            4: "America/Los_Angeles", 5: "America/Chicago"}
"""Each region's depot keeps its own calendar: an order's day is the depot's day."""

SEED = int(os.getenv("GROUNDWORK_SEED", "20261001"))
"""The one seed, for the few listings that sample."""

THREADS = int(os.getenv("GROUNDWORK_THREADS", "1"))
"""DuckDB threads for listings that sum floating-point numbers, where the order of
addition changes the last digit. Integer cents need no such care."""


# --------------------------------------------------------------------------- paths
DATA = ROOT / "data"
"""Everything generated. Not committed: it is rebuilt from seeds in about two minutes."""

MERIDIAN = DATA / "meridian"
"""Book 1's Meridian, untouched."""

MERIDIAN_ML = DATA / "meridian-ml"
"""Book 2's Meridian, untouched. Chapter 26 reconciles Groundwork against it."""

BOOK2_WAREHOUSE = MERIDIAN_ML / "warehouse" / "meridian_ml.db"

SOURCES = DATA / "meridian-sources"
"""Meridian's operational systems, from code/meridian/generate_sources.py."""

LANDING = SOURCES / "landing"
"""Where other teams' files arrive: relay/, suppliers/, catalogue/, portal/, ledger/."""

REFERENCE = SOURCES / "reference"
"""Files a person maintains: regions, procurement's supplier master, the price calendar."""

TRADELINE_DB = SOURCES / "tradeline" / "tradeline.db"
"""The orders application's database as it stands today. Open it read-only."""

TRUTH = SOURCES / "truth"
"""Answer keys. Read only by listings about checking a method against the truth."""

GROUNDWORK = DATA / "groundwork"
"""Everything Groundwork writes."""

RAW = GROUNDWORK / "raw"
"""The raw layer: what each source sent, as Parquet, partitioned by source and load date."""

WAREHOUSE = GROUNDWORK / "warehouse.duckdb"
"""The warehouse: staging, marts, and the compat schema Books 1 and 2 read."""

SANDBOX = DATA / "sandbox"
"""Scratch space for listings that simulate a run. Safe to delete at any time."""
