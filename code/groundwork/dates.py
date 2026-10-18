"""
The date dimension, and the depot's calendar.

`dim_date` is the one table in the marts that no source sends. Every row is worked out
from the calendar: one row per day from 1 January 2021 to 31 December 2026, a year either
side of Meridian's records, so that a fact dated on any day in range finds its row.

Two rules from DECISIONS.md live here:

1.  **An order's day is the depot's day.** `local_date()` turns a UTC moment into the
    calendar date at the depot of the region it belongs to. A fact joins `dim_date` on
    that date, never on the UTC date of its timestamp.

2.  **Meridian's financial year is the calendar year.** `FISCAL_START_MONTH` is 1. The
    fiscal columns are kept anyway, because the day it changes is the day every report
    that grouped by `year` is wrong. A fiscal year is named for the calendar year it ends in.

    from groundwork.dates import build, load, local_date
    rows = build()                     # 2,191 tuples, in COLUMNS order
    load(con)                          # fills marts.dim_date, replacing what was there
"""
from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from groundwork.config import DEPOT_TZ, UTC

FIRST = date(2021, 1, 1)
LAST = date(2026, 12, 31)
FISCAL_START_MONTH = 1

COLUMNS = (
    "date_key", "date", "year", "quarter", "month", "month_name", "day_of_month",
    "day_of_year", "day_of_week", "day_name", "is_weekend", "is_working_day",
    "working_day_of_month", "iso_year", "iso_week", "week_start", "month_start",
    "month_end", "is_month_end", "fiscal_year", "fiscal_quarter", "fiscal_month",
)
"""The columns of marts.dim_date, in table order (see ddl/marts.sql)."""


def date_key(d: date) -> int:
    """The row's key, 20251231 for 31 December 2025: sortable, and readable in a query."""
    return d.year * 10000 + d.month * 100 + d.day


def fiscal(d: date, start_month: int = FISCAL_START_MONTH) -> tuple[int, int, int]:
    """(fiscal year, fiscal quarter, fiscal month) of a day, the year named for its end."""
    if not 1 <= start_month <= 12:
        raise ValueError(f"a fiscal year starts in a month from 1 to 12, not {start_month}")
    month = (d.month - start_month) % 12 + 1
    year = d.year + (1 if start_month > 1 and d.month >= start_month else 0)
    return year, (month - 1) // 3 + 1, month


def build(first: date = FIRST, last: date = LAST,
          fiscal_start_month: int = FISCAL_START_MONTH) -> list[tuple]:
    """One tuple per day from `first` to `last`, in COLUMNS order."""
    if last < first:
        raise ValueError(f"the calendar ends ({last}) "
                         f"before it starts ({first})")
    rows, d = [], first
    worked, month = 0, None
    while d <= last:
        if (d.year, d.month) != month:
            # Count working days from the 1st, even if `first` is not.
            month = (d.year, d.month)
            start = d.replace(day=1)
            worked = sum(1 for i in range(d.day - 1)
                         if (start + timedelta(days=i)).weekday() < 5)
        weekday = d.isoweekday()           # 1 Monday ... 7 Sunday
        working = weekday <= 5             # finance: Monday to Friday
        worked += working
        iso_year, iso_week, _ = d.isocalendar()
        last_day = calendar.monthrange(d.year, d.month)[1]
        fy, fq, fm = fiscal(d, fiscal_start_month)
        rows.append((
            date_key(d), d, d.year, (d.month - 1) // 3 + 1, d.month,
            calendar.month_name[d.month], d.day,
            d.timetuple().tm_yday, weekday,
            calendar.day_name[d.weekday()], weekday >= 6, working,
            worked if working else None, iso_year, iso_week,
            d - timedelta(days=weekday - 1), d.replace(day=1),
            d.replace(day=last_day), d.day == last_day, fy, fq, fm,
        ))
        d += timedelta(days=1)
    return rows


def local_date(moment: datetime, region_id: int) -> date:
    """
    The calendar date at the region's depot of a moment. A naive datetime is read as UTC,
    because every `*_at` column Groundwork stores is UTC without a zone.
    """
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(ZoneInfo(DEPOT_TZ[region_id])).date()


def load(con, first: date = FIRST, last: date = LAST, table: str = "marts.dim_date",
         fiscal_start_month: int = FISCAL_START_MONTH) -> int:
    """
    Replace the contents of `table` with the calendar, in one transaction, so a rerun
    leaves the same rows and a failure leaves the old ones. Returns the rows written.
    """
    rows = build(first, last, fiscal_start_month)
    marks = ", ".join("?" for _ in COLUMNS)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {table}")
        con.executemany(f"INSERT INTO {table} ({', '.join(COLUMNS)}) VALUES ({marks})",
                        rows)
    except Exception:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")
    return len(rows)
