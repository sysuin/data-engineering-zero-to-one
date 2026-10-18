"""Tests for groundwork.dates (the date dimension) and the marts DDL it fills."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import duckdb
import pytest

from groundwork import dates
from groundwork.ddl import create


@pytest.fixture(scope="module")
def rows():
    return [dict(zip(dates.COLUMNS, r)) for r in dates.build()]


def test_one_row_per_day_with_no_gaps(rows):
    assert len(rows) == (dates.LAST - dates.FIRST).days + 1 == 2191
    assert rows[0]["date"] == date(2021, 1, 1) and rows[-1]["date"] == date(2026, 12, 31)
    for a, b in zip(rows, rows[1:]):
        assert b["date"] - a["date"] == timedelta(days=1)
    assert len({r["date_key"] for r in rows}) == len(rows)


def test_date_key_is_the_date_as_a_number():
    assert dates.date_key(date(2025, 12, 31)) == 20251231
    assert dates.date_key(date(2024, 2, 29)) == 20240229


def test_calendar_columns(rows):
    day = {r["date"]: r for r in rows}
    leap = day[date(2024, 2, 29)]
    assert leap["is_month_end"] and leap["month_end"] == date(2024, 2, 29)
    assert leap["day_of_year"] == 60 and leap["quarter"] == 1
    assert day[date(2025, 2, 28)]["is_month_end"]              # 2025 is not a leap year
    sat = day[date(2026, 1, 3)]
    assert sat["day_name"] == "Saturday" and sat["day_of_week"] == 6
    assert sat["is_weekend"] and not sat["is_working_day"]
    assert sat["working_day_of_month"] is None
    assert sat["week_start"] == date(2025, 12, 29)


def test_iso_week_can_belong_to_the_next_year(rows):
    nye = next(r for r in rows if r["date"] == date(2025, 12, 31))
    assert nye["year"] == 2025 and nye["iso_year"] == 2026 and nye["iso_week"] == 1


def test_working_days_count_monday_to_friday():
    jan = {r[1]: r for r in dates.build(date(2026, 1, 1), date(2026, 1, 31))}
    ix = dates.COLUMNS.index("working_day_of_month")
    assert jan[date(2026, 1, 1)][ix] == 1          # a Thursday
    assert jan[date(2026, 1, 7)][ix] == 5          # the fifth working day
    assert jan[date(2026, 1, 30)][ix] == 22


def test_working_days_count_from_the_first_even_mid_month():
    whole = {r[1]: r for r in dates.build(date(2025, 12, 1), date(2025, 12, 31))}
    part = {r[1]: r for r in dates.build(date(2025, 12, 15), date(2025, 12, 31))}
    ix = dates.COLUMNS.index("working_day_of_month")
    for d, r in part.items():
        assert r[ix] == whole[d][ix]


@pytest.mark.parametrize("d, start, expected", [
    (date(2025, 1, 15), 1, (2025, 1, 1)),
    (date(2025, 12, 31), 1, (2025, 4, 12)),
    (date(2025, 3, 31), 4, (2025, 4, 12)),     # an April year: FY2025 ends in March
    (date(2025, 4, 1), 4, (2026, 1, 1)),
    (date(2025, 7, 1), 7, (2026, 1, 1)),
])
def test_fiscal_year_is_named_for_the_year_it_ends(d, start, expected):
    assert dates.fiscal(d, start) == expected


def test_fiscal_start_month_must_be_a_month():
    with pytest.raises(ValueError):
        dates.fiscal(date(2025, 1, 1), 13)


def test_a_backwards_range_is_refused():
    with pytest.raises(ValueError):
        dates.build(date(2026, 1, 1), date(2025, 1, 1))


@pytest.mark.parametrize("moment, region, expected", [
    # 22:50 on New Year's Eve in Sacramento is 06:50 on 1 January in UTC.
    (datetime(2026, 1, 1, 6, 50, 33), 4, date(2025, 12, 31)),
    # The same moment is already 1 January in Newark and Dallas.
    (datetime(2026, 1, 1, 6, 50, 33), 1, date(2026, 1, 1)),
    (datetime(2026, 1, 1, 6, 50, 33), 5, date(2026, 1, 1)),
    # 04:30 UTC is still the evening before in Newark, in winter and in summer.
    (datetime(2025, 1, 15, 4, 30), 1, date(2025, 1, 14)),
    (datetime(2025, 7, 15, 3, 30), 1, date(2025, 7, 14)),
    # 04:30 UTC in summer is 00:30 in Newark: the same day.
    (datetime(2025, 7, 15, 4, 30), 1, date(2025, 7, 15)),
])
def test_local_date_is_the_depots_calendar(moment, region, expected):
    assert dates.local_date(moment, region) == expected


def test_local_date_accepts_an_aware_moment():
    aware = datetime(2026, 1, 1, 6, 50, 33, tzinfo=timezone.utc)
    assert dates.local_date(aware, 4) == date(2025, 12, 31)


def test_load_fills_the_mart_and_reruns_cleanly():
    con = duckdb.connect()
    tables = create(con, "marts")
    assert "dim_date" in tables and "fct_order_lines" in tables
    assert create(con, "marts") == tables                  # the DDL is rerunnable
    assert dates.load(con) == 2191
    assert dates.load(con) == 2191                         # and so is the load
    (n, lo, hi) = con.execute("SELECT COUNT(*), MIN(date), MAX(date) "
                              "FROM marts.dim_date").fetchone()
    assert (n, lo, hi) == (2191, date(2021, 1, 1), date(2026, 12, 31))


def test_money_in_the_marts_is_decimal():
    con = duckdb.connect()
    create(con, "marts")
    types = con.execute("SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = 'marts' AND column_name IN "
                        "('revenue', 'cost', 'gross_margin', 'unit_price', 'unit_cost', "
                        "'list_price')").fetchall()
    assert types and all(t == "DECIMAL(18,2)" for _, t in types)
