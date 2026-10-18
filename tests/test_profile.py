"""
groundwork.profile: every number in a profile, checked on a table small
enough to count by hand, plus one real source.
"""
from __future__ import annotations

import pytest

from groundwork.profile import WIDTH, connect, fit, profile


@pytest.fixture()
def con():
    return connect()


@pytest.fixture()
def orders(con):
    """Six orders written the way an application might write them."""
    con.execute("""
        CREATE TABLE orders AS SELECT * FROM (VALUES
          (1, 'placed',    '2025-11-30 23:10:00', 'MRD-001',  '$88.64', 10),
          (2, 'placed',    '2025-12-01 04:59:59', 'MRD-002',  '$5.23',  20),
          (3, 'cancelled', '2025-12-01 05:00:00', 'mrd-003',  '$12.00', NULL),
          (4, 'placed',    '2025-12-02 14:00:00', 'MRD-004 ', '$1,050.00', 40),
          (5, 'placed',    NULL,                  'MRD-001',  '$88.64', 50),
          (6, 'placed',    '2025-12-03 09:30:00', 'MRD-002',  '$5.23',  60)
        ) AS t(id, status, placed_at, sku, list_price, qty)""")
    return con.table("orders")


def column(p, name):
    return next(c for c in p.columns if c.name == name)


def test_counts_by_hand(orders):
    p = profile(orders, "orders")
    assert p.rows == 6 and len(p.columns) == 6
    placed_at = column(p, "placed_at")
    assert (placed_at.nulls, placed_at.distinct) == (1, 5)
    assert placed_at.min == "2025-11-30 23:10:00"
    assert placed_at.max == "2025-12-03 09:30:00"
    qty = column(p, "qty")
    assert (qty.type, qty.nulls, qty.min, qty.max) == \
        ("INTEGER", 1, "10", "60")


def test_top_values_only_when_they_repeat(orders):
    p = profile(orders, "orders")
    assert column(p, "status").top == [("placed", 5), ("cancelled", 1)]
    assert column(p, "id").top == []          # every value unique


def test_notes(orders):
    p = profile(orders, "orders")
    assert column(p, "placed_at").notes == ["timestamps held as text"]
    assert column(p, "list_price").notes == ["money held as text"]
    assert column(p, "sku").notes == [
        "1 in lower case, 5 in upper case",
        "1 with leading or trailing spaces"]
    assert column(p, "status").notes == []    # all lower case is fine
    assert "sku: 1 with leading or trailing spaces" in p.notes()


def test_dates_are_not_timestamps(con):
    rel = con.sql("SELECT * FROM (VALUES ('2025-12-01'), ('2025-12-31')) "
                  "AS t(effective_from)")
    assert profile(rel).columns[0].notes == ["dates held as text"]


def test_nested_columns(con):
    rel = con.sql("""SELECT * FROM (VALUES
        ({'id': 1}, ['a', 'b']), ({'id': 1}, ['a']), (NULL, ['a']))
        AS t(requester, tags)""")
    p = profile(rel)
    requester, tags = p.columns
    assert (requester.type, requester.nulls, requester.distinct) == \
        ("STRUCT", 1, 1)
    assert requester.min is None              # no order among structs
    assert (tags.type, tags.distinct) == ("VARCHAR[]", 2)


def test_empty_table(con):
    rel = con.sql("SELECT 1 AS a WHERE false")
    p = profile(rel, "empty")
    assert p.rows == 0 and p.columns[0].distinct == 0
    assert "empty: 0 rows, 1 columns" in str(p)


def test_a_file_by_its_path(tmp_path):
    path = tmp_path / "prices.csv"
    path.write_text("SKU,Supplier\nMRD-001,Voss Industrial \n"
                    "MRD-002,Pemberton\n", encoding="utf-8")
    p = profile(path)
    assert p.name == "prices.csv" and p.rows == 2
    assert column(p, "Supplier").notes == [
        "1 with leading or trailing spaces"]


def test_prints_within_the_page(con):
    long = "x" * 200
    rel = con.sql(f"SELECT '{long}' AS a_column_with_a_long_name_indeed, "
                  f"'{long}' AS b UNION ALL SELECT 'y', '{long}'")
    text = str(profile(rel, "wide"))
    assert max(len(line) for line in text.splitlines()) <= WIDTH


def test_fit():
    assert fit("short", 10) == "short"
    assert fit("a much longer value", 10) == "a much ..."
    assert len(fit("a much longer value", 10)) == 10


@pytest.mark.slow
def test_tradeline_channels(sources):
    from groundwork.config import TRADELINE_DB
    con = connect()
    con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
    p = profile(con.table("t.channels"), "channels")
    assert p.rows == 4
    assert column(p, "code").distinct == 4
