"""Integration: Groundwork's ingest and dbt's build, run as the night
runs them, over a tiny Meridian whose files and database this suite
writes (groundwork.testing.example). Five orders, two accounts, two
contracts, two products; every row is there for a case, and every
answer is exact."""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import duckdb
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def con(tiny):
    c = duckdb.connect(str(tiny.warehouse), read_only=True)
    yield c
    c.close()


def test_every_source_landed_and_was_held_to_its_contract(con):
    runs = con.execute("SELECT table_name, status, rows FROM "
                       "audit.contract_runs ORDER BY 1").fetchall()
    assert {s for _, s, _ in runs} == {"passed"}
    assert con.execute("SELECT count(*) FROM audit.quarantine"
                       ).fetchone() == (0,)
    assert dict((t, n) for t, _, n in runs)["orders"] == 5


def test_each_line_is_dated_costed_and_owned_as_the_night_would(con):
    got = con.execute("""
        SELECT f.order_id, f.line_no, f.order_date, a.account_id,
               f.revenue, f.unit_cost, f.gross_margin
        FROM marts.fct_order_lines AS f JOIN marts.dim_account AS a
            USING (account_key) ORDER BY 1, 2""").fetchall()
    d = Decimal
    assert got == [
        (1, 1, date(2025, 12, 1), 10, d("39.98"), d("13.00"), d("13.98")),
        (1, 2, date(2025, 12, 1), 10, d("9.99"), d("5.00"), d("4.99")),
        # 03:30 UTC on 2 December is the evening before in Sacramento
        (2, 1, date(2025, 12, 1), 20, d("19.99"), d("13.00"), d("6.99")),
        # order 3 was cancelled; order 4's account Relay never sent
        (4, 1, date(2025, 12, 4), -1, d("19.99"), d("13.00"), d("6.99")),
        # 04:30 UTC on 1 December is 30 November in Newark: old cost
        (5, 1, date(2025, 11, 30), 10, d("19.99"), d("12.00"), d("7.99"))]


def test_orders_are_their_lines_and_tie_to_tradeline(con, tiny):
    src = sqlite3.connect(tiny.tradeline_db)
    (cents,) = src.execute(
        "SELECT sum(qty * unit_price_cents) FROM order_lines JOIN orders"
        " ON id = order_id WHERE status = 'placed'").fetchone()
    src.close()
    (lines,) = con.execute("SELECT sum(revenue) FROM marts.fct_order_lines"
                           ).fetchone()
    (orders,) = con.execute("SELECT sum(revenue) FROM marts.fct_orders"
                            ).fetchone()
    assert lines == orders == Decimal(cents) / 100 == Decimal("109.94")


def test_contracts_are_in_force_on_their_month_ends(con):
    got = con.execute("""
        SELECT contract_id, count(*), min(month_end), max(month_end),
               any_value(discount_pct)
        FROM marts.fct_contract_month GROUP BY 1 ORDER BY 1""").fetchall()
    assert got == [(1, 12, date(2025, 1, 31), date(2025, 12, 31), 5),
                   # legacy terms: in force, and no discount recorded
                   (2, 6, date(2025, 7, 31), date(2025, 12, 31), None)]
