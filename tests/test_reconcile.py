"""Chapter 15's reconciliations over a small hand-made warehouse: the
revenue tie to the ledger, the bridge from the dashboard's number, and
counts against a source."""
from __future__ import annotations

import csv
import shutil
import sqlite3
from datetime import date, datetime
from decimal import Decimal

import duckdb
import pytest

from groundwork import loadlog, reconcile
from groundwork.reconcile import Journal

NOV = date(2025, 11, 1)
RUN = datetime(2026, 1, 5, 6, 0)
REBATE = "Manual adjustment: rebate"

# order id, placed_at (UTC), local day (Chicago), status, qty, cents
ORDERS = [
    (1, "2025-11-01 03:00:00", date(2025, 10, 31), "placed", 1, 1000),
    (2, "2025-11-15 12:00:00", date(2025, 11, 15), "placed", 2, 2500),
    (3, "2025-11-20 12:00:00", date(2025, 11, 20), "cancelled", 1, 700),
    (4, "2025-12-01 04:00:00", date(2025, 11, 30), "placed", 1, 333),
]


def build(path, *, sw_nov_sales=5333):
    """Southwest only: four orders, and October and November ledgers."""
    con = duckdb.connect(str(path))
    for s in ("raw", "staging", "marts"):
        con.execute(f"CREATE SCHEMA {s}")
    con.execute("CREATE TABLE marts.dim_region AS SELECT 'r5' AS "
                "region_key, 'SW' AS region_code")
    con.execute("CREATE TABLE marts.fct_order_lines (order_id BIGINT, "
                "region_key VARCHAR, placed_at TIMESTAMP, order_date DATE,"
                " revenue DECIMAL(18, 2))")
    con.execute("CREATE TABLE staging.stg_tradeline__orders (order_id "
                "BIGINT, status VARCHAR, placed_at TIMESTAMP)")
    con.execute("CREATE TABLE staging.stg_tradeline__order_lines "
                "(order_id BIGINT, qty INTEGER, unit_price_cents BIGINT)")
    con.execute("CREATE TABLE raw.tradeline_orders_current (id BIGINT, "
                "placed_at VARCHAR, status VARCHAR)")
    con.execute("CREATE TABLE raw.tradeline_order_lines_current (order_id"
                " BIGINT, qty BIGINT, unit_price_cents BIGINT)")
    for oid, at, day, status, qty, cents in ORDERS:
        con.execute("INSERT INTO staging.stg_tradeline__orders VALUES "
                    "(?, ?, ?)", [oid, status, at])
        con.execute("INSERT INTO staging.stg_tradeline__order_lines "
                    "VALUES (?, ?, ?)", [oid, qty, cents])
        con.execute("INSERT INTO raw.tradeline_orders_current VALUES "
                    "(?, ?, ?)", [oid, at, status])
        con.execute("INSERT INTO raw.tradeline_order_lines_current "
                    "VALUES (?, ?, ?)", [oid, qty, cents])
        if status == "placed":
            con.execute("INSERT INTO marts.fct_order_lines VALUES "
                        "(?, 'r5', ?, ?, ?)",
                        [oid, at, day, Decimal(qty * cents) / 100])
    con.execute("CREATE TABLE staging.stg_ledger__gl_revenue ("
                "period_month DATE, region_code VARCHAR, description "
                "VARCHAR, amount_cents BIGINT)")
    con.executemany(
        "INSERT INTO staging.stg_ledger__gl_revenue VALUES (?,?,?,?)",
        [(date(2025, 10, 1), "SW", "Sales journal", 1000),
         (NOV, "SW", "Sales journal", sw_nov_sales),
         (NOV, "SW", REBATE, -100)])
    return con


REGISTER = [Journal(NOV, "SW", -100, REBATE, "a rebate", "finance")]


@pytest.fixture
def con(tmp_path):
    c = build(tmp_path / "warehouse.duckdb")
    yield c
    c.close()


def test_the_register_in_the_project_holds_finances_two_journals():
    js = reconcile.journals()
    assert [(j.period, j.region, j.cents) for j in js] == [
        (date(2024, 9, 1), "MW", -1850000),
        (date(2025, 11, 1), "SW", -1240000)]
    assert all(j.reason and j.confirmed_by for j in js)


def test_revenue_ties_each_cell_and_reaches_the_ledger(con):
    cells = reconcile.revenue(con, REGISTER)
    assert [(c.period, c.ties, c.closes, c.documented_ok) for c in cells
            ] == [(date(2025, 10, 1), True, True, True),
                  (NOV, True, True, True)]
    nov = cells[1]
    assert (nov.warehouse, nov.sales, nov.ledger) == (5333, 5333, 5233)


def test_an_undocumented_journal_is_named(con):
    nov = reconcile.revenue(con, register=[])[1]
    assert nov.ties and not nov.documented_ok and not nov.closes
    b = reconcile.bridge(con, NOV, register=[])
    assert b.steps[-1].note.startswith("NOT in the register")


def test_the_bridge_walks_the_dashboard_to_the_ledger_exactly(con):
    b = reconcile.bridge(con, "2025-11", register=REGISTER)
    names = [s.name for s in b.steps]
    assert names == ["floating-point dust", "cancelled orders",
                     "UTC Nov, the depot's Oct", "UTC Dec, the depot's Nov",
                     "manual journal, SW"]
    amounts = [s.amount for s in b.steps]
    assert amounts[1:] == [Decimal("-7.00"), Decimal("-10.00"),
                           Decimal("3.33"), Decimal("-1.00")]
    assert [s.orders for s in b.steps[1:4]] == [1, 1, 1]
    assert b.start + amounts[0] == Decimal("67.00")
    assert b.ledger == Decimal("52.33") and b.closes


def test_the_bridge_starts_from_the_number_it_is_given(con):
    b = reconcile.bridge(con, NOV, start=66.99999999999999, exact=6700,
                         orders=3, register=REGISTER)
    assert b.start == Decimal("66.99999999999999")
    assert b.steps[0].amount == Decimal("0.00000000000001")
    assert b.closes


def test_a_cell_that_does_not_tie_leaves_an_unexplained_step(tmp_path):
    c = build(tmp_path / "warehouse.duckdb", sw_nov_sales=5400)
    b = reconcile.bridge(c, NOV, register=REGISTER)
    odd = [s for s in b.steps if s.note.startswith("UNEXPLAINED")]
    assert [s.amount for s in odd] == [Decimal("0.67")] and b.closes
    assert not reconcile.revenue(c, REGISTER)[1].ties


def test_a_warehouse_cell_the_ledger_lacks_is_not_dropped(con):
    con.execute("INSERT INTO marts.dim_region VALUES ('r-1', '--')")
    con.execute("INSERT INTO marts.fct_order_lines VALUES (9, 'r-1', "
                "'2025-11-03 12:00', '2025-11-03', 5)")
    cells = reconcile.revenue(con, REGISTER)
    odd = [c for c in cells if c.region == "--"]
    assert [(c.warehouse, c.sales, c.ties) for c in odd] == [
        (500, 0, False)]


def test_counts_name_what_the_warehouse_leaves_out(tmp_path, con):
    src = sqlite3.connect(tmp_path / "tradeline.db")
    src.execute("CREATE TABLE orders (id INTEGER, status TEXT)")
    src.execute("CREATE TABLE order_lines (order_id INTEGER)")
    src.executemany("INSERT INTO orders VALUES (?, ?)",
                    [(o[0], o[3]) for o in ORDERS])
    src.executemany("INSERT INTO order_lines VALUES (?)",
                    [(o[0],) for o in ORDERS])
    src.commit()
    src.close()
    landing = tmp_path / "landing"
    (landing / "relay" / "2026-01-04").mkdir(parents=True)
    (landing / "ledger").mkdir()
    for name, rows in (("accounts", 2), ("contracts", 3)):
        with open(landing / "relay" / "2026-01-04" / f"{name}.csv", "w",
                  newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["id"])
            w.writerows([[i] for i in range(rows)])
    shutil.copy(reconcile.JOURNALS, landing / "ledger" / "x.csv")
    con.execute("CREATE TABLE marts.fct_orders AS SELECT DISTINCT "
                "order_id FROM marts.fct_order_lines")
    con.execute("CREATE TABLE marts.dim_account AS SELECT * FROM (VALUES "
                "(1, true), (2, true), (2, false), (-1, true)) "
                "t(account_id, is_current)")
    con.execute("CREATE TABLE marts.fct_contracts AS SELECT * FROM "
                "range(3)")
    con.execute("CREATE TABLE marts.fct_tickets AS SELECT * FROM range(5)")
    load = loadlog.start(con, "frontline", "tickets", date(2026, 1, 4),
                         started_at=RUN)
    loadlog.finish(con, load, finished_at=RUN, source_count=5)
    cs = {c.name: c for c in reconcile.counts(
        con, tradeline=tmp_path / "tradeline.db", landing=landing)}
    assert (cs["Tradeline orders"].source, cs["Tradeline orders"].warehouse,
            cs["Tradeline orders"].explained) == (4, 3, (("cancelled", 1),))
    assert all(c.ties for c in cs.values() if c.name != "ledger lines")
    assert not cs["ledger lines"].ties          # 2 lines vs 3 staged


def test_record_writes_one_run_once(con):
    cells = reconcile.revenue(con, REGISTER)
    b = reconcile.bridge(con, NOV, register=REGISTER)
    cs = [reconcile.Count("x", 3, 2, (("left out", 1),))]
    n = reconcile.record(con, RUN, cells, b, cs)
    assert reconcile.record(con, RUN, cells, b, cs) == n
    rows = dict(con.execute("SELECT status, count(*) FROM "
                            "audit.reconciliations GROUP BY 1").fetchall())
    assert rows == {"ties": 2, "explained": 7}


def test_the_report_keeps_to_the_page(con):
    cells = reconcile.revenue(con, REGISTER)
    b = reconcile.bridge(con, NOV, register=REGISTER)
    lines = reconcile.report(cells, b, [reconcile.Count(
        "Tradeline lines", 1243789, 1229062, (("cancelled", 14727),))])
    assert max(len(line) for line in lines) <= 68


@pytest.mark.slow
def test_the_real_warehouse_ties(warehouse, tmp_path):
    copy = tmp_path / "warehouse.duckdb"
    shutil.copy(warehouse, copy)
    con = duckdb.connect(str(copy), read_only=True)
    cells = reconcile.revenue(con)
    assert all(c.ties and c.closes for c in cells)
    assert reconcile.bridge(con, NOV).closes
    assert all(c.ties for c in reconcile.counts(con))
