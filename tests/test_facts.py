"""Chapter 13's fact models, built by the runner over small staged tables
and hand-made dimensions; facts.check and facts.incremental; the
transform command; the comments loader."""
from __future__ import annotations

import hashlib
from datetime import date, datetime

import duckdb
import pytest

from groundwork import ddl, facts
from groundwork.transform import runner

AT = datetime(2026, 1, 5, 6, 30)


def key(*parts) -> str:
    return hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:16]


def table(con, name, columns, rows):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {name.split('.')[0]}")
    con.execute(f"CREATE OR REPLACE TABLE {name} ({columns})")
    if rows:
        marks = ", ".join("?" for _ in rows[0])
        con.executemany(f"INSERT INTO {name} VALUES ({marks})", rows)


def ts(s):
    return datetime.fromisoformat(s)


ORDERS = ("order_id BIGINT, order_number BIGINT, account_id BIGINT, "
          "placed_at TIMESTAMP, channel VARCHAR, status VARCHAR, "
          "created_at TIMESTAMP, updated_at TIMESTAMP, _load_id VARCHAR")
LINES = ("order_id BIGINT, line_no BIGINT, sku VARCHAR, qty BIGINT, "
         "unit_price_cents BIGINT, discount_pct BIGINT, "
         "created_at TIMESTAMP, updated_at TIMESTAMP, _load_id VARCHAR")


def order(i, account, placed, status="placed", updated=None):
    return (i, 1000 + i, account, ts(placed), "WEB", status, ts(placed),
            ts(updated or placed), "x")


def line(i, n, sku, qty, cents, updated=None, placed="2025-01-01"):
    return (i, n, sku, qty, cents, 0, ts(placed), ts(updated or placed),
            "x")


def dims(con):
    """Every mart empty, as marts.sql makes them; a few dimension rows."""
    ddl.create(con, "marts")
    con.executemany("INSERT INTO marts.dim_region VALUES (?,?,?,?,?,?)", [
        (key(4), 4, "West", "WE", "Sacramento", "America/Los_Angeles"),
        (key(-1), -1, "Unknown", "--", "Unknown", "UTC")])
    con.executemany("INSERT INTO marts.dim_supplier VALUES (?,?,?,?,?)", [
        (key(7), 7, "Voss Industrial", "US", "A"),
        (key(-1), -1, "Unknown supplier", "--", "Unknown")])
    product = "INSERT INTO marts.dim_product VALUES (?,?,?,?,?,?,?,?,?,?)"
    con.executemany(product, [
        (key("MRD-A", "2022-01-01"), "MRD-A", "A", "Cleaning", key(7),
         20, 10, date(2022, 1, 1), date(2025, 1, 1), False),
        (key("MRD-A", "2025-01-01"), "MRD-A", "A", "Cleaning", key(7),
         20, 12, date(2025, 1, 1), None, True),
        (key("UNKNOWN", "2021-01-01"), "UNKNOWN", "Unknown product",
         "Unknown", key(-1), None, None, date(2021, 1, 1), None, True)])
    account = ("INSERT INTO marts.dim_account VALUES "
               "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    con.executemany(account, [
        (key(1, "2022-01-01"), 1, "One", key(4), "Small business",
         "Priti Nair", "active", False, None, None, "95814",
         date(2022, 1, 1), date(2025, 1, 15), False),
        (key(1, "2025-01-15"), 1, "One", key(4), "Small business",
         "Owen Castell", "active", False, None, None, "95814",
         date(2025, 1, 15), None, True),
        (key(-1, "2021-01-01"), -1, "Unknown account", key(-1), "Unknown",
         None, "unknown", False, None, None, None, date(2021, 1, 1),
         None, True)])
    table(con, "intermediate.int_account_identity",
          "crm_id INTEGER, account_id INTEGER, matched_by VARCHAR, "
          "last_seen DATE",
          [(1, 1, "relay", date(2026, 1, 4)),
           (90001, 1, "it_map", date(2024, 2, 29)),
           (90009, None, "unresolved", date(2024, 2, 29))])


@pytest.fixture
def wh(tmp_path):
    """Orders for one West account; the runner builds the order facts."""
    path = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(path))
    dims(con)
    table(con, "staging.stg_tradeline__orders", ORDERS, [
        order(1, 1, "2024-12-31 20:00:00"),      # 12:00 in Sacramento
        order(2, 1, "2025-01-02 03:30:00"),      # 1 January there
        order(3, 90001, "2025-01-20 18:00:00"),  # an old CRM id
        order(4, 1, "2025-01-21 18:00:00", status="cancelled"),
        order(5, 90009, "2025-01-22 18:00:00"),  # resolves to nothing
    ])
    table(con, "staging.stg_tradeline__order_lines", LINES, [
        line(1, 1, "MRD-A", 2, 2000), line(1, 2, "MRD-A", 1, 1999),
        line(2, 1, "MRD-A", 3, 2000), line(3, 1, "MRD-B", 1, 500),
        line(4, 1, "MRD-A", 5, 2000), line(5, 1, "MRD-A", 1, 2000)])
    con.close()
    results = runner.run(path, run_at=AT,
                         select=["channel_codes", "int_order_lines"])
    assert all(r.status == "built" for r in results), results
    return duckdb.connect(str(path))


def lines(con):
    return con.execute(
        "SELECT order_id, line_no, order_date, date_key, account_key, "
        "product_key, unit_cost, revenue, gross_margin, region_key "
        "FROM marts.fct_order_lines ORDER BY 1, 2").fetchall()


def test_cancelled_orders_are_not_facts(wh):
    got = {r[0] for r in lines(wh)}
    assert got == {1, 2, 3, 5}


def test_the_order_day_is_the_depot_day(wh):
    by = {r[0]: r for r in lines(wh)}
    assert by[1][2:4] == (date(2024, 12, 31), 20241231)
    assert by[2][2:4] == (date(2025, 1, 1), 20250101)   # UTC: 2 January


def test_keys_are_the_versions_of_that_day(wh):
    by = {(r[0], r[1]): r for r in lines(wh)}
    assert by[(1, 1)][4] == key(1, "2022-01-01")
    assert by[(1, 1)][6] == 10          # the cost before 1 January
    assert by[(2, 1)][6] == 12          # and after
    assert by[(3, 1)][4] == key(1, "2025-01-15")   # 90001 is account 1


def test_what_the_dimensions_lack_goes_to_the_unknown_member(wh):
    by = {r[0]: r for r in lines(wh)}
    assert by[3][5] == key("UNKNOWN", "2021-01-01")   # no MRD-B
    assert by[3][6] is None and by[3][8] is None       # cost unknown
    assert by[5][4] == key(-1, "2021-01-01")
    assert by[5][9] == key(-1)                          # region too


def test_orders_roll_up_their_lines(wh):
    rows = wh.execute("SELECT order_id, line_count, units, revenue, cost "
                      "FROM marts.fct_orders ORDER BY 1").fetchall()
    assert [(r[0], r[1], r[2], str(r[3])) for r in rows] == [
        (1, 2, 3, "59.99"), (2, 1, 3, "60.00"), (3, 1, 1, "5.00"),
        (5, 1, 1, "20.00")]
    assert rows[0][4] == 30 and rows[2][4] is None     # one cost unknown


def test_check_holds_the_order_facts_to_marts_sql(wh):
    problems = facts.check(wh)
    assert not [p for p in problems if p.startswith("fct_order")]
    wh.execute("UPDATE marts.fct_orders SET revenue = revenue + 1 "
               "WHERE order_id = 1")
    wh.execute("UPDATE marts.fct_order_lines SET date_key = 20250102 "
               "WHERE order_id = 2")
    problems = facts.check(wh)
    assert "fct_order_lines.date_key: 1 not order_date" in problems


def full(con):
    """Rebuild both order facts in full, from the same models."""
    models = runner.discover()
    for m in ("fct_order_lines", "fct_orders"):
        con.execute(f"CREATE OR REPLACE TABLE marts.{m} AS "
                    + runner.compile_sql(models[m], models))
    return lines(con), con.execute(
        "SELECT * FROM marts.fct_orders ORDER BY 1").fetchall()


def snapshot(con):
    return lines(con), con.execute(
        "SELECT * FROM marts.fct_orders ORDER BY 1").fetchall()


def test_incremental_takes_amendments_cancellations_and_new_dims(wh):
    # A night later: order 2 amended, order 1 cancelled, a new order 6,
    # and a late price list that moves MRD-A's change to 1 January
    # 2025 ... from 31 December 2024.
    wh.execute("UPDATE staging.stg_tradeline__order_lines SET qty = 4, "
               "updated_at = '2025-01-23 02:00' WHERE order_id = 2")
    wh.execute("UPDATE staging.stg_tradeline__orders SET status = "
               "'cancelled', updated_at = '2025-01-23 02:00' "
               "WHERE order_id = 1")
    wh.executemany("INSERT INTO staging.stg_tradeline__orders VALUES "
                   "(?,?,?,?,?,?,?,?,?)",
                   [order(6, 1, "2025-01-22 20:00:00")])
    wh.executemany("INSERT INTO staging.stg_tradeline__order_lines VALUES "
                   "(?,?,?,?,?,?,?,?,?)",
                   [line(6, 1, "MRD-A", 1, 2000,
                         placed="2025-01-22 20:00:00")])
    got = facts.incremental(wh, since=datetime(2025, 1, 22, 6, 0))
    # 1, 2 and 6 changed; 5 was placed inside the hour's lookback
    assert got["changed"] == 4 and got["gone"] == 1     # 1, cancelled
    after = snapshot(wh)
    assert after == full(wh)
    assert {r[0] for r in after[0]} == {2, 3, 5, 6}


def test_incremental_restates_facts_a_dimension_moved_under(wh):
    wh.execute("UPDATE marts.dim_product SET valid_to = '2024-12-31' "
               "WHERE sku = 'MRD-A' AND valid_from = '2022-01-01'")
    wh.execute("UPDATE marts.dim_product SET valid_from = '2024-12-31', "
               "product_key = ? WHERE sku = 'MRD-A' AND is_current",
               [key("MRD-A", "2024-12-31")])
    stale = facts.check(wh)
    assert "fct_order_lines.product_key: 2 not in dim_product" in stale
    assert "fct_order_lines.product_key: 4 not the version of " \
           "order_date" in stale
    got = facts.incremental(wh, since=AT)
    assert got["stale"] == 3 and got["changed"] == 0     # 1, 2 and 5
    assert snapshot(wh) == full(wh)


def test_incremental_repoints_the_unknown_member(wh):
    wh.execute("UPDATE intermediate.int_account_identity SET "
               "account_id = 1 WHERE crm_id = 90009")
    got = facts.incremental(wh, since=AT)
    assert got["unknown"] == 2          # order 5's account, 3's product
    by = {r[0]: r for r in lines(wh)}
    assert by[5][4] == key(1, "2025-01-15")


# ------------------------------------------------------------ tickets
VERSIONS = ("ticket_id VARCHAR, updated_at TIMESTAMP, status VARCHAR, "
            "priority VARCHAR, assignee VARCHAR, pulled_on DATE, "
            "_load_id VARCHAR")
TICKETS = ("ticket_id VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP,"
           " status VARCHAR, priority VARCHAR, channel VARCHAR, "
           "language VARCHAR, account_id BIGINT, requester_name VARCHAR, "
           "requester_email VARCHAR, assignee VARCHAR, description VARCHAR,"
           " sku VARCHAR, pulled_on DATE, _load_id VARCHAR")


def ticket(i, created, status, updated, account=1, sku=None):
    return (i, ts(created), ts(updated), status, "low", "web", "en",
            account, "R", "r@x.example", "A", "d", sku,
            date(2026, 1, 4), "x")


@pytest.fixture
def desk(tmp_path):
    path = tmp_path / "desk.duckdb"
    con = duckdb.connect(str(path))
    dims(con)
    table(con, "staging.stg_frontline__tickets", TICKETS, [
        ticket("T1", "2025-03-01 17:00", "closed", "2025-03-07 10:00"),
        ticket("T2", "2025-03-02 17:00", "closed", "2025-03-08 10:00",
               sku="MRD-A"),
        ticket("T3", "2025-03-03 17:00", "open", "2025-03-03 18:00",
               account=None)])
    table(con, "staging.stg_frontline__ticket_versions", VERSIONS, [
        # T1, seen by nightly pulls: open, then solved, then closed
        ("T1", ts("2025-03-01 18:30"), "open", "low", "A", None, "x"),
        ("T1", ts("2025-03-03 10:00"), "solved", "low", "A", None, "x"),
        ("T1", ts("2025-03-07 10:00"), "closed", "low", "A", None, "x"),
        # T2: first seen already closed; nothing earlier is known
        ("T2", ts("2025-03-08 10:00"), "closed", "low", "A", None, "x"),
        ("T3", ts("2025-03-03 18:00"), "open", "low", "A", None, "x")])
    table(con, "staging.stg_frontline__ticket_fields",
          "ticket_id VARCHAR, updated_at TIMESTAMP, field_id BIGINT, "
          "field_name VARCHAR, value VARCHAR, _load_id VARCHAR",
          [("T1", ts("2025-03-07 10:00"), 1, "desk", "west_desk", "x"),
           ("T1", ts("2025-03-07 10:00"), 2, "category", "billing", "x")])
    table(con, "staging.stg_frontline__ticket_comments",
          "comment_id BIGINT, ticket_id VARCHAR, created_at TIMESTAMP, "
          "author VARCHAR, is_public BOOLEAN, body VARCHAR, "
          "pulled_on DATE, _load_id VARCHAR",
          [(1, "T1", ts("2025-03-02 09:00"), "R", True, "Any update?",
            date(2026, 1, 4), "x"),
           (2, "T1", ts("2025-03-02 10:00"), "R", True, "Thanks.",
            date(2026, 1, 4), "x")])
    con.close()
    models = runner.discover()
    con = duckdb.connect(str(path))
    con.execute("CREATE OR REPLACE TABLE marts.fct_tickets AS "
                + runner.compile_sql(models["fct_tickets"], models))
    return con


def test_a_ticket_row_holds_each_milestone_first_seen(desk):
    row = desk.execute(
        "SELECT opened_date_key, first_reply_at, solved_at, closed_at, "
        "hours_to_first_reply, hours_to_solve, comment_count, desk, "
        "category FROM marts.fct_tickets WHERE ticket_id = 'T1'"
    ).fetchone()
    assert row[0] == 20250301                  # 09:00 in Sacramento
    assert row[1:4] == (ts("2025-03-01 18:30"), ts("2025-03-03 10:00"),
                        ts("2025-03-07 10:00"))
    assert (str(row[4]), str(row[5])) == ("1.50", "41.00")
    assert row[6:] == (2, "west_desk", "billing")


def test_a_milestone_no_pull_saw_is_null_not_guessed(desk):
    row = desk.execute(
        "SELECT first_reply_at, solved_at, closed_at, hours_to_solve, "
        "product_key, comment_count FROM marts.fct_tickets "
        "WHERE ticket_id = 'T2'").fetchone()
    assert row == (None, None, ts("2025-03-08 10:00"), None,
                   key("MRD-A", "2025-01-01"), 0)


def test_a_requester_with_no_account_has_no_account_key(desk):
    row = desk.execute("SELECT account_key, region_key FROM "
                       "marts.fct_tickets WHERE ticket_id = 'T3'"
                       ).fetchone()
    assert row == (None, None)


# ---------------------------------------------------------- contracts
def test_a_contract_is_in_force_on_each_month_end_it_covers(tmp_path):
    path = tmp_path / "c.duckdb"
    con = duckdb.connect(str(path))
    dims(con)
    table(con, "staging.stg_relay__contracts",
          "snapshot_date DATE, contract_id INTEGER, account_id INTEGER, "
          "start_date DATE, end_date DATE, term_months INTEGER, "
          "is_legacy_terms BOOLEAN, discount_pct INTEGER, outcome VARCHAR,"
          " notice_date DATE, cancellation_reason VARCHAR, "
          "updated_at TIMESTAMP, source_format VARCHAR, _load_id VARCHAR",
          [(date(2025, 6, 1), 9, 1, date(2024, 12, 1), date(2025, 2, 28),
            3, False, 5, None, None, None, None, "relay", "x"),
           (date(2025, 6, 1), 10, 1, date(2021, 3, 1), date(2022, 2, 28),
            12, True, None, "renewed", None, None, None, "relay", "x")])
    con.close()
    models = runner.discover()
    con = duckdb.connect(str(path))
    for m in ("fct_contracts", "fct_contract_month"):
        con.execute(f"CREATE OR REPLACE TABLE marts.{m} AS "
                    + runner.compile_sql(models[m], models))
    rows = con.execute(
        "SELECT contract_id, month_end, months_in_force, months_remaining,"
        " account_key FROM marts.fct_contract_month ORDER BY 1, 2"
    ).fetchall()
    assert [r[:4] for r in rows] == [
        (9, date(2024, 12, 31), 1, 2), (9, date(2025, 1, 31), 2, 1),
        (9, date(2025, 2, 28), 3, 0),
        (10, date(2022, 1, 31), 11, 1), (10, date(2022, 2, 28), 12, 0)]
    assert rows[0][4] == key(1, "2022-01-01")       # Priti Nair's
    assert rows[1][4] == key(1, "2025-01-15")       # Owen Castell's
    # A contract older than the account's history: its first version.
    (k,) = con.execute("SELECT account_key FROM marts.fct_contracts "
                       "WHERE contract_id = 10").fetchone()
    assert k == key(1, "2022-01-01")


# ------------------------------------------------- the comments loader
class Answer:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class Desk:
    """Stands in for frontline.Client: comments for two tickets."""
    fingerprint = "sha256:test"

    def __init__(self):
        self.asked = []

    def get(self, path, params):
        self.asked.append(path)
        tid = path.split("/")[4]
        return Answer({"comments": [
            {"id": 7, "created_at": "2025-03-02T09:00:00Z", "author": "R",
             "public": True, "body": "Any update?"}] if tid == "T1"
            else []})

    def close(self):
        pass


def test_comments_are_fetched_for_the_tickets_a_pull_wrote(tmp_path):
    from groundwork.ingest import frontline_comments
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    table(con, "raw.frontline_tickets", "ticket_id VARCHAR, load_date DATE",
          [("T1", date(2026, 1, 4)), ("T2", date(2026, 1, 4)),
           ("T9", date(2026, 1, 3))])
    con.close()
    desk = Desk()
    done = frontline_comments.load("2026-01-04", run_at=AT,
                                   root=tmp_path / "raw", warehouse=wh,
                                   client=desk)
    assert desk.asked == ["/api/v2/tickets/T1/comments",
                          "/api/v2/tickets/T2/comments"]
    assert (done.status, done.rows_written, done.extra["tickets"]) == \
        ("succeeded", 1, 2)
    con = duckdb.connect(str(wh))
    assert con.execute("SELECT comment_id, ticket_id, body FROM "
                       "raw.frontline_ticket_comments").fetchall() == [
        (7, "T1", "Any update?")]


# ------------------------------------------------------- the real thing
@pytest.mark.slow
def test_the_real_facts_keep_their_promises():
    from groundwork.config import WAREHOUSE
    if not WAREHOUSE.exists():
        pytest.skip("the warehouse is not built yet")
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        (n,) = con.execute("SELECT count(*) FROM marts.fct_order_lines"
                           ).fetchone()
        if not n:
            pytest.skip("the facts are not built yet")
        assert facts.check(con) == []
    finally:
        con.close()
