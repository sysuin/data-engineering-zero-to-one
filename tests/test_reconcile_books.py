"""groundwork.compat.sql and groundwork.reconcile_books: Book 2's tables
as views over Groundwork, and the comparison that proves them."""
from __future__ import annotations

import duckdb
import pytest

from groundwork import reconcile_books as rb
from groundwork.ddl import create

pytestmark = pytest.mark.unit

# The few staging and intermediate relations compat reads besides the
# marts, with only the columns it reads.
STAGING = """
CREATE SCHEMA staging; CREATE SCHEMA intermediate;
CREATE TABLE staging.stg_catalogue__products (sku VARCHAR,
    launched_date DATE);
CREATE TABLE staging.stg_relay__accounts (snapshot_date DATE,
    account_id INTEGER, account_name VARCHAR, region_name VARCHAR,
    segment VARCHAR, owner_name VARCHAR, customer_since_date DATE,
    postcode VARCHAR, source_format VARCHAR);
CREATE TABLE intermediate.int_account_identity (crm_id INTEGER,
    account_id INTEGER, matched_by VARCHAR);
CREATE TABLE staging.stg_tradeline__orders (order_id BIGINT,
    account_id BIGINT);
CREATE TABLE staging.stg_frontline__tickets (ticket_id VARCHAR,
    account_id BIGINT, sku VARCHAR, language VARCHAR,
    description VARCHAR);
CREATE TABLE staging.stg_reference__price_events (effective_date DATE,
    scope VARCHAR, change_pct DECIMAL(4,1), note VARCHAR);
"""

ROWS = """
INSERT INTO marts.dim_region VALUES
    ('r1', 1, 'Northeast', 'NE', 'Newark', 'America/New_York'),
    ('r-', -1, 'Unknown', '--', 'Unknown', 'UTC');
INSERT INTO marts.dim_supplier VALUES ('s1', 1, 'Voss', 'US', 'Strategic');
INSERT INTO marts.dim_product VALUES
    ('p1', 'MRD-CLE-001', 'Cloth', 'Cleaning', 's1', 20.00, 12.00,
     DATE '2022-01-01', DATE '2025-12-01', false),
    ('p2', 'MRD-CLE-001', 'Cloth', 'Cleaning', 's1', 20.00, 13.00,
     DATE '2025-12-01', NULL, true);
INSERT INTO staging.stg_catalogue__products VALUES ('MRD-CLE-001', NULL);
-- account 10: a segment change, then a change of status alone
INSERT INTO marts.dim_account VALUES
    ('a1', 10, 'Ashby', 'r1', 'Retail', 'Ana', 'active', true,
     DATE '2019-05-01', NULL, '07001', DATE '2022-01-01',
     DATE '2024-06-01', false),
    ('a2', 10, 'Ashby', 'r1', 'Enterprise', 'Ana', 'active', true,
     DATE '2019-05-01', NULL, '07001', DATE '2024-06-01',
     DATE '2025-03-01', false),
    ('a3', 10, 'Ashby', 'r1', 'Enterprise', 'Ana', 'closed', true,
     DATE '2019-05-01', DATE '2025-03-01', '07001', DATE '2025-03-01',
     NULL, true);
INSERT INTO staging.stg_relay__accounts VALUES
    (DATE '2024-02-29', 90001, 'ASHBY (old)', 'Northeast', 'Retail',
     'Ana', DATE '2019-05-01', '07001', 'legacy'),
    (DATE '2024-02-29', 10, 'Ashby', 'Northeast', 'Retail', 'Ana',
     DATE '2019-05-01', '07001', 'legacy');
INSERT INTO intermediate.int_account_identity VALUES
    (10, 10, 'relay'), (90001, 10, 'it_map');
-- order 7 was placed under the old record; the marts credit account 10
INSERT INTO marts.fct_orders VALUES
    (1, 7, 20240110, 'a1', 'r1', 'Web', TIMESTAMP '2024-01-10 15:00',
     DATE '2024-01-10', 1, 2, 40.00, 24.00, 16.00);
INSERT INTO staging.stg_tradeline__orders VALUES (1, 90001);
INSERT INTO marts.fct_order_lines VALUES
    (1, 1, 7, 20240110, 'a1', 'p1', 's1', 'r1', 'Web',
     TIMESTAMP '2024-01-10 15:00', DATE '2024-01-10', 2, 0, 20.00, 12.00,
     40.00, 24.00, 16.00);
INSERT INTO marts.fct_contracts VALUES
    (5, 'a2', 'r1', 20240601, 20250531, 20250301, DATE '2024-06-01',
     DATE '2025-05-31', 12, false, 4, 'not_renewed', 'Budget cuts');
INSERT INTO marts.fct_tickets (ticket_id, account_key, region_key,
    opened_date_key, opened_at, channel, category, priority, desk,
    status, comment_count) VALUES
    ('FT-1', 'a1', 'r1', 20240110, TIMESTAMP '2024-01-11 02:30',
     'voice', 'billing', 'urgent', 'north_desk', 'closed', 0);
INSERT INTO staging.stg_frontline__tickets VALUES
    ('FT-1', 90001, NULL, 'en', 'Where is my order?');
INSERT INTO staging.stg_reference__price_events VALUES
    (DATE '2025-01-01', 'Voss', 12.0, 'cost');
"""


@pytest.fixture
def con():
    c = duckdb.connect()
    create(c, "marts")
    c.execute(STAGING)
    c.execute(ROWS)
    rb.install(c)
    yield c
    c.close()


def test_every_book2_table_is_a_view(con):
    views = rb.install(con)               # and installing twice is safe
    assert set(rb.BOOK2_TABLES) | {"account_history"} == set(views)


def test_the_old_crms_record_is_presented_as_book2_had_it(con):
    assert con.execute("SELECT account_id, name, crm_source, "
                       "is_key_account FROM compat.accounts ORDER BY 1"
                       ).fetchall() == [
        (10, "Ashby", "Meridian CRM", 1),
        (90001, "ASHBY (old)", "Legacy CRM", 0)]
    # The marts credit the order to account 10; compat says where it
    # was placed, and v_sales finds that record's region.
    assert con.execute("SELECT account_id FROM compat.orders"
                       ).fetchall() == [(90001,)]
    assert con.execute("SELECT account_id, revenue FROM compat.v_sales"
                       ).fetchall() == [(90001, 40.0)]


def test_history_folds_status_and_ends_on_the_last_day(con):
    assert con.execute("SELECT * FROM compat.account_history ORDER BY 2"
                       ).fetchall() == [
        (10, "2022-01-01", "2024-05-31", "Retail", "Ana"),
        (10, "2024-06-01", None, "Enterprise", "Ana")]


def test_types_and_labels_are_book2s(con):
    assert con.execute("SELECT * FROM compat.products").fetchall() == [
        ("MRD-CLE-001", "Cloth", "Cleaning", 1, 12.0, 20.0, None)]
    assert con.execute("SELECT * FROM compat.contracts").fetchall() == [
        (5, 10, "2024-06-01", "2025-05-31", 12, 0, 4, "not_renewed",
         "2025-03-01", "Budget cuts")]
    # 02:30 UTC on the 11th is 21:30 on the 10th in Newark
    assert con.execute("SELECT * FROM compat.tickets").fetchall() == [
        ("FT-1", 90001, "2024-01-10 21:30:00", "Phone", None, "en",
         "Where is my order?", "North desk", "Billing", "Urgent")]
    kinds = dict(con.execute("SELECT column_name, data_type FROM "
                             "information_schema.columns WHERE "
                             "table_name = 'order_lines' AND "
                             "table_schema = 'compat'").fetchall())
    assert kinds["unit_price"] == "DOUBLE"


def test_compare_counts_rows_on_either_side_and_duplicates(con):
    con.execute("CREATE TABLE a AS SELECT k, v::DOUBLE AS v FROM "
                "(VALUES (1, 1.004), (2, 2.0), (2, 2.0)) t(k, v)")
    con.execute("CREATE TABLE b AS SELECT k, v::DOUBLE AS v FROM "
                "(VALUES (1, 1.0), (2, 2.0), (3, 3.0)) t(k, v)")
    # 1.004 is 1.00 to the cent; one 2 is extra, and 3 is only in b
    assert rb.compare(con, "a", "b") == (3, 3, 2)
    assert rb.compare(con, "a", "(SELECT * FROM a)") == (3, 3, 0)


def test_quarters_report_which_measure_differs():
    q = rb.Quarter("2023 Q1", dict(revenue=1.0, cost=0.5, orders=2,
                                   units=3),
                   dict(revenue=1.0, cost=0.49, orders=2, units=3))
    assert q.differing == ["cost"]


def test_the_record_replaces_a_run_at_the_same_moment(con):
    from datetime import datetime
    at = datetime(2026, 1, 5, 7)
    lines = [rb.Line("Book 2", "orders", 2, 2, 0),
             rb.Line("Book 2", "tickets", 1, 2, 1)]
    rb.record(con, lines, at)
    rb.record(con, lines, at)
    assert con.execute("SELECT item, status FROM "
                       "audit.book_reconciliations ORDER BY 1").fetchall() \
        == [("orders", "ties"), ("tickets", "differs")]


@pytest.mark.slow
def test_the_real_warehouse_reconciles_with_both_books(warehouse):
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        if not con.execute("SELECT count(*) FROM information_schema."
                           "schemata WHERE schema_name = 'compat'"
                           ).fetchone()[0]:
            pytest.skip("compat is not installed: run "
                        "python -m groundwork.reconcile_books")
        lines = [*rb.book2(con), rb.history(con)[0], *rb.book1(con)]
        assert [x.item for x in lines if not x.ties] == []
        assert all(not q.differing for q in rb.quarters(con))
    finally:
        con.close()
