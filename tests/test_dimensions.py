"""Chapter 12's dimension models, built by the runner over small staged
tables, and dimensions.check holding them to marts.sql."""
from __future__ import annotations

import hashlib
from datetime import date, datetime

import duckdb
import pytest

from groundwork import dimensions
from groundwork.transform import runner

AT = datetime(2026, 1, 5, 6, 30)
TODAY = date(2026, 1, 4)


def key(*parts) -> str:
    return hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:16]


def dims_only(results):
    """The runner builds what depends on a selection, and Chapter 13's
    facts depend on these dimensions; this fixture stages none of the
    tables the facts read, so they are left out of the verdict."""
    return [r for r in results
            if not r.model.startswith(("fct_", "int_order_"))]


def table(con, name, columns, rows):
    schema = name.split(".")[0]
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"CREATE TABLE {name} ({columns})")
    if rows:
        marks = ", ".join("?" for _ in rows[0])
        con.executemany(f"INSERT INTO {name} VALUES ({marks})", rows)


ACCOUNTS = ("snapshot_date DATE, account_id INTEGER, account_name VARCHAR,"
            " region_name VARCHAR, segment VARCHAR, owner_name VARCHAR, "
            "customer_since_date DATE, postcode VARCHAR, "
            "is_key_account BOOLEAN, status VARCHAR, closed_date DATE")
HISTORY = ("changed_at TIMESTAMP, record_type VARCHAR, record_id INTEGER, "
           "field_name VARCHAR, old_value VARCHAR, new_value VARCHAR, "
           "changed_by VARCHAR")


def account(i, owner, status="active", closed=None, night=TODAY,
            region="West"):
    return (night, i, f"Account {i}", region, "Small business", owner,
            date(2015, 6, 1), "01880", False, status, closed)


def change(at, i, field, old, new):
    return (datetime.fromisoformat(at), "account", i, field, old, new, "x")


@pytest.fixture
def warehouse(tmp_path):
    """Staged tables for three accounts; the runner builds the rest."""
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    table(con, "staging.stg_reference__regions",
          "region_id INTEGER, region_name VARCHAR, depot VARCHAR, "
          "timezone VARCHAR",
          [(4, "West", "Sacramento", "America/Los_Angeles")])
    table(con, "staging.stg_relay__accounts", ACCOUNTS, [
        account(1, "Retention desk", "closed", date(2024, 7, 31)),
        account(2, "Kit Ansel"),
        account(3, "Maya Tolland"),
        account(90003, "Kit Ansel", night=date(2024, 2, 29))])
    table(con, "staging.stg_relay__field_history", HISTORY, [
        # account 1: two managers, then the Retention desk, then closed
        change("2023-11-01 22:44:00", 1, "owner", "Priti Nair",
               "Owen Castell"),
        change("2024-06-01 21:37:00", 1, "owner", "Owen Castell",
               "Retention desk"),
        change("2024-07-31 23:58:00", 1, "status", "active", "closed"),
        change("2024-07-31 23:58:00", 1, "closed_on", None, "2024-07-31"),
        # account 2: created, then two changes on one day, and an evening
        # change whose UTC date is the next day
        change("2023-03-01 18:00:00", 2, "created", None, None),
        change("2023-05-02 17:00:00", 2, "owner", "Kit Ansel", "Hana Osei"),
        change("2023-05-02 23:00:00", 2, "owner", "Hana Osei", "Bea M"),
        change("2023-09-01 03:30:00", 2, "owner", "Bea M", "Kit Ansel"),
    ])
    table(con, "staging.stg_relay__migration_map",
          "old_crm_id INTEGER, account_id INTEGER, matched_by VARCHAR",
          [(90003, 3, "name+postcode")])
    con.close()
    results = runner.run(wh, run_at=AT, select=[
        "region_codes", "account_matches", "int_account_identity",
        "int_account_changes", "dim_region"])
    bad = [r for r in dims_only(results) if r.status != "built"]
    assert not bad, bad
    return duckdb.connect(str(wh))


def versions(con, i):
    return con.execute(
        "SELECT valid_from, valid_to, owner, status, closed_on, is_current "
        "FROM marts.dim_account WHERE account_id = ? ORDER BY 1",
        [i]).fetchall()


def test_history_is_walked_back_from_today(warehouse):
    assert versions(warehouse, 1) == [
        (date(2022, 1, 1), date(2023, 11, 1), "Priti Nair", "active",
         None, False),
        (date(2023, 11, 1), date(2024, 6, 1), "Owen Castell", "active",
         None, False),
        (date(2024, 6, 1), date(2024, 7, 31), "Retention desk", "active",
         None, False),
        (date(2024, 7, 31), None, "Retention desk", "closed",
         date(2024, 7, 31), True)]


def test_a_day_ends_in_its_last_change_on_the_depot_calendar(warehouse):
    # created 1 March (10:00 in Sacramento); 2 May ends with Bea M; the
    # change at 03:30 UTC on 1 September was 31 August in Sacramento
    assert [(v[0], v[2]) for v in versions(warehouse, 2)] == [
        (date(2023, 3, 1), "Kit Ansel"), (date(2023, 5, 2), "Bea M"),
        (date(2023, 8, 31), "Kit Ansel")]


def test_an_account_with_no_changes_has_one_version(warehouse):
    assert versions(warehouse, 3) == [
        (date(2022, 1, 1), None, "Maya Tolland", "active", None, True)]


def test_keys_follow_the_md5_rule(warehouse):
    got = warehouse.execute(
        "SELECT account_key FROM marts.dim_account WHERE account_id = 1 "
        "ORDER BY valid_from").fetchall()
    assert [k for (k,) in got] == [key(1, d) for d in (
        "2022-01-01", "2023-11-01", "2024-06-01", "2024-07-31")]
    (region,) = warehouse.execute("SELECT region_key FROM marts.dim_region"
                                  " WHERE region_id = 4").fetchone()
    assert region == key(4)


def test_old_ids_resolve_and_are_not_dimension_rows(warehouse):
    assert warehouse.execute(
        "SELECT crm_id, account_id, matched_by FROM "
        "intermediate.int_account_identity ORDER BY 1").fetchall() == [
        (1, 1, "relay"), (2, 2, "relay"), (3, 3, "relay"),
        (90003, 3, "it_map")]
    (n,) = warehouse.execute("SELECT count(*) FROM marts.dim_account "
                             "WHERE account_id = 90003").fetchone()
    assert n == 0


def test_the_unknown_member_is_there_once(warehouse):
    assert warehouse.execute(
        "SELECT account_key, valid_from, is_current FROM marts.dim_account"
        " WHERE account_id = -1").fetchall() == [
        (key(-1, "2021-01-01"), date(2021, 1, 1), True)]


def test_product_costs_collapse_repeats_and_gaps(tmp_path):
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    table(con, "staging.stg_suppliers__price_lists",
          "sku VARCHAR, effective_date DATE, unit_cost_cents BIGINT", [
              ("MRD-A", date(2022, 1, 1), 1000),
              ("MRD-A", date(2022, 2, 1), 1000),     # the same again
              ("MRD-A", date(2022, 4, 1), 1100),     # March never came
              ("MRD-A", date(2022, 5, 1), 1000)])
    table(con, "staging.stg_catalogue__products",
          "sku VARCHAR, product_name VARCHAR, category VARCHAR, "
          "supplier_id INTEGER, list_price_cents BIGINT",
          [("MRD-A", "A", "Cleaning", 7, 1999)])
    con.close()
    results = runner.run(wh, run_at=AT, select=["int_product_costs"])
    assert all(r.status == "built" for r in dims_only(results)), results
    con = duckdb.connect(str(wh))
    rows = con.execute("SELECT valid_from, valid_to, unit_cost, list_price "
                       "FROM marts.dim_product WHERE sku = 'MRD-A' "
                       "ORDER BY 1").fetchall()
    assert [(r[0], r[1], str(r[2])) for r in rows] == [
        (date(2022, 1, 1), date(2022, 4, 1), "10.00"),
        (date(2022, 4, 1), date(2022, 5, 1), "11.00"),
        (date(2022, 5, 1), None, "10.00")]
    assert str(rows[0][3]) == "19.99"


def test_check_finds_broken_promises(warehouse):
    con = warehouse
    con.execute("UPDATE marts.dim_account SET valid_to = DATE '2023-10-01'"
                " WHERE account_id = 1 AND valid_from = DATE '2022-01-01'")
    con.execute("UPDATE marts.dim_account SET segment = NULL "
                "WHERE account_id = 2")
    problems = dimensions.check(con)
    assert "dim_account: 1 versions overlap, gap or end early" in problems
    assert "dim_account.segment: 3 NULLs" in problems
    assert "dim_date: not built" in problems


@pytest.mark.slow
def test_the_real_dimensions_keep_their_promises(warehouse_path):
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE "
                           "table_name = 'dim_account'").fetchone()[0]:
            pytest.skip("the dimensions are not built yet")
        assert dimensions.check(con) == []
    finally:
        con.close()


@pytest.fixture
def warehouse_path():
    from groundwork.config import WAREHOUSE
    if not WAREHOUSE.exists():
        pytest.skip("the warehouse is not built yet")
    return WAREHOUSE
