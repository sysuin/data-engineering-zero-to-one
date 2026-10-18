"""Unit tests for the SQL models dbt builds every night: each model's
own file, over a handful of rows written here, with the exact rows it
must return. (Chapter 11's tests run the runner's copies of these
models; production has run dbt's since Chapter 17.)"""
from __future__ import annotations

from datetime import date, datetime

import duckdb
import pytest

from groundwork.testing import (current_files, raw_table, render,
                                run_model, seed, table)

pytestmark = pytest.mark.unit


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def contract_row(contract_id, **kw):
    row = dict(contract_id=str(contract_id), account_id="2485",
               start_date="2025-01-01", end_date="2025-12-31",
               term_months="12", legacy_terms="false", discount_pct="10",
               outcome="", notice_date="", cancellation_reason="",
               updated_at="2025-01-02T10:00:00Z", source_format="relay",
               file="relay/2026-01-04/contracts.csv", file_sha256="new",
               _load_id="relay.contracts.2026-01-04.1")
    row.update(kw)
    return row


def test_a_legacy_contract_keeps_no_discount(con):
    raw_table(con, "relay_contracts", [
        contract_row(1),                                   # a discount
        contract_row(2, legacy_terms="true", discount_pct=""),
        contract_row(3, start_date="01/01/2025", end_date="12/31/2025",
                     legacy_terms="Y", discount_pct="", updated_at=None,
                     source_format="legacy", file_sha256="old",
                     file="relay/2024-02-29/contracts.csv")])
    current_files(con,
                  ("relay", "relay/2026-01-04/contracts.csv", "new"),
                  ("relay", "relay/2024-02-29/contracts.csv", "old"))
    got = run_model(con, "stg_relay__contracts",
                    select="contract_id, start_date, end_date, "
                           "is_legacy_terms, discount_pct",
                    order_by="contract_id")
    year = (date(2025, 1, 1), date(2025, 12, 31))
    # Blank means not recorded: legacy terms have no discount field,
    # in either CRM's export. It is not a discount of nothing.
    assert got == [(1, *year, False, 10),
                   (2, *year, True, None),
                   (3, *year, True, None)]


def test_order_lines_take_skus_as_tradeline_writes_them(con):
    table(con, "raw.tradeline_order_lines_current", [
        dict(order_id=7, line_no=1, sku=" mrd-cle-001 ", qty=2,
             unit_price_cents=1999, discount_pct=0,
             created_at="2025-12-01 15:00:00",
             updated_at="2025-12-02 02:00:00", _load_id="t.1")])
    assert run_model(con, "stg_tradeline__order_lines") == [
        (7, 1, "MRD-CLE-001", 2, 1999, 0, datetime(2025, 12, 1, 15),
         datetime(2025, 12, 2, 2), "t.1")]


def test_only_placed_orders_become_lines_and_ids_resolve(con):
    at = datetime(2025, 12, 1, 15)
    table(con, "staging.stg_tradeline__orders", [
        dict(order_id=i, order_number=100 + i, account_id=acct,
             placed_at=at, channel="TEL", status=status, created_at=at,
             updated_at=at, _load_id="t.1")
        for i, acct, status in ((1, 10, "placed"), (2, 10, "cancelled"),
                                (3, 99, "placed"))])
    table(con, "staging.stg_tradeline__order_lines", [
        dict(order_id=i, line_no=1, sku="MRD-CLE-001", qty=1,
             unit_price_cents=500, discount_pct=0, created_at=at,
             updated_at=at, _load_id="t.1") for i in (1, 2, 3)])
    table(con, "intermediate.int_account_identity", [
        dict(crm_id=10, account_id=10, matched_by="relay",
             last_seen=date(2026, 1, 4))])
    seed(con, "channel_codes")
    got = run_model(con, "int_order_lines",
                    select="order_id, crm_id, account_id, channel")
    # The cancelled order is gone; an id the CRM never sent has no
    # account, and says so with a NULL rather than vanishing.
    assert got == [(1, 10, 10, "Telesales"), (3, 99, None, "Telesales")]


def test_a_contract_is_in_force_on_each_month_end_it_covers(con):
    table(con, "staging.stg_relay__contracts", [
        dict(snapshot_date=date(2026, 1, 4), contract_id=5, account_id=10,
             start_date=date(2025, 1, 15), end_date=date(2025, 3, 31),
             discount_pct=None)], types={"discount_pct": "INTEGER"})
    table(con, "intermediate.int_account_identity", [
        dict(crm_id=10, account_id=10, matched_by="relay",
             last_seen=date(2026, 1, 4))])
    table(con, "marts.dim_account", [
        dict(account_key="a10", account_id=10, region_key="r1",
             valid_from=date(2022, 1, 1), valid_to=None, is_current=True),
        dict(account_key="a-1", account_id=-1, region_key="r-1",
             valid_from=date(2021, 1, 1), valid_to=None, is_current=True)],
          types={"valid_to": "DATE"})
    got = run_model(con, "fct_contract_month",
                    select="contract_id, month_end_date_key, account_key, "
                           "month_end, months_in_force, months_remaining, "
                           "discount_pct", order_by="month_end")
    # Relay's last day is in force: 31 March counts, both ends included.
    assert got == [
        (5, 20250131, "a10", date(2025, 1, 31), 1, 2, None),
        (5, 20250228, "a10", date(2025, 2, 28), 2, 1, None),
        (5, 20250331, "a10", date(2025, 3, 31), 3, 0, None)]


def test_region_keys_follow_the_md5_rule(con):
    raw_table(con, "reference_regions", [
        dict(region_id="3", name=" Midwest", depot="Columbus",
             timezone="America/New_York", file="regions.csv",
             file_sha256="r", _load_id="ref.1")])
    current_files(con, ("reference", "regions.csv", "r"))
    con.execute("CREATE SCHEMA staging; CREATE VIEW staging."
                "stg_reference__regions AS "
                + render("stg_reference__regions"))
    seed(con, "region_codes")
    assert run_model(con, "dim_region") == [
        ("6bb61e3b7bce0931", -1, "Unknown", "--", "Unknown", "UTC"),
        ("eccbc87e4b5ce2fe", 3, "Midwest", "MW", "Columbus",
         "America/New_York")]
