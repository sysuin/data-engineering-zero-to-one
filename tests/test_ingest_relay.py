"""Relay's loader, on exports written for the test in both formats."""
from __future__ import annotations

import os
from datetime import date, datetime

import duckdb
import pyarrow.parquet as pq
import pytest

from groundwork import loadlog
from groundwork.ingest import files, relay
from groundwork.storage import raw_path

LEGACY_A = ("Acct ID;Account Name;Region;Segment;Owner;Customer Since;Zip;"
            "Key Acct;Status;Closed Date;Primary Contact;Contact Email;"
            "Contact Phone\r\n"
            "2485;Easby Dental;Northeast;Mid-market;Tom Ashby;04/01/2022;"
            "1498;N;Active;;Brennan, Zoë;zoe@x.example;(973) 555-0191\r\n")
LEGACY_C = ("Contract ID;Acct ID;Start;End;Term (months);Legacy Terms;"
            "Discount %;Outcome;Notice Date;Cancellation Reason\r\n"
            "7;2485;04/01/2022;03/31/2023;12;N;5;Renewed;;\r\n")
RELAY_A = ("account_id,account_name,region,segment,owner,customer_since,"
           "postcode,key_account,status,closed_on,contact_name,"
           "contact_email,contact_phone,updated_at\n"
           "2485,Easby Dental,Northeast,Mid-market,Tom Ashby,2022-04-01,"
           "01498,false,active,,Zoë Brennan,zoe@x.example,"
           "(973) 555-0191,2022-04-01T13:38:09Z\n")
RELAY_C = ("contract_id,account_id,start_date,end_date,term_months,"
           "legacy_terms,discount_pct,outcome,notice_date,"
           "cancellation_reason,updated_at\n"
           "7,2485,2022-04-01,2023-03-31,12,false,5,renewed,,,"
           "2023-04-01T00:00:00Z\n")


def export(landing, night, legacy):
    """A night's export as the CRM of the day wrote it, at 04:00 UTC."""
    folder = landing / "relay" / night
    folder.mkdir(parents=True)
    pairs = ((LEGACY_A, LEGACY_C, "cp1252") if legacy
             else (RELAY_A, RELAY_C, "utf-8-sig"))
    at = datetime.fromisoformat(night).replace(hour=4).timestamp() + 86400
    for name, text in zip(("accounts.csv", "contracts.csv"), pairs[:2]):
        p = folder / name
        p.write_bytes(text.encode(pairs[2]))
        os.utime(p, (at, at))


@pytest.fixture
def env(tmp_path):
    landing = tmp_path / "landing"
    export(landing, "2024-02-29", legacy=True)
    export(landing, "2024-03-01", legacy=False)
    export(landing, "2024-03-03", legacy=False)
    return {"landing": landing, "root": tmp_path / "raw",
            "warehouse": tmp_path / "w.duckdb",
            "archive": tmp_path / "landed"}


def test_sniff_tells_the_two_formats_apart():
    assert relay.sniff(RELAY_A.encode("utf-8-sig"))[0] == "relay"
    assert relay.sniff(LEGACY_A.encode("cp1252")) == ("legacy", "cp1252",
                                                      ";")
    with pytest.raises(relay.UnknownFormat):
        relay.sniff(b"id,name\n1,a\n")


def test_both_formats_land_in_one_table_as_text(env):
    for night in ("2024-02-29", "2024-03-01"):
        loads = relay.load(night, **env)
        assert {x.table_name for x in loads} == {"accounts", "contracts"}
        assert all(x.status == "succeeded" and x.reconciled for x in loads)
    con = duckdb.connect(str(env["warehouse"]))
    rows = con.execute(
        "SELECT source_format, postcode, customer_since, contact_name, "
        "updated_at FROM raw.relay_accounts ORDER BY load_date").fetchall()
    assert rows == [("legacy", "1498", "04/01/2022", "Brennan, Zoë", None),
                    ("relay", "01498", "2022-04-01", "Zoë Brennan",
                     "2022-04-01T13:38:09Z")]
    t = pq.read_table(raw_path("relay", "contracts", "2024-02-29",
                               env["root"]))
    assert t.column("start_date").to_pylist() == ["04/01/2022"]
    assert t.column("_source_file").to_pylist() == ["relay/2024-02-29"]


def test_a_missing_night_is_a_failed_load_with_a_reason(env):
    with pytest.raises(files.MissingFile):
        relay.load("2024-03-02", **env)
    con = duckdb.connect(str(env["warehouse"]))
    x = loadlog.last(con, "relay", "accounts", status=None)
    assert x.status == "failed" and x.load_date == date(2024, 3, 2)
    assert "no relay/2024-03-02/accounts.csv by 2024-03-03 06:00" in \
        x.extra["error"]
    assert relay.missing_nights(con, "2024-02-29", "2024-03-03") == [
        date(2024, 2, 29), date(2024, 3, 1), date(2024, 3, 2),
        date(2024, 3, 3)]


def test_identical_nights_are_both_loaded(env):
    for night in ("2024-03-01", "2024-03-03"):
        relay.load(night, **env)
    con = duckdb.connect(str(env["warehouse"]))
    assert relay.missing_nights(con, "2024-03-01", "2024-03-03") == [
        date(2024, 3, 2)]
    (n,) = con.execute("SELECT count(*) FROM raw.relay_accounts "
                       "WHERE load_date = '2024-03-03'").fetchone()
    assert n == 1


def test_a_rerun_of_a_night_changes_nothing(env):
    relay.load("2024-03-01", **env)
    before = pq.read_table(raw_path("relay", "accounts", "2024-03-01",
                                    env["root"]))
    loads = relay.load("2024-03-01",
                       run_at=datetime(2024, 3, 2, 6, 40), **env)
    assert all(x.extra["new_files"] == 0 for x in loads)
    after = pq.read_table(raw_path("relay", "accounts", "2024-03-01",
                                   env["root"]))
    assert after.equals(before)


def test_an_unknown_file_in_the_nights_folder_fails_loudly(env):
    p = env["landing"] / "relay" / "2024-03-03" / "accounts.csv"
    p.write_bytes(b"id,name\n1,a\n")
    os.utime(p, (0, 0))
    with pytest.raises(relay.UnknownFormat):
        relay.load("2024-03-03", **env)
    con = duckdb.connect(str(env["warehouse"]))
    failed = [s for s in files.ledger(con, "relay")
              if s.status == "failed"]
    assert [s.name for s in failed] == ["relay/2024-03-03/accounts.csv"]


@pytest.mark.slow
def test_the_real_migration_week(sources, tmp_path):
    from meridian.sources import relay as crm
    env = {"landing": tmp_path / "landing", "root": tmp_path / "raw",
           "warehouse": tmp_path / "w.duckdb",
           "archive": tmp_path / "landed"}
    for night in ("2024-02-29", "2024-03-01", "2024-03-02"):
        crm.export(night, env["landing"] / "relay")
    a = relay.load("2024-02-29", **env)
    b = relay.load("2024-03-01", **env)
    assert [x.rows_written for x in a][:2] == [5188, 11143]
    assert [x.rows_written for x in b][:2] == [5037, 11408]
    with pytest.raises(files.MissingFile):
        relay.load("2024-03-02", **env)
