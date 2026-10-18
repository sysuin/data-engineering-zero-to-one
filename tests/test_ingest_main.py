"""v0.4's ingest: every source for a day, each held to its contract."""
from __future__ import annotations

import os
from datetime import date, datetime

import duckdb
import pytest

from groundwork.ingest import files, price_lists, suppliers
from groundwork.ingest.__main__ import land, report, run

RUN = datetime(2024, 8, 2, 6, 0)
DAY = date(2024, 8, 1)
STD = (b"sku,description,unit_cost,currency,effective_from\n"
       b"MRD-FAC-034,Facilities item 5,87.71,USD,2024-08-01\n")
PEM = (b"Item Code,Item,Unit Cost USD,Valid From\n"
       b"MRD-CLE-001,Cleaning item 1,58.50,01/08/2024\n")


def put(root, name, data, when=datetime(2024, 8, 1, 9)):
    f = root / name
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(data)
    t = when.timestamp()
    os.utime(f, (t, t))
    return f


@pytest.fixture
def place(tmp_path):
    landing = tmp_path / "landing"
    put(landing, "suppliers/voss-industrial/voss-industrial_2024-08.csv",
        STD)
    put(landing, "suppliers/pemberton-mills/PEMBERTON-PRICES-202408.csv",
        PEM)
    kw = dict(root=tmp_path / "raw", archive=tmp_path / "landed")
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    return landing, kw, con


def test_one_broken_file_stops_itself_not_the_others(place):
    landing, kw, con = place
    found = files.discover(landing, "suppliers", by=RUN)
    x = land(con, "suppliers", "price_lists", found, DAY, RUN,
             suppliers.read, "suppliers", **kw)
    assert (x.status, x.files, x.rows, x.loaded) == ("broken", 2, 2, 1)
    assert "read as pemberton" in x.detail
    led = dict(con.execute("SELECT name, status FROM audit.file_ledger"
                           ).fetchall())
    assert sorted(led.values()) == ["failed", "loaded"]
    # With the reader that knows the header, the next run loads it.
    x = land(con, "suppliers", "price_lists", found, DAY, RUN,
             price_lists.read, "suppliers", **kw)
    assert (x.status, x.loaded) == ("passed", 2)


def test_nothing_new_is_said_as_such(place):
    landing, kw, con = place
    found = files.discover(landing, "suppliers", by=RUN)
    land(con, "suppliers", "price_lists", found, DAY, RUN,
         price_lists.read, "suppliers", **kw)
    x = land(con, "suppliers", "price_lists", found, DAY, RUN,
             price_lists.read, "suppliers", **kw)
    assert x.status == "nothing new" and x.files == 0


def test_a_missing_relay_night_fails_the_run(tmp_path):
    lines = run(DAY, run_at=RUN, only=["relay"],
                landing=tmp_path / "landing", root=tmp_path / "raw",
                warehouse=tmp_path / "w.duckdb",
                archive=tmp_path / "landed")
    assert [(x.table, x.status) for x in lines] == [
        ("accounts", "missing"), ("contracts", "missing")]
    text = report(lines, DAY, RUN)
    assert "2 not landed" in text and "MissingFile" in text


@pytest.mark.slow
def test_the_file_sources_land_today_against_their_contracts(sources,
                                                             tmp_path):
    lines = run("2026-01-04", only=["relay", "catalogue", "ledger",
                                    "reference"],
                root=tmp_path / "raw", warehouse=tmp_path / "w.duckdb",
                archive=tmp_path / "landed")
    assert {x.status for x in lines} == {"passed"}
    assert all(x.rows == x.loaded for x in lines)
