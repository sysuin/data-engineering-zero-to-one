"""The catalogue, the ledger and the reference files, loaded once."""
from __future__ import annotations

import os
from datetime import datetime

import duckdb
import pytest

from groundwork.ingest import catalogue, files, ledger, reference


def put(path, text, at):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    t = (at - datetime(1970, 1, 1)).total_seconds()
    os.utime(path, (t, t))


@pytest.fixture
def env(tmp_path):
    return {"root": tmp_path / "raw", "warehouse": tmp_path / "w.duckdb",
            "archive": tmp_path / "landed"}


def test_each_saved_version_of_the_catalogue_is_kept(tmp_path, env):
    path = tmp_path / "landing" / "catalogue" / "catalogue.csv"
    put(path, "SKU,List Price\nMRD-A,$1.00\n", datetime(2025, 11, 1, 9))
    catalogue.load("2025-11-01", run_at=datetime(2025, 11, 2, 6),
                   landing=tmp_path / "landing", **env)
    put(path, "SKU,List Price\nMRD-A,$1.10\n", datetime(2025, 11, 14, 9))
    x = catalogue.load("2025-11-14", run_at=datetime(2025, 11, 15, 6),
                       landing=tmp_path / "landing", **env)
    assert x.rows_written == 1 and x.reconciled
    con = duckdb.connect(str(env["warehouse"]))
    assert len(files.ledger(con, "catalogue")) == 2
    (now,) = files.current(con, "catalogue")
    assert now.arrived_at == datetime(2025, 11, 14, 9)
    prices = con.execute('SELECT "List Price" FROM raw.catalogue_products '
                         "ORDER BY load_date").fetchall()
    assert prices == [("$1.00",), ("$1.10",)]


def test_a_ledger_month_is_keyed_by_the_period_inside_it(tmp_path, env):
    head = "period,entity,region,gl_account,gl_name,description,amount\n"
    put(tmp_path / "landing/ledger/gl_revenue_2025-10.csv",
        head + "2025-10,M,NE,4000,Sales,Sales journal,10.00\n",
        datetime(2025, 11, 7, 22))
    x = ledger.load("2025-11-07", run_at=datetime(2025, 11, 8, 6),
                    landing=tmp_path / "landing", **env)
    assert x.rows_written == 1
    con = duckdb.connect(str(env["warehouse"]))
    (seen,) = files.ledger(con, "ledger")
    assert seen.logical_key == "2025-10"
    (amount,) = con.execute("SELECT amount FROM raw.ledger_gl_revenue"
                            ).fetchone()
    assert amount == "10.00"               # as finance wrote it


def test_a_journal_with_two_periods_is_refused(tmp_path, env):
    head = "period,entity,region,gl_account,gl_name,description,amount\n"
    put(tmp_path / "landing/ledger/gl_revenue_2025-10.csv",
        head + "2025-10,M,NE,4000,S,S,1\n2025-11,M,NE,4000,S,S,1\n",
        datetime(2025, 11, 7, 22))
    with pytest.raises(ValueError, match="periods"):
        ledger.load("2025-11-07", run_at=datetime(2025, 11, 8, 6),
                    landing=tmp_path / "landing", **env)


def test_reference_files_load_whatever_their_time(tmp_path, env):
    ref = tmp_path / "reference"
    put(ref / "regions.csv", "region_id,name\n1,Northeast\n",
        datetime(2026, 9, 1))                # saved after the run
    put(ref / "notes.txt", "not a table", datetime(2026, 9, 1))
    loads = reference.load("2026-01-04", run_at=datetime(2026, 1, 5, 6),
                           reference=ref, **env)
    assert [x.table_name for x in loads] == ["regions"]
    again = reference.load("2026-01-05", run_at=datetime(2026, 1, 6, 6),
                           reference=ref, **env)
    assert [x.extra["new_files"] for x in again] == [0]
