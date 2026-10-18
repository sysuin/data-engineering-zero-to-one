"""The full-refresh loader, on a six-order Tradeline built for the test."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime

import duckdb
import pyarrow.parquet as pq
import pytest

from groundwork import loadlog
from groundwork.ingest import tradeline_full
from groundwork.storage import partitions, raw_path

RUN = datetime(2026, 1, 5, 2, 5)
SCHEMA = """
CREATE TABLE orders (id INTEGER PRIMARY KEY, order_number INTEGER,
  account_id INTEGER, placed_at TEXT, channel TEXT, status TEXT,
  created_at TEXT, updated_at TEXT);
CREATE TABLE order_lines (order_id INTEGER, line_no INTEGER, sku TEXT,
  qty INTEGER, unit_price_cents INTEGER, discount_pct INTEGER,
  created_at TEXT, updated_at TEXT, PRIMARY KEY (order_id, line_no));
"""


def make_source(path, n=6):
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    for i in range(1, n + 1):
        t = f"2026-01-0{min(i, 4)} 1{i}:00:00"
        con.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
                    (i, 1000 + i, 7, t, "WEB", "placed", t, t))
        for line in (1, 2):
            con.execute("INSERT INTO order_lines VALUES (?,?,?,?,?,?,?,?)",
                        (i, line, f"MRD-FAC-00{line}", line, 1999, 0, t, t))
    con.commit()
    con.close()
    return path


@pytest.fixture
def env(tmp_path):
    return {"source": make_source(tmp_path / "tradeline.db"),
            "root": tmp_path / "raw",
            "warehouse": tmp_path / "warehouse.duckdb"}


def run(env, day="2026-01-04", at=RUN):
    return tradeline_full.load(day, run_at=at, **env)


def test_both_tables_land_as_they_were_typed(env):
    loads = run(env)
    assert [x.table_name for x in loads] == ["orders", "order_lines"]
    assert all(x.status == "succeeded" and x.reconciled for x in loads)
    assert [x.rows_written for x in loads] == [6, 12]
    t = pq.read_table(raw_path("tradeline", "order_lines", "2026-01-04",
                               env["root"]))
    assert str(t.schema.field("unit_price_cents").type) == "int64"
    assert str(t.schema.field("created_at").type) == "string"
    assert set(t.column("_load_id").to_pylist()) == {
        "tradeline.order_lines.2026-01-04.1"}
    assert t.column("_source_query")[0].as_py().startswith("SELECT")


def test_running_twice_replaces_and_logs_both_attempts(env):
    run(env)
    second = run(env)
    assert [x.load_id[-1] for x in second] == ["2", "2"]
    con = duckdb.connect(str(env["warehouse"]))
    (n,) = con.execute("SELECT count(*) FROM raw.tradeline_orders"
                       ).fetchone()
    assert n == 6                     # not 12: one partition, replaced
    assert len(loadlog.history(con, "tradeline")) == 4


def test_each_day_is_a_whole_copy(env):
    run(env, "2026-01-03")
    run(env, "2026-01-04")
    assert partitions("tradeline", "orders", env["root"]) == [
        date(2026, 1, 3), date(2026, 1, 4)]
    con = duckdb.connect(str(env["warehouse"]))
    got = con.execute("SELECT load_date, count(*) FROM "
                      "raw.tradeline_order_lines GROUP BY 1 ORDER BY 1"
                      ).fetchall()
    assert got == [(date(2026, 1, 3), 12), (date(2026, 1, 4), 12)]


def test_a_failed_write_is_logged_and_keeps_the_old_partition(env,
                                                              monkeypatch):
    run(env)
    before = raw_path("tradeline", "orders", "2026-01-04",
                      env["root"]).read_bytes()

    def broken(*a, **kw):
        raise OSError("disk full")
    monkeypatch.setattr(tradeline_full, "write_partition", broken)
    with pytest.raises(OSError):
        run(env)
    after = raw_path("tradeline", "orders", "2026-01-04",
                     env["root"]).read_bytes()
    assert after == before
    con = duckdb.connect(str(env["warehouse"]))
    failed = loadlog.last(con, "tradeline", "orders", status="failed")
    assert failed.extra["error"] == "OSError: disk full"
    assert failed.source_count == 6 and failed.rows_read == 6
    assert loadlog.last(con, "tradeline", "orders").load_id.endswith(".1")
    # the loader stopped at the first table: no second attempt at lines
    assert loadlog.last(con, "tradeline", "order_lines", status=None
                        ).load_id.endswith(".1")


def test_a_short_read_fails_before_anything_is_written(env, monkeypatch):
    real = tradeline_full.extract
    monkeypatch.setattr(tradeline_full, "extract",
                        lambda src, t: real(src, t).slice(1))
    with pytest.raises(tradeline_full.LoadError):
        run(env)
    assert not raw_path("tradeline", "orders", "2026-01-04",
                        env["root"]).exists()
    con = duckdb.connect(str(env["warehouse"]))
    (x,) = loadlog.history(con)
    assert x.status == "failed" and "holds 6" in x.extra["error"]


def test_started_at_is_the_run_not_the_clock(env):
    loads = run(env, at=datetime(2025, 12, 2, 2, 5))
    assert loads[0].started_at.replace(microsecond=0) == datetime(
        2025, 12, 2, 2, 5)
    assert all(x.finished_at >= x.started_at for x in loads)


def test_the_source_is_opened_read_only(env):
    run(env)
    con = sqlite3.connect(env["source"])
    assert con.execute("SELECT count(*) FROM orders").fetchone() == (6,)


def test_the_command_line_loads_a_day(env, monkeypatch, capsys):
    real = tradeline_full.load
    monkeypatch.setattr(tradeline_full, "load", lambda day, run_at:
                        real(day, run_at=run_at, **env))
    assert tradeline_full.main(["--at", "2026-01-05 02:05"]) == 0
    out = capsys.readouterr().out
    assert "tradeline.orders.2026-01-04.1" in out
    assert "succeeded" in out


@pytest.mark.slow
def test_todays_tradeline_reconciles(sources, tmp_path):
    loads = tradeline_full.load(
        "2026-01-04", run_at=RUN, root=tmp_path / "raw",
        warehouse=tmp_path / "w.duckdb")
    assert [x.rows_written for x in loads] == [483_980, 1_243_789]
    assert all(x.reconciled for x in loads)
