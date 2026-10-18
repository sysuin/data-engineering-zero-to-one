"""The incremental and CDC loader, on a small Tradeline built for the test.

The fake keeps a replication log the way a database does: every change
it commits is appended with the next log sequence number.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import duckdb
import pytest

from groundwork import loadlog
from groundwork.ingest import tradeline
from groundwork.storage import partitions

SCHEMA = """
CREATE TABLE orders (id INTEGER PRIMARY KEY, order_number INTEGER,
  account_id INTEGER, placed_at TEXT, channel TEXT, status TEXT,
  created_at TEXT, updated_at TEXT);
CREATE TABLE order_lines (order_id INTEGER, line_no INTEGER, sku TEXT,
  qty INTEGER, unit_price_cents INTEGER, discount_pct INTEGER,
  created_at TEXT, updated_at TEXT, PRIMARY KEY (order_id, line_no));
CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE TABLE replication_log (lsn INTEGER PRIMARY KEY,
  committed_at TEXT, table_name TEXT, op TEXT, row_json TEXT);
"""
ORDER = ("id", "order_number", "account_id", "placed_at", "channel",
         "status", "created_at", "updated_at")
LINE = ("order_id", "line_no", "sku", "qty", "unit_price_cents",
        "discount_pct", "created_at", "updated_at")


class Fake:
    """A Tradeline whose every committed change goes into its log."""

    def __init__(self, path):
        self.path = path
        self.con = sqlite3.connect(path)
        self.con.executescript(SCHEMA)

    def _log(self, when, table, op, row):
        self.con.execute(
            "INSERT INTO replication_log (committed_at, table_name, op, "
            "row_json) VALUES (?, ?, ?, ?)",
            (when, table, op, json.dumps(row)))

    def order(self, oid, stamp, committed=None, qty=1, status="placed"):
        row = dict(zip(ORDER, (oid, 1000 + oid, 7, stamp, "WEB", status,
                               stamp, stamp)))
        line = dict(zip(LINE, (oid, 1, "MRD-FAC-001", qty, 1999, 0,
                               stamp, stamp)))
        self.con.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
                         tuple(row.values()))
        self.con.execute("INSERT INTO order_lines VALUES "
                         "(?,?,?,?,?,?,?,?)", tuple(line.values()))
        self._log(committed or stamp, "orders", "I", row)
        self._log(committed or stamp, "order_lines", "I", line)
        self.con.commit()

    def amend(self, oid, stamp, qty, committed=None):
        """Change a line's quantity, stamped `stamp`, committed later."""
        self.con.execute("UPDATE orders SET updated_at = ? WHERE id = ?",
                         (stamp, oid))
        self.con.execute("UPDATE order_lines SET qty = ?, updated_at = ?"
                         " WHERE order_id = ?", (qty, stamp, oid))
        for table, key in (("orders", "id"), ("order_lines", "order_id")):
            cur = self.con.execute(f"SELECT * FROM {table} WHERE "
                                   f"{key} = ?", (oid,))
            names = [d[0] for d in cur.description]
            self._log(committed or stamp, table, "U",
                      dict(zip(names, cur.fetchone())))
        self.con.commit()

    def delete(self, oid, when):
        self.con.execute("DELETE FROM order_lines WHERE order_id = ?",
                         (oid,))
        self.con.execute("DELETE FROM orders WHERE id = ?", (oid,))
        self._log(when, "orders", "D", {"id": oid})
        self._log(when, "order_lines", "D", {"order_id": oid,
                                             "line_no": 1})
        self.con.commit()

    def forget(self, below):
        """Drop log entries below an lsn, as retention does."""
        self.con.execute("DELETE FROM replication_log WHERE lsn < ?",
                         (below,))
        self.con.commit()


@pytest.fixture
def env(tmp_path):
    fake = Fake(tmp_path / "tradeline.db")
    for i in range(1, 5):
        fake.order(i, f"2026-01-01 1{i}:00:00")
    return fake, {"source": fake.path, "root": tmp_path / "raw",
                  "warehouse": tmp_path / "warehouse.duckdb"}


def night(n):
    """The run at 02:05 on 2 January plus n days, and its load date."""
    at = datetime(2026, 1, 2, 2, 5) + timedelta(days=n)
    return at.date() - timedelta(days=1), at


def run(env, n, mode, **kw):
    day, at = night(n)
    return tradeline.load(day, run_at=at, mode=mode, **env[1], **kw)


def current(env, table="order_lines"):
    con = duckdb.connect(str(env[1]["warehouse"]))
    cols = "order_id, qty" if table == "order_lines" else "id, status"
    rows = con.execute(f"SELECT {cols} FROM raw.tradeline_{table}_current"
                       " ORDER BY 1").fetchall()
    con.close()
    return rows


@pytest.mark.parametrize("mode", tradeline.MODES)
def test_the_first_load_is_a_snapshot_that_agrees(env, mode):
    loads = run(env, 0, mode)
    assert [x.extra["snapshot"] for x in loads] == [True, True]
    assert [x.rows_written for x in loads] == [4, 4]
    assert all(x.reconciled and x.extra["check"]["agrees"]
               for x in loads)


@pytest.mark.parametrize("mode", tradeline.MODES)
def test_the_next_load_reads_only_what_changed(env, mode):
    fake, _ = env
    run(env, 0, mode)
    fake.order(5, "2026-01-02 12:00:00")
    fake.amend(2, "2026-01-02 13:00:00", qty=9)
    loads = run(env, 1, mode)
    assert not loads[0].extra["snapshot"]
    # lines 5 and 2; the lookback also rereads the hour before the
    # watermark, which holds lines 3 and 4
    assert loads[1].rows_read == (2 if mode == "cdc" else 4)
    assert all(x.extra["check"]["agrees"] for x in loads)
    assert current(env) == [(1, 1), (2, 9), (3, 1), (4, 1), (5, 1)]
    target = "order_lines_log" if mode == "cdc" else "order_lines"
    assert len(partitions("tradeline", target, env[1]["root"])) == (
        1 if mode == "cdc" else 2)


def test_the_bookmark_is_kept_in_the_load_log(env):
    run(env, 0, "incremental")
    run(env, 0, "cdc")
    con = duckdb.connect(str(env[1]["warehouse"]))
    # both runs share a run_at, so pick each load by its mode, not by
    # its place in the log
    by_mode = {x.extra["mode"]: x.extra
               for x in loadlog.history(con, "tradeline", "orders")}
    assert by_mode["incremental"]["watermark"] == "2026-01-01 14:00:00"
    assert by_mode["cdc"]["position"] == 8       # 4 orders, 4 lines


def test_a_lookback_reads_a_row_that_committed_late(env):
    fake, _ = env
    none = timedelta(0)
    run(env, 0, "incremental", lookback=none)
    fake.order(7, "2026-01-03 02:04:00")         # seen at 02:05 ...
    run(env, 1, "incremental", lookback=none)
    # ... so the watermark passes a change stamped 02:00:00 that is
    # still to commit, at 02:09
    fake.amend(1, "2026-01-03 02:00:00", qty=7,
               committed="2026-01-03 02:09:00")
    run(env, 2, "incremental", lookback=none)
    assert (1, 1) in current(env)                # lost for good
    loads = run(env, 2, "incremental")           # the default hour
    assert (1, 7) in current(env)
    assert all(x.extra["check"]["agrees"] for x in loads)


def test_a_rerun_starts_from_the_night_before_and_replaces(env):
    fake, _ = env
    run(env, 0, "incremental")
    fake.order(5, "2026-01-02 12:00:00")
    first = run(env, 1, "incremental")
    fake.order(6, "2026-01-03 01:00:00")         # arrives before rerun
    again = run(env, 1, "incremental")
    assert again[0].load_id.endswith(".2")
    assert again[0].extra["since"] == first[0].extra["since"]
    assert again[0].rows_read == first[0].rows_read + 1
    con = duckdb.connect(str(env[1]["warehouse"]))
    (n,) = con.execute("SELECT count(*) FROM raw.tradeline_orders "
                       "WHERE load_date = DATE '2026-01-02'").fetchone()
    assert n == again[0].rows_read               # replaced, not added


def test_a_delete_is_invisible_to_incremental_and_applied_by_cdc(env):
    fake, _ = env
    run(env, 0, "incremental")
    run(env, 0, "cdc")
    fake.delete(3, "2026-01-02 09:00:00")
    inc = run(env, 1, "incremental")
    assert not inc[0].extra["check"]["agrees"]
    assert inc[0].extra["check"]["copy"][0] == 4
    assert inc[0].extra["check"]["source"][0] == 3
    cdc = run(env, 1, "cdc")
    assert all(x.extra["check"]["agrees"] for x in cdc)
    assert [r[0] for r in current(env, "orders")] == [1, 2, 4]


def test_cdc_applies_changes_in_log_order(env):
    fake, _ = env
    run(env, 0, "cdc")
    fake.order(5, "2026-01-02 10:00:00")
    fake.amend(5, "2026-01-02 11:00:00", qty=3)
    fake.amend(5, "2026-01-02 12:00:00", qty=4)
    run(env, 1, "cdc")
    assert (5, 4) in current(env)


def test_a_gap_in_the_log_means_a_new_snapshot(env):
    fake, _ = env
    run(env, 0, "cdc")
    fake.order(5, "2026-01-02 10:00:00")
    fake.order(6, "2026-01-02 11:00:00")
    fake.forget(below=11)                        # lsn 9 and 10 are gone
    loads = run(env, 1, "cdc")
    assert all(x.extra["snapshot"] for x in loads)
    assert loads[0].extra["since"] == 8          # where it had been
    assert all(x.extra["check"]["agrees"] for x in loads)
    assert len(current(env)) == 6


def test_a_short_read_fails_and_is_logged(env, monkeypatch):
    real = tradeline.read
    monkeypatch.setattr(tradeline, "read",
                        lambda src, p, cols: real(src, p, cols).slice(1))
    with pytest.raises(tradeline.LoadError):
        run(env, 0, "cdc")
    con = duckdb.connect(str(env[1]["warehouse"]))
    (x,) = loadlog.history(con)
    assert x.status == "failed" and "counts 4" in x.extra["error"]


def test_an_unknown_mode_is_refused(env):
    with pytest.raises(ValueError):
        run(env, 0, "full")


def test_the_command_line_loads_a_day(env, monkeypatch, capsys):
    real = tradeline.load
    monkeypatch.setattr(tradeline, "load", lambda day, run_at, mode:
                        real(day, run_at=run_at, mode=mode, **env[1]))
    assert tradeline.main(["--at", "2026-01-02 02:05"]) == 0
    out = capsys.readouterr().out
    assert "tradeline.orders.2026-01-01.1" in out
    assert "snapshot" in out and "agrees" in out


@pytest.mark.slow
def test_todays_tradeline_snapshot_reconciles(sources, tmp_path):
    loads = tradeline.load("2026-01-04", run_at=datetime(2026, 1, 5, 6),
                           root=tmp_path / "raw",
                           warehouse=tmp_path / "w.duckdb")
    assert [x.rows_written for x in loads] == [483_980, 1_243_789]
    assert all(x.extra["check"]["agrees"] for x in loads)
