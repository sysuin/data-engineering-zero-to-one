"""Chapter 18's backfill: order, resuming, gaps, the snapshot's order,
and marts rebuilt a day at a time."""
from __future__ import annotations

import os
import threading
import time
from datetime import date, datetime, timezone

import duckdb
import pytest

from groundwork.orchestrate import backfill, runner
from groundwork.orchestrate.dag import DAG, Fatal, Task

FIRST, LAST = date(2025, 2, 10), date(2025, 2, 14)


class FakeReplay:
    """Sources that need no preparing: a backfill's order and record,
    without the stand-ins."""
    slots = 1
    requests = 0

    def __init__(self):
        self.prepared = []

    def prepare(self, day, slot=0):
        self.prepared.append(day)
        return backfill.Prepared(day, backfill.due(day), None, None, True)

    def env(self, base, p):
        return base


def stub(seen, fail=None, crash=None):
    """A one-task DAG that notes each night, and fails or crashes on
    the nights asked."""
    def night(ctx):
        seen.append(ctx.day)
        if ctx.day == crash:
            raise KeyboardInterrupt("the machine went down")
        if ctx.day == fail:
            raise Fatal("a contract broke")
        return "done"

    def after(ctx):
        seen.append(("after", ctx.day))
        return "done"
    return DAG("backfill", [Task("load", night),
                            Task("check", after, ("load",))])


def go(tmp_path, dag, **kw):
    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    return backfill.run(FIRST, LAST, env=env, replay=FakeReplay(),
                        dag=dag, build=False, **kw), env


def runs(env):
    con = duckdb.connect(str(env.warehouse), read_only=True)
    got = con.execute("SELECT logical_date, kind, state FROM "
                      "audit.dag_runs ORDER BY run_at, run_id").fetchall()
    con.close()
    return got


def test_due_is_six_the_next_morning_and_nights_are_in_order():
    assert backfill.due(date(2025, 3, 14)) == datetime(2025, 3, 15, 6)
    days = backfill.nights("2025-02-27", "2025-03-02")
    assert days == sorted(days) and len(days) == 4
    with pytest.raises(ValueError):
        backfill.nights(LAST, FIRST)


def test_nights_run_oldest_first_each_once(tmp_path):
    seen = []
    b, env = go(tmp_path, stub(seen))
    nights = [d for d in seen if isinstance(d, date)]
    assert nights == backfill.nights(FIRST, LAST)
    assert b.state == "succeeded" and b.count("succeeded") == 5
    assert [r[2] for r in runs(env)] == ["succeeded"] * 5


def test_a_second_backfill_keeps_what_the_first_finished(tmp_path):
    seen = []
    go(tmp_path, stub(seen))
    seen.clear()
    b, env = go(tmp_path, stub(seen))
    assert seen == [] and b.count("kept") == 5
    assert len(runs(env)) == 5


def test_it_stops_at_the_first_failure_and_resumes_there(tmp_path):
    seen = []
    bad = date(2025, 2, 12)
    b, env = go(tmp_path, stub(seen, fail=bad))
    assert [n.state for n in b.nights] == [
        "succeeded", "succeeded", "failed", "not run", "not run"]
    assert "contract broke" in b.nights[2].detail
    assert b.state == "stopped"
    seen.clear()
    b, env = go(tmp_path, stub(seen))
    assert [n.state for n in b.nights] == [
        "kept", "kept", "succeeded", "succeeded", "succeeded"]
    assert [d for d in seen if isinstance(d, date)][0] == bad
    kinds = [(d, k) for d, k, _ in runs(env) if d == bad]
    assert kinds == [(bad, "scheduled"), (bad, "rerun")]


def test_a_crash_mid_night_reruns_only_what_did_not_finish(tmp_path):
    seen, crashed = [], []
    crash = date(2025, 2, 11)

    def night(ctx):
        seen.append(("load", ctx.day))
        return "done"

    def check(ctx):
        seen.append(("check", ctx.day))
        if ctx.day == crash and not crashed:
            crashed.append(ctx.day)
            raise KeyboardInterrupt("the machine went down")
        return "done"
    dag = DAG("backfill", [Task("load", night),
                           Task("check", check, ("load",))])
    with pytest.raises(KeyboardInterrupt):
        go(tmp_path, dag)
    seen.clear()
    b, env = go(tmp_path, dag)
    assert b.state == "succeeded"
    assert seen[0] == ("check", crash)          # load was kept
    assert ("load", crash) not in seen


def test_the_snapshot_refuses_a_night_before_one_it_has_seen(tmp_path):
    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    con = runner._connect(env)
    con.execute("INSERT INTO audit.task_runs VALUES ('backfill.x.1', "
                "'backfill', DATE '2025-03-01', 'snapshot', 1, "
                "'succeeded', TIMESTAMP '2025-03-02 06:00', NULL, "
                "'3 opened, 1 closed')")
    con.close()
    ctx = runner.Context("backfill", date(2025, 2, 1), "r", datetime(
        2025, 2, 2, 6), env, None, clock=runner.SimClock(datetime(
            2025, 2, 2, 6)))
    with pytest.raises(Fatal, match="oldest first"):
        backfill.snapshot(ctx)


def test_a_night_with_no_export_is_a_gap_not_a_failure(tmp_path):
    land = tmp_path / "landing"
    (land / "relay").mkdir(parents=True)
    env = runner.Env(warehouse=tmp_path / "w.duckdb", root=tmp_path / "r",
                     archive=tmp_path / "k", landing=land)
    ctx = runner.Context("backfill", date(2024, 12, 25), "r",
                         datetime(2024, 12, 26, 6), env, None,
                         clock=runner.SimClock(datetime(2024, 12, 26, 6)))
    assert "gap" in backfill.relay_night(ctx)
    con = duckdb.connect(str(env.warehouse), read_only=True)
    states = con.execute("SELECT table_name, status FROM audit.load_log "
                         "ORDER BY 1").fetchall()
    con.close()
    assert states == [("accounts", "failed"), ("contracts", "failed")]
    half = land / "relay" / "2024-12-27"
    half.mkdir()
    (half / "accounts.csv").write_text("account_id\n1\n")
    t = datetime(2024, 12, 28, 4, tzinfo=timezone.utc).timestamp()
    os.utime(half / "accounts.csv", (t, t))
    ctx = runner.Context("backfill", date(2024, 12, 27), "r",
                         datetime(2024, 12, 28, 6), env, None,
                         clock=runner.SimClock(datetime(2024, 12, 28, 6)))
    with pytest.raises(Exception):
        backfill.relay_night(ctx)


def test_the_backfill_gives_way_to_the_nightly_run(tmp_path):
    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    order = []

    def nightly():
        with runner.lock(env, "nightly"):
            order.append("nightly starts")
            time.sleep(0.2)
            order.append("nightly ends")
    t = threading.Thread(target=nightly)
    t.start()
    time.sleep(0.05)
    with backfill._give_way(env, wait=0.02):
        order.append("backfill night")
    t.join()
    assert order == ["nightly starts", "nightly ends", "backfill night"]


def test_replace_days_swaps_only_those_days(tmp_path):
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    con.execute("CREATE SCHEMA marts")
    con.execute("CREATE TABLE marts.fct_orders AS SELECT i AS order_id, "
                "DATE '2025-01-01' + CAST(i % 5 AS INTEGER) AS order_date, 1 AS units "
                "FROM range(50) t(i)")
    sql = ("SELECT i AS order_id, DATE '2025-01-01' + CAST(i % 5 AS INTEGER) AS "
           "order_date, 2 AS units FROM range(50) t(i)")
    days = [date(2025, 1, 2), date(2025, 1, 4)]
    gone, now = backfill.replace_days(con, "fct_orders", days, sql)
    assert (gone, now) == (20, 20)
    got = dict(con.execute("SELECT order_date, sum(units) FROM "
                           "marts.fct_orders GROUP BY 1").fetchall())
    con.close()
    assert got == {date(2025, 1, 1): 10, date(2025, 1, 2): 20,
                   date(2025, 1, 3): 10, date(2025, 1, 4): 20,
                   date(2025, 1, 5): 10}


def test_a_failed_replace_leaves_the_days_as_they_were(tmp_path):
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    con.execute("CREATE SCHEMA marts")
    con.execute("CREATE TABLE marts.fct_orders AS SELECT 1 AS order_id, "
                "DATE '2025-01-01' AS order_date")
    with pytest.raises(duckdb.Error):
        backfill.replace_days(con, "fct_orders", [date(2025, 1, 1)],
                              "SELECT no_such_column")
    (n,) = con.execute("SELECT count(*) FROM marts.fct_orders").fetchone()
    con.close()
    assert n == 1
