"""Chapter 17's orchestrator: the DAG, the runner, its record."""
from __future__ import annotations

import time
from dataclasses import replace
from datetime import date, datetime, timedelta

import duckdb
import pytest

from groundwork import quality
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import (DAG, CycleError, Fatal, Sensor,
                                        Task, dbt_vars, judged_at,
                                        nightly)

DAY = date(2025, 3, 14)
DUE = datetime(2025, 3, 15, 6, 0)


def ok(minutes=1):
    def fn(ctx):
        ctx.clock.sleep(minutes * 60)
        return "done"
    return fn


def boom(ctx):
    raise RuntimeError("no")


def sleeper(ctx):                  # module level: a child can import it
    time.sleep(30)


def go(dag, tmp_path, **kw):
    env = kw.pop("env", runner.Env(warehouse=tmp_path / "w.duckdb"))
    clock = kw.pop("clock", runner.SimClock(kw.get("run_at", DUE)))
    return runner.run(dag, DAY, env=env, clock=clock, **kw), env


def rows(env, task=None):
    con = duckdb.connect(str(env.warehouse), read_only=True)
    q = ("SELECT run_id, task, try_number, state, started_at "
         "FROM audit.task_runs")
    got = con.execute(q + " ORDER BY run_id, started_at, task, "
                      "try_number").fetchall()
    con.close()
    return [r for r in got if task is None or r[1] == task]


def test_a_dag_refuses_unknown_names_and_cycles():
    with pytest.raises(ValueError, match="not a task"):
        DAG("x", [Task("a", ok(), ("b",))])
    with pytest.raises(CycleError):
        DAG("x", [Task("a", ok(), ("b",)), Task("b", ok(), ("a",))])
    with pytest.raises(ValueError, match="two tasks"):
        DAG("x", [Task("a", ok()), Task("a", ok())])


def test_order_levels_and_downstream():
    d = DAG("x", [Task("c", ok(), ("b",)), Task("a", ok()),
                  Task("b", ok(), ("a",)), Task("d", ok())])
    assert d.order() == ["a", "d", "b", "c"]
    assert d.levels() == {"a": 0, "d": 0, "b": 1, "c": 2}
    assert d.downstream(["a"]) == {"b", "c"}


def test_the_nightly_dag():
    d = nightly()
    assert len(d.tasks) == 14
    assert d.order()[-3:] == ["dbt_build", "reconcile", "publish"]
    assert d.tasks["ingest_relay"].upstream == ("wait_relay",)
    assert d.tasks["wait_relay"].sensor
    ingests = {n for n in d.tasks if n.startswith("ingest_")}
    # Chapter 15's order: the load checks before the build, the
    # reconciliation after it and before publish.
    for gate in ("check_contracts", "check_quality"):
        assert set(d.tasks[gate].upstream) == ingests
        assert gate in d.tasks["dbt_build"].upstream
    assert d.tasks["reconcile"].upstream == ("dbt_build",)
    assert d.tasks["publish"].upstream == ("reconcile",)
    assert d.downstream(["ingest_frontline"]) == {
        "load_comments", "check_contracts", "check_quality", "dbt_build",
        "reconcile", "publish"}
    extra = nightly(checks=[Task("monitor", ok())])
    assert extra.tasks["monitor"].upstream == ("dbt_build",)
    assert set(extra.tasks["publish"].upstream) == {"reconcile",
                                                     "monitor"}


def test_the_checks_judge_the_logical_day_not_the_clock():
    env = runner.Env()
    for run_at in (DUE, datetime(2025, 3, 20, 9, 30)):     # a rerun
        ctx = runner.Context("nightly", DAY, "r", run_at, env, None)
        assert judged_at(ctx) == datetime(2025, 3, 15, 7, 0)
        for rule in quality.RULES:          # ... and so is due by then
            if rule.cadence == "daily":
                assert quality.expected(rule, judged_at(ctx)) == DAY


def test_a_run_records_every_task_and_counts_attempts(tmp_path):
    d = DAG("x", [Task("a", ok()), Task("b", ok(), ("a",))])
    r, env = go(d, tmp_path)
    assert r.state == "succeeded" and r.run_id == "x.2025-03-14.1"
    assert [x[1:4] for x in rows(env)] == [("a", 1, "succeeded"),
                                           ("b", 1, "succeeded")]
    r2, _ = go(d, tmp_path)
    assert r2.run_id == "x.2025-03-14.2"


def test_a_failure_stops_only_what_waits_for_it(tmp_path):
    d = DAG("x", [Task("a", boom), Task("b", ok(), ("a",)),
                  Task("c", ok()), Task("d", ok(), ("b",))])
    r, env = go(d, tmp_path)
    states = {n: t.state for n, t in r.tasks.items()}
    assert states == {"a": "failed", "c": "succeeded",
                      "b": "upstream_failed", "d": "upstream_failed"}
    assert r.state == "failed"


def test_retries_back_off_and_fatal_is_not_retried(tmp_path):
    seen = []

    def flaky(ctx):
        seen.append(ctx.now())
        if ctx.try_number < 3:
            raise ConnectionError("reset")
        return "ok"

    def bad(ctx):
        raise Fatal("contract broke")

    d = DAG("x", [Task("f", flaky, retries=3, retry_delay=300),
                  Task("g", bad, retries=3)])
    r, env = go(d, tmp_path)
    assert r.tasks["f"].state == "succeeded" and r.tasks["f"].tries == 3
    gaps = [(b - a).total_seconds() for a, b in zip(seen, seen[1:])]
    assert gaps == [300, 600]
    assert r.tasks["g"].tries == 1
    assert r.tasks["g"].detail.startswith("fatal")


def test_a_timeout_stops_a_task_on_the_clock(tmp_path):
    def hangs(ctx):
        ctx.clock.sleep(7200)

    d = DAG("x", [Task("h", hangs, timeout=600)])
    r, env = go(d, tmp_path)
    assert r.tasks["h"].state == "failed"
    assert "timed out" in r.tasks["h"].detail
    (_, _, _, _, start), = rows(env)
    assert start == DUE


def test_an_isolated_task_is_killed_at_its_timeout(tmp_path):
    d = DAG("x", [Task("s", sleeper, timeout=2, isolate=True)])
    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    t0 = time.perf_counter()
    r = runner.run(d, DAY, env=env)
    assert time.perf_counter() - t0 < 25
    assert r.tasks["s"].state == "failed"
    assert "timed out" in r.tasks["s"].detail


def test_a_sensor_waits_and_then_gives_up(tmp_path):
    ready = DUE + timedelta(minutes=35)
    d = DAG("x", [Sensor("w", lambda ctx: ctx.now() >= ready, every=600,
                         timeout=3600),
                  Task("t", ok(), ("w",))])
    r, env = go(d, tmp_path)
    assert r.tasks["w"].state == "succeeded"
    assert r.tasks["w"].detail.startswith("5 pokes")
    never = DAG("y", [Sensor("w", lambda ctx: False, every=600,
                             timeout=3600), Task("t", ok(), ("w",))])
    r, env = go(never, tmp_path)
    assert r.tasks["w"].state == "failed"
    assert r.tasks["t"].state == "upstream_failed"


def test_a_rerun_runs_only_what_failed_and_what_waits(tmp_path):
    calls = []

    def count(name, fail=False):
        def fn(ctx):
            calls.append(name)
            if fail and ctx.run_id.endswith(".1"):
                raise RuntimeError("down")
            return "ok"
        return fn

    d = DAG("x", [Task("a", count("a")), Task("b", count("b", True)),
                  Task("c", count("c"), ("a", "b")),
                  Task("e", count("e"))])
    r, env = go(d, tmp_path)
    assert r.state == "failed"
    calls.clear()
    r2, _ = go(d, tmp_path, rerun=True)
    assert r2.state == "succeeded"
    assert calls == ["b", "c"]
    assert r2.tasks["a"].state == "kept"


def test_a_second_run_is_refused(tmp_path):
    d = DAG("x", [Task("a", ok())])
    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    with runner.lock(env, "x"):
        with pytest.raises(runner.RunInProgress):
            go(d, tmp_path, env=env)


def test_since_is_the_read_the_last_good_build_used(tmp_path):
    d = DAG("nightly", [Task("ingest_tradeline", ok(3)),
                        Task("dbt_build", ok(), ("ingest_tradeline",))])
    seen = {}

    def look(ctx):
        seen["since"] = ctx.since
        return "ok"

    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    runner.run(d, DAY, env=env, clock=runner.SimClock(DUE))
    nxt = DAG("nightly", [replace(t, fn=look) if t.name == "dbt_build"
                          else t for t in d.tasks.values()])
    runner.run(nxt, DAY + timedelta(days=1), env=env,
               clock=runner.SimClock(DUE + timedelta(days=1)))
    assert seen["since"] == DUE
    ctx = runner.Context("nightly", DAY, "r", DUE, env, DUE)
    assert dbt_vars(ctx) == {"night": "2025-03-14",
                             "since": "2025-03-15 06:00:00"}
