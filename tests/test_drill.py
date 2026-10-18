"""Chapter 18's rerun drill: Groundwork as of a night, comparing
partitions, the snapshot against the exports, a seeded pick."""
from __future__ import annotations

import os
from datetime import date, datetime

import duckdb
import pyarrow as pa

from groundwork import loadlog
from groundwork.orchestrate import drill, runner
from groundwork.storage import raw_path, write_partition

DAYS = [date(2025, 2, 10), date(2025, 2, 11), date(2025, 2, 12)]


def partition(root, day, rows, at=datetime(2025, 3, 1, 6)):
    t = pa.table({"id": [r[0] for r in rows], "v": [r[1] for r in rows]})
    return write_partition(t, "tradeline", "orders_log", day,
                           loaded_at=at, load_id=f"x.{day}.1", root=root)


def groundwork(tmp_path):
    env = runner.Env(warehouse=tmp_path / "gw" / "warehouse.duckdb",
                     root=tmp_path / "gw" / "raw",
                     archive=tmp_path / "gw" / "landed")
    for i, d in enumerate(DAYS):
        partition(env.root, d, [(i, "a"), (i + 10, "b")])
    con = runner._connect(env)
    loadlog.ensure(con)
    for d in DAYS:
        lid = loadlog.start(con, "tradeline", "orders", d,
                            started_at=datetime(2025, 3, 1))
        loadlog.finish(con, lid, finished_at=datetime(2025, 3, 1),
                       rows_read=2, rows_written=2, source_count=2)
        con.execute("INSERT INTO audit.dag_runs VALUES (?, 'backfill', "
                    "?, 'scheduled', ?, 'succeeded', NULL)",
                    [f"backfill.{d}.1", d, datetime(2025, 3, 1)])
    con.close()
    return env


def test_as_of_keeps_the_nights_before_and_links_not_copies(tmp_path):
    env = groundwork(tmp_path)
    out = drill.as_of(env, DAYS[1], tmp_path / "scratch")
    kept = sorted(p.parent.name for p in
                  out.root.glob("tradeline/orders_log/*/part-0.parquet"))
    assert kept == [f"load_date={d}" for d in DAYS[:2]]
    a = raw_path("tradeline", "orders_log", DAYS[0], env.root)
    b = raw_path("tradeline", "orders_log", DAYS[0], out.root)
    assert os.stat(a).st_ino == os.stat(b).st_ino
    con = duckdb.connect(str(out.warehouse), read_only=True)
    logged = con.execute("SELECT load_date FROM audit.load_log "
                         "ORDER BY 1").fetchall()
    runs = con.execute("SELECT count(*) FROM audit.dag_runs").fetchone()
    view = con.execute("SELECT count(*) FROM raw.tradeline_orders_log"
                       ).fetchone()
    con.close()
    assert [d for (d,) in logged] == DAYS[:2]
    assert runs == (2,) and view == (4,)


def test_writing_a_night_again_leaves_the_original_alone(tmp_path):
    env = groundwork(tmp_path)
    out = drill.as_of(env, DAYS[1], tmp_path / "scratch")
    partition(out.root, DAYS[0], [(99, "z")])
    con = duckdb.connect()
    (n,) = con.execute("SELECT count(*) FROM read_parquet(?)", [str(
        raw_path("tradeline", "orders_log", DAYS[0], env.root))]
                       ).fetchone()
    con.close()
    assert n == 2


def test_same_rows_ignores_what_the_run_adds(tmp_path):
    a = partition(tmp_path / "a", DAYS[0], [(1, "a"), (2, "b")])
    b = partition(tmp_path / "b", DAYS[0], [(2, "b"), (1, "a")],
                  at=datetime(2026, 1, 1))
    assert drill.same_rows(a, b).differ == 0
    c = partition(tmp_path / "c", DAYS[0], [(1, "a"), (2, "B")])
    got = drill.same_rows(a, c)
    assert (got.rows, got.differ) == (2, 2)


def test_compare_raw_names_a_partition_only_one_side_has(tmp_path):
    partition(tmp_path / "a", DAYS[0], [(1, "a")])
    got = drill.compare_raw(tmp_path / "a", tmp_path / "b", DAYS[0])
    assert [(c.what, c.differ, c.note) for c in got] == [
        ("tradeline.orders_log", 1, "only in Groundwork")]


def snapshot_warehouse(path, held):
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA staging; CREATE SCHEMA snapshots")
    con.execute("""CREATE TABLE staging.stg_relay__accounts AS
        SELECT * FROM (VALUES
          (DATE '2025-02-10', 1, 'smb', 'Ann', 'active'),
          (DATE '2025-02-10', 2, 'smb', 'Bo', 'active'),
          (DATE '2025-02-11', 1, 'mid', 'Ann', 'active'),
          (DATE '2025-02-11', 3, 'smb', 'Cy', 'active'))
        t(snapshot_date, account_id, segment, owner_name, status)""")
    con.execute("CREATE TABLE snapshots.snp_relay__accounts (account_id "
                "INT, dbt_valid_from TIMESTAMP, dbt_valid_to TIMESTAMP)")
    con.executemany("INSERT INTO snapshots.snp_relay__accounts VALUES "
                    "(?, ?, ?)", held)
    con.close()


def test_the_snapshot_must_match_the_change_between_exports(tmp_path):
    a, b = datetime(2025, 2, 10), datetime(2025, 2, 11)
    right = [(1, a, b), (2, a, b), (1, b, None), (3, b, None)]
    snapshot_warehouse(tmp_path / "w.duckdb", right)
    env = runner.Env(warehouse=tmp_path / "w.duckdb")
    got = drill.compare_snapshot(env, DAYS[1])
    assert (got.rows, got.differ) == (4, 0)
    wrong = [(1, a, None), (2, a, b), (3, b, None)]     # missed a change
    snapshot_warehouse(tmp_path / "x.duckdb", wrong)
    env = runner.Env(warehouse=tmp_path / "x.duckdb")
    assert drill.compare_snapshot(env, DAYS[1]).differ == 2


def test_the_pick_is_seeded_sorted_and_from_backfilled_nights(tmp_path):
    env = groundwork(tmp_path)
    one = drill.pick(env, n=2, seed=7)
    assert one == drill.pick(env, n=2, seed=7)
    assert one == sorted(one) and set(one) <= set(DAYS)
    assert drill.pick(env, n=50) == DAYS


def test_a_result_passes_only_when_nothing_differs():
    r = drill.Result(DAYS[0], [drill.Check("a", 5, 0),
                               drill.Check("b", 3, 0)])
    assert r.passed
    r.checks.append(drill.Check("c", 4, 1))
    assert not r.passed and [c.what for c in r.failures()] == ["c"]
    assert "DIFFERS" in drill.report([r])
