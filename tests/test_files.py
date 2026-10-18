"""The file ledger: discovery, fingerprints, kept copies, loading once."""
from __future__ import annotations

import gzip
import hashlib
import os
from datetime import datetime, timedelta

import duckdb
import pyarrow.parquet as pq
import pytest

from groundwork import loadlog
from groundwork.ingest import files
from groundwork.storage import raw_path

T0 = datetime(2024, 3, 1, 4, 0)


def put(root, name, text, at=T0):
    """A file landing at `at` (UTC), as a sender would drop it."""
    p = root / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    t = (at - datetime(1970, 1, 1)).total_seconds()
    os.utime(p, (t, t))
    return p


def read(data, arrival):
    """A reader for 'key,value' CSVs whose logical key is line 2's key."""
    lines = data.decode().splitlines()[1:]
    rows = [dict(zip(("key", "value"), x.split(","))) for x in lines]
    key = rows[0]["key"] if rows else None
    return files.Parsed(rows, files.lines(data), key)


@pytest.fixture
def env(tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    yield {"landing": landing, "con": con, "root": tmp_path / "raw",
           "archive": tmp_path / "landed"}
    con.close()


def run(env, day="2024-03-01", at=T0 + timedelta(hours=2), under="s"):
    arrivals = files.discover(env["landing"], under, by=at)
    return files.load(env["con"], "src", "t", arrivals, day, run_at=at,
                      read=read, folder=under, root=env["root"],
                      archive=env["archive"])


# ------------------------------------------------------------ discover
def test_discover_sees_only_what_had_arrived_and_settled(env):
    put(env["landing"], "s/a.csv", "k,v\n", T0)
    put(env["landing"], "s/b.csv", "k,v\n", T0 + timedelta(hours=1))
    put(env["landing"], "s/.c.csv", "k,v\n", T0)
    put(env["landing"], "s/d.csv.part", "k,v\n", T0)
    names = [a.name for a in files.discover(env["landing"], "s",
                                            by=T0 + timedelta(hours=2))]
    assert names == ["s/a.csv", "s/b.csv"]
    # b arrived five minutes before the run: it may still be writing
    names = [a.name for a in files.discover(
        env["landing"], "s", by=T0 + timedelta(hours=1, minutes=5))]
    assert names == ["s/a.csv"]


def test_discover_reports_arrival_in_utc_and_takes_a_single_file(env):
    put(env["landing"], "one.csv", "k,v\n", T0)
    (a,) = files.discover(env["landing"], "one.csv")
    assert a.arrived_at == T0 and a.name == "one.csv" and a.size == 4


# ------------------------------------------------------------ fingerprint
def test_fingerprint_is_the_sha256_of_the_bytes_not_the_name(env):
    p = put(env["landing"], "s/a.csv", "k,v\nx,1\n")
    q = put(env["landing"], "s/b.csv", "k,v\nx,1\n", T0 + timedelta(1))
    assert files.fingerprint(p) == hashlib.sha256(p.read_bytes()).hexdigest()
    assert files.fingerprint(p) == files.fingerprint(q)
    os.utime(p, (0, 0))
    assert files.fingerprint(p) == files.fingerprint(q)


def test_keep_writes_a_gzip_named_by_its_hash(env):
    p = put(env["landing"], "s/a.csv", "k,v\nx,1\n")
    sha = files.fingerprint(p)
    kept = files.keep(p, "src", sha, env["archive"])
    assert kept.name == f"{sha}.gz"
    assert gzip.decompress(kept.read_bytes()) == p.read_bytes()
    assert files.keep(p, "src", sha, env["archive"]) == kept
    assert files.read_kept("src", sha, env["archive"]) == p.read_bytes()


def test_keep_refuses_bytes_that_do_not_match(env):
    p = put(env["landing"], "s/a.csv", "k,v\nx,1\n")
    with pytest.raises(ValueError):
        files.keep(p, "src", "0" * 64, env["archive"])
    assert not [p for p in env["archive"].rglob("*") if p.is_file()]


def test_lines_counts_data_rows_with_or_without_a_last_newline():
    assert files.lines(b"h\n1\n2\n") == 2
    assert files.lines(b"h\n1\n2") == 2
    assert files.lines(b"h\n") == 0
    assert files.lines(b"") == 0


# ------------------------------------------------------------ loading once
def test_a_second_run_loads_nothing(env):
    put(env["landing"], "s/a.csv", "k,v\nx,1\nx,2\n")
    first = run(env)
    assert first.status == "succeeded" and first.reconciled
    assert first.rows_written == 2 and first.extra["new_files"] == 1
    second = run(env, at=T0 + timedelta(hours=3))
    assert second.extra["new_files"] == 0 and second.rows_written == 0
    assert second.load_id.endswith(".2")
    assert len(files.ledger(env["con"], "src")) == 1
    t = pq.read_table(raw_path("src", "t", "2024-03-01", env["root"]))
    assert t.num_rows == 2
    assert set(t.column("file").to_pylist()) == {"s/a.csv"}


def test_a_file_found_by_a_rerun_joins_the_days_partition(env):
    put(env["landing"], "s/a.csv", "k,v\nx,1\n")
    run(env)
    put(env["landing"], "s/b.csv", "k,v\ny,1\n", T0 + timedelta(hours=2))
    x = run(env, at=T0 + timedelta(hours=3))
    assert x.extra == {"new_files": 1, "duplicates": 0,
                       "files_in_partition": 2}
    t = pq.read_table(raw_path("src", "t", "2024-03-01", env["root"]))
    assert sorted(t.column("key").to_pylist()) == ["x", "y"]


def test_a_file_with_an_old_time_is_still_new(env):
    put(env["landing"], "s/a.csv", "k,v\nx,1\n")
    run(env)
    # restored from a backup with its time preserved: older than the run
    put(env["landing"], "s/restored.csv", "k,v\nz,1\n", T0 - timedelta(30))
    x = run(env, day="2024-03-02", at=T0 + timedelta(days=1))
    assert x.extra["new_files"] == 1


def test_same_bytes_as_the_same_logical_file_is_a_duplicate(env):
    put(env["landing"], "s/a.csv", "k,v\nx,1\n")
    run(env)
    put(env["landing"], "s/a copy.csv", "k,v\nx,1\n", T0 + timedelta(1))
    x = run(env, day="2024-03-02", at=T0 + timedelta(days=1, hours=2))
    assert x.extra["duplicates"] == 1 and x.rows_written == 0
    seen = {s.name: s for s in files.ledger(env["con"], "src")}
    assert seen["s/a copy.csv"].status == "duplicate"
    assert "s/a.csv" in seen["s/a copy.csv"].note
    assert not raw_path("src", "t", "2024-03-02", env["root"]).exists()


def test_same_bytes_for_another_night_are_loaded(env):
    put(env["landing"], "s/2024-03-01/a.csv", "k,v\nx,1\n")
    put(env["landing"], "s/2024-03-02/a.csv", "k,v\nx,1\n",
        T0 + timedelta(1))

    def by_folder(data, arrival):
        p = read(data, arrival)
        return files.Parsed(p.rows, p.counted, arrival.name.split("/")[1])

    for day in ("2024-03-01", "2024-03-02"):
        at = datetime.fromisoformat(day) + timedelta(days=1, hours=6)
        arrivals = files.discover(env["landing"], f"s/{day}", by=at)
        x = files.load(env["con"], "src", "t", arrivals, day, run_at=at,
                       read=by_folder, folder="s", root=env["root"],
                       archive=env["archive"])
        assert x.extra["new_files"] == 1
    assert len(list(env["archive"].rglob("*.gz"))) == 1   # kept once


def test_latest_arrival_of_a_logical_file_is_in_force(env):
    put(env["landing"], "s/list.csv", "k,v\nm,790.7\n")
    run(env)
    put(env["landing"], "s/list (corrected).csv", "k,v\nm,79.07\n",
        T0 + timedelta(days=3))
    run(env, day="2024-03-04", at=T0 + timedelta(days=3, hours=2))
    (now,) = files.current(env["con"], "src")
    assert now.name == "s/list (corrected).csv"
    # the first version sent again, unchanged, never wins
    put(env["landing"], "s/list (2).csv", "k,v\nm,790.7\n",
        T0 + timedelta(days=5))
    run(env, day="2024-03-06", at=T0 + timedelta(days=5, hours=2))
    (now,) = files.current(env["con"], "src")
    assert now.name == "s/list (corrected).csv"
    loaded = [s for s in files.ledger(env["con"], "src")
              if s.status == "loaded"]
    assert len(loaded) == 2                # raw keeps both versions


def test_a_file_that_cannot_be_read_fails_and_is_tried_again(env):
    put(env["landing"], "s/a.csv", "k,v\nx,1\n")

    def broken(data, arrival):
        raise ValueError("cannot read this")

    at = T0 + timedelta(hours=2)
    arrivals = files.discover(env["landing"], "s", by=at)
    with pytest.raises(ValueError):
        files.load(env["con"], "src", "t", arrivals, "2024-03-01",
                   run_at=at, read=broken, folder="s", root=env["root"],
                   archive=env["archive"])
    (seen,) = files.ledger(env["con"], "src")
    assert seen.status == "failed" and "cannot read" in seen.note
    assert loadlog.last(env["con"], "src", "t", status=None).status == \
        "failed"
    x = run(env, at=T0 + timedelta(hours=3))
    assert x.status == "succeeded" and x.rows_written == 1
    (seen,) = files.ledger(env["con"], "src")
    assert seen.status == "loaded"


def test_a_count_that_does_not_reconcile_fails_the_load(env):
    put(env["landing"], "s/a.csv", "k,v\nx,1\ny,2\n")

    def short(data, arrival):
        p = read(data, arrival)
        return files.Parsed(p.rows[:1], p.counted, p.logical_key)

    at = T0 + timedelta(hours=2)
    with pytest.raises(ValueError, match="the files hold 2"):
        files.load(env["con"], "src", "t",
                   files.discover(env["landing"], "s", by=at),
                   "2024-03-01", run_at=at, read=short, folder="s",
                   root=env["root"], archive=env["archive"])
    assert not raw_path("src", "t", "2024-03-01", env["root"]).exists()
    assert files.ledger(env["con"], "src") == []
