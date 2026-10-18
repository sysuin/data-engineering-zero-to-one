"""The load log: attempts counted, outcomes recorded, the last good load found."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import duckdb
import pytest

from groundwork import loadlog

RUN = datetime(2026, 1, 5, 2, 5)


@pytest.fixture
def con():
    c = duckdb.connect()
    loadlog.ensure(c)
    return c


def test_ensure_is_safe_twice(con):
    loadlog.ensure(con)
    (n,) = con.execute("SELECT count(*) FROM audit.load_log").fetchone()
    assert n == 0


def test_the_table_has_the_agreed_columns(con):
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE "
        "table_schema = 'audit' AND table_name = 'load_log' "
        "ORDER BY ordinal_position").fetchall()]
    assert tuple(cols) == loadlog.COLUMNS


def test_a_started_load_is_running_until_closed(con):
    lid = loadlog.start(con, "tradeline", "orders", "2026-01-04",
                        started_at=RUN)
    assert lid == "tradeline.orders.2026-01-04.1"
    (x,) = loadlog.history(con)
    assert x.status == "running"
    assert x.finished_at is None and x.seconds is None
    assert x.load_date == date(2026, 1, 4)


def test_attempts_are_numbered_per_table_and_day(con):
    ids = [loadlog.start(con, "tradeline", "orders", "2026-01-04",
                         started_at=RUN) for _ in range(3)]
    other = loadlog.start(con, "tradeline", "order_lines", "2026-01-04",
                          started_at=RUN)
    nextday = loadlog.start(con, "tradeline", "orders", "2026-01-05",
                            started_at=RUN)
    assert [i.rsplit(".", 1)[1] for i in ids] == ["1", "2", "3"]
    assert other.endswith(".1") and nextday.endswith(".1")


def test_finish_records_counts_extra_and_duration(con):
    lid = loadlog.start(con, "t", "x", "2026-01-04", started_at=RUN)
    loadlog.finish(con, lid, finished_at=RUN + timedelta(seconds=7.5),
                   rows_read=10, rows_written=10, source_count=10,
                   extra={"bytes": 123})
    x = loadlog.last(con, "t", "x")
    assert x.status == "succeeded"
    assert (x.rows_read, x.rows_written, x.source_count) == (10, 10, 10)
    assert x.extra == {"bytes": 123}
    assert x.seconds == 7.5
    assert x.reconciled


def test_counts_that_disagree_are_not_reconciled(con):
    lid = loadlog.start(con, "t", "x", "2026-01-04", started_at=RUN)
    loadlog.finish(con, lid, finished_at=RUN, rows_read=10,
                   rows_written=9, source_count=10)
    assert not loadlog.last(con, "t", "x").reconciled


def test_fail_keeps_the_error_and_known_counts(con):
    lid = loadlog.start(con, "t", "x", "2026-01-04", started_at=RUN)
    loadlog.fail(con, lid, finished_at=RUN, error="OSError: disk full",
                 source_count=5)
    (x,) = loadlog.history(con)
    assert x.status == "failed"
    assert x.extra == {"error": "OSError: disk full"}
    assert x.source_count == 5 and x.rows_written is None
    assert loadlog.last(con, "t", "x") is None
    assert loadlog.last(con, "t", "x", status="failed") == x


def test_fail_refuses_unknown_counts(con):
    lid = loadlog.start(con, "t", "x", "2026-01-04", started_at=RUN)
    with pytest.raises(TypeError):
        loadlog.fail(con, lid, finished_at=RUN, error="e", rows=3)


def test_a_load_is_closed_once(con):
    lid = loadlog.start(con, "t", "x", "2026-01-04", started_at=RUN)
    loadlog.finish(con, lid, finished_at=RUN)
    with pytest.raises(ValueError):
        loadlog.fail(con, lid, finished_at=RUN, error="late")
    with pytest.raises(KeyError):
        loadlog.finish(con, "t.x.2026-01-04.9", finished_at=RUN)


def test_last_is_the_latest_day_then_the_latest_attempt(con):
    def done(day, at):
        lid = loadlog.start(con, "t", "x", day, started_at=at)
        loadlog.finish(con, lid, finished_at=at)
        return lid
    done("2026-01-03", RUN)
    done("2026-01-04", RUN)
    second = done("2026-01-04", RUN + timedelta(hours=1))
    # a backfill of an older day, run later, is not the latest load
    done("2026-01-02", RUN + timedelta(hours=2))
    assert loadlog.last(con, "t", "x").load_id == second
    assert loadlog.last(con, "t", "missing") is None
    with pytest.raises(ValueError):
        loadlog.last(con, "t", "x", status="done")


def test_history_filters_and_orders(con):
    for table, minutes in (("b", 2), ("a", 1)):
        loadlog.start(con, "s", table, "2026-01-04",
                      started_at=RUN + timedelta(minutes=minutes))
    loadlog.start(con, "other", "a", "2026-01-04", started_at=RUN)
    assert [x.table_name for x in loadlog.history(con, "s")] == ["a", "b"]
    assert len(loadlog.history(con, table="a")) == 2
    assert len(loadlog.history(con)) == 3


def test_moments_are_stored_as_utc_without_a_zone(con):
    newark = timezone(timedelta(hours=-5))
    lid = loadlog.start(con, "t", "x", date(2026, 1, 4),
                        started_at=datetime(2026, 1, 4, 21, 5,
                                            tzinfo=newark))
    loadlog.finish(con, lid, finished_at=datetime(2026, 1, 5, 2, 6,
                                                  tzinfo=timezone.utc))
    x = loadlog.last(con, "t", "x")
    assert x.started_at == RUN and x.finished_at.tzinfo is None
    assert x.seconds == 60


def test_a_moment_is_not_a_load_date(con):
    with pytest.raises(TypeError):
        loadlog.start(con, "t", "x", RUN, started_at=RUN)
    with pytest.raises(TypeError):
        loadlog.start(con, "t", "x", "2026-01-04", started_at="02:05")


def test_status_is_checked_by_the_table(con):
    lid = loadlog.start(con, "t", "x", "2026-01-04", started_at=RUN)
    with pytest.raises(duckdb.ConstraintException):
        con.execute("UPDATE audit.load_log SET status = 'done' "
                    "WHERE load_id = ?", [lid])
