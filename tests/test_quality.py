"""Chapter 15's checks on loads: freshness from the load log, volume
against a robust band, and partitions the log says were written."""
from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timedelta

import duckdb
import pytest

from groundwork import loadlog, quality, storage

D = date(2026, 1, 4)
RUN = datetime(2026, 1, 5, 6, 0)


def log(con, source, table, day, *, rows, status="succeeded",
        extra=None, error=None):
    """One load in the log, as a loader would leave it."""
    at = datetime.combine(day + timedelta(days=1), datetime.min.time()) \
        + timedelta(hours=6)
    load_id = loadlog.start(con, source, table, day, started_at=at)
    if status == "succeeded":
        loadlog.finish(con, load_id, finished_at=at, rows_read=rows,
                       rows_written=rows, source_count=rows,
                       extra=extra or {})
    else:
        loadlog.fail(con, load_id, finished_at=at, error=error or "boom")
    return load_id


@pytest.fixture
def con(tmp_path):
    c = duckdb.connect(str(tmp_path / "w.duckdb"))
    loadlog.ensure(c)
    yield c
    c.close()


def statuses(checks, kind):
    return {c.name: c.status for c in checks if c.kind == kind}


def test_a_day_is_due_an_hour_after_its_run_starts():
    rule = quality.RULES[0]
    assert quality.expected(rule, datetime(2026, 1, 5, 6, 59)) == \
        date(2026, 1, 3)
    assert quality.expected(rule, datetime(2026, 1, 5, 7, 0)) == D


def test_a_month_is_due_on_the_fifth_working_day_after_it():
    ledger = next(r for r in quality.RULES if r.cadence == "monthly")
    assert quality.fifth_working_day(date(2026, 1, 1)) == \
        date(2026, 1, 7)
    assert quality.fifth_working_day(date(2025, 12, 1)) == \
        date(2025, 12, 5)
    assert quality.expected(ledger, datetime(2026, 1, 6, 12)) == \
        date(2025, 11, 1)
    assert quality.expected(ledger, datetime(2026, 1, 7, 12)) == \
        date(2025, 12, 1)


def test_freshness_names_the_failed_load_and_uses_the_rules_severity(con):
    for day in (date(2026, 1, 2), date(2026, 1, 3)):
        log(con, "relay", "accounts", day, rows=6058)
        log(con, "tradeline", "orders", day, rows=1400)
    log(con, "relay", "accounts", D, rows=0, status="failed",
        error="MissingFile: no export for 2026-01-04")
    checks = quality.freshness(con, RUN + timedelta(hours=1))
    by = {c.name: c for c in checks}
    assert by["relay.accounts"].status == "warn"
    assert "failed" in by["relay.accounts"].detail
    assert "MissingFile" in by["relay.accounts"].detail
    assert by["tradeline.orders"].status == "error"
    assert "no load ran" in by["tradeline.orders"].detail
    assert by["catalogue.products"].detail.startswith("none")


def test_the_ledger_is_judged_by_its_newest_month(con):
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE TABLE raw.ledger_gl_revenue AS "
                "SELECT '2025-11' AS period")
    ok = quality.freshness(con, datetime(2026, 1, 6, 12))
    late = quality.freshness(con, datetime(2026, 1, 8, 12))
    assert statuses(ok, "freshness")["ledger.gl_revenue"] == "pass"
    assert statuses(late, "freshness")["ledger.gl_revenue"] == "warn"


def test_a_rerun_that_wrote_nothing_does_not_hide_the_first(con):
    log(con, "relay", "accounts", D, rows=6058)
    log(con, "relay", "accounts", D, rows=0)
    assert quality.daily_rows(con, "relay", "accounts") == \
        [(D, 6058, "rows")]


def test_volume_judges_a_short_day_by_the_rule_and_an_empty_one_fails(
        con):
    for i in range(30, 0, -1):
        log(con, "relay", "accounts", D - timedelta(days=i),
            rows=6000 + i)
        log(con, "tradeline", "orders", D - timedelta(days=i),
            rows=1400 + i)
    log(con, "relay", "accounts", D, rows=3029)
    log(con, "tradeline", "orders", D, rows=0)
    v = statuses(quality.volume(con, RUN + timedelta(hours=1)), "volume")
    assert v["relay.accounts"] == "error"        # half a snapshot
    assert v["tradeline.orders"] == "error"      # nothing at all
    assert v["relay.contracts"] == "skip"


def test_a_busy_night_is_a_warning(con):
    for i in range(30, 0, -1):
        log(con, "tradeline", "orders", D - timedelta(days=i), rows=1400)
    log(con, "tradeline", "orders", D, rows=4000)
    v = statuses(quality.volume(con, RUN + timedelta(hours=1)), "volume")
    assert v["tradeline.orders"] == "warn"


def test_a_snapshot_is_not_compared_with_nightly_changes(con):
    log(con, "tradeline", "orders", D - timedelta(days=20),
        rows=480000, extra={"snapshot": True, "mode": "cdc"})
    for i in range(19, 0, -1):
        log(con, "tradeline", "orders", D - timedelta(days=i), rows=1400,
            extra={"mode": "cdc"})
    log(con, "tradeline", "orders", D, rows=1390, extra={"mode": "cdc"})
    v = {c.name: c for c in quality.volume(con, RUN + timedelta(hours=1))}
    assert v["tradeline.orders"].status == "pass"


def test_the_band_survives_a_flat_series_and_an_outlier():
    days = [date(2025, 1, 1) + timedelta(days=i) for i in range(40)]
    flat = [(d, 6058) for d in days]
    assert not any(p.alarm for p in quality.anomalies(flat))
    bumped = [(d, 6058 + (1 if i == 39 else 0)) for i, d in
              enumerate(days)]
    assert not quality.anomalies(bumped)[-1].alarm
    # One empty night inside the window leaves the band where it was.
    holed = [(d, 0 if i == 20 else 6058) for i, d in enumerate(days)]
    points = quality.anomalies(holed)
    assert [p.day for p in points if p.alarm] == [days[20]]
    assert points[-1].low == pytest.approx(6058 * 0.95)


def test_partitions_finds_the_one_the_log_wrote_and_the_disk_lost(
        con, tmp_path):
    root = tmp_path / "raw"
    for day in (date(2026, 1, 2), D):
        storage.write_partition([{"a": 1}], "relay", "accounts", day,
                                loaded_at=RUN, load_id="x", root=root)
        log(con, "relay", "accounts", day, rows=1)
    log(con, "relay", "field_history", D, rows=0)
    assert statuses(quality.partitions(con, root), "partitions") == \
        {"relay.accounts": "pass"}
    shutil.rmtree(storage.raw_path("relay", "accounts", D, root).parent)
    (c,) = quality.partitions(con, root)
    assert c.status == "error" and str(D) in c.detail


def test_a_cdc_night_is_looked_for_in_the_tables_log(con, tmp_path):
    root = tmp_path / "raw"
    storage.write_partition([{"a": 1}], "tradeline", "orders_log", D,
                            loaded_at=RUN, load_id="x", root=root)
    log(con, "tradeline", "orders", D, rows=1, extra={"mode": "cdc"})
    (c,) = quality.partitions(con, root)
    assert (c.table, c.status) == ("orders_log", "pass")


def test_checks_run_read_only_and_record_their_verdicts(tmp_path):
    path = tmp_path / "w.duckdb"
    c = duckdb.connect(str(path))
    loadlog.ensure(c)
    log(c, "relay", "accounts", D, rows=6058)
    c.close()
    ro = duckdb.connect(str(path), read_only=True)
    checks = quality.check(ro, RUN + timedelta(hours=1), tmp_path / "raw")
    ro.close()
    assert {c.kind for c in checks} == {"freshness", "volume",
                                        "partitions"}
    rw = duckdb.connect(str(path))
    assert quality.record(rw, checks, RUN) == len(checks)
    assert quality.record(rw, checks, RUN) == len(checks)
    (n,) = rw.execute("SELECT count(*) FROM audit.quality_checks"
                      ).fetchone()
    assert n == len(checks)


def test_a_warehouse_with_no_load_log_says_never_loaded(tmp_path):
    con = duckdb.connect(str(tmp_path / "empty.duckdb"))
    checks = quality.freshness(con, RUN)
    assert all(c.status in ("error", "warn") for c in checks)
    assert json.dumps([c.detail for c in checks])


def test_report_keeps_to_the_page():
    long = quality.Check("freshness", "frontline", "ticket_comments",
                         "error", "x" * 120)
    assert all(len(line) <= 68 for line in quality.report([long]))
