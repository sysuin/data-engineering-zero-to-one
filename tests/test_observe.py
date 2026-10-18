"""Chapter 21's observability: signals, lineage, alerts and the SLO."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from groundwork import loadlog
from groundwork.contracts import check
from groundwork.observe import MONITORS, alerts, lineage, metrics, monitor
from groundwork.observe import nightly as observed
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import Fatal

DAY = date(2024, 7, 2)
AT = datetime(2024, 7, 3, 7, 0)


# ------------------------------------------------------------- helpers
def warehouse(tmp_path):
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    loadlog.ensure(con)
    check.ensure(con)
    runner.ensure(con)
    return con


def load(con, source, table, day, rows, status="succeeded", n=1,
         extra="{}"):
    lid = f"{source}.{table}.{day}.{n}"
    started = datetime.combine(day + timedelta(days=1),
                               datetime.min.time()).replace(hour=6)
    con.execute("INSERT INTO audit.load_log VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [lid, source, table, day, started,
                 started + timedelta(seconds=5), status, rows, rows,
                 rows, extra])
    return lid


def batch(con, lid, name, status, rows, parked=0, fmt="standard",
          ref=None, at=datetime(2024, 7, 3, 6, 0)):
    con.execute("INSERT INTO audit.contract_runs VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [lid, ref or name, "suppliers", "price_lists", name, at,
                 1, fmt, rows, parked, status,
                 "header renamed?" if status == "broken" else None])


def run(con, day, state, failed=None, published=None):
    rid = f"nightly.{day}.1"
    at = datetime.combine(day + timedelta(days=1),
                          datetime.min.time()).replace(hour=6)
    con.execute("INSERT INTO audit.dag_runs VALUES "
                "(?, 'nightly', ?, 'scheduled', ?, ?, ?)",
                [rid, day, at, state, at])
    if failed:
        con.execute("INSERT INTO audit.task_runs VALUES "
                    "(?, 'nightly', ?, ?, 1, 'failed', ?, ?, ?)",
                    [rid, day, failed, at, at, "fatal: broken"])
        con.execute("INSERT INTO audit.task_runs VALUES "
                    "(?, 'nightly', ?, 'publish', 0, 'upstream_failed',"
                    " ?, ?, '')", [rid, day, at, at])
    if published:
        con.execute("INSERT INTO audit.task_runs VALUES "
                    "(?, 'nightly', ?, 'publish', 1, 'succeeded', ?, ?,"
                    " 'published')", [rid, day, published, published])


# ------------------------------------------------------------- lineage
SCHEMA = {
    "raw": {"orders": {"id": "INTEGER", "status": "VARCHAR"},
            "lines": {"order_id": "INTEGER", "qty": "INTEGER",
                      "price_cents": "INTEGER"},
            "unknown": {"id": "INTEGER"}},
    "marts": {"f": {"order_id": "INTEGER", "revenue": "INTEGER",
                    "unknown_id": "INTEGER"}},
}
SQL = """
WITH o AS (SELECT id, status FROM raw.orders WHERE status = 'placed')
SELECT l.order_id, l.qty * l.price_cents AS revenue,
       (SELECT max(id) FROM raw.unknown WHERE id < 0) AS unknown_id
FROM raw.lines AS l JOIN o ON o.id = l.order_id
"""


def test_column_lineage_finds_values_and_the_filter_through_a_cte():
    edges, lost = lineage.column_edges("marts.f", SQL, SCHEMA)
    assert lost == []
    assert ("raw.lines.qty", "marts.f.revenue", "value") in edges
    assert ("raw.lines.price_cents", "marts.f.revenue", "value") in edges
    assert ("raw.orders.status", "marts.f", "filter") in edges


def test_a_subquery_that_computes_a_value_chooses_no_rows():
    edges, _ = lineage.column_edges("marts.f", SQL, SCHEMA)
    assert ("raw.unknown.id", "marts.f", "filter") not in edges
    assert ("raw.unknown.id", "marts.f.unknown_id", "value") in edges


def test_impact_follows_values_and_optionally_rows():
    g = lineage.Graph()
    for a, b, h in lineage.column_edges("marts.f", SQL, SCHEMA)[0]:
        g.add(a, b, h, "column", "column" if h == "value" else "relation")
    status = lineage.impact(g, lineage.Graph(), "raw.orders.status")
    assert set(status["columns"]) == {"marts.f.order_id",
                                      "marts.f.revenue",
                                      "marts.f.unknown_id"}
    values = lineage.impact(g, lineage.Graph(), "raw.orders.status",
                            rows=False)
    assert values["columns"] == []
    qty = lineage.impact(g, lineage.Graph(), "raw.lines.qty")
    assert qty["columns"] == ["marts.f.revenue"]


def test_table_lineage_joins_the_dag_to_the_manifest():
    src = {"resource_type": "source", "schema": "raw",
           "name": "suppliers_price_lists",
           "identifier": "suppliers_price_lists"}
    stg = {"resource_type": "model", "schema": "staging",
           "name": "stg_suppliers__price_lists"}
    test = {"resource_type": "test", "schema": "x", "name": "t1"}
    manifest = {"sources": {"source.g.raw.s": src},
                "nodes": {"model.g.stg": stg, "test.g.t1": test},
                "parent_map": {"model.g.stg": ["source.g.raw.s"],
                               "test.g.t1": ["model.g.stg"],
                               "source.g.raw.s": []}}
    g = lineage.tables(observed(), manifest)
    assert ("task:ingest_suppliers", "raw.suppliers_price_lists",
            "writes") in g.edges
    assert g.downstream("suppliers.price_lists") >= {
        "task:ingest_suppliers", "raw.suppliers_price_lists",
        "staging.stg_suppliers__price_lists", "test:t1"}
    assert g.kinds["staging.stg_suppliers__price_lists"] == "staging"


# ------------------------------------------------------------- metrics
def test_held_counts_parked_rows_and_rows_in_broken_files(tmp_path):
    con = warehouse(tmp_path)
    lid = load(con, "suppliers", "price_lists", DAY, 9)
    batch(con, lid, "suppliers/p/JULY.csv", "broken", 6, ref="aa" * 32)
    batch(con, lid, "suppliers/a/A.csv", "parked", 2, parked=1)
    batch(con, lid, "suppliers/b/B.csv", "passed", 7)
    (h,) = metrics.held(con, DAY, AT)
    assert (h.rows, h.parked, h.broken, h.files) == (15, 1, 6,
                                                    ("JULY.csv",))
    assert h.share == pytest.approx(7 / 15)


def test_schema_signals_a_break_and_a_sender_new_format(tmp_path):
    con = warehouse(tmp_path)
    old = load(con, "suppliers", "price_lists", DAY - timedelta(1), 6)
    batch(con, old, "suppliers/p/JUNE.csv", "passed", 6, fmt="pemberton",
          at=datetime(2024, 7, 2, 6))
    lid = load(con, "suppliers", "price_lists", DAY, 12)
    batch(con, lid, "suppliers/p/JULY.csv", "passed", 6,
          fmt="pemberton-2024")
    (s,) = metrics.schema(con, DAY, AT)
    assert s.alarm and s.value == 1 and "new format" in s.detail
    assert s.severity == "warn"
    batch(con, lid, "suppliers/q/Q.csv", "broken", 6)
    (s,) = metrics.schema(con, DAY, AT)
    assert s.value == 2 and s.severity == "error"


def test_freshness_reads_the_log_as_it_stood_at_the_moment(tmp_path):
    con = warehouse(tmp_path)
    load(con, "relay", "accounts", DAY - timedelta(1), 6000)
    load(con, "relay", "accounts", DAY, 6000)
    run(con, DAY - timedelta(1), "succeeded",
        published=datetime(2024, 7, 2, 6, 2))
    got = {s.subject: s for s in metrics.freshness(con, DAY, AT)}
    assert got["relay.accounts"].value == 0
    assert got["published"].value == 1 and got["published"].alarm
    earlier = {s.subject: s for s in metrics.freshness(
        con, DAY, datetime(2024, 7, 2, 7))}
    assert earlier["relay.accounts"].value == 1   # not loaded by then


def test_a_distribution_with_a_limit_alarms_at_error(tmp_path):
    for day, costs in ((DAY - timedelta(1), ["1.00", "2.00"]),
                       (DAY, ["3.00", ""])):
        path = (tmp_path / "suppliers" / "price_lists"
                / f"load_date={day}" / "part-0.parquet")
        path.parent.mkdir(parents=True)
        pq.write_table(pa.table({"cost": costs}), path)
    got = [s for s in metrics.distribution(DAY, tmp_path)
           if s.subject == "suppliers.price_lists"]
    assert len(got) == 1
    assert got[0].value == 0.5 and got[0].alarm
    assert got[0].severity == "error"


# --------------------------------------------------------------- alerts
def test_a_broken_file_pages_once_and_copies_the_sender(tmp_path):
    con = warehouse(tmp_path)
    lid = load(con, "suppliers", "price_lists", DAY, 6)
    batch(con, lid, "suppliers/p/JULY.csv", "broken", 6, ref="ab" * 32)
    run(con, DAY, "failed", failed="ingest_suppliers")
    w = alerts.watch(con, AT)
    assert {a.rule for a in w.opened} == {"held_rows", "night_failed"}
    held = [s for s in w.sent if s.key.startswith("held")]
    assert {(s.kind, s.to) for s in held} == {
        ("page", "data engineering"), ("copy", "procurement")}
    again = alerts.watch(con, AT)
    assert again.opened == [] and again.sent == []
    assert len(again.repeated) == 2


def test_parked_rows_are_a_ticket_unless_most_of_a_batch(tmp_path):
    con = warehouse(tmp_path)
    lid = load(con, "suppliers", "price_lists", DAY, 9)
    batch(con, lid, "suppliers/a/A.csv", "parked", 10, parked=1)
    (a,) = alerts.held_rows(con, AT)
    assert a.severity == "ticket"
    batch(con, lid, "suppliers/b/B.csv", "parked", 2, parked=2)
    (a,) = alerts.held_rows(con, AT)
    assert a.severity == "ticket"            # 3 of 12
    batch(con, lid, "suppliers/c/C.csv", "parked", 10, parked=10)
    (a,) = alerts.held_rows(con, AT)
    assert a.severity == "page"              # 13 of 22


def test_an_alert_resolves_when_its_rule_stops_firing(tmp_path):
    con = warehouse(tmp_path)
    run(con, DAY, "failed", failed="ingest_suppliers")
    assert [a.rule for a in alerts.watch(con, AT).opened] == [
        "night_failed"]
    nxt = DAY + timedelta(days=1)
    run(con, nxt, "succeeded", published=datetime(2024, 7, 4, 6, 2))
    w = alerts.watch(con, AT + timedelta(days=1))
    assert w.resolved == ["night:nightly"]
    assert [s.kind for s in w.sent] == ["resolved"]


def test_a_page_at_three_in_new_york_waits_for_eight():
    page = alerts.Alert("k", "r", "page", "data engineering", "s", "t")
    assert alerts.deliver_at(page, AT) == datetime(2024, 7, 3, 12, 0)
    winter = datetime(2026, 1, 5, 7, 0)       # 02:00 in New York
    assert alerts.deliver_at(page, winter) == datetime(2026, 1, 5, 13)
    ticket = alerts.Alert("k", "r", "ticket", "x", "s", "t")
    assert alerts.deliver_at(ticket, AT) == AT
    woken = alerts.Alert("k", "r", "page", "x", "s", "t", wake=True)
    assert alerts.deliver_at(woken, AT) == AT


def test_the_budget_counts_late_days_since_the_dag_began(tmp_path):
    con = warehouse(tmp_path)
    for i in range(10):
        d = DAY + timedelta(days=i)
        late = i in (2, 3)
        run(con, d, "succeeded", published=datetime.combine(
            d + timedelta(days=1), datetime.min.time()).replace(
                hour=9 if late else 6))
    b = alerts.budget(con, datetime(2024, 7, 12, 7))
    assert (b.first, b.last, b.good, b.bad) == (DAY, DAY + timedelta(9),
                                                8, 2)
    assert b.left == pytest.approx(-0.5)
    assert alerts.budget_spent(con, datetime(2024, 7, 12, 7))


# --------------------------------------------------------- the monitor
def test_the_monitor_sits_between_the_build_and_publish():
    dag = observed()
    assert dag.tasks["observe"].upstream == ("dbt_build",)
    assert "observe" in dag.tasks["publish"].upstream
    assert "observe" in dag.downstream(["ingest_suppliers"])
    assert MONITORS[0].isolate


def test_the_monitor_stops_publish_on_an_error(tmp_path):
    path = (tmp_path / "raw" / "suppliers" / "price_lists"
            / f"load_date={DAY}" / "part-0.parquet")
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"cost": ["1.00", ""]}), path)
    warehouse(tmp_path).close()
    ctx = SimpleNamespace(day=DAY, env=SimpleNamespace(
        warehouse=tmp_path / "w.duckdb", root=tmp_path / "raw"))
    with pytest.raises(Fatal, match="share no cost"):
        monitor(ctx)
    con = duckdb.connect(str(tmp_path / "w.duckdb"), read_only=True)
    (n,) = con.execute("SELECT count(*) FROM audit.metrics WHERE "
                       "status = 'alarm'").fetchone()
    assert n == 1
