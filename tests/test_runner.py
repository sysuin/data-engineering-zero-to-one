"""The transformation runner: discovery, ref(), the graph, and building only what changed."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from groundwork import loadlog
from groundwork.transform import runner

AT = datetime(2026, 1, 5, 6, 30)


def write(folder: Path, rel: str, text: str) -> Path:
    path = folder / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@pytest.fixture
def project(tmp_path):
    """A warehouse with one raw table, and three models and a seed over it."""
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    con.execute("CREATE SCHEMA raw; CREATE TABLE raw.things AS "
                "SELECT * FROM (VALUES ('1', 'a'), ('2', 'b'), ('3', 'b')) "
                "t(id, kind)")
    con.close()
    models = tmp_path / "models"
    write(models, "staging/stg_src__things.sql",
          "-- one row per thing\n-- materialised: view\n"
          "SELECT CAST(id AS INTEGER) AS thing_id, kind\n"
          "FROM {{ source('raw', 'things') }}\n")
    write(models, "seeds/kinds.csv", "kind,label\na,Apple\nb,Banana\n")
    write(models, "marts/by_kind.sql",
          "-- materialised: table\n"
          "SELECT k.label, count(*) AS n\n"
          "FROM {{ ref('stg_src__things') }} t\n"
          "JOIN {{ ref('kinds') }} k USING (kind) GROUP BY ALL\n")
    write(models, "marts/report.sql",
          "SELECT * FROM {{ ref( 'by_kind' ) }} ORDER BY label\n")
    return wh, models


def statuses(results):
    return {r.model: r.status for r in results}


def test_discover_reads_names_schemas_headers_and_refs(project):
    _, models = project
    found = runner.discover(models)
    assert set(found) == {"stg_src__things", "kinds", "by_kind", "report"}
    m = found["by_kind"]
    assert (m.schema, m.materialised) == ("marts", "table")
    assert m.refs == ("kinds", "stg_src__things")
    assert found["stg_src__things"].sources == (("raw", "things"),)
    assert found["report"].materialised == "view"          # the default
    assert found["kinds"].relation == "seeds.kinds"
    assert found["kinds"].materialised == "seed"


def test_unknown_materialisation_and_duplicate_names_fail(project):
    _, models = project
    write(models, "marts/odd.sql", "-- materialised: incremental\nSELECT 1\n")
    with pytest.raises(ValueError, match="incremental"):
        runner.discover(models)
    (models / "marts" / "odd.sql").unlink()
    write(models, "intermediate/report.sql", "SELECT 1\n")
    with pytest.raises(ValueError, match="two models"):
        runner.discover(models)


def test_compile_resolves_ref_and_source(project):
    _, models = project
    found = runner.discover(models)
    sql = runner.compile_sql(found["by_kind"], found)
    assert "staging.stg_src__things t" in sql
    assert "seeds.kinds k" in sql and "{{" not in sql
    sql = runner.compile_sql(found["stg_src__things"], found)
    assert "FROM raw.things" in sql


def test_a_ref_to_nothing_fails_before_anything_is_built(project):
    wh, models = project
    write(models, "marts/broken.sql", "SELECT * FROM {{ ref('nope') }}\n")
    with pytest.raises(KeyError, match="nope"):
        runner.run(wh, folder=models, run_at=AT)
    con = duckdb.connect(str(wh))
    assert con.execute("SELECT count(*) FROM duckdb_views() "
                       "WHERE schema_name = 'staging'").fetchone() == (0,)


def test_order_puts_every_model_after_what_it_refs(project):
    _, models = project
    order = runner.order(runner.discover(models))
    assert order.index("stg_src__things") < order.index("by_kind")
    assert order.index("kinds") < order.index("by_kind")
    assert order.index("by_kind") < order.index("report")
    assert order == runner.order(runner.discover(models))   # deterministic


def test_a_cycle_is_named(project):
    _, models = project
    write(models, "staging/stg_src__things.sql",
          "SELECT * FROM {{ ref('report') }}\n")
    with pytest.raises(ValueError, match="a cycle: .*report.*refs"):
        runner.order(runner.discover(models))


def test_first_run_builds_everything_and_logs_it(project):
    wh, models = project
    results = runner.run(wh, folder=models, run_at=AT)
    assert set(statuses(results).values()) == {"built"}
    con = duckdb.connect(str(wh))
    assert con.execute("SELECT * FROM marts.report").fetchall() == [
        ("Apple", 1), ("Banana", 2)]
    kinds = dict(con.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema IN ('staging', 'marts', 'seeds')").fetchall())
    assert kinds == {"stg_src__things": "VIEW", "by_kind": "BASE TABLE",
                     "report": "VIEW", "kinds": "BASE TABLE"}
    rows = con.execute("SELECT run_id, model, status, rows, started_at "
                       "FROM audit.model_runs ORDER BY model").fetchall()
    assert [r[1] for r in rows] == ["by_kind", "kinds", "report",
                                    "stg_src__things"]
    assert {r[0] for r in rows} == {1}
    assert dict((r[1], r[3]) for r in rows)["stg_src__things"] == 3
    assert all(r[4] >= AT for r in rows)


def test_seeds_load_as_text(project):
    wh, models = project
    write(models, "seeds/kinds.csv", "kind,label\na,007\nb,Banana\n")
    runner.run(wh, folder=models, run_at=AT)
    con = duckdb.connect(str(wh))
    assert con.execute("SELECT label FROM seeds.kinds WHERE kind = 'a'"
                       ).fetchone() == ("007",)


def test_second_run_skips_and_full_rebuilds(project):
    wh, models = project
    runner.run(wh, folder=models, run_at=AT)
    again = runner.run(wh, folder=models, run_at=AT)
    assert set(statuses(again).values()) == {"skipped"}
    full = runner.run(wh, folder=models, run_at=AT, full=True)
    assert set(statuses(full).values()) == {"built"}


def test_a_change_rebuilds_the_model_and_what_is_downstream(project):
    wh, models = project
    runner.run(wh, folder=models, run_at=AT)
    path = models / "marts" / "by_kind.sql"
    path.write_text(path.read_text().replace("count(*)", "count(*) "))
    got = statuses(runner.run(wh, folder=models, run_at=AT))
    assert got == {"stg_src__things": "skipped", "kinds": "skipped",
                   "by_kind": "built", "report": "built"}
    write(models, "seeds/kinds.csv", "kind,label\na,Apricot\nb,Banana\n")
    got = statuses(runner.run(wh, folder=models, run_at=AT))
    assert got["kinds"] == got["by_kind"] == got["report"] == "built"
    assert got["stg_src__things"] == "skipped"


def test_a_new_load_rebuilds_tables_not_views(project):
    wh, models = project
    runner.run(wh, folder=models, run_at=AT)
    con = duckdb.connect(str(wh))
    load_id = loadlog.start(con, "things", "things", date(2026, 1, 5),
                            started_at=AT)
    loadlog.finish(con, load_id, finished_at=AT)
    con.close()
    got = statuses(runner.run(wh, folder=models, run_at=AT))
    assert got["stg_src__things"] == "skipped"       # a view is never stale
    assert got["by_kind"] == "built"                 # a table can be
    assert got["report"] == "built"                  # downstream of it


def test_a_failure_blocks_downstream_and_keeps_the_last_version(project):
    wh, models = project
    runner.run(wh, folder=models, run_at=AT)
    write(models, "marts/by_kind.sql",
          "-- materialised: table\nSELECT CAST(kind AS INTEGER) AS n\n"
          "FROM {{ ref('stg_src__things') }}\n")
    write(models, "marts/other.sql",
          "SELECT count(*) AS n FROM {{ ref('stg_src__things') }}\n")
    results = runner.run(wh, folder=models, run_at=AT)
    got = statuses(results)
    assert got["by_kind"] == "failed" and got["report"] == "blocked"
    assert got["other"] == "built"
    assert "Conversion" in next(r.error for r in results
                                if r.model == "by_kind")
    con = duckdb.connect(str(wh))
    assert con.execute("SELECT count(*) FROM marts.by_kind").fetchone() == (2,)
    assert con.execute("SELECT count(*) FROM audit.model_runs WHERE "
                       "status IN ('failed', 'blocked')").fetchone() == (2,)


def test_switching_a_view_to_a_table(project):
    wh, models = project
    runner.run(wh, folder=models, run_at=AT)
    path = models / "marts" / "report.sql"
    path.write_text("-- materialised: table\n" + path.read_text())
    got = statuses(runner.run(wh, folder=models, run_at=AT))
    assert got["report"] == "built"
    con = duckdb.connect(str(wh))
    assert con.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_name = 'report'").fetchone() == ("BASE TABLE",)


def test_select_builds_a_model_and_its_downstream_only(project):
    wh, models = project
    runner.run(wh, folder=models, run_at=AT)
    results = runner.run(wh, folder=models, run_at=AT,
                         select=["by_kind"], full=True)
    assert [r.model for r in results] == ["by_kind", "report"]
    with pytest.raises(KeyError, match="no such model"):
        runner.run(wh, folder=models, run_at=AT, select=["nope"])


def test_the_real_models_compile_and_order():
    found = runner.discover()
    order = runner.order(found)
    # 15 from Chapter 11; Chapter 13 adds ticket versions and comments
    assert len([n for n in order if n.startswith("stg_")]) == 17
    for name in order:
        assert "{{" not in runner.compile_sql(found[name], found)
        if name.startswith("stg_"):
            assert found[name].schema == "staging"
            source = name.removeprefix("stg_").split("__")[0]
            assert any(t.startswith(source) for _, t in found[name].sources)
