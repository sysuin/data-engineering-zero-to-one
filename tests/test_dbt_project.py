"""
Groundwork's dbt project (Chapter 14) against the runner's models it
was ported from: the same models in the same schemas, the same SQL
where nothing needed to change, the same seeds byte for byte; and
dim_date, which was Python and is now a model, the same calendar.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from groundwork import dates
from groundwork.transform import runner

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "dbt" / "groundwork"
HEADER = re.compile(r"^--\s*materialised:\s*\w+\s*\n", re.M)
CONFIG = re.compile(r"^\{\{ config\(materialized='\w+'\) \}\}\n\n", re.M)


def dbt_models() -> dict[str, Path]:
    return {p.stem: p for p in (PROJECT / "models").rglob("*.sql")}


def test_every_runner_model_is_a_dbt_model_in_the_same_schema():
    ported = dbt_models()
    for m in runner.discover().values():
        if m.materialised == "seed":
            assert (PROJECT / "seeds" / m.path.name).exists(), m.name
        else:
            assert m.name in ported, m.name
            assert ported[m.name].parent.name == m.schema, m.name
    assert set(ported) - set(runner.discover()) == {"dim_date"}


def test_seeds_are_the_runners_byte_for_byte():
    for p in sorted(runner.MODELS.glob("seeds/*.csv")):
        assert (PROJECT / "seeds" / p.name).read_bytes() == p.read_bytes()


def test_staging_and_intermediate_sql_is_unchanged_but_for_config():
    ported = dbt_models()
    for m in runner.discover().values():
        if m.schema not in ("staging", "intermediate"):
            continue
        ours = CONFIG.sub("", ported[m.name].read_text())
        assert HEADER.sub("", m.sql) == ours, m.name


def test_marts_say_how_they_are_materialised_once():
    project = (PROJECT / "dbt_project.yml").read_text()
    assert "+materialized: table" in project
    for p in (PROJECT / "models" / "marts").glob("*.sql"):
        assert "-- materialised:" not in p.read_text(), p.name


def test_keys_use_the_macro_in_every_dimension():
    for name in ("dim_account", "dim_product", "dim_region",
                 "dim_supplier"):
        sql = (PROJECT / "models" / "marts" / f"{name}.sql").read_text()
        assert "md5_key(" in sql and "substr(md5(" not in sql, name


def test_profiles_live_in_the_project_and_build_into_the_sandbox():
    text = (PROJECT / "profiles.yml").read_text()
    assert "data/sandbox/ch14/warehouse.duckdb" in text
    assert "data/groundwork/warehouse.duckdb" in text


def _dbt(args, env):
    return subprocess.run(
        [str(Path(sys.executable).with_name("dbt")), *args,
         "--profiles-dir", "."], cwd=PROJECT, env=env,
        capture_output=True, text=True)


@pytest.mark.slow
def test_dim_date_is_the_calendar_dates_py_builds(tmp_path):
    env = {**os.environ, "GROUNDWORK_DBT_PATH": str(tmp_path / "w.duckdb"),
           "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    out = _dbt(["run", "--select", "dim_date"], env)
    assert out.returncode == 0, out.stdout[-2000:]
    con = duckdb.connect(str(tmp_path / "w.duckdb"), read_only=True)
    rows = con.execute("SELECT * FROM marts.dim_date "
                       "ORDER BY date_key").fetchall()
    assert [tuple(r) for r in rows] == dates.build()


@pytest.mark.slow
def test_the_project_parses_and_its_graph_is_the_runners(tmp_path):
    env = {**os.environ, "GROUNDWORK_DBT_PATH": str(tmp_path / "w.duckdb"),
           "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    out = _dbt(["parse"], env)
    assert out.returncode == 0, out.stdout[-2000:]
    manifest = json.loads((PROJECT / "target" / "manifest.json")
                          .read_text())
    name = {u: n["name"] for u, n in {**manifest["nodes"],
                                      **manifest["sources"]}.items()}
    refs = {(name[p], n["name"]) for n in manifest["nodes"].values()
            if n["resource_type"] == "model"
            for p in n["depends_on"]["nodes"] if not
            p.startswith("source.")}
    mine = {(r, m.name) for m in runner.discover().values()
            for r in m.refs}
    # dbt has two more: fct_order_lines' incremental branch declares
    # the staging models it reads, with -- depends_on hints.
    assert refs - mine == {("stg_tradeline__orders", "fct_order_lines"),
                           ("stg_tradeline__order_lines",
                            "fct_order_lines")}
    assert mine <= refs
