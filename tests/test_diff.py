"""groundwork.diff: two builds of the warehouse compared table by
table. The unit tests compare warehouses written by hand; the diff
tests build the tiny Meridian twice with dbt and compare the builds."""
from __future__ import annotations

import shutil

import duckdb
import pytest

from groundwork import diff, testing


def two(tmp_path, a_sql, b_sql):
    paths = []
    for name, sql in (("a", a_sql), ("b", b_sql)):
        p = tmp_path / name / "warehouse.duckdb"
        p.parent.mkdir()
        con = duckdb.connect(str(p))
        con.execute("CREATE SCHEMA marts; CREATE SCHEMA staging")
        con.execute(sql)
        con.close()
        paths.append(p)
    return paths


T = ("CREATE TABLE marts.t (id INTEGER, x INTEGER, y VARCHAR); "
     "INSERT INTO marts.t VALUES ")


@pytest.mark.unit
def test_keys_say_changed_added_and_removed_and_columns_say_where(
        tmp_path):
    a, b = two(tmp_path, T + "(1, 10, 'p'), (2, 20, 'q'), (3, 30, 'r')",
               T + "(1, 10, 'p'), (2, 21, 'Q'), (4, 40, 's')")
    (d,) = diff.compare(a, b, key_of={"marts.t": ("id",)})
    assert (d.rows_a, d.rows_b, d.changed, d.added, d.removed) == (
        3, 3, 1, 1, 1)
    assert d.columns == {"x": 1, "y": 1} and not d.same


@pytest.mark.unit
def test_a_table_without_a_key_is_compared_as_rows(tmp_path):
    a, b = two(tmp_path, T + "(1, 10, 'p'), (1, 10, 'p')",
               T + "(1, 10, 'p'), (1, 11, 'p')")
    (d,) = diff.compare(a, b, key_of={})
    assert (d.key, d.removed, d.added) == ((), 1, 1)


@pytest.mark.unit
def test_nulls_are_equal_and_a_new_column_is_reported(tmp_path):
    a, b = two(tmp_path, T + "(1, NULL, 'p')",
               "CREATE TABLE marts.t (id INTEGER, x INTEGER, y VARCHAR, "
               "z INTEGER); INSERT INTO marts.t VALUES (1, NULL, 'p', 0)")
    (d,) = diff.compare(a, b, key_of={"marts.t": ("id",)})
    assert d.changed == 0 and d.schema == ["+z"] and not d.same


@pytest.mark.unit
def test_a_view_is_read_in_its_own_build(tmp_path):
    v = "; CREATE VIEW staging.v AS SELECT id, x * 2 AS x2 FROM marts.t"
    a, b = two(tmp_path, T + "(1, 10, 'p')" + v, T + "(1, 11, 'p')" + v)
    got = {d.name: d for d in diff.compare(a, b, key_of={})}
    assert got["staging.v"].added == got["staging.v"].removed == 1


@pytest.mark.unit
def test_the_marts_keys_come_from_marts_sql():
    keys = diff.marts_keys()
    assert keys["marts.fct_order_lines"] == ("order_id", "line_no")
    assert keys["marts.fct_contract_month"] == ("contract_id",
                                                "month_end_date_key")


@pytest.mark.diff
def test_the_same_sources_built_twice_are_the_same_warehouse(
        tiny, tmp_path):
    again = testing.example(tmp_path / "again")
    again.build()
    diffs = diff.compare(tiny.warehouse, again.warehouse)
    assert len(diffs) == 24 and all(d.same for d in diffs)


@pytest.mark.diff
def test_a_one_line_change_shows_where_it_landed(tiny, tmp_path):
    project = tmp_path / "project"
    shutil.copytree(testing.PROJECT, project,
                    ignore=shutil.ignore_patterns("target", "logs"))
    f = project / "models" / "staging" / "stg_relay__contracts.sql"
    old = "CAST(nullif(c.discount_pct, '') AS INTEGER)"
    f.write_text(f.read_text().replace(
        old, f"coalesce({old}, 0)", 1))
    branch = testing.Sources(tmp_path / "branch")
    branch.__dict__.update({k: v for k, v in tiny.__dict__.items()
                            if k != "root"})
    branch.build(project)
    changed = {d.name: d.changed for d in
               diff.compare(tiny.warehouse, branch.warehouse)
               if not d.same}
    assert changed == {"staging.stg_relay__contracts": 1,
                       "marts.fct_contracts": 1,
                       "marts.fct_contract_month": 6}
