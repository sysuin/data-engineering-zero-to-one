"""groundwork.nightly: v1.0's nightly DAG, and building from nothing."""
from __future__ import annotations

import pytest

from groundwork import nightly
from groundwork.config import GROUNDWORK
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import nightly as chapter17

pytestmark = pytest.mark.contract


def test_v1_is_chapter_17s_dag_with_its_tasks_added():
    dag, base = nightly.nightly(), chapter17()
    assert dag.name == base.name == "nightly"
    added = set(dag.tasks) - set(base.tasks)
    assert added == {"restore_old_crm", "observe", "sessions", "serve",
                     "compat", "govern"}
    for name, t in base.tasks.items():          # nothing else moved
        if name not in ("dbt_build", "publish"):
            assert dag.tasks[name].upstream == t.upstream


def test_each_new_task_waits_for_what_it_reads():
    dag = nightly.nightly()
    up = {n: set(t.upstream) for n, t in dag.tasks.items()}
    assert "restore_old_crm" in up["dbt_build"]
    assert up["sessions"] == {"dbt_build"}
    assert up["serve"] == {"sessions"}
    assert up["govern"] == {"observe", "serve", "compat"}
    assert dag.downstream(["dbt_build"]) >= {"publish", "govern", "serve"}
    assert dag.order()[-2:] == ["govern", "publish"]


def test_build_refuses_a_folder_that_holds_a_warehouse(tmp_path):
    (tmp_path / "warehouse.duckdb").write_bytes(b"")
    with pytest.raises(FileExistsError):
        nightly.build(tmp_path)


def test_the_real_folder_is_the_real_groundwork(tmp_path):
    assert nightly.folder_env(GROUNDWORK) == runner.Env()
    env = nightly.folder_env(tmp_path)
    assert env.warehouse == (tmp_path / "warehouse.duckdb").resolve()
    assert env.dbt_target == "dev"
