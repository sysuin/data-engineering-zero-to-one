"""
Shared fixtures for the test suite, and the map of its layers.

Every test belongs to exactly one layer, and CI runs each layer when
its answer is worth its time (Chapter 23):

    unit         one function or one model, on rows the test writes,
                 with an exact answer. Seconds for the whole layer.
    contract     a promise kept at a boundary: what a source sends,
                 what a layer hands the next, what a reader is told.
    integration  stages run together as the pipeline runs them: ingest
                 then dbt on a tiny Meridian, the nightly DAG, a whole
                 module over the real data.
    diff         two builds, or two ways of building, that must agree.

A test is also `slow` when it needs the generated sources or the built
warehouse; that is about its data, not its layer. `make test` runs the
unit and contract layers without slow tests, on every push.

A new test file names its layer with `pytestmark = pytest.mark.<layer>`
(or marks each test). The files written before Chapter 23 are mapped
in LAYERS below, file by file, with the few tests that belong to
another layer named in MOVED. A test with no layer, or with two, stops
the collection: the suite cannot be run by layer if any test is
unsorted.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code"))

from groundwork import config  # noqa: E402

LAYER_HELP = {
    "unit": "one function or model, rows the test writes, an exact answer",
    "contract": "a promise kept at a boundary between two parts",
    "integration": "stages run together, as the pipeline runs them",
    "diff": "two builds, or two ways of building, that must agree",
}

# The suite as Chapters 3 to 22 left it: each file's layer.
LAYERS = {
    "test_backfill.py": "integration",
    "test_contracts.py": "contract",
    "test_dates.py": "unit",
    "test_dbt_project.py": "diff",
    "test_dimensions.py": "unit",
    "test_drill.py": "diff",
    "test_facts.py": "unit",
    "test_files.py": "unit",
    "test_frontline.py": "unit",
    "test_govern.py": "unit",
    "test_identity.py": "unit",
    "test_ingest_file_sources.py": "unit",
    "test_ingest_main.py": "integration",
    "test_ingest_relay.py": "unit",
    "test_ingest_suppliers.py": "unit",
    "test_loadlog.py": "unit",
    "test_observe.py": "unit",
    "test_orchestrate.py": "unit",
    "test_profile.py": "unit",
    "test_quality.py": "unit",
    "test_reconcile.py": "unit",
    "test_runner.py": "unit",
    "test_serve.py": "unit",
    "test_staging.py": "unit",
    "test_storage.py": "unit",
    "test_stream_consumer.py": "unit",
    "test_stream_log.py": "unit",
    "test_stream_sessions.py": "unit",
    "test_tradeline.py": "unit",
    "test_tradeline_full.py": "unit",
}

# Tests whose layer is not their file's.
MOVED = {
    # promises at a boundary
    "test_dates.py::test_money_in_the_marts_is_decimal": "contract",
    "test_dbt_project.py::test_marts_say_how_they_are_materialised_once":
        "contract",
    "test_dbt_project.py::test_keys_use_the_macro_in_every_dimension":
        "contract",
    "test_dbt_project.py::"
    "test_profiles_live_in_the_project_and_build_into_the_sandbox":
        "contract",
    "test_facts.py::test_check_holds_the_order_facts_to_marts_sql":
        "contract",
    "test_ingest_suppliers.py::test_every_list_on_disk_reads": "contract",
    "test_loadlog.py::test_the_table_has_the_agreed_columns": "contract",
    "test_orchestrate.py::test_the_nightly_dag": "contract",
    "test_reconcile.py::"
    "test_the_register_in_the_project_holds_finances_two_journals":
        "contract",
    "test_runner.py::test_the_real_models_compile_and_order": "contract",
    "test_serve.py::test_the_api_answers_refuses_and_caches": "contract",
    "test_staging.py::test_seed_covers_every_spelling_in_raw": "contract",
    # stages together, or a module over the real data
    "test_dimensions.py::test_the_real_dimensions_keep_their_promises":
        "integration",
    "test_facts.py::test_the_real_facts_keep_their_promises":
        "integration",
    "test_frontline.py::"
    "test_cursor_pull_misses_nothing_while_the_desk_moves": "integration",
    "test_govern.py::test_forget_finds_erases_and_proves": "integration",
    "test_identity.py::test_the_real_resolution": "integration",
    "test_ingest_relay.py::test_the_real_migration_week": "integration",
    "test_profile.py::test_tradeline_channels": "integration",
    "test_reconcile.py::test_the_real_warehouse_ties": "integration",
    "test_serve.py::test_the_command_publishes_and_checks": "integration",
    "test_stream_sessions.py::test_the_bots_morning_from_the_real_files":
        "integration",
    "test_tradeline.py::test_todays_tradeline_snapshot_reconciles":
        "integration",
    "test_tradeline_full.py::test_todays_tradeline_reconciles":
        "integration",
    # two runs that must agree
    "test_stream_sessions.py::"
    "test_a_run_that_stops_and_resumes_matches_one_that_does_not": "diff",
}


def pytest_configure(config):  # noqa: F811 - pytest names this argument
    config.addinivalue_line("markers", "slow: needs the generated sources "
                            "or the warehouse")
    for name, text in LAYER_HELP.items():
        config.addinivalue_line("markers", f"{name}: {text}")


def layer_of(item) -> list[str]:
    """The layers a collected test is in: its own marks, else the
    map."""
    marked = [m.name for m in item.iter_markers()
              if m.name in LAYER_HELP]
    if marked:
        return marked
    file = Path(str(item.fspath)).name
    name = item.originalname or item.name
    found = MOVED.get(f"{file}::{name}") or LAYERS.get(file)
    return [found] if found else []


def pytest_collection_modifyitems(config, items):
    unsorted = []
    for item in items:
        layers = layer_of(item)
        if len(set(layers)) != 1:
            unsorted.append(f"{item.nodeid}: {layers or 'no layer'}")
            continue
        if not any(m.name == layers[0] for m in item.iter_markers()):
            item.add_marker(layers[0])
    if unsorted:
        raise pytest.UsageError(
            "every test needs exactly one layer (unit, contract, "
            "integration, diff); see tests/conftest.py:\n  "
            + "\n  ".join(unsorted))


@pytest.fixture(scope="session")
def sources() -> Path:
    """Meridian's generated sources, or a skip that says how to make them."""
    if not (config.SOURCES / "manifest.json").exists():
        pytest.skip("the sources are not generated: run `make data`")
    return config.SOURCES


@pytest.fixture(scope="session")
def warehouse() -> Path:
    """Groundwork's built warehouse, or a skip that says how to build it."""
    if not config.WAREHOUSE.exists():
        pytest.skip("the warehouse is not built yet")
    return config.WAREHOUSE


@pytest.fixture(scope="session")
def tiny(tmp_path_factory):
    """A tiny Meridian, ingested and built by dbt once per session: the
    tiny warehouse that integration and diff tests read. Three accounts,
    six orders, three contracts; groundwork.testing says how."""
    from groundwork.testing import example
    s = example(tmp_path_factory.mktemp("tiny"))
    s.build()
    return s
