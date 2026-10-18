"""Contract tests: the promises Groundwork keeps at its boundaries,
checked without building anything. Every dbt model is bound, as a
view, over empty raw tables made from the contracts, so a model that
reads a column no contract promises fails here, in a second; the marts
it produces must be the columns and types marts.sql promises their
readers; and a contract that changes in a way that can break someone
must say so with a new version."""
from __future__ import annotations

import subprocess

import duckdb
import pytest
import yaml

from groundwork import contracts, ddl
from groundwork.config import ROOT
from groundwork.testing import SOURCE, bind_project, contract_for

pytestmark = pytest.mark.contract

# Chapter 13's comments loader landed raw.frontline_ticket_comments
# without a contract. Until it has one, the columns its staging model
# reads are written here, and only here.
NO_CONTRACT = {"frontline_ticket_comments": {
    "comment_id": "BIGINT", "ticket_id": "VARCHAR",
    "created_at": "VARCHAR", "author": "VARCHAR", "public": "BOOLEAN",
    "body": "VARCHAR", "_load_id": "VARCHAR", "load_date": "DATE"}}


@pytest.fixture(scope="module")
def bound():
    con = duckdb.connect()
    got = bind_project(con, uncontracted=NO_CONTRACT)
    yield con, got
    con.close()


def test_every_model_binds_to_what_the_contracts_promise(bound):
    _, got = bound
    assert {m: e for m, e in got.items() if e} == {}
    assert len(got) == 32              # 17 staging, 5 intermediate, 10 marts


def test_only_the_comments_are_read_without_a_contract():
    read = {t for p in (ROOT / "dbt" / "groundwork" / "models").rglob(
        "*.sql") for s, t in SOURCE.findall(p.read_text()) if s == "raw"}
    assert {t for t in read if contract_for(t) is None} == set(NO_CONTRACT)


def test_the_marts_are_the_columns_and_types_their_readers_were_promised(
        bound):
    con, _ = bound
    promised = duckdb.connect()
    ddl.create(promised, "marts")
    q = ("SELECT table_name, column_name, data_type FROM "
         "information_schema.columns WHERE table_schema = 'marts' "
         "ORDER BY table_name, ordinal_position")
    want, got = {}, {}
    for t, c, ty in promised.execute(q).fetchall():
        want.setdefault(t, []).append((c, ty))
    for t, c, ty in con.execute(q).fetchall():
        got.setdefault(t, []).append((c, ty))
    assert len(got) == 10
    for t, cols in got.items():           # fct_portal_sessions: Ch. 19
        assert cols == want[t], t


def _at(ref: str, path) -> str | None:
    out = subprocess.run(["git", "show",
                          f"{ref}:{path.relative_to(ROOT).as_posix()}"],
                         cwd=ROOT, capture_output=True, text=True)
    return out.stdout if out.returncode == 0 else None


def test_a_contract_change_that_can_break_someone_has_a_new_version():
    if _at("main", ROOT / "Makefile") is None:
        pytest.skip("no git history with a main branch to compare with")
    for name in contracts.names():
        path = contracts.path(name)
        old_text = _at("main", path)
        if old_text is None or old_text == path.read_text():
            continue                          # new, or unchanged
        old = contracts.parse(yaml.safe_load(old_text))
        new = contracts.load(name)
        breaks = [what for who, what in contracts.compatible(old, new)
                  if who != "nobody"]
        if breaks:
            assert new.version > old.version, (name, breaks)
            assert any(int(c["version"]) == new.version
                       for c in new.changes), (name, "no note")
