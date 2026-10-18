"""Entity resolution: normalising, blocking, scoring and resolving."""
from __future__ import annotations

import csv
from datetime import date

import duckdb
import pytest

from groundwork import identity
from groundwork.identity import Record

OLD, NEW = date(2024, 2, 29), date(2026, 1, 4)


def rec(i, name, *, region="Northeast", since=date(2015, 6, 1),
        postcode="01880", seen=NEW):
    return Record(i, name, region, since, postcode, seen)


def test_normalise_undoes_what_the_old_crm_did():
    for name in ("REDCLIFFE MOTORS", "Redcliffe  Motors",
                 "Redcliffe Motors (old)", "Redcliffe Motors Inc",
                 " redcliffe motors inc. "):
        assert identity.normalise(name) == "redcliffe motors"


def test_normalise_keeps_inc_inside_a_name():
    assert identity.normalise("Incline Fitness") == "incline fitness"


def test_score_is_one_for_variants_and_low_for_strangers():
    assert identity.score("GREYHAM BUILDERS", "Greyham Builders") == 1.0
    assert identity.score("Carlworth Clinic", "Langstead Dental") < 0.6


def test_candidates_pair_only_within_a_block_and_put_the_old_first():
    recs = [rec(5161, "Redcliffe Motors"),
            rec(90004, "REDCLIFFE MOTORS", seen=OLD),
            rec(7, "Kingswick Hotels", postcode="01357"),
            rec(8, "Draystead Engineering", postcode="01357"),
            rec(9, "Elsewhere Ltd", region="West")]
    pairs = identity.candidates(recs)
    assert [(p.a.account_id, p.b.account_id) for p in pairs] == [
        (7, 8), (90004, 5161)]
    assert pairs[1].score == 1.0 and pairs[0].score < 0.6


def test_a_postcode_without_its_zero_misses_its_block():
    recs = [rec(5161, "Redcliffe Motors"),
            rec(90004, "Redcliffe Motors", postcode="1880", seen=OLD)]
    assert identity.candidates(recs) == []


def staged(tmp_path, accounts, it_map):
    """A warehouse holding only the two staging tables resolve reads."""
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    con.execute("CREATE SCHEMA staging")
    con.execute("CREATE TABLE staging.stg_relay__accounts (account_id "
                "INTEGER, account_name VARCHAR, region_name VARCHAR, "
                "customer_since_date DATE, postcode VARCHAR, "
                "snapshot_date DATE)")
    con.executemany("INSERT INTO staging.stg_relay__accounts "
                    "VALUES (?, ?, ?, ?, ?, ?)", accounts)
    con.execute("CREATE TABLE staging.stg_relay__migration_map "
                "(old_crm_id INTEGER, account_id INTEGER)")
    if it_map:
        con.executemany("INSERT INTO staging.stg_relay__migration_map "
                        "VALUES (?, ?)", it_map)
    return con


def test_resolve_matches_the_vanished_and_never_merges_two_live(tmp_path):
    since = date(2015, 6, 1)
    rows = []
    for night in (OLD, NEW):            # both live accounts, both nights
        rows += [(5161, "Redcliffe Motors", "Midwest", since, "46181",
                  night),
                 (7, "Kingswick Hotels", "Midwest", since, "01357", night),
                 (8, "Kingswick Hotels", "Midwest", since, "01357", night)]
    rows += [(90004, "REDCLIFFE MOTORS", "Midwest", since, "46181", OLD),
             (90005, "Lost Name Ltd", "Midwest", since, "99999", OLD)]
    con = staged(tmp_path, rows, [(90005, 8)])
    res = identity.resolve(con)
    assert res.records == 5
    assert res.matches == {90004: 5161}
    assert res.new == {90004: 5161}
    assert res.agree == 0 and res.disagree == []
    # two live accounts alike enough to be one are listed, not merged
    assert [(p.a.account_id, p.b.account_id) for p in res.review] == [
        (7, 8)]


def test_disagreement_with_it_map_is_reported(tmp_path):
    since = date(2015, 6, 1)
    rows = [(5161, "Redcliffe Motors", "Midwest", since, "46181", NEW),
            (90004, "Redcliffe Motors", "Midwest", since, "46181", OLD)]
    con = staged(tmp_path, rows, [(90004, 1)])
    res = identity.resolve(con)
    assert res.disagree == [90004]


def test_proposal_is_what_the_seed_reads(tmp_path):
    since = date(2015, 6, 1)
    rows = [(5161, "Redcliffe Motors", "Midwest", since, "46181", NEW),
            (90004, "REDCLIFFE MOTORS", "Midwest", since, "46181", OLD)]
    res = identity.resolve(staged(tmp_path, rows, []))
    path = identity.propose(res, tmp_path / "proposal.csv")
    assert identity.seeded(path) == {90004: 5161}
    with open(path) as f:
        assert next(csv.DictReader(f))["old_name"] == "REDCLIFFE MOTORS"


def test_the_seed_holds_only_what_it_map_lacks():
    seed = identity.seeded()
    assert len(seed) == 25
    assert all(90001 <= o <= 90212 for o in seed)


@pytest.mark.slow
def test_the_real_resolution(warehouse):
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        res = identity.resolve(con)
    finally:
        con.close()
    if res.records < 6270:
        pytest.skip("the old CRM's last night is not restored yet")
    assert len(res.candidates) == 221
    assert len(res.matches) == 212
    assert res.agree == 187 and not res.disagree
    assert res.new == identity.seeded()
