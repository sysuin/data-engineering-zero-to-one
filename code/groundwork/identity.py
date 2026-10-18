"""
Who is who: the companies the old CRM held twice, found by rule.

Before 1 March 2024 Meridian's CRM was another system. When Relay
replaced it, 212 companies had two records there: the one Relay kept,
and an older second record, frozen as it was created, which orders and
tickets had used. The second records vanished from the export the
night Relay went live. IT matched most of them to Relay's ids in
relay/migration_map.csv; this module finds every pair by rule, and says
which of its matches nobody else vouches for, so that a person can look
at those before the warehouse relies on them.

Entity resolution in four steps:

    records(con)          every account id the CRM ever exported, as it
                          was last exported
    candidates(records)   pairs that share a block: region, the month
                          the company joined, and postcode
    score(a, b)           how alike two names are once normalised, 0-1
    resolve(con)          pairs at or above THRESHOLD, set beside IT's
                          map; returns a Resolution

The rule's matches that IT's map lacks are proposed for review and,
once a person has confirmed them, kept as the seed
models/seeds/account_matches.csv, which the runner loads and
int_account_identity unions with IT's map. The old CRM is gone, so the
set never grows: this runs once, and again only if someone doubts it.

The rule needs the old CRM's last export (29 February 2024) in raw.
The landing zone keeps a fortnight, so restore() asks IT's archive for
that one night and loads it with Chapter 9's loader:

    python -m groundwork.identity --restore     # once
    python -m groundwork.identity               # propose and compare
"""
from __future__ import annotations

import argparse
import csv
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from difflib import SequenceMatcher
from itertools import combinations
from pathlib import Path

import duckdb

from groundwork.config import GROUNDWORK, NOW, WAREHOUSE

LEGACY_NIGHT = date(2024, 2, 29)
"""The old CRM's last export; the next night was Relay's first."""

THRESHOLD = 0.9
"""Names that score this or more, once normalised: the same company."""

REVIEW_FROM = 0.6
"""Pairs scoring from here up to THRESHOLD are listed for a person."""

SEED = Path(__file__).resolve().parent / "models" / "seeds" / \
    "account_matches.csv"
PROPOSAL = GROUNDWORK / "review" / "account_matches.csv"

# What the old CRM's second records did to a name: upper case, a doubled
# space, " (old)" or " Inc" on the end. Normalising undoes all four.
SUFFIXES = re.compile(r"\s*(\(old\)|\binc\.?)\s*$")


@dataclass(frozen=True)
class Record:
    account_id: int
    name: str
    region: str
    since: date
    postcode: str
    last_seen: date               # the last night an export held it


@dataclass(frozen=True)
class Pair:
    a: Record                     # the record that vanished, if one did
    b: Record
    score: float


@dataclass
class Resolution:
    records: int
    candidates: list[Pair]
    accepted: list[Pair]          # score >= THRESHOLD, one side gone
    review: list[Pair]            # in between, or alike but both live
    it_map: dict[int, int]        # old id -> Relay id, as IT matched
    matches: dict[int, int]       # old id -> Relay id, by the rule

    @property
    def agree(self) -> int:
        return sum(self.matches.get(o) == r
                   for o, r in self.it_map.items())

    @property
    def disagree(self) -> list[int]:
        return sorted(o for o, r in self.it_map.items()
                      if o in self.matches and self.matches[o] != r)

    @property
    def new(self) -> dict[int, int]:
        """The rule's matches that IT's map does not have."""
        return {o: r for o, r in self.matches.items()
                if o not in self.it_map}


def normalise(name: str) -> str:
    """A name as a comparison should see it: no case, spaces, suffix."""
    n = re.sub(r"\s+", " ", name.strip().lower())
    return SUFFIXES.sub("", n).strip()


def score(a: str, b: str) -> float:
    """How alike two names are once normalised: 1.0 is identical."""
    return round(SequenceMatcher(None, normalise(a),
                                 normalise(b)).ratio(), 3)


def block(r: Record) -> tuple[str, date, str]:
    """The blocking key: region, the month joined, postcode."""
    return r.region, r.since, r.postcode


def records(con) -> list[Record]:
    """Every account id in staged exports, as its last export had it."""
    rows = con.execute("""
        SELECT account_id, account_name, region_name,
               customer_since_date, postcode, snapshot_date
        FROM staging.stg_relay__accounts
        QUALIFY snapshot_date = max(snapshot_date)
                OVER (PARTITION BY account_id)
        ORDER BY account_id""").fetchall()
    return [Record(*r) for r in rows]


def candidates(recs: list[Record], key=block) -> list[Pair]:
    """Every pair of records that share a block, scored."""
    blocks: dict[tuple, list[Record]] = defaultdict(list)
    for r in recs:
        blocks[key(r)].append(r)
    latest = max(r.last_seen for r in recs)
    pairs = []
    for members in blocks.values():
        for a, b in combinations(members, 2):
            if b.last_seen < latest <= a.last_seen:
                a, b = b, a       # the vanished record first
            pairs.append(Pair(a, b, score(a.name, b.name)))
    return sorted(pairs, key=lambda p: (p.a.account_id, p.b.account_id))


def it_map(con) -> dict[int, int]:
    """IT's migration map: old CRM id -> Relay id."""
    return dict(con.execute(
        "SELECT old_crm_id, account_id FROM "
        "staging.stg_relay__migration_map ORDER BY old_crm_id"
    ).fetchall())


def resolve(con, threshold: float = THRESHOLD,
            review_from: float = REVIEW_FROM) -> Resolution:
    """Candidates, scores and matches, set beside IT's map."""
    recs = records(con)
    latest = max(r.last_seen for r in recs)
    pairs = candidates(recs)
    merges = [p for p in pairs
              if p.a.last_seen < latest <= p.b.last_seen]
    accepted = [p for p in merges if p.score >= threshold]
    review = [p for p in pairs if review_from <= p.score < threshold
              or (p.score >= threshold and p not in merges)]
    matches = {p.a.account_id: p.b.account_id for p in accepted}
    return Resolution(len(recs), pairs, accepted, review, it_map(con),
                      matches)


def propose(res: Resolution, path: Path = PROPOSAL) -> Path:
    """Write the rule's new matches as a seed would hold them."""
    path.parent.mkdir(parents=True, exist_ok=True)
    by_old = {p.a.account_id: p for p in res.accepted}
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["old_crm_id", "account_id", "score", "old_name",
                    "relay_name"])
        for old, new in sorted(res.new.items()):
            p = by_old[old]
            w.writerow([old, new, f"{p.score:.3f}", p.a.name, p.b.name])
    return path


def seeded(path: Path = SEED) -> dict[int, int]:
    """The matches a person has confirmed: old CRM id -> Relay id."""
    if not path.exists():
        return {}
    with open(path, newline="") as f:
        return {int(r["old_crm_id"]): int(r["account_id"])
                for r in csv.DictReader(f)}


def restore(night: date = LEGACY_NIGHT, *, warehouse=WAREHOUSE,
            root: Path | None = None, archive=None,
            run_at: datetime = NOW) -> int:
    """
    Load one old night from IT's archive into raw, unless it is there.

    The landing zone keeps a fortnight of exports; IT keeps every night.
    meridian.sources.relay stands in for asking IT. Returns the account
    rows loaded (0 if the night was already in force).
    """
    from groundwork.ingest import files, relay
    from meridian.sources import relay as archive_of_it

    con = duckdb.connect(str(warehouse))
    try:
        files.ensure(con)
        (have,) = con.execute(
            "SELECT count(*) FROM audit.current_files WHERE source = "
            "'relay' AND table_name = 'accounts' AND logical_key = ?",
            [night.isoformat()]).fetchone()
    finally:
        con.close()
    if have:
        return 0
    landing = Path(warehouse).parent / "restore"
    shutil.rmtree(landing, ignore_errors=True)
    archive_of_it.export(night, landing / "relay")
    kw = {} if archive is None else {"archive": archive}
    loads = relay.load(night, run_at=run_at.replace(tzinfo=None),
                       landing=landing, root=root, warehouse=warehouse,
                       **kw)
    shutil.rmtree(landing, ignore_errors=True)
    return sum(x.rows_written for x in loads
               if x.table_name == "accounts")


def report(res: Resolution, seed: dict[int, int]) -> list[str]:
    """The lines the CLI prints."""
    agreed = sum(seed.get(o) == r for o, r in res.new.items())
    lines = [
        f"records {res.records:,}; candidate pairs "
        f"{len(res.candidates):,}; matched {len(res.matches):,}",
        f"IT's map {len(res.it_map):,}: the rule agrees on "
        f"{res.agree:,}, disagrees on {len(res.disagree):,}",
        f"new matches, not in IT's map: {len(res.new):,}; "
        f"in the seed: {agreed:,} of {len(seed):,}",
        f"pairs for a person to review: "
        f"{len(res.review):,}",
    ]
    for p in res.review:
        lines.append(f"  {p.a.account_id} {p.a.name!r} ~ "
                     f"{p.b.account_id} {p.b.name!r} {p.score:.3f}")
    return lines


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--restore", action="store_true",
                   help="load the old CRM's last export from IT's "
                        "archive")
    a = p.parse_args(argv)
    if a.restore:
        n = restore()
        print(f"{LEGACY_NIGHT}: {n:,} account rows restored"
              if n else f"{LEGACY_NIGHT}: already in raw")
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        res = resolve(con)
    finally:
        con.close()
    seed = seeded()
    for line in report(res, seed):
        print(line)
    print(f"proposal written to {propose(res).relative_to(GROUNDWORK)}")
    differs = res.new != seed or res.disagree
    return 1 if differs else 0


if __name__ == "__main__":
    raise SystemExit(main())
