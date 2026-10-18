"""
How long Groundwork keeps what, and the job that lets go of the rest.

Each policy names what it covers, how many days it is kept, who owns
the decision and why. An exemption is a policy's too: a file kept past
its time is kept for a reason someone wrote down.

    POLICIES                      the rules
    due(policy, root, landed, con, today)
                                  what a policy would remove today
    run(con, root, landed, today, dry_run=False)
                                  remove it, and record each action in
                                  audit.retention_runs
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq


@dataclass(frozen=True)
class Policy:
    name: str
    covers: str
    keep_days: int
    owner: str
    why: str
    exempt: tuple[tuple[str, str], ...] = ()     # (name, reason)


POLICIES = (
    Policy("portal_events", "raw.portal_events, by day received", 90,
           "web team",
           "who clicked what, by login; sessions keep what analysis "
           "needs"),
    Policy("frontline_pages", "raw Frontline pages of an attempt a "
           "later attempt replaced", 0, "data engineering",
           "a second copy of every requester's name and email"),
    Policy("relay_exports", "archived Relay exports, by arrival", 30,
           "data engineering",
           "each night's contacts; raw keeps the parsed rows",
           exempt=(("relay/2024-02-29/accounts.csv",
                    "the old CRM's 212 ids (Chapter 12)"),
                   ("relay/2024-02-29/contracts.csv",
                    "the old CRM's contracts (Chapter 12)"))),
    Policy("query_log", "audit.query_log", 400, "security",
           "who read what, kept for a year and a month"),
)


@dataclass(frozen=True)
class Action:
    policy: str
    target: str
    action: str          # deleted | exempt | kept
    rows: int
    bytes: int
    note: str = ""


def _size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _portal(policy, root, landed, con, today):
    folder = Path(root) / "portal" / "events"
    cut = today - timedelta(days=policy.keep_days)
    out = []
    for part in sorted(folder.glob("load_date=*")):
        day = date.fromisoformat(part.name.split("=")[1])
        if day < cut:
            rows = pq.ParquetFile(part / "part-0.parquet").metadata.num_rows
            out.append(Action(policy.name, f"portal/events/{part.name}",
                              "deleted", rows, _size(part)))
    return out


def _pages(policy, root, landed, con, today):
    used = {lid.rsplit(".", 1)[1] for (lid,) in con.execute(
        "SELECT DISTINCT _load_id FROM raw.frontline_tickets").fetchall()}
    out = []
    pages_root = Path(root) / "frontline" / "pages"
    for day in sorted(pages_root.glob("load_date=*")):
        for attempt in sorted(day.glob("attempt-*")):
            if attempt.name.split("-")[1] in used:
                continue
            pages = len(list(attempt.glob("*.json")))
            out.append(Action(policy.name,
                              f"frontline/pages/{day.name}/{attempt.name}",
                              "deleted", pages, _size(attempt),
                              "pages, not rows"))
    return out


def _relay(policy, root, landed, con, today):
    cut = datetime.combine(today - timedelta(days=policy.keep_days),
                           datetime.min.time())
    exempt = dict(policy.exempt)
    files: dict[str, list] = {}
    for name, sha, arrived in con.execute(
            "SELECT name, sha256, arrived_at FROM audit.file_ledger"
            " WHERE source = 'relay' AND table_name IN"
            " ('accounts', 'contracts') ORDER BY name").fetchall():
        files.setdefault(sha, []).append((name, arrived))
    out = []
    for sha, nights in sorted(files.items(), key=lambda kv: kv[1]):
        path = Path(landed) / "relay" / sha[:2] / f"{sha}.gz"
        if not path.exists():
            continue
        names = [n for n, _ in nights]
        held = [n for n in names if n in exempt]
        if held:
            out.append(Action(policy.name, f"landed/relay/{sha[:12]}",
                              "exempt", len(names), _size(path),
                              exempt[held[0]]))
        elif all(a < cut for _, a in nights):
            out.append(Action(policy.name, f"landed/relay/{sha[:12]}",
                              "deleted", len(names), _size(path)))
        else:
            out.append(Action(policy.name, f"landed/relay/{sha[:12]}",
                              "kept", len(names), _size(path)))
    return out


def _query_log(policy, root, landed, con, today):
    (exists,) = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE"
        " table_schema = 'audit' AND table_name = 'query_log'").fetchone()
    if not exists:
        return []
    cut = today - timedelta(days=policy.keep_days)
    (n,) = con.execute("SELECT count(*) FROM audit.query_log"
                       " WHERE CAST(read_at AS DATE) < ?", [cut]).fetchone()
    return [Action(policy.name, "audit.query_log", "deleted", n, 0)] \
        if n else []


FINDERS = {"portal_events": _portal, "frontline_pages": _pages,
           "relay_exports": _relay, "query_log": _query_log}


def due(policy: Policy, root, landed, con, today: date) -> list[Action]:
    return FINDERS[policy.name](policy, root, landed, con, today)


def _remove(action: Action, root, landed, con, today, policy) -> None:
    if action.policy == "query_log":
        cut = today - timedelta(days=policy.keep_days)
        con.execute("DELETE FROM audit.query_log WHERE CAST(read_at AS DATE)"
                    " < ?", [cut])
        return
    if action.target.startswith("landed/relay/"):
        short = action.target.rsplit("/", 1)[1]
        for path in (Path(landed) / "relay").glob(f"*/{short}*.gz"):
            path.unlink()
        return
    shutil.rmtree(Path(root) / action.target)


def run(con, root, landed, today: date, dry_run: bool = False,
        policies=POLICIES) -> list[Action]:
    """Apply every policy as of `today`; what was done (or would be)."""
    done = []
    for policy in policies:
        for action in due(policy, root, landed, con, today):
            if action.action == "deleted" and not dry_run:
                _remove(action, root, landed, con, today, policy)
            done.append(action)
    if not dry_run:
        con.execute("""
            CREATE TABLE IF NOT EXISTS audit.retention_runs (
                run_date DATE, policy VARCHAR, target VARCHAR,
                action VARCHAR, rows BIGINT, bytes BIGINT, note VARCHAR)""")
        con.executemany(
            "INSERT INTO audit.retention_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(today, a.policy, a.target, a.action, a.rows, a.bytes, a.note)
             for a in done])
    return done
