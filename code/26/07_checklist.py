# Groundwork's launch checklist: every line a script can test, run
# against the build from nothing, and the result written to
# docs/groundwork-launch-checklist.md.
import os
import re
from datetime import datetime, time, timedelta

from _sandbox import FRESH, connect

from groundwork import quality
from groundwork import reconcile_books as rb
from groundwork.config import ROOT
from groundwork.contracts import load, names
from groundwork.govern import roles
from groundwork.observe.metrics import stream_lag
from groundwork.stream.portal import GROUP, log_path

con = connect()
one = lambda sql: con.execute(sql).fetchone()            # noqa: E731
(run_id, day, published), = con.execute(
    "SELECT run_id, logical_date, published_at FROM audit.published"
).fetchall()


def built():
    tasks, first = one(f"""SELECT count(DISTINCT task),
        count(DISTINCT task) FILTER (try_number = 1 AND state =
        'succeeded') FROM audit.task_runs WHERE run_id = '{run_id}'""")
    return tasks == first, f"{tasks} tasks, all first try"


def contracts():
    broken, parked = one("""SELECT count(*) FILTER (status = 'broken'),
        (SELECT count(*) FROM audit.quarantine)
        FROM audit.contract_runs""")
    return broken == 0, f"{broken} broken, {parked} row parked"


def ledger():
    bad, n = one("SELECT count(*) FILTER (status = 'differs'), "
                 "count(*) FROM audit.reconciliations")
    return bad == 0, f"{n} lines, {bad} differ"


def books():
    lines = [*rb.book2(con), rb.history(con)[0], *rb.book1(con)]
    bad = sum(x.differing for x in lines) + sum(
        len(q.differing) for q in rb.quarters(con))
    return bad == 0, f"{len(lines)} tables, 12 quarters, {bad} differ"


def checks():
    bad, n = one("SELECT count(*) FILTER (status = 'error'), count(*) "
                 "FROM audit.quality_checks")
    return bad == 0, f"{n} checks, {bad} errors"


def slo():
    due = datetime.combine(day + timedelta(days=1), time(7))
    return published <= due, f"published {published:%H:%M}, due 07:00"


def stream():
    lag = stream_lag(log_path(FRESH / "warehouse.duckdb"), GROUP)
    return lag["records"] == 0, f"lag {lag['records']} records"


def keys():
    (n,) = one("SELECT count(*) FROM marts.fct_portal_sessions "
               "WHERE account_key IS NULL")
    return n == 0, f"{n} sessions without a key"


def access():
    problems = roles.check(con)
    return not problems, f"{len(roles.ROLES)} roles, {len(problems)} " \
                         "problems"


def reviewed():
    (n,) = one("SELECT count(*) FROM govern.classification "
               "WHERE reason LIKE 'no rule%'")
    return n == 0, f"{n} columns await their owner"


def key():
    example = re.search(r"^GROUNDWORK_PSEUDONYM_KEY=(.*)$",
                        (ROOT / ".env.example").read_text(), re.M)
    value = os.environ.get("GROUNDWORK_PSEUDONYM_KEY", "")
    ok = bool(value) and value != example.group(1)
    return ok, "set, not the example's" if ok else "the example's value"


def owners():
    missing = [t for t in names()
               if not load(t).producer.get("contact")]
    rules = [r for r in quality.RULES if not r.owner]
    ok = not missing and not rules
    return ok, f"{len(names())} contracts, {len(quality.RULES)} rules"


def delivered():
    sends = [p for p in (ROOT / "code" / "groundwork").rglob("*.py")
             if re.search(r"smtplib|\.post\(|webhook", p.read_text())]
    return bool(sends), ("audit.notifications only" if not sends
                         else f"{len(sends)} senders")


def cron():
    runbook = (ROOT / "docs" / "groundwork-runbook.md").read_text()
    ok = "groundwork.nightly run" in runbook
    return ok, "names v1.0's night" if ok else "names v0.8's night"


def backups():
    jobs = [p for p in (ROOT / "code" / "groundwork").rglob("*.py")
            if "def backup" in p.read_text()]
    return bool(jobs), "a backup job" if jobs else "no job copies them"


def ci():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    ok = "make diff" in ci
    return ok, "diff on every pull request" if ok else "no diff"


BY_HAND = None
LINES = [
    ("Run", "One command builds Groundwork from nothing", built),
    ("Run", "Every batch held to its contract", contracts),
    ("Run", "The cron line runs v1.0's night", cron),
    ("Run", "Published by 07:00 UTC (the freshness SLO)", slo),
    ("Run", "A failed night reaches a person by 08:00", delivered),
    ("Run", "Raw, landed and the snapshot backed up", backups),
    ("Prove", "The ledger ties, every region and month", ledger),
    ("Prove", "Books 1 and 2 reconcile", books),
    ("Prove", "Chapter 15's checks: no errors", checks),
    ("Prove", "A change is diffed before it merges", ci),
    ("Prove", "make test passes", BY_HAND),
    ("Serve", "The stream has read everything that arrived", stream),
    ("Serve", "Every session points at an account version", keys),
    ("Govern", "No role reads what it should not", access),
    ("Govern", "Every column classified by a rule", reviewed),
    ("Govern", "The pseudonym key is a real secret", key),
    ("Govern", "Retention's owners agree; --retain is on", BY_HAND),
    ("Own", "Every source and check has an owner", owners),
    ("Own", "Each owner has read their contract", BY_HAND),
    ("Own", "Someone else builds it from a clean clone", BY_HAND),
]

results = []
for area, line, test in LINES:
    ok, evidence = test() if test else (None, "a person ticks it")
    results.append((area, line, {True: "yes", False: "no",
                                 None: "by hand"}[ok], evidence))
con.close()

for area, line, verdict, evidence in results:
    print(f"{area:<7}{line:<44}{verdict}")
counts = {v: sum(r[2] == v for r in results) for v in
          ("yes", "no", "by hand")}
print(f"{len(results)} lines: " + ", ".join(f"{n} {v}" for v, n in
                                            counts.items()))
print("\nthe noes:")
for area, line, verdict, evidence in results:
    if verdict == "no":
        print(f"  {line}: {evidence}")

doc = ["# Groundwork launch checklist", "",
       f"Checked by `code/26/07_checklist.py` against the build from "
       f"nothing ({run_id}).",
       "A line marked *by hand* is one no script can tick. Rerun the",
       "listing after changing anything below.", "",
       "| Area | Line | Result | Evidence |", "|---|---|---|---|"]
doc += [f"| {a} | {line} | {v} | {e} |" for a, line, v, e in results]
doc += ["", f"{len(results)} lines: " + ", ".join(
    f"{n} {v}" for v, n in counts.items()) + ".", ""]
(ROOT / "docs" / "groundwork-launch-checklist.md").write_text(
    "\n".join(doc))
