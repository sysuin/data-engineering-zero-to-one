"""
Alerts people read: what fires, who hears, how, and when.

The watch runs on its own, after the night, and reads only the audit
tables. It must not be a task in the DAG it watches: a night that
fails before its last task never reaches a monitor placed at the end
of it, and that is the night that most needs one.

    rules       what fires, each evaluated at a moment `at`:
                night_failed   the due night's run failed, or none ran
                held_rows      a contract kept rows out of raw: a batch
                               that broke (page) or rows parked (ticket)
                signal         one of the four signals in alarm
                checks         Chapter 15's checks: an error or a warning,
                               or a reconciliation that does not tie
                stuck_load     a load still 'running' hours after it began
                budget_spent   the freshness SLO's error budget is gone
    severity    page: a person, now, in working hours
                ticket: a person, today, in the owner's queue
    routing     the owner hears; the source's own team is copied when the
                fault may be theirs (Chapter 15's contacts)
    dedup       one open alert per key: the same broken file on fourteen
                nights is one alert seen fourteen times, not fourteen pages
    quiet hours a page outside 08:00-20:00 New York waits for 08:00,
                unless its rule says waiting costs more than waking
    SLO         the share of days published by 07:00 UTC the next
                morning, over a window, and the error budget it leaves

    watch(con, at, root)    evaluate, dedup, route; audit.alerts and
                            audit.notifications; returns what was sent
    budget(con, at, slo)    the freshness SLO and what is left of it
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from groundwork import quality
from groundwork.observe import metrics

OFFICE = ZoneInfo("America/New_York")
HOURS = (time(8), time(20))           # when a page may ring
STUCK = timedelta(hours=2)            # a load running this long died


@dataclass(frozen=True)
class Alert:
    key: str                # what makes two firings the same alert
    rule: str
    severity: str           # page or ticket
    owner: str              # who hears, and acts
    subject: str            # what it is about, in a few words
    text: str               # what a person reads first
    copy: tuple = ()        # who else hears: a source's own team
    wake: bool = False      # may ring outside working hours


# ------------------------------------------------------------ the rules
def night_failed(con, at: datetime, dag: str = "nightly") -> list[Alert]:
    """The run for the due day failed, or never ran. One key for the
    DAG, so a run that fails for a fortnight is one alert."""
    due = quality.expected(quality.RULES[0], at)
    try:
        row = con.execute("""
            SELECT run_id, state FROM audit.dag_runs
            WHERE dag = ? AND logical_date = ? AND run_at <= ?
            ORDER BY run_at DESC, run_id DESC LIMIT 1""",
                          [dag, due, at]).fetchone()
    except duckdb.CatalogException:
        row = None
    if row is not None and row[1] in ("succeeded", "running"):
        return []
    if row is None:
        return [Alert(f"night:{dag}", "night_failed", "page",
                      "data engineering", f"{dag} {due}",
                      f"no {dag} run for {due} by {at:%H:%M} UTC")]
    run_id = row[0]
    bad = con.execute("""
        SELECT task, detail FROM audit.task_runs
        WHERE run_id = ? AND state = 'failed'
        ORDER BY started_at, task""", [run_id]).fetchall()
    (stopped,) = con.execute(
        "SELECT count(*) FROM audit.task_runs WHERE run_id = ? "
        "AND state = 'upstream_failed'", [run_id]).fetchone()
    what = "; ".join(f"{t}: {d}" for t, d in bad) or "failed"
    return [Alert(f"night:{dag}", "night_failed", "page",
                  "data engineering", f"{run_id} failed",
                  f"{run_id} failed: {what}. {stopped} task"
                  f"{'s' * (stopped != 1)} never ran; nothing new "
                  "was published.")]


def _contact(source: str, table: str) -> str:
    for r in quality.RULES:
        if (r.source, r.table) == (source, table):
            return r.contact
    return ""


def held_rows(con, at: datetime, parked_page: float = 0.5
              ) -> list[Alert]:
    """
    The quarantine's volume for the due day. A batch that broke its
    contract held every row it had: a page, keyed on the bytes, so the
    same file failing night after night stays one alert. Parked rows
    are a ticket for the table's owner, unless they are more than
    `parked_page` of what arrived, which is a file that changed.
    """
    due = quality.expected(quality.RULES[0], at)
    out = []
    for h in metrics.held(con, due, at):
        contact = _contact(h.source, h.table)
        for name, ref in zip(h.files, h.refs):
            out.append(Alert(
                f"held:{h.name}:{ref[:16]}", "held_rows", "page",
                "data engineering", name,
                f"{h.name}: {name} broke its contract and was held, "
                f"{h.broken:,} row{'s' * (h.broken != 1)} in "
                f"{len(h.files)} file{'s' * (len(h.files) != 1)}. "
                "It will be tried again every night until the "
                "contract or the file changes.",
                (contact,) if contact else ()))
        if h.parked:
            share = h.parked / h.rows
            out.append(Alert(
                f"parked:{h.name}:{due}", "held_rows",
                "page" if share > parked_page else "ticket",
                "data engineering", f"{h.name} {h.parked} parked",
                f"{h.name}: {h.parked:,} of {h.rows:,} rows parked in "
                f"audit.quarantine for {due}, {share:.2%}.",
                (contact,) if contact else ()))
    return out


def signal(con, at: datetime, root: Path | None = None) -> list[Alert]:
    """Any of the four signals in alarm for the due day, one alert per
    signal and source: Relay's accounts and contracts late on the same
    night are one thing to look at, not two. An error is a page; a
    warning, a ticket. A schema change that broke a contract is
    held_rows' business; a new format that passed is a ticket here."""
    due = quality.expected(quality.RULES[0], at)
    groups: dict = {}
    for s in metrics.collect(con, due, at, root):
        if not s.alarm or (s.signal == "schema" and s.severity == "error"):
            continue
        if s.signal == "freshness" and s.subject == "published":
            continue                      # night_failed says it
        source = s.subject.partition(".")[0]
        groups.setdefault((s.signal, source), []).append(s)
    out = []
    for (sig, source), found in groups.items():
        lines = []
        for s in found:
            value = "none" if s.value is None else f"{s.value:,.4g}"
            band = ("" if s.high is None else f" (allowed {s.low:,.4g}"
                    f" to {s.high:,.4g})" if s.low is not None else
                    f" (limit {s.high:g})")
            lines.append(f"{s.subject} {s.measure}: {value}{band}")
        table = found[0].subject.partition(".")[2].removesuffix("_log")
        contact = _contact(source, table)
        error = any(s.severity == "error" for s in found)
        out.append(Alert(
            f"signal:{sig}:{source}", "signal",
            "page" if error else "ticket", "data engineering",
            f"{sig} {source}",
            f"{sig} for {due}: " + "; ".join(lines) + ".",
            (contact,) if contact else ()))
    return out


def checks(con, at: datetime) -> list[Alert]:
    """Chapter 15's verdicts, recorded at the moment the due day was
    judged: errors page their owner, warnings are tickets."""
    due = quality.expected(quality.RULES[0], at)
    judged = metrics.judged_at(due)
    out = []
    try:
        rows = con.execute("""
            SELECT kind, source, table_name, status, owner, detail
            FROM audit.quality_checks
            WHERE run_at = ? AND status IN ('warn', 'error')
            ORDER BY kind, source, table_name""", [judged]).fetchall()
    except duckdb.CatalogException:
        rows = []
    for kind, source, table, status, owner, detail in rows:
        out.append(Alert(f"check:{kind}:{source}.{table}", "checks",
                         "page" if status == "error" else "ticket",
                         owner, f"{kind} {source}.{table}",
                         f"{kind} {source}.{table} {status}: {detail}"))
    try:
        (n,) = con.execute("""
            SELECT count(*) FROM audit.reconciliations
            WHERE run_at = ? AND status = 'differs'""",
                           [judged]).fetchone()
    except duckdb.CatalogException:
        n = 0
    if n:
        out.append(Alert("check:reconcile", "checks", "page", "finance",
                         f"{n} reconciliation rows differ",
                         f"{n} reconciliation rows for {due} do not tie "
                         "to the ledger or the sources.",
                         ("data engineering",)))
    return out


def stuck_load(con, at: datetime) -> list[Alert]:
    """A load still 'running' long after it began died without a word
    (Chapter 6)."""
    out = []
    for x in quality.loads(con):
        if x.status == "running" and at - x.started_at > STUCK:
            out.append(Alert(f"stuck:{x.load_id}", "stuck_load", "page",
                             "data engineering", x.load_id,
                             f"{x.load_id} has been running since "
                             f"{x.started_at:%Y-%m-%d %H:%M} UTC."))
    return out


def budget_spent(con, at: datetime) -> list[Alert]:
    """The freshness SLO's error budget is spent: a ticket, because it
    is a decision about the coming weeks (stop changing things that
    can fail; mend what spent it), not about tonight."""
    b = budget(con, at)
    if b.left >= 0:
        return []
    return [Alert("slo:freshness", "budget_spent", "ticket",
                  "data engineering", "freshness budget spent",
                  f"{b.slo.name}: {b.bad} late days from {b.first} to "
                  f"{b.last}, against a budget of {b.slo.budget:g}.")]


def evaluate(con, at: datetime, root: Path | None = None) -> list[Alert]:
    """Every rule, at `at`."""
    return (night_failed(con, at) + held_rows(con, at)
            + signal(con, at, root) + checks(con, at)
            + stuck_load(con, at) + budget_spent(con, at))


# ------------------------------------------------------------ routing
def deliver_at(a: Alert, at: datetime) -> datetime:
    """When a notification reaches a person: a page outside working
    hours waits for the next 08:00 in New York, unless it may wake."""
    if a.severity != "page" or a.wake:
        return at
    local = at.replace(tzinfo=ZoneInfo("UTC")).astimezone(OFFICE)
    if HOURS[0] <= local.time() < HOURS[1]:
        return at
    day = local.date() + timedelta(days=local.time() >= HOURS[1])
    start = datetime.combine(day, HOURS[0], OFFICE)
    return start.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


CHANNEL = {"page": "pager", "ticket": "queue"}


@dataclass
class Sent:
    at: datetime            # when it reached them
    key: str
    kind: str               # page, ticket, copy or resolved
    to: str
    text: str


@dataclass
class Watch:
    """What one watch did: alerts opened, seen again, resolved."""
    at: datetime
    opened: list = field(default_factory=list)
    repeated: list = field(default_factory=list)
    resolved: list = field(default_factory=list)
    sent: list = field(default_factory=list)


DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.alerts (
    key          VARCHAR NOT NULL,
    opened_at    TIMESTAMP NOT NULL,
    rule         VARCHAR NOT NULL,
    severity     VARCHAR NOT NULL,
    owner        VARCHAR NOT NULL,
    subject      VARCHAR NOT NULL,
    text         VARCHAR NOT NULL,
    last_seen    TIMESTAMP NOT NULL,
    times        INTEGER NOT NULL,
    resolved_at  TIMESTAMP,
    PRIMARY KEY (key, opened_at)
);
CREATE TABLE IF NOT EXISTS audit.notifications (
    sent_at      TIMESTAMP NOT NULL,
    key          VARCHAR NOT NULL,
    kind         VARCHAR NOT NULL,
    recipient    VARCHAR NOT NULL,
    text         VARCHAR NOT NULL
);
"""


def watch(con, at: datetime, root: Path | None = None) -> Watch:
    """
    Evaluate every rule at `at`. A key with no open alert opens one and
    notifies its owner (and copies its contacts); a key already open is
    counted again and notifies nobody; an open alert whose rule no
    longer fires is resolved, and its owner told. Run it twice at the
    same moment and the second does nothing new.
    """
    at = at.replace(tzinfo=None)
    con.execute(DDL)
    w = Watch(at)
    fired = {}
    for a in evaluate(con, at, root):
        fired.setdefault(a.key, a)
    open_ = {k: (o, last) for k, o, last in con.execute(
        "SELECT key, opened_at, last_seen FROM audit.alerts "
        "WHERE resolved_at IS NULL "
        "ORDER BY opened_at, key").fetchall()}
    for key, a in fired.items():
        if key in open_:
            opened, last = open_[key]
            if last < at:
                con.execute("UPDATE audit.alerts SET last_seen = ?, "
                            "times = times + 1 WHERE key = ? AND "
                            "opened_at = ?", [at, key, opened])
            w.repeated.append(a)
            continue
        con.execute("INSERT INTO audit.alerts VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, 1, NULL)",
                    [key, at, a.rule, a.severity, a.owner, a.subject,
                     a.text, at])
        w.opened.append(a)
        when = deliver_at(a, at)
        w.sent.append(Sent(when, key, a.severity, a.owner, a.text))
        for c in a.copy:
            w.sent.append(Sent(when, key, "copy", c, a.text))
    for key, (opened, _) in open_.items():
        if key in fired:
            continue
        rule, owner, subject = con.execute(
            "SELECT rule, owner, subject FROM audit.alerts WHERE key = ? "
            "AND opened_at = ?", [key, opened]).fetchone()
        con.execute("UPDATE audit.alerts SET resolved_at = ? WHERE "
                    "key = ? AND opened_at = ?", [at, key, opened])
        w.resolved.append(key)
        w.sent.append(Sent(at, key, "resolved", owner,
                           f"resolved: {subject}"))
    if w.sent:
        con.executemany("INSERT INTO audit.notifications VALUES "
                        "(?, ?, ?, ?, ?)", [(s.at, s.key, s.kind, s.to,
                                             s.text) for s in w.sent])
    return w


def messages(sent: list) -> list[tuple]:
    """What people receive: notifications to one person, of one kind,
    at one moment, as one message. [(at, kind, to, [texts])]"""
    order = ("page", "ticket", "copy", "resolved")
    out: dict = {}
    for s in sent:
        out.setdefault((s.at, order.index(s.kind), s.to), []).append(
            s.text)
    return [(at, order[k], to, texts) for (at, k, to), texts
            in sorted(out.items())]


# ------------------------------------------------------------- the SLO
@dataclass(frozen=True)
class SLO:
    """A day is good when its run published it by 07:00 UTC the next
    morning; the target is the share of good days in any window."""
    name: str = "published by 07:00 UTC"
    target: float = 0.95
    window: int = 30

    @property
    def budget(self) -> float:
        """Bad days the window can afford."""
        return round((1 - self.target) * self.window, 6)


FRESHNESS = SLO()


def days(con, first: date, last: date, dag: str = "nightly"
         ) -> list[tuple[date, bool]]:
    """Each logical day from first to last: was it published on time?"""
    try:
        good = {d for (d,) in con.execute("""
            SELECT DISTINCT logical_date FROM audit.task_runs
            WHERE dag = ? AND task = 'publish' AND state = 'succeeded'
              AND finished_at <= logical_date + INTERVAL 31 HOUR
            """, [dag]).fetchall()}
    except duckdb.CatalogException:
        good = set()
    out, d = [], first
    while d <= last:
        out.append((d, d in good))
        d += timedelta(days=1)
    return out


@dataclass(frozen=True)
class Budget:
    slo: SLO
    first: date
    last: date
    good: int
    bad: int

    @property
    def met(self) -> float:
        return self.good / (self.good + self.bad)

    @property
    def left(self) -> float:
        """Bad days still affordable in this window; below 0, spent."""
        return round(self.slo.budget - self.bad, 6)


def budget(con, at: datetime, slo: SLO = FRESHNESS,
           dag: str = "nightly") -> Budget:
    """The SLO over the window of days due by `at`. Days before the
    DAG's first run are not counted: nothing was promised for them."""
    last = quality.expected(quality.RULES[0], at)
    first = last - timedelta(days=slo.window - 1)
    try:
        (began,) = con.execute("SELECT min(logical_date) FROM "
                               "audit.dag_runs WHERE dag = ?",
                               [dag]).fetchone()
    except duckdb.CatalogException:
        began = None
    if began is not None and began > first:
        first = began
    got = days(con, first, last)
    bad = sum(not ok for _, ok in got)
    return Budget(slo, first, last, len(got) - bad, bad)
