"""
The orchestrator: one run of a DAG for one logical date.

A run has a logical date, the day its data is about, and a moment it
starts, which for the nightly run is 06:00 UTC on the next day. It
walks the DAG in order and gives every task a state:

    waiting          its upstream tasks are not all done
    running          a try is under way
    sensing          a sensor said "not yet" and will ask again
    up_for_retry     a try failed and another is due later
    succeeded        done
    failed           out of tries, out of time, or fatal
    upstream_failed  something it waits for failed: it never ran
    kept             a rerun found it done by an earlier run

Every try is a row of audit.task_runs, every run a row of
audit.dag_runs, in the warehouse the run builds:

    run_id       <dag>.<logical date>.<attempt>, as load ids are
    task, try_number, state, started_at, finished_at, detail

A rerun of a logical date keeps what succeeded and runs only what
failed and what waits for it. Only one run of a DAG may hold a
warehouse at a time; a second is refused, not queued.

Time comes from a clock. RunClock is the run's start plus the time
that has really passed, so nothing reads the wall clock; SimClock
moves only when told, so a listing can play a night forward in no
time at all and print the same thing every time.

    run(dag, day, run_at=None, env=None, clock=None, rerun=False)
    history(warehouse, dag, day)      the rows of every try
"""
from __future__ import annotations

import fcntl
import multiprocessing as mp
import textwrap
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

import duckdb

from groundwork.config import (LANDING, RAW, REFERENCE, TRADELINE_DB,
                               WAREHOUSE)
from groundwork.orchestrate.dag import DAG, Fatal

DONE = ("succeeded", "kept")
BAD = ("failed", "upstream_failed")

DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.dag_runs (
    run_id       VARCHAR PRIMARY KEY,
    dag          VARCHAR NOT NULL,
    logical_date DATE NOT NULL,
    kind         VARCHAR NOT NULL,      -- scheduled or rerun
    run_at       TIMESTAMP NOT NULL,
    state        VARCHAR NOT NULL,      -- running, succeeded, failed
    finished_at  TIMESTAMP
);
CREATE TABLE IF NOT EXISTS audit.task_runs (
    run_id       VARCHAR NOT NULL,
    dag          VARCHAR NOT NULL,
    logical_date DATE NOT NULL,
    task         VARCHAR NOT NULL,
    try_number   INTEGER NOT NULL,
    state        VARCHAR NOT NULL,
    started_at   TIMESTAMP NOT NULL,
    finished_at  TIMESTAMP,
    detail       VARCHAR,
    PRIMARY KEY (run_id, task, try_number)
);
"""


# ------------------------------------------------------------- clocks
class TaskTimeout(Exception):
    pass


class SimClock:
    """Simulated time: sleeping moves it on, instantly."""

    def __init__(self, start: datetime):
        self.t = start
        self.deadline = None

    def now(self) -> datetime:
        return self.t

    def sleep(self, seconds: float) -> None:
        later = self.t + timedelta(seconds=seconds)
        if self.deadline is not None and later > self.deadline:
            self.t = self.deadline
            raise TaskTimeout("timed out")
        self.t = later


class RunClock:
    """The run's start plus the time that has really passed."""

    def __init__(self, start: datetime):
        self.start, self.t0 = start, time.perf_counter()
        self.deadline = None

    def now(self) -> datetime:
        return self.start + timedelta(
            seconds=round(time.perf_counter() - self.t0, 3))

    def sleep(self, seconds: float) -> None:
        if self.deadline is not None:
            left = (self.deadline - self.now()).total_seconds()
            if seconds > left:
                time.sleep(max(left, 0))
                raise TaskTimeout("timed out")
        time.sleep(seconds)


# --------------------------------------- where a run reads and writes
@dataclass(frozen=True)
class Env:
    """Where a run reads its sources and writes Groundwork. The
    defaults are the real ones; a listing points them at a sandbox,
    and a backfill (Chapter 18) at sources replayed to a moment."""
    warehouse: Path = WAREHOUSE
    root: Path = RAW                     # the raw layer
    archive: Path | None = None          # kept files (files.ARCHIVE)
    landing: Path = LANDING
    reference: Path = REFERENCE
    tradeline_db: Path = TRADELINE_DB
    frontline_base: str | None = None    # None: serve the stand-in
    dbt_target: str = "prod"             # dev builds env.warehouse
    dbt_target_path: Path | None = None  # dbt's target/ folder

    def __post_init__(self):
        if self.archive is None:
            from groundwork.ingest.files import ARCHIVE
            object.__setattr__(self, "archive", ARCHIVE)
        for name in ("warehouse", "root", "archive", "landing",
                     "reference", "tradeline_db", "dbt_target_path"):
            value = getattr(self, name)       # views store full paths
            if value is not None:
                object.__setattr__(self, name, Path(value).resolve())


@dataclass
class Context:
    """What a task is told about the run it belongs to."""
    dag: str
    day: date                 # the logical date
    run_id: str
    run_at: datetime          # when the run was due to start
    env: Env
    since: datetime | None    # last Tradeline read a build consumed
    try_number: int = 1
    clock: object = None

    def now(self) -> datetime:
        return self.clock.now()


@dataclass
class TaskRun:
    state: str = "waiting"
    tries: int = 0
    detail: str = ""
    started_at: datetime | None = None
    pokes: int = 0


@dataclass
class Run:
    run_id: str
    dag: str
    day: date
    run_at: datetime
    kind: str
    tasks: dict = field(default_factory=dict)
    since: datetime | None = None

    @property
    def state(self) -> str:
        return ("succeeded" if all(t.state in DONE for t in
                                   self.tasks.values()) else "failed")

    def report(self, width: int = 28) -> str:
        out = [f"{self.run_id}  ({self.kind}, due "
               f"{self.run_at:%Y-%m-%d %H:%M} UTC)",
               f"{'task':<17}{'state':<16}{'tries':>5}  detail"]
        for name, t in self.tasks.items():
            d = t.detail if len(t.detail) <= width else (
                t.detail[:width - 3].rstrip() + "...")
            out.append(f"{name:<17}{t.state:<16}{t.tries:>5}  {d}"
                       .rstrip())
        for name, t in self.tasks.items():   # what failed, in full
            if t.state == "failed":
                out += textwrap.wrap(f"{name}: {t.detail}", 66,
                                     initial_indent="  ",
                                     subsequent_indent="    ")
        out.append(f"run {self.state}")
        return "\n".join(out)


# ---------------------------------------------------------- the record
def ensure(con) -> None:
    con.execute(DDL)


def _connect(env: Env):
    Path(env.warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(env.warehouse))
    ensure(con)
    return con


def _write(env, sql, params) -> None:
    """One statement, on a connection held only for it: a task may
    need the warehouse to itself while the run waits."""
    con = _connect(env)
    try:
        con.execute(sql, params)
    finally:
        con.close()


def _attempt(con, dag, day) -> int:
    (n,) = con.execute("SELECT count(*) FROM audit.dag_runs "
                       "WHERE dag = ? AND logical_date = ?",
                       [dag, day]).fetchone()
    return n + 1


def _since(con, dag, day) -> datetime | None:
    """The moment of the Tradeline read that the last successful
    build of an earlier logical date consumed."""
    (at,) = con.execute("""
        SELECT max(t.started_at) FROM audit.task_runs t
        WHERE t.dag = ? AND t.task = 'ingest_tradeline'
          AND t.state = 'succeeded'
          AND t.started_at <= (SELECT max(started_at)
                               FROM audit.task_runs
                               WHERE dag = ? AND task = 'dbt_build'
                                 AND state = 'succeeded'
                                 AND logical_date < ?)""",
                        [dag, dag, day]).fetchone()
    return at


def _last_states(con, dag, day) -> tuple[str, dict]:
    """The latest run of a logical date, and each task's last state
    in it."""
    row = con.execute("SELECT run_id FROM audit.dag_runs WHERE dag = ? "
                      "AND logical_date = ? ORDER BY run_at DESC, "
                      "run_id DESC LIMIT 1", [dag, day]).fetchone()
    if row is None:
        raise ValueError(f"{dag} has never run for {day}")
    rows = con.execute("""
        SELECT task, state FROM audit.task_runs WHERE run_id = ?
        QUALIFY row_number() OVER (PARTITION BY task
                                   ORDER BY try_number DESC) = 1""",
                       [row[0]]).fetchall()
    return row[0], dict(rows)


def history(warehouse, dag: str, day) -> list[tuple]:
    """Every try of every run of a logical date, in order."""
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute("""
            SELECT run_id, task, try_number, state, started_at,
                   finished_at, detail FROM audit.task_runs
            WHERE dag = ? AND logical_date = ?
            ORDER BY run_id, started_at, try_number""",
                           [dag, day]).fetchall()
    finally:
        con.close()


# ------------------------------------------------------- one at a time
class RunInProgress(RuntimeError):
    pass


@contextmanager
def lock(env: Env, dag: str):
    """Hold the warehouse for one run of `dag`; refuse a second."""
    path = Path(env.warehouse).with_name(
        Path(env.warehouse).name + f".{dag}.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunInProgress(f"another {dag} run holds "
                                f"{env.warehouse}") from None
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


# ------------------------------------------------------------- one try
def _child(fn, ctx, conn, log) -> None:
    """In a process of its own: run the task, with whatever it prints
    sent to its log file, and say how it went."""
    import os
    Path(log).parent.mkdir(parents=True, exist_ok=True)
    out = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.dup2(out, 1)
    os.dup2(out, 2)
    ctx.clock = RunClock(ctx.run_at_try)
    try:
        conn.send(("ok", fn(ctx)))
    except Fatal as e:
        conn.send(("fatal", str(e)))
    except BaseException as e:            # noqa: BLE001 everything
        conn.send(("error", f"{type(e).__name__}: {e}"))


def _isolated(task, ctx, clock) -> tuple[str, str]:
    """Run a try in a new process; kill it if it overruns."""
    spawn = mp.get_context("spawn")
    mine, theirs = spawn.Pipe(duplex=False)
    child = replace(ctx, clock=None)
    child.run_at_try = clock.now()
    log = (Path(ctx.env.warehouse).parent / "logs" / ctx.run_id
           / f"{task.name}.{ctx.try_number}.log")
    p = spawn.Process(target=_child,
                      args=(task.fn, child, theirs, str(log)))
    p.start()
    p.join(task.timeout)
    if p.is_alive():
        p.kill()
        p.join()
        return "timeout", f"timed out after {task.timeout:,.0f} s"
    if mine.poll():
        return mine.recv()
    return "error", f"exit code {p.exitcode}"


def _inline(task, ctx, clock) -> tuple[str, str]:
    """Run a try here. A timeout is enforced by the clock: a task that
    sleeps past its deadline is stopped there."""
    if task.timeout is not None:
        clock.deadline = clock.now() + timedelta(seconds=task.timeout)
    try:
        return "ok", task.fn(ctx)
    except TaskTimeout:
        return "timeout", f"timed out after {task.timeout:,.0f} s"
    except Fatal as e:
        return "fatal", str(e)
    except Exception as e:                # noqa: BLE001 a task failed
        return "error", f"{type(e).__name__}: {e}"
    finally:
        clock.deadline = None


# ------------------------------------------------------------- the run
def run(dag: DAG, day, *, run_at: datetime | None = None,
        env: Env | None = None, clock=None, rerun: bool = False) -> Run:
    """Run `dag` for logical date `day`. A rerun starts from the latest
    run of that date and runs only what did not succeed in it."""
    day = date.fromisoformat(str(day))
    run_at = run_at or datetime.combine(day + timedelta(days=1),
                                        dtime(6))
    env = env or Env()
    clock = clock or RunClock(run_at)
    with lock(env, dag.name):
        return _run(dag, day, run_at, env, clock, rerun)


def _run(dag, day, run_at, env, clock, rerun) -> Run:
    con = _connect(env)
    try:
        attempt = _attempt(con, dag.name, day)
        since = _since(con, dag.name, day)
        before = _last_states(con, dag.name, day) if rerun else None
        run_id = f"{dag.name}.{day.isoformat()}.{attempt}"
        con.execute("INSERT INTO audit.dag_runs VALUES "
                    "(?, ?, ?, ?, ?, 'running', NULL)",
                    [run_id, dag.name, day,
                     "rerun" if rerun else "scheduled", run_at])
    finally:
        con.close()
    r = Run(run_id, dag.name, day, run_at,
            "rerun" if rerun else "scheduled",
            {n: TaskRun() for n in dag.order()}, since)
    if before:
        old_id, states = before
        again = {n for n, s in states.items() if s not in DONE}
        again |= dag.downstream(again)
        again |= {n for n in dag.tasks if n not in states}
        for n in dag.order():
            if n not in again:
                _record(env, r, n, "kept", clock, try_number=0,
                        detail=f"done in run {old_id.rsplit('.')[-1]}")
    ctx = Context(dag.name, day, run_id, run_at, env, since,
                  clock=clock)
    due = {}                              # task -> not before
    while True:
        name = _next(dag, r, due, clock, env)
        if name is None:
            waits = [due[n] for n, t in r.tasks.items()
                     if t.state in ("sensing", "up_for_retry")]
            if not waits:
                break
            clock.sleep(max(0.0, (min(waits) - clock.now())
                            .total_seconds()))
            continue
        task = dag.tasks[name]
        if task.sensor:
            _poke(task, r, ctx, clock, due, env)
        else:
            _try(task, r, ctx, clock, due, env)
    _write(env, "UPDATE audit.dag_runs SET state = ?, finished_at = ? "
                "WHERE run_id = ?", [r.state, clock.now(), run_id])
    return r


def _next(dag, r, due, clock, env) -> str | None:
    """The first task, in DAG order, that may start now; marks tasks
    upstream_failed on the way."""
    for name in dag.order():
        t = r.tasks[name]
        if t.state not in ("waiting", "sensing", "up_for_retry"):
            continue
        ups = [r.tasks[u].state for u in dag.tasks[name].upstream]
        if any(s in BAD for s in ups):
            _record(env, r, name, "upstream_failed", clock,
                    try_number=0, detail="")
            return _next(dag, r, due, clock, env)
        if all(s in DONE for s in ups) and \
                due.get(name, clock.now()) <= clock.now():
            return name
    return None


def _record(env, r, name, state, clock, *, try_number, detail,
            started=None) -> None:
    t = r.tasks[name]
    t.state, t.detail = state, detail
    now = clock.now()
    _write(env, "INSERT OR REPLACE INTO audit.task_runs VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
           [r.run_id, r.dag, r.day, name, try_number, state,
            started or now, now, detail])


def _poke(task, r, ctx, clock, due, env) -> None:
    """Ask a sensor once. Yes: succeeded. No: ask again later, or
    fail if its time is up."""
    t = r.tasks[task.name]
    if t.started_at is None:
        t.started_at, t.tries = clock.now(), 1
    t.pokes += 1
    state, out = _inline(replace(task, timeout=None), ctx, clock)
    pokes = t.pokes
    waited = clock.now() - t.started_at
    if state == "ok" and out:
        _record(env, r, task.name, "succeeded", clock, try_number=1,
                started=t.started_at,
                detail=f"{pokes} poke{'s' * (pokes > 1)}, "
                       f"waited {_hm(waited)}")
        return
    if state not in ("ok",):
        _record(env, r, task.name, "failed", clock, try_number=1,
                started=t.started_at, detail=out)
        return
    if (waited.total_seconds() + task.retry_delay > task.timeout):
        _record(env, r, task.name, "failed", clock, try_number=1,
                started=t.started_at,
                detail=f"nothing after {pokes} pokes, {_hm(waited)}")
        return
    due[task.name] = clock.now() + timedelta(seconds=task.retry_delay)
    _record(env, r, task.name, "sensing", clock, try_number=1,
            started=t.started_at, detail=f"{pokes} pokes so far")


def _try(task, r, ctx, clock, due, env) -> None:
    """One try of a task, and what comes next."""
    t = r.tasks[task.name]
    t.tries += 1
    start = clock.now()
    _record(env, r, task.name, "running", clock, try_number=t.tries,
            detail="")
    ctx = replace(ctx, try_number=t.tries)
    go = _isolated if task.isolate else _inline
    state, out = go(task, ctx, clock)
    if state == "ok":
        _record(env, r, task.name, "succeeded", clock,
                try_number=t.tries, started=start, detail=out or "")
        return
    if state != "fatal" and t.tries <= task.retries:
        wait = timedelta(seconds=task.delay(t.tries))
        due[task.name] = clock.now() + wait
        _record(env, r, task.name, "up_for_retry", clock,
                try_number=t.tries, started=start,
                detail=f"{out}; again in {_hm(wait)}")
        return
    why = out if state != "fatal" else f"fatal: {out}"
    _record(env, r, task.name, "failed", clock, try_number=t.tries,
            started=start, detail=why)


def _hm(d: timedelta) -> str:
    s = int(d.total_seconds())
    if s < 60:
        return f"{s} s"
    h, m = divmod(s // 60, 60)
    return f"{h} h {m:02d} min" if h else f"{m} min"
