"""
Portal sessions from the event stream: session windows in event time,
closed by a watermark, written to marts.fct_portal_sessions.

The portal gives each visit a session_id. A session's window stays
open while events keep arriving for it, and closes once the watermark
has passed its last event by GAP. The watermark is the stream's
estimate of how far event time has got: the newest event time seen,
less DISORDER for the ordinary jitter of arrival. When a session
closes its row is written; an event that arrives afterwards, within
LATENESS, updates the row (the sink replaces by session_id); anything
later still is counted as too late and left out.

Three things about the portal's clocks shape the code:

  * occurred_at is the device's clock and received_at the server's.
    No event can happen after the server received it, so an event
    from the future counts at received_at (it cannot drag the
    watermark forward by a day).
  * A mobile event can arrive days late, and a phone's clock can be
    hours wrong. One event cannot tell you which; a session can. Late
    delivery only ever adds delay, so the session's quickest event
    shows the clock: when even that one is more than CLOCK_TOLERANCE
    off, the device's clock was wrong, and the row's times are shifted
    by it (clock_offset_s says by how much).
  * A session with more events than a person could make is a bot:
    flagged, not dropped, so the funnel can leave it out and anyone
    can see that it did.

Columns beyond Chapter 4's design (sessions.sql adds them): account_id,
because account_key needs the dimension Chapter 12 builds; is_bot,
late_events and clock_offset_s, which say how far to trust the row.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa

from groundwork.dates import date_key, local_date
from groundwork.stream.consumer import Consumer, parse
from groundwork.stream.log import Log

GAP = timedelta(minutes=30)       # no event for this long: session over
DISORDER = timedelta(minutes=5)   # jitter the watermark allows for
LATENESS = timedelta(days=3)      # how long a closed session can change
CLOCK_TOLERANCE = timedelta(minutes=1)
BOT_EVENTS, BOT_PER_MINUTE = 100, 30

COUNTED = {"search": "searches", "product_view": "product_views",
           "add_to_cart": "add_to_carts",
           "checkout_started": "checkouts_started",
           "order_submitted": "orders_submitted"}
ROW = ("session_id", "account_key", "region_key", "date_key",
       "started_at", "ended_at", "device", "event_count",
       *COUNTED.values(), "account_id", "is_bot", "late_events",
       "clock_offset_s")


class _Session:
    __slots__ = ("account_id", "device", "events", "last", "emitted",
                 "dirty", "late")

    def __init__(self, ev: dict):
        self.account_id, self.device = ev["account_id"], ev["device"]
        self.events: dict[str, tuple] = {}
        self.last = datetime.min
        self.emitted = self.dirty = False
        self.late = 0


class Sessioniser:
    def __init__(self, region_of: dict[int, int], gap=GAP,
                 disorder=DISORDER, lateness=LATENESS, clamp=True):
        self.region_of = region_of
        self.gap, self.disorder, self.lateness = gap, disorder, lateness
        self.clamp = clamp
        self.open: dict[str, _Session] = {}
        self.done: set[str] = set()
        self.newest = datetime.min
        self.watermark = datetime.min
        self.counts = dict.fromkeys(
            ("events", "duplicates", "late", "too_late", "rows"), 0)

    def event_time(self, ev: dict) -> datetime:
        if self.clamp:
            return min(ev["occurred_at"], ev["received_at"])
        return ev["occurred_at"]

    def add(self, ev: dict) -> str:
        """Take one event; say what became of it."""
        self.counts["events"] += 1
        t = self.event_time(ev)
        sid = ev["session_id"]
        s = self.open.get(sid)
        if sid in self.done or (
                s is None and t + self.gap + self.lateness
                < self.watermark):
            self.counts["too_late"] += 1
            return "too_late"
        if s is None:
            s = self.open[sid] = _Session(ev)
        if ev["event_id"] in s.events:
            self.counts["duplicates"] += 1
            return "duplicate"
        s.events[ev["event_id"]] = (ev["event_type"], ev["occurred_at"],
                                    ev["received_at"])
        s.last = max(s.last, t)
        self.newest = max(self.newest, t)
        if s.emitted:
            s.dirty = True
            s.late += 1
            self.counts["late"] += 1
            return "late"
        return "ok"

    def advance(self) -> list[dict]:
        """Move the watermark; return rows for sessions closed or
        changed since the last call, and forget sessions too old to
        change again."""
        self.watermark = max(self.watermark,
                             self.newest - self.disorder)
        rows = []
        for sid, s in list(self.open.items()):
            closed = s.last + self.gap <= self.watermark
            if closed and (not s.emitted or s.dirty):
                rows.append(self.row(sid, s))
                s.emitted, s.dirty = True, False
            if s.last + self.gap + self.lateness < self.watermark:
                del self.open[sid]
                self.done.add(sid)
        self.counts["rows"] += len(rows)
        return rows

    def close_all(self) -> list[dict]:
        """The end of a bounded stream: close whatever is still open."""
        self.newest = datetime.max - self.gap - self.lateness * 2
        return self.advance()

    def row(self, sid: str, s: _Session) -> dict:
        quickest = min(r - o for _, o, r in s.events.values())
        shift = (quickest if abs(quickest) > CLOCK_TOLERANCE
                 else timedelta(0))
        started = min(o for _, o, _ in s.events.values()) + shift
        ended = max(o for _, o, _ in s.events.values()) + shift
        n = len(s.events)
        minutes = max((ended - started).total_seconds() / 60, 1)
        region = self.region_of.get(s.account_id)
        row = dict.fromkeys(COUNTED.values(), 0)
        for etype, _, _ in s.events.values():
            if etype in COUNTED:
                row[COUNTED[etype]] += 1
        row.update(
            session_id=sid, account_key=None,
            region_key=None if region is None else
            hashlib.md5(str(region).encode()).hexdigest()[:16],
            date_key=date_key(started.date() if region is None
                              else local_date(started, region)),
            started_at=started, ended_at=ended, device=s.device,
            event_count=n, account_id=s.account_id,
            is_bot=n >= BOT_EVENTS and n / minutes >= BOT_PER_MINUTE,
            late_events=s.late,
            clock_offset_s=round(shift.total_seconds()))
        return row


class SessionSink:
    """fct_portal_sessions in DuckDB; a row written again replaces
    the one before."""

    def __init__(self, con):
        from groundwork.ddl import create
        self.con = con
        create(con, "marts")
        con.execute((Path(__file__).with_name("sessions.sql"))
                    .read_text())

    def upsert(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        self.con.register("_rows", pa.Table.from_pylist(rows))
        try:
            cols = ", ".join(ROW)
            self.con.execute(
                f"INSERT OR REPLACE INTO marts.fct_portal_sessions "
                f"({cols}) SELECT {cols} FROM _rows")
        finally:
            self.con.unregister("_rows")
        return len(rows)


def run(log: Log, con, region_of: dict[int, int], group="sessions",
        batch: int = 5000, until: int | None = None,
        **settings) -> Sessioniser:
    """
    Consume the log from the group's checkpoint to `until` (or the
    end), keeping fct_portal_sessions up to date after every batch.
    The sessioniser's state is saved with each committed offset, so a
    restart carries on where the last checkpoint left it.
    """
    consumer, sink = Consumer(log, group), SessionSink(con)
    sessions = consumer.restore() or Sessioniser(region_of, **settings)
    while True:
        room = batch if until is None else min(
            batch, until - consumer.position)
        records = consumer.poll(room) if room > 0 else []
        if not records:
            return sessions
        for _, value in records:
            sessions.add(parse(value))
        sink.upsert(sessions.advance())
        consumer.commit(state=sessions)


FUNNEL = """
SELECT count(*)                                       AS sessions,
       count(*) FILTER (WHERE add_to_carts > 0)       AS carted,
       count(*) FILTER (WHERE checkouts_started > 0)  AS checked_out,
       count(*) FILTER (WHERE orders_submitted > 0)   AS ordered,
       sum(orders_submitted)                          AS orders
FROM marts.fct_portal_sessions
WHERE NOT is_bot AND started_at >= ? AND started_at < ?
"""

ABANDONED = """
SELECT count(*) FROM marts.fct_portal_sessions
WHERE NOT is_bot AND add_to_carts > 0 AND orders_submitted = 0
  AND ended_at + ? >= ? - INTERVAL 1 HOUR AND ended_at + ? < ?
"""


def funnel(con, start: datetime, end: datetime) -> dict:
    """Sessions, then those that carted, checked out and ordered."""
    names = ("sessions", "carted", "checked_out", "ordered", "orders")
    return dict(zip(names,
                    con.execute(FUNNEL, [start, end]).fetchone()))


def abandoned_carts(con, now: datetime, gap=GAP) -> int:
    """Carts abandoned in the hour before `now`: sessions with a cart
    and no order, whose GAP of silence ran out in that hour."""
    return con.execute(ABANDONED, [gap, now, gap, now]).fetchone()[0]
