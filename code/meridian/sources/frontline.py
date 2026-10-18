"""
Frontline, Meridian's support desk: its API, served on your own machine.

A real support desk is someone else's service on the internet. This is a small HTTP
server over the desk's records that behaves like one: it wants a token, pages its
results, limits how fast you may call it, fails now and then, and answers as of a clock
the listing controls, so tickets can change while you are paging through them.

    from meridian.sources import Clock, frontline
    clock = Clock("2025-12-01 09:00")
    base = frontline.serve(clock)                    # e.g. http://127.0.0.1:53219
    # GET {base}/api/v2/tickets?updated_since=2025-11-01T00:00:00Z&limit=100

Endpoints

    GET /api/v2/tickets?updated_since=&limit=&cursor=    cursor pages, oldest change first
    GET /api/v2/tickets?updated_since=&page=&per_page=   numbered pages, oldest change first
    GET /api/v2/tickets/{id}                             one ticket
    GET /api/v2/tickets/{id}/comments                    its comments
    GET /api/v2/health                                   200, no token needed

Faults, set when serving and counted per server so every run misbehaves identically:

    rate_limit=N    every Nth request is refused with 429 and Retry-After: 1
    fail_every=N    every Nth request fails with 503
    tick=timedelta  the clock moves this far on every request: the data moves under you
"""
from __future__ import annotations

import base64
import json
import os
import sqlite3
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from meridian.sources import SOURCES, Clock, parse_moment

STORE = SOURCES / "frontline" / "_store.db"
TOKEN = os.getenv("FRONTLINE_TOKEN", "frontline-demo-token")
MAX_LIMIT = 100


def _z(s: str) -> str:
    return s.replace(" ", "T") + "Z"


def _stamp(s: str) -> str:
    return parse_moment(s).strftime("%Y-%m-%d %H:%M:%S")


CURRENT = """
WITH cur AS (
    SELECT v.*, ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY updated_at DESC) AS rn
    FROM ticket_versions v WHERE updated_at <= :now)
SELECT t.ticket_id, t.created_at, c.updated_at, c.status, c.priority, c.category, c.desk,
       c.assignee, t.channel, t.language, t.requester_account_id, t.requester_name,
       t.requester_email, t.description, t.sku
FROM cur c JOIN tickets t USING (ticket_id)
WHERE c.rn = 1
"""


def _ticket_json(r) -> dict:
    (tid, created, updated, status, priority, category, desk, assignee, channel, lang,
     acct, rname, remail, desc, sku) = r
    tags = [category] + ([lang] if lang != "en" else []) + ([f"sku:{sku}"] if sku else [])
    return {
        "id": tid, "created_at": _z(created), "updated_at": _z(updated), "status": status,
        "priority": priority, "channel": channel, "language": lang,
        "requester": {"account_id": acct, "name": rname, "email": remail},
        "assignee": assignee, "description": desc, "sku": sku,
        "custom_fields": [{"id": 360001, "name": "desk", "value": desk},
                          {"id": 360002, "name": "category", "value": category}],
        "tags": tags,
    }


class _State:
    def __init__(self, clock, store, rate_limit, fail_every, tick):
        self.clock, self.store = clock, str(store)
        self.rate_limit, self.fail_every, self.tick = rate_limit, fail_every, tick
        self.requests = 0
        self.lock = threading.Lock()
        self.log: list[tuple[str, int]] = []


def _handler(state: _State):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):          # the book prints what the client sees
            pass

        def _send(self, code, body=None, headers=None):
            data = json.dumps(body, ensure_ascii=False).encode() if body is not None else b""
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
            state.log.append((self.path, code))

        def do_GET(self):                      # noqa: N802
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            if url.path == "/api/v2/health":
                return self._send(200, {"status": "ok"})
            with state.lock:
                state.requests += 1
                n = state.requests
                now = state.clock.now
                if state.tick:
                    state.clock.advance(seconds=state.tick.total_seconds())
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                return self._send(401, {"error": "unauthorized",
                                        "message": "Send Authorization: Bearer <token>."})
            if state.rate_limit and n % state.rate_limit == 0:
                return self._send(429, {"error": "rate_limited",
                                        "message": "Too many requests."}, {"Retry-After": "1"})
            if state.fail_every and n % state.fail_every == 0:
                return self._send(503, {"error": "unavailable",
                                        "message": "Service temporarily unavailable."})
            con = sqlite3.connect(f"file:{state.store}?mode=ro", uri=True)
            try:
                return self._route(con, url.path, q, now.strftime("%Y-%m-%d %H:%M:%S"))
            except ValueError as e:
                return self._send(400, {"error": "bad_request", "message": str(e)})
            finally:
                con.close()

        def _route(self, con, path, q, now):
            parts = [p for p in path.split("/") if p]
            if parts[:3] != ["api", "v2", "tickets"]:
                return self._send(404, {"error": "not_found"})
            if len(parts) >= 4:
                row = con.execute(CURRENT + " AND t.ticket_id = :id AND t.created_at <= :now",
                                  {"now": now, "id": parts[3]}).fetchone()
                if row is None:
                    return self._send(404, {"error": "not_found"})
                if len(parts) == 5 and parts[4] == "comments":
                    cs = con.execute("SELECT comment_id, created_at, author, public, body FROM "
                                     "comments WHERE ticket_id = ? AND created_at <= ? "
                                     "ORDER BY created_at, comment_id", (parts[3], now)).fetchall()
                    return self._send(200, {"comments": [
                        {"id": c, "created_at": _z(t), "author": a, "public": bool(p), "body": b}
                        for c, t, a, p, b in cs]})
                return self._send(200, {"ticket": _ticket_json(row)})
            since = _stamp(q.get("updated_since", "2000-01-01T00:00:00Z"))
            where = CURRENT + " AND c.updated_at >= :since"
            params = {"now": now, "since": since}
            if "page" in q or "per_page" in q:
                page, per = int(q.get("page", 1)), min(int(q.get("per_page", 25)), MAX_LIMIT)
                if page < 1:
                    raise ValueError("page starts at 1")
                (total,) = con.execute(f"SELECT COUNT(*) FROM ({where})", params).fetchone()
                rows = con.execute(where + " ORDER BY c.updated_at, t.ticket_id LIMIT :n OFFSET :o",
                                   dict(params, n=per, o=(page - 1) * per)).fetchall()
                return self._send(200, {"tickets": [_ticket_json(r) for r in rows],
                                        "meta": {"page": page, "per_page": per, "total": total}})
            limit = min(int(q.get("limit", 25)), MAX_LIMIT)
            if "cursor" in q:
                cu, ci = base64.urlsafe_b64decode(q["cursor"].encode()).decode().split("|")
                where += " AND (c.updated_at > :cu OR (c.updated_at = :cu AND t.ticket_id > :ci))"
                params.update(cu=cu, ci=ci)
            rows = con.execute(where + " ORDER BY c.updated_at, t.ticket_id LIMIT :n",
                               dict(params, n=limit + 1)).fetchall()
            more = len(rows) > limit
            rows = rows[:limit]
            cursor = (base64.urlsafe_b64encode(f"{rows[-1][2]}|{rows[-1][0]}".encode()).decode()
                      if rows else None)
            return self._send(200, {"tickets": [_ticket_json(r) for r in rows],
                                    "meta": {"has_more": more, "after_cursor": cursor}})
    return Handler


def serve(clock: Clock | None = None, *, rate_limit: int = 0, fail_every: int = 0,
          tick: timedelta | None = None, store=STORE):
    """Start the API on a free local port in a background thread; return its base URL."""
    state = _State(clock or Clock(), store, rate_limit, fail_every, tick)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(state))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    serve.last = (server, state)
    return base


def requests_made() -> list[tuple[str, int]]:
    """What the most recent server was asked, and what it answered: for listings that count."""
    return list(serve.last[1].log)
