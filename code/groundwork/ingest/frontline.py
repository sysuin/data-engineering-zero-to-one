"""
Frontline, the support desk, pulled through its API: every ticket
changed since the last good pull, page by page, landed, then parsed.

Each night's load asks for the tickets updated since the previous
good load's watermark, less an overlap, and follows the cursor to the
end. Every response body is written to disk as it arrives, before
anything reads it:

    raw/frontline/pages/load_date=YYYY-MM-DD/attempt-N/
        page-00001.json, page-00002.json, ...

Only then are the pages parsed, one row per ticket (the latest copy,
if a ticket changed while the pull was paging and came back twice),
into three raw tables:

    raw/frontline/tickets/        one row per ticket
    raw/frontline/ticket_fields/  one row per custom field
    raw/frontline/ticket_tags/    one row per tag

Each row keeps `_page`, the page it came from; tickets keep the whole
ticket as `payload`. A parser bug is fixed by parsing the pages
again, without asking the API for anything.

Each partition holds the tickets that changed, not the whole desk:
the view raw.frontline_tickets holds a ticket once for every load
that saw it change, and the latest by updated_at is the ticket now.

The watermark is the largest updated_at the pull received: the
source's own clock, never ours. It is kept in the load log's `extra`,
and the next load starts OVERLAP before it. A rerun of a load date
starts from the load before it, not from itself.

Requests carry the token from the environment (FRONTLINE_TOKEN, from
.env). A 429 waits as long as Retry-After says; a 5xx or a dropped
connection waits a capped, jittered, growing time and tries again;
any other 4xx stops the load at once. The count is checked against
the API's own total, asked before and after the pull.

    load(load_date, run_at=..., base=...)   one night; its Load
    python -m groundwork.ingest.frontline [--load-date D] [--at T]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from groundwork import loadlog
from groundwork.config import NOW, RAW, SEED, WAREHOUSE
from groundwork.storage import raw_views, write_partition

SOURCE = "frontline"
TICKETS = "/api/v2/tickets"
LIMIT = 100                    # the most the API gives in a page
OVERLAP = timedelta(hours=1)
BEGINNING = "2000-01-01T00:00:00Z"   # a first load asks for all
RETRY_ON = {429, 500, 502, 503, 504}  # worth asking again


class FrontlineError(Exception):
    """A request the loader will not retry, or has given up on."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class LoadError(Exception):
    """A pull whose counts do not reconcile with the API's total."""


def token() -> str:
    """The bearer token, from the environment; never from a file."""
    value = os.environ.get("FRONTLINE_TOKEN")
    if not value:
        raise FrontlineError("FRONTLINE_TOKEN is not set: copy "
                             ".env.example to .env")
    return value


def fingerprint(secret: str) -> str:
    """Enough to tell two tokens apart in a log; not enough to use."""
    digest = hashlib.sha256(secret.encode()).hexdigest()
    return "sha256:" + digest[:12]


class Client:
    """
    GET with manners: waits when told to, retries what may pass,
    stops on what will not, and keeps a record of every attempt.
    """

    def __init__(self, base: str, *, secret: str | None = None,
                 attempts: int = 6, first_wait: float = 0.5,
                 cap: float = 30.0, rng: random.Random | None = None,
                 sleep=time.sleep, timeout: float = 30.0):
        secret = secret or token()
        self.fingerprint = fingerprint(secret)       # safe to log
        self.http = httpx.Client(
            base_url=base, timeout=timeout,
            headers={"Authorization": f"Bearer {secret}"})
        self.attempts, self.first_wait = attempts, first_wait
        self.cap, self.sleep = cap, sleep
        self.rng = rng or random.Random(SEED)
        self.log: list[tuple[int, str, float]] = []  # try, got, wait

    def get(self, path: str, params: dict) -> httpx.Response:
        """One GET, tried up to `attempts` times: the 200, or raise."""
        for n in range(1, self.attempts + 1):
            try:
                r = self.http.get(path, params=params)
                got = str(r.status_code)
            except httpx.TransportError as e:    # no answer at all
                r, got = None, type(e).__name__
            if r is not None and r.status_code == 200:
                self.log.append((n, got, 0.0))
                return r
            if r is not None and r.status_code not in RETRY_ON:
                self.log.append((n, got, 0.0))
                raise FrontlineError(f"{got} {_message(r)}",
                                     r.status_code)
            if n == self.attempts:
                self.log.append((n, got, 0.0))
                raise FrontlineError(
                    f"gave up after {n} attempts, last {got}")
            wait = self.wait(n, r)
            self.log.append((n, got, wait))
            self.sleep(wait)

    def wait(self, n: int, r: httpx.Response | None) -> float:
        """How long before try n + 1: what a 429 says, or backoff."""
        if r is not None and r.status_code == 429:
            told = r.headers.get("Retry-After", "")
            if told.isdigit():
                if int(told) > self.cap:
                    raise FrontlineError(f"told to wait {told} s",
                                         429)
                return float(told)
        # Full jitter: anywhere from 0 to first_wait * 2^(n-1), capped.
        ceiling = min(self.cap, self.first_wait * 2 ** (n - 1))
        return self.rng.uniform(0, ceiling)

    def close(self):
        self.http.close()


def _message(r: httpx.Response) -> str:
    try:
        return r.json().get("message", "")
    except ValueError:
        return r.text[:80]


def total(client: Client, since: str) -> int:
    """How many tickets the API says changed since `since`."""
    r = client.get(TICKETS, {"updated_since": since, "page": 1,
                             "per_page": 1})
    return r.json()["meta"]["total"]


def pull(client: Client, since: str, limit: int = LIMIT):
    """Every page of tickets changed since `since`, as body bytes."""
    params = {"updated_since": since, "limit": limit}
    while True:
        r = client.get(TICKETS, params)
        yield r.content
        meta = r.json()["meta"]
        if not meta["has_more"]:
            return
        params["cursor"] = meta["after_cursor"]


def land(folder: Path, n: int, body: bytes) -> Path:
    """One response body, byte for byte, written whole or not at all."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"page-{n:05d}.json"
    tmp = folder / f".{path.name}.tmp"
    tmp.write_bytes(body)
    os.replace(tmp, path)
    return path


def flatten(t: dict, page: int) -> tuple[dict, list, list]:
    """One ticket as a row, its custom fields and its tags as rows."""
    who = t.get("requester") or {}
    row = {"ticket_id": t["id"], "created_at": t["created_at"],
           "updated_at": t["updated_at"], "status": t.get("status"),
           "priority": t.get("priority"), "channel": t.get("channel"),
           "language": t.get("language"),
           "requester_account_id": who.get("account_id"),
           "requester_name": who.get("name"),
           "requester_email": who.get("email"),
           "assignee": t.get("assignee"),
           "description": t.get("description"), "sku": t.get("sku"),
           "payload": json.dumps(t, ensure_ascii=False), "_page": page}
    key = {"ticket_id": t["id"], "updated_at": t["updated_at"]}
    fields = [dict(key, field_id=f["id"], name=f.get("name"),
                   value=None if f.get("value") is None
                   else str(f["value"]), _page=page)
              for f in t.get("custom_fields") or []]
    tags = [dict(key, position=i, tag=tag, _page=page)
            for i, tag in enumerate(t.get("tags") or [])]
    return row, fields, tags


SCHEMAS = {
    "tickets": pa.schema(
        [(c, pa.string()) for c in ("ticket_id", "created_at",
                                    "updated_at", "status", "priority",
                                    "channel", "language")]
        + [("requester_account_id", pa.int64())]
        + [(c, pa.string()) for c in (
            "requester_name", "requester_email", "assignee",
            "description", "sku", "payload")]
        + [("_page", pa.int32())]),
    "ticket_fields": pa.schema(
        [("ticket_id", pa.string()), ("updated_at", pa.string()),
         ("field_id", pa.int64()), ("name", pa.string()),
         ("value", pa.string()), ("_page", pa.int32())]),
    "ticket_tags": pa.schema(
        [("ticket_id", pa.string()), ("updated_at", pa.string()),
         ("position", pa.int32()), ("tag", pa.string()),
         ("_page", pa.int32())]),
}


def parse(pages: list[Path]) -> tuple[dict, int]:
    """
    Landed pages to rows for the three tables, keeping each ticket's
    latest copy. Returns the tables and how many tickets were read.
    """
    latest: dict[str, tuple] = {}
    read = 0
    for page in sorted(pages):
        n = int(page.stem.split("-")[1])
        for t in json.loads(page.read_bytes())["tickets"]:
            read += 1
            seen = latest.get(t["id"])
            if seen is None or t["updated_at"] >= seen[0]["updated_at"]:
                latest[t["id"]] = flatten(t, n)
    rows = {"tickets": [], "ticket_fields": [], "ticket_tags": []}
    for tid in sorted(latest):
        ticket, fields, tags = latest[tid]
        rows["tickets"].append(ticket)
        rows["ticket_fields"] += fields
        rows["ticket_tags"] += tags
    tables = {k: pa.Table.from_pylist(v, schema=SCHEMAS[k])
              for k, v in rows.items()}
    return tables, read


def _since(con, day: date, overlap: timedelta,
           start: str) -> tuple[str, str]:
    """Where this load starts, and the watermark it starts from: the
    last good load before this day's, less the overlap; or `start`."""
    before = [x for x in loadlog.history(con, SOURCE, "tickets")
              if x.status == "succeeded" and x.load_date < day]
    if not before:
        return start, start
    mark = max(before, key=lambda x: (x.load_date, x.started_at))
    wm = mark.extra["watermark"]
    since = datetime.fromisoformat(wm.rstrip("Z")) - overlap
    return since.strftime("%Y-%m-%dT%H:%M:%SZ"), wm


def load(load_date, *, run_at: datetime, base: str | None = None,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         overlap: timedelta = OVERLAP, start: str = BEGINNING,
         client: Client | None = None) -> loadlog.Load:
    """
    Pull, land and parse one night's changed tickets, logging it.

    `base` is the API's address; a `client` may be passed instead (a
    test passes one that does not sleep). `start` is where the very
    first load begins. Raises if the pull fails or does not
    reconcile; the load log records which, and why.
    """
    clock = time.perf_counter()

    def now():                      # the run's moment, plus elapsed
        return run_at + timedelta(seconds=time.perf_counter() - clock)

    day = date.fromisoformat(str(load_date))
    root = Path(root) if root is not None else RAW
    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    mine, client = client is None, client or Client(base)
    tried = len(client.log)
    load_id = loadlog.start(con, SOURCE, "tickets", day,
                            started_at=now())
    folder = (root / SOURCE / "pages" / f"load_date={day}"
              / f"attempt-{load_id.rsplit('.', 1)[1]}")
    counts, extra = {}, {"token": client.fingerprint,
                         "overlap_s": int(overlap.total_seconds())}
    try:
        since, was = _since(con, day, overlap, start)
        extra["since"] = since
        before = total(client, since)
        pages = [land(folder, n, body)            # land first ...
                 for n, body in enumerate(pull(client, since), 1)]
        counts["source_count"] = after = total(client, since)
        tables, counts["rows_read"] = parse(pages)   # ... parse second
        got = tables["tickets"].num_rows
        if not before <= got <= after:
            raise LoadError(f"{got:,} tickets; the API said "
                            f"{before:,} before and {after:,} after")
        marks = tables["tickets"].column("updated_at").to_pylist()
        mark = max(marks, default=was)       # nothing new: unmoved
        log = client.log[tried:]
        extra.update(pages=len(pages), total_before=before,
                     watermark=mark, requests=len(log),
                     retries=sum(1 for *_, wait in log if wait))
        for name, tbl in tables.items():
            path = write_partition(
                tbl, SOURCE, name, day, loaded_at=run_at,
                load_id=load_id, root=root,
                source_file=folder.relative_to(root).as_posix())
            n = pq.ParquetFile(path).metadata.num_rows
            if name == "tickets":
                counts["rows_written"] = n
            else:
                extra[name] = n
    except Exception as e:
        loadlog.fail(con, load_id, finished_at=now(), extra=extra,
                     error=f"{type(e).__name__}: {e}", **counts)
        con.close()
        raise
    finally:
        if mine:
            client.close()
    loadlog.finish(con, load_id, finished_at=now(), extra=extra,
                   **counts)
    raw_views(con, root)
    (done,) = [x for x in loadlog.history(con, SOURCE, "tickets")
               if x.load_id == load_id]
    con.close()
    return done


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--at", default=NOW.strftime("%Y-%m-%d %H:%M"),
                   help="the run's moment, UTC (default: NOW)")
    p.add_argument("--load-date", type=date.fromisoformat,
                   help="the logical date (default: the day before)")
    p.add_argument("--rate-limit", type=int, default=0,
                   help="local stand-in only: 429 every Nth request")
    p.add_argument("--fail-every", type=int, default=0,
                   help="local stand-in only: 503 every Nth request")
    a = p.parse_args(argv)
    run_at = datetime.fromisoformat(a.at)
    day = a.load_date or (run_at.date() - timedelta(days=1))
    base = os.environ.get("FRONTLINE_URL")
    if not base:                      # Meridian's desk, served locally
        from meridian.sources import Clock, frontline
        base = frontline.serve(Clock(run_at), rate_limit=a.rate_limit,
                               fail_every=a.fail_every)
    x = load(day, run_at=run_at, base=base)
    e = x.extra
    print(f"{x.load_id}  {x.status}")
    print(f"  since {e['since']}  watermark {e['watermark']}")
    print(f"  {e['pages']} pages, {e['requests']} requests, "
          f"{e['retries']} retried")
    print(f"  tickets {x.rows_written:,} (API total "
          f"{x.source_count:,}), fields {e['ticket_fields']:,}, "
          f"tags {e['ticket_tags']:,}")
    print(f"  token {e['token']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
