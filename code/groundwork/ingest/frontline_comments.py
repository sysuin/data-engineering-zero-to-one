"""
Frontline's ticket comments: one request per ticket, for the tickets a
night's pull saw change.

The desk's API lists tickets in pages but keeps comments behind each
ticket (GET /api/v2/tickets/{id}/comments). So after Chapter 8's
loader has written a night's tickets, this loader asks for the
comments of every ticket in that partition and writes them, as the
API sent them, to

    raw/frontline/ticket_comments/load_date=YYYY-MM-DD/part-0.parquet

one row per comment, with the ticket it belongs to. A comment does not
change a ticket's updated_at, so a comment added to a ticket nobody
else touched waits until the ticket next changes. Bodies are kept, as
raw keeps everything: they are customers' words, and some hold a phone
number (Chapter 24).

    load(load_date, run_at=..., base=...)    one night's comments
    python -m groundwork.ingest.frontline_comments [--load-date D]
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from groundwork import loadlog
from groundwork.config import NOW, RAW, WAREHOUSE
from groundwork.ingest.frontline import TICKETS, Client
from groundwork.storage import raw_views, write_partition

SOURCE, TABLE = "frontline", "ticket_comments"
SCHEMA = pa.schema([("comment_id", pa.int64()),
                    ("ticket_id", pa.string()),
                    ("created_at", pa.string()),
                    ("author", pa.string()),
                    ("public", pa.bool_()),
                    ("body", pa.string())])


def tickets(con, day: date) -> list[str]:
    """The tickets the night's pull wrote, in order."""
    return [t for (t,) in con.execute(
        "SELECT DISTINCT ticket_id FROM raw.frontline_tickets "
        "WHERE load_date = ? ORDER BY 1", [day]).fetchall()]


def load(load_date, *, run_at: datetime, base: str | None = None,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         client: Client | None = None) -> loadlog.Load:
    """Every comment on the tickets pulled for load_date, logged."""
    clock = time.perf_counter()

    def now():
        return run_at + timedelta(seconds=time.perf_counter() - clock)

    day = date.fromisoformat(str(load_date))
    root = Path(root) if root is not None else RAW
    con = duckdb.connect(str(warehouse))
    mine, client = client is None, client or Client(base)
    load_id = loadlog.start(con, SOURCE, TABLE, day, started_at=now())
    try:
        ids = tickets(con, day)
        rows = []
        for tid in ids:
            r = client.get(f"{TICKETS}/{tid}/comments", {})
            rows += [{"comment_id": c["id"], "ticket_id": tid,
                      "created_at": c["created_at"],
                      "author": c.get("author"),
                      "public": c.get("public"), "body": c.get("body")}
                     for c in r.json()["comments"]]
        tbl = pa.Table.from_pylist(rows, schema=SCHEMA)
        write_partition(tbl, SOURCE, TABLE, day, loaded_at=run_at,
                        load_id=load_id, root=root,
                        source_file=f"{TICKETS}/{{id}}/comments")
    except Exception as e:
        loadlog.fail(con, load_id, finished_at=now(),
                     error=f"{type(e).__name__}: {e}")
        con.close()
        raise
    finally:
        if mine:
            client.close()
    loadlog.finish(con, load_id, finished_at=now(), rows_read=len(rows),
                   rows_written=tbl.num_rows,
                   extra={"tickets": len(ids)})
    raw_views(con, root)
    (done,) = [x for x in loadlog.history(con, SOURCE, TABLE)
               if x.load_id == load_id]
    con.close()
    return done


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--at", default=NOW.strftime("%Y-%m-%d %H:%M"),
                   help="the run's moment, UTC (default: NOW)")
    p.add_argument("--load-date", type=date.fromisoformat,
                   help="the logical date (default: the day before)")
    a = p.parse_args(argv)
    run_at = datetime.fromisoformat(a.at)
    day = a.load_date or (run_at.date() - timedelta(days=1))
    base = os.environ.get("FRONTLINE_URL")
    if not base:                      # Meridian's desk, served locally
        from meridian.sources import Clock, frontline
        base = frontline.serve(Clock(run_at))
    x = load(day, run_at=run_at, base=base)
    print(f"{x.load_id}  {x.status}")
    print(f"  tickets asked {x.extra['tickets']:,}, "
          f"comments {x.rows_written:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
