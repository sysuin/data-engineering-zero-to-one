"""
A request to be forgotten, carried out across every layer, proved, and
recorded without recording the person.

Meridian's customers are businesses, and their contacts are people.
A request comes from a contact: forget me. The account stays (its
orders and contracts are Meridian's records, which the law requires it
to keep); the person's name, email, phone, words and clicks go.

    subject(con, account_id)    who the contact is, in every spelling
                                Groundwork holds: the CRM ids that
                                resolve to the account, the name both
                                ways round, email, phone, portal login,
                                and the tickets they raised
    find(con, s, root, landed)  every place they appear, as Places
    erase(con, s, root, landed) redact or delete each one; rebuild what
                                is built from them
    sweep(con, s, root, landed) look everywhere again, by identifier
    record(con, ...)            audit.forget_requests and _actions, and
                                the suppression list, which holds keyed
                                hashes and no personal data
    resurfaced(con)             rows a restore or a reload brought back,
                                recognised by their keyed hashes

What is redacted and what is deleted:

    Relay contact columns, Frontline requester columns
                                redacted to [erased]: the account and
                                the ticket stay
    comments the person wrote   author and body [erased]: their words
    the ticket's payload        the requester inside it [erased]
    raw pages from the API      the same, in the JSON files
    portal events               deleted: they are only the person's
    archived Relay exports      the contact's fields [erased] in the
                                file; its name, the hash of what
                                arrived, no longer matches its content,
                                and the request says why

Raw keeps what the source sent, and this is the one exception to that
rule: a request to be forgotten outranks the record of what arrived.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from groundwork import storage
from groundwork.govern import masking

ERASED = "[erased]"


@dataclass
class Subject:
    account_id: int
    crm_ids: list[int]
    names: list[str]
    email: str
    phone: str
    user_ids: list[str]
    tickets: list[str] = field(default_factory=list)

    def strong(self) -> list[tuple[str, str]]:
        """Identifiers that belong to this person alone."""
        return ([("email", self.email)]
                + [("portal login", u) for u in self.user_ids])

    def weak(self) -> list[tuple[str, str]]:
        """Identifiers other people can share: a phone, a name."""
        return [("phone", self.phone)] + [("name", n) for n in self.names]


@dataclass(frozen=True)
class Place:
    layer: str          # landed, raw, raw files, warehouse
    location: str       # a relation or a folder
    rows: int           # rows, or for files the files
    action: str         # redact, delete, rebuild, none


def subject(con, account_id: int) -> Subject:
    """The contact of one Relay account, in every form Groundwork has."""
    ids = [i for (i,) in con.execute(
        "SELECT crm_id FROM intermediate.int_account_identity"
        " WHERE account_id = ? ORDER BY crm_id", [account_id]).fetchall()]
    marks = ", ".join(str(int(i)) for i in ids)
    name, email, phone = con.execute(f"""
        SELECT contact_name, contact_email, contact_phone
        FROM raw.relay_accounts
        WHERE CAST(account_id AS INTEGER) = {int(account_id)}
        ORDER BY load_date DESC LIMIT 1""").fetchone()
    first, _, last = name.rpartition(" ")
    users = [u for (u,) in con.execute(
        f"SELECT DISTINCT user_id FROM raw.portal_events"
        f" WHERE account_id IN ({marks}) ORDER BY 1").fetchall()]
    tickets = [t for (t,) in con.execute(
        f"SELECT DISTINCT ticket_id FROM raw.frontline_tickets"
        f" WHERE requester_account_id IN ({marks})"
        f" OR lower(requester_email) = lower(?) ORDER BY 1",
        [email]).fetchall()]
    return Subject(account_id, ids, [name, f"{last}, {first}"], email,
                   phone, users, tickets)


# The raw tables a person lives in, how a row is tied to them, and what
# happens to it. Staging is views over raw and follows on its own.
def _ids(s): return ", ".join(f"'{i}'" for i in s.crm_ids)
def _list(xs): return ", ".join("'" + x.replace("'", "''") + "'"
                                for x in xs) or "NULL"


TIES = {
    "relay_accounts": (
        lambda s: f"CAST(account_id AS INTEGER) IN ({_ids(s)})",
        "redact", ("contact_name", "contact_email", "contact_phone")),
    "frontline_tickets": (
        lambda s: f"requester_account_id IN ({_ids(s)})"
                  f" OR lower(requester_email) = lower('{s.email}')",
        "redact", ("requester_name", "requester_email", "payload")),
    "frontline_ticket_comments": (
        lambda s: f"ticket_id IN ({_list(s.tickets)})"
                  f" AND author IN ({_list(s.names)})",
        "redact", ("author", "body")),
    "portal_events": (
        lambda s: f"user_id IN ({_list(s.user_ids)})", "delete", ()),
}
PAYLOAD = ("json_merge_patch(payload, '{\"requester\": {\"name\": "
           f"\"{ERASED}\", \"email\": \"{ERASED}\"}}}}')")


def _value(column: str) -> str:
    if column == "payload":
        return "json_extract_string(payload, '$.requester.email')"
    return column


def _parts(root, table: str) -> list[Path]:
    source, name = table.split("_", 1)
    folder = Path(root) / source / name
    return sorted(folder.glob(f"load_date=*/{storage.PART}"))


def _rewrite(con, path: Path, select: str) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        con.execute(f"COPY ({select}) TO '{tmp}' "
                    "(FORMAT parquet, COMPRESSION zstd)")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _raw(con, s: Subject, root, apply: bool) -> list[Place]:
    out = []
    for table, (tie, action, cols) in TIES.items():
        where = tie(s)
        if action == "redact":         # tied, and not yet erased
            held = " OR ".join(
                f"{_value(c)} IS DISTINCT FROM '{ERASED}'" for c in cols)
            where = f"({where}) AND ({held})"
        total = 0
        for path in _parts(root, table):
            src = f"read_parquet('{path}')"
            (n,) = con.execute(
                f"SELECT count(*) FROM {src} WHERE {where}").fetchone()
            total += n
            if not (n and apply):
                continue
            if action == "delete":
                select = f"SELECT * FROM {src} WHERE NOT ({where})"
            else:
                swap = ", ".join(
                    f"CASE WHEN {where} THEN "
                    + (PAYLOAD if c == "payload" else f"'{ERASED}'")
                    + f" ELSE {c} END AS {c}" for c in cols)
                select = f"SELECT * REPLACE ({swap}) FROM {src}"
            _rewrite(con, path, select)
        if total:
            out.append(Place("raw", f"raw.{table}", total, action))
    return out


def _pages(s: Subject, root, apply: bool) -> list[Place]:
    ids = set(s.crm_ids)
    out = []
    folder = Path(root) / "frontline" / "pages"
    for attempt in sorted(folder.glob("load_date=*/attempt-*")):
        files = rows = 0
        for page in sorted(attempt.glob("*.json")):
            body = json.loads(page.read_text())
            hit = [t for t in body.get("tickets", [])
                   if (t["requester"]["account_id"] in ids
                       or t["requester"]["email"].lower()
                       == s.email.lower())
                   and t["requester"]["email"] != ERASED]
            if not hit:
                continue
            files, rows = files + 1, rows + len(hit)
            if apply:
                for t in hit:
                    t["requester"]["name"] = ERASED
                    t["requester"]["email"] = ERASED
                page.write_text(json.dumps(body, ensure_ascii=False))
        if files:
            day = attempt.parent.name.split("=")[1]
            out.append(Place("raw files", f"raw pages {day} {attempt.name}"
                             f" ({files} files)", rows, "redact"))
    return out


def _read_export(path: Path) -> tuple[str, str, str]:
    data = gzip.decompress(path.read_bytes())
    try:
        return data.decode("utf-8-sig"), "utf-8-sig", ","
    except UnicodeDecodeError:
        return data.decode("cp1252"), "cp1252", ";"


def _landed(con, s: Subject, landed, apply: bool) -> list[Place]:
    ids = {str(i) for i in s.crm_ids}
    shas = sorted({sha for (sha,) in con.execute(
        "SELECT sha256 FROM audit.file_ledger WHERE source = 'relay'"
        " AND table_name = 'accounts'").fetchall()})
    files = rows = 0
    for sha in shas:
        path = Path(landed) / "relay" / sha[:2] / f"{sha}.gz"
        if not path.exists():
            continue
        text, encoding, sep = _read_export(path)
        lines = text.splitlines(keepends=True)
        header = next(csv.reader([lines[0]], delimiter=sep))
        contact = [i for i, h in enumerate(header)
                   if "contact" in h.lower()]
        changed = 0
        for k, line in enumerate(lines[1:], 1):
            row = next(csv.reader([line], delimiter=sep))
            if row and row[0] in ids and any(
                    row[i] != ERASED for i in contact):
                changed += 1
                for i in contact:
                    row[i] = ERASED
                buf = io.StringIO()
                csv.writer(buf, delimiter=sep, lineterminator="").writerow(
                    row)
                end = line[len(line.rstrip("\r\n")):]
                lines[k] = buf.getvalue() + end
        if changed:
            files, rows = files + 1, rows + changed
            if apply:
                out = "".join(lines).encode(encoding)
                path.write_bytes(gzip.compress(out, mtime=0))
    return [Place("landed", "landed/relay (account exports)", rows,
                  "redact")] if rows else []


def find(con, s: Subject, root, landed) -> list[Place]:
    """Every place the person is held, by how Groundwork ties them."""
    return (_landed(con, s, landed, False) + _raw(con, s, root, False)
            + _pages(s, root, False))


def erase(con, s: Subject, root, landed) -> list[Place]:
    """Redact or delete each place; rebuild the masked tables."""
    done = (_landed(con, s, landed, True) + _raw(con, s, root, True)
            + _pages(s, root, True))
    storage.raw_views(con, root=root)
    (masked,) = con.execute(
        "SELECT count(*) FROM information_schema.schemata"
        " WHERE schema_name = 'masked'").fetchone()
    if masked:
        rows = masking.build(con)
        done.append(Place("warehouse", "masked.* (rebuilt)",
                          sum(rows.values()), "rebuild"))
    return done


def _like(value: str) -> str:
    return "%" + value.lower().replace("'", "''") + "%"


def sweep(con, s: Subject, root, landed,
          kinds=("strong", "weak")) -> dict[str, dict[str, int]]:
    """
    Look for the person everywhere, by identifier rather than by tie:
    every text column of every relation in the warehouse, and every
    file under raw and landed that is not Parquet (which raw's views
    already cover). Returns {place: {identifier kind: rows or files}}.
    """
    wanted = []
    if "strong" in kinds:
        wanted += s.strong()
    if "weak" in kinds:
        wanted += s.weak()
    found: dict[str, dict[str, int]] = {}
    for schema, name in con.execute(
            "SELECT table_schema, table_name FROM information_schema"
            ".tables WHERE table_schema NOT LIKE 'role_%'"
            " AND table_catalog = current_database()"
            " ORDER BY 1, 2").fetchall():
        cols = [c for (c,) in con.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = ? AND table_name = ?"
            " AND data_type IN ('VARCHAR', 'JSON')",
            [schema, name]).fetchall()]
        if not cols:
            continue
        parts = []
        for kind, value in wanted:
            cond = " OR ".join(f"lower(CAST(\"{c}\" AS VARCHAR)) LIKE "
                               f"'{_like(value)}'" for c in cols)
            parts.append(f"count(*) FILTER (WHERE {cond})")
        counts = con.execute(
            f"SELECT {', '.join(parts)} FROM {schema}.{name}").fetchone()
        for (kind, _), n in zip(wanted, counts):
            if n:
                where = found.setdefault(f"{schema}.{name}", {})
                where[kind] = where.get(kind, 0) + n
    for top in (Path(root), Path(landed)):
        for path in sorted(top.rglob("*")):
            if not path.is_file() or path.suffix == ".parquet":
                continue
            data = path.read_bytes()
            if path.suffix == ".gz":
                data = gzip.decompress(data)
            text = data.decode("utf-8", errors="replace").lower()
            if path.suffix == ".gz" and "�" in text:
                text = data.decode("cp1252", errors="replace").lower()
            for kind, value in wanted:
                if value.lower() in text:
                    where = found.setdefault(
                        str(path.parent.relative_to(top.parent)), {})
                    where[kind] = where.get(kind, 0) + 1
    return found


def outside(s: Subject, folders) -> dict[str, int]:
    """Files in other teams' systems that still hold the person."""
    out = {}
    for folder in folders:
        n = 0
        for path in sorted(Path(folder).rglob("*")):
            if not path.is_file():
                continue
            data = path.read_bytes()
            if path.suffix == ".gz":
                data = gzip.decompress(data)
            text = data.decode("utf-8", errors="replace").lower()
            n += any(v.lower() in text for _, v in s.strong())
        out[str(folder)] = n
    return out


AUDIT = """
CREATE TABLE IF NOT EXISTS audit.forget_requests (
    request_id VARCHAR PRIMARY KEY, received_at TIMESTAMP,
    completed_at TIMESTAMP, subject_ref VARCHAR, places INTEGER,
    rows_redacted BIGINT, rows_deleted BIGINT, left_strong BIGINT,
    left_weak BIGINT, outside VARCHAR);
CREATE TABLE IF NOT EXISTS audit.forget_actions (
    request_id VARCHAR, layer VARCHAR, location VARCHAR,
    action VARCHAR, rows BIGINT);
CREATE TABLE IF NOT EXISTS audit.forget_suppress (
    request_id VARCHAR, kind VARCHAR, ref VARCHAR);
"""


def record(con, request_id: str, s: Subject, received_at: datetime,
           completed_at: datetime, done: list[Place],
           left: dict[str, dict[str, int]], outside: str,
           secret: bytes | None = None) -> None:
    """The request in the audit tables, with keyed hashes, not values."""
    con.execute(AUDIT)
    strong = {k for k, _ in s.strong()}
    n_strong = sum(n for w in left.values() for k, n in w.items()
                   if k in strong)
    n_weak = sum(n for w in left.values() for k, n in w.items()
                 if k not in strong)
    con.execute(
        "INSERT INTO audit.forget_requests VALUES (?, ?, ?, ?, ?, ?, ?,"
        " ?, ?, ?)",
        [request_id, received_at, completed_at,
         masking.pseudonym(s.email, secret), len(done),
         sum(p.rows for p in done if p.action == "redact"),
         sum(p.rows for p in done if p.action == "delete"),
         n_strong, n_weak, outside])
    con.executemany(
        "INSERT INTO audit.forget_actions VALUES (?, ?, ?, ?, ?)",
        [(request_id, p.layer, p.location, p.action, p.rows)
         for p in done])
    refs = s.strong() + [("crm id", str(i)) for i in s.crm_ids]
    con.executemany(
        "INSERT INTO audit.forget_suppress VALUES (?, ?, ?)",
        [(request_id, kind, masking.pseudonym(v, secret))
         for kind, v in refs])


# Where a forgotten person could come back from, and the column that
# would show it: a restored partition, a reload, a source that did not
# delete them.
WATCH = (("raw.relay_accounts", "contact_email", "email"),
         ("raw.frontline_tickets", "requester_email", "email"),
         ("raw.portal_events", "user_id", "portal login"))


def resurfaced(con, secret: bytes | None = None) -> dict[str, int]:
    """Rows whose keyed hash is on the suppression list."""
    secret = masking.key() if secret is None else secret
    fn = masking.udf(con, lambda v: masking.pseudonym(v, secret))
    out = {}
    try:
        for relation, column, kind in WATCH:
            (n,) = con.execute(f"""
                SELECT count(*) FROM {relation}
                WHERE {column} <> '{ERASED}' AND {fn}({column}) IN
                  (SELECT ref FROM audit.forget_suppress
                   WHERE kind = '{kind}')""").fetchone()
            out[relation] = n
    finally:
        con.remove_function(fn)
    return out


def reapply(con, root, landed, secret: bytes | None = None) -> list[Place]:
    """
    Forget again whoever a restore or a reload brought back. The
    suppression list cannot name them; the rows that came back can.
    """
    secret = masking.key() if secret is None else secret
    fn = masking.udf(con, lambda v: masking.pseudonym(v, secret))
    try:
        accounts = [a for (a,) in con.execute(f"""
            SELECT DISTINCT i.account_id
            FROM raw.relay_accounts AS r
            JOIN intermediate.int_account_identity AS i
              ON i.crm_id = CAST(r.account_id AS INTEGER)
            WHERE r.contact_email <> '{ERASED}'
              AND {fn}(r.contact_email) IN (SELECT ref FROM
                  audit.forget_suppress WHERE kind = 'email')
            UNION
            SELECT DISTINCT i.account_id
            FROM raw.frontline_tickets AS t
            JOIN intermediate.int_account_identity AS i
              ON i.crm_id = t.requester_account_id
            WHERE t.requester_email <> '{ERASED}'
              AND {fn}(t.requester_email) IN (SELECT ref FROM
                  audit.forget_suppress WHERE kind = 'email')
            ORDER BY 1""").fetchall()]
    finally:
        con.remove_function(fn)
    done = []
    for account in accounts:
        done += erase(con, subject(con, account), root, landed)
    return done
