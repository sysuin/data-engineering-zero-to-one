"""
Holding a batch to its contract: the header, every row, and the
batch as a whole.

Three kinds of finding, and what each does to a load:

    breaking   the batch is not what the contract describes: a column
               missing or renamed, a header no format allows, a value
               the reader made up, or more of its rows failing than
               the contract's max_bad_share allows. The load fails,
               with the reason, and nothing from the batch lands.
    bad rows   a row breaks a column's promise (empty where it may
               not be, not a number, outside its range, a value not
               allowed, a cost far from the last list's). Under
               policy `quarantine` the row is parked in
               audit.quarantine with the reason and the rest load;
               under policy `fail` any bad row breaks the batch.
    notes      a change that breaks nobody: an added column, columns
               in a new order. Recorded, not acted on.

    check(contract, rows, header=..., note=..., history=...) -> Result
    Guard(read, contract, con=...)     wraps a file reader of Ch 9
    park(con, ...), record(con, ...)   audit.quarantine, contract_runs
    audit(con, table)                  hold what raw already holds
    python -m groundwork.contracts.check [TABLE ...] [--reopen]
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

import duckdb
import pyarrow as pa

from groundwork import contracts
from groundwork.contracts import Contract

DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.quarantine (
    source        VARCHAR NOT NULL,
    table_name    VARCHAR NOT NULL,
    ref           VARCHAR NOT NULL,   -- file sha256, or the load id
    row_number    INTEGER NOT NULL,   -- its place in the file
    rule          VARCHAR NOT NULL,   -- column: rule, the first broken
    reason        VARCHAR NOT NULL,   -- every rule it broke, in words
    row_json      VARCHAR NOT NULL,   -- the row, as it arrived
    file          VARCHAR,
    load_id       VARCHAR,
    load_date     DATE,
    contract_version INTEGER,
    parked_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (source, table_name, ref, row_number)
);
CREATE TABLE IF NOT EXISTS audit.contract_runs (
    load_id       VARCHAR NOT NULL,
    ref           VARCHAR NOT NULL,   -- file sha256, or the load id
    source        VARCHAR NOT NULL,
    table_name    VARCHAR NOT NULL,
    name          VARCHAR,            -- the file, where there is one
    checked_at    TIMESTAMP NOT NULL,
    contract_version INTEGER NOT NULL,
    format        VARCHAR,
    rows          BIGINT NOT NULL,
    parked        BIGINT NOT NULL,
    status        VARCHAR NOT NULL
                  CHECK (status IN ('passed', 'parked', 'broken')),
    detail        VARCHAR,
    PRIMARY KEY (load_id, ref)
);
"""
MISSING = "nullif(trim({v}), '') IS NULL"


class ContractBroken(ValueError):
    """A batch that is not what its contract describes."""


@dataclass(frozen=True)
class Bad:
    row: int                  # 1-based, in the batch's own order
    column: str
    rule: str
    reason: str


@dataclass
class Result:
    contract: Contract
    rows: int
    bad: list = field(default_factory=list)
    breaking: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    format: str | None = None

    @property
    def failing(self) -> dict:
        """Row number -> its findings, for every row that failed."""
        out: dict = {}
        for b in self.bad:
            out.setdefault(b.row, []).append(b)
        return out

    @property
    def status(self) -> str:
        if self.breaking:
            return "broken"
        return "parked" if self.bad else "passed"

    @property
    def parked(self) -> int:
        return 0 if self.breaking else len(self.failing)

    def keep(self, rows: list) -> list:
        """The rows that passed."""
        out = self.failing
        return [r for i, r in enumerate(rows, 1) if i not in out]


# ------------------------------------------------------------ headers
def header_of(data: bytes) -> list[str]:
    """The column names a file arrived with: a CSV's first line (comma
    or semicolon, UTF-8 or Windows-1252), or a spreadsheet's header
    row, the first with two or more names in it."""
    if data[:2] == b"PK":
        import openpyxl
        book = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        for row in book.worksheets[0].iter_rows(values_only=True):
            names = [v for v in row if isinstance(v, str)]
            if len(names) >= 2 and len(names) == sum(
                    v is not None for v in row):
                return names
        return []
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("cp1252")
    first = text.split("\n", 1)[0].rstrip("\r")
    sep = ";" if first.count(";") > first.count(",") else ","
    return next(csv.reader([first], delimiter=sep), [])


def drift(expected, header) -> tuple[str, list, list]:
    """How a header differs from the one expected: (kind, missing,
    added). Kinds: same, reordered, added, removed, renamed, changed.
    Only 'same', 'reordered' and 'added' leave every promised column
    where a reader by name will find it."""
    exp, got = list(expected), list(header)
    missing = [c for c in exp if c not in got]
    added = [c for c in got if c not in exp]
    if not missing and not added:
        return ("same" if exp == got else "reordered"), [], []
    if not missing:
        return "added", [], added
    if not added:
        return "removed", missing, []
    if len(missing) == len(added):
        return "renamed", missing, added
    return "changed", missing, added


def match(contract: Contract, header) -> tuple[str | None, str, list,
                                               list]:
    """The format a header belongs to, and how it drifted from it:
    an exact match if there is one, else the nearest by shared names."""
    best = None
    for name, expected in contract.formats.items():
        kind, missing, added = drift(expected, header)
        if kind in ("same", "reordered", "added"):
            rank = (0, len(added), kind != "same")
        else:
            rank = (1, len(missing) + len(added), 0)
        if best is None or rank < best[0]:
            best = (rank, name, kind, missing, added)
    if best is None:
        return None, "same", [], []
    return best[1:]


def schema(contract: Contract, header, note=None) -> tuple[str | None,
                                                         list, list]:
    """(format, breaking, notes) for a file's header."""
    if not contract.formats:
        return None, [], []
    fmt, kind, missing, added = match(contract, header)
    breaking, notes = [], []
    if kind in ("removed", "renamed", "changed"):
        what = {"removed": "removed", "renamed": "renamed?",
                "changed": "changed"}[kind]
        breaking.append(f"header {what}: expected {fmt}'s "
                        f"{_names(missing)}, found {_names(added)}"
                        if added else
                        f"header {what}: {fmt}'s {_names(missing)} "
                        "missing")
    elif kind == "added":
        notes.append(f"added {_names(added)}: not loaded")
    elif kind == "reordered":
        notes.append("columns in a new order")
    if (not breaking and note in contract.formats and note != fmt):
        breaking.append(f"read as {note}, but the header is {fmt}'s")
    return fmt, breaking, notes


def _names(cols) -> str:
    return ", ".join(repr(c) for c in cols) or "nothing"


# ------------------------------------------------------------ rows
def _rules(col: str, spec: dict) -> list[tuple[str, str, str]]:
    """(rule, SQL that is true when the row breaks it, reason SQL)."""
    v = f'"{col}"'
    shown = f"'''' || {v} || ''''"
    present = f"NOT ({MISSING.format(v=v)})"
    out = []
    if not spec.get("nullable"):
        out.append(("missing", MISSING.format(v=v), "'is empty'"))
    kind, fmt = spec.get("type", "text"), spec.get("format")
    ok = None
    if kind == "integer":
        ok = f"regexp_full_match(trim({v}), '-?[0-9]+')"
        out.append(("type", f"{present} AND NOT {ok}",
                    f"{shown} || ' is not a whole number'"))
    elif kind == "decimal":
        s = spec.get("scale") or 6
        ok = f"regexp_full_match(trim({v}), '-?[0-9]+(\\.[0-9]{{1,{s}}})?')"
        out.append(("type", f"{present} AND NOT {ok}",
                    f"{shown} || ' is not a number with at most {s} "
                    "decimals'"))
    elif kind in ("date", "timestamp"):
        cast = (f"try_strptime({v}, '{fmt}')" if fmt else
                f"TRY_CAST({v} AS {kind.upper()})")
        out.append(("type", f"{present} AND {cast} IS NULL",
                    f"{shown} || ' is not a {kind}"
                    + (f" like {fmt}'" if fmt else "'")))
    elif kind == "boolean":
        out.append(("type", f"{present} AND {v} NOT IN ('true', 'false')",
                    f"{shown} || ' is not true or false'"))
    if spec.get("allowed"):
        vals = ", ".join("'" + a.replace("'", "''") + "'"
                         for a in spec["allowed"])
        out.append(("allowed", f"{present} AND {v} NOT IN ({vals})",
                    f"{shown} || ' is not an allowed value'"))
    if spec.get("pattern"):
        pat = spec["pattern"].replace("'", "''")
        out.append(("pattern",
                    f"{present} AND NOT regexp_full_match({v}, '{pat}')",
                    f"{shown} || ' does not look like {pat}'"))
    num = f"TRY_CAST(trim({v}) AS DECIMAL(38, 6))"
    guard = f"{present} AND {ok}" if ok else present
    for side, op in (("min", "<"), ("max", ">")):
        if spec.get(side) is not None:
            lim = spec[side]
            out.append(("range", f"{guard} AND {num} {op} {lim}",
                        f"{shown} || ' is {'below' if side == 'min' else 'above'} "
                        f"{lim}'"))
    return out


def rules(contract: Contract, present) -> list[tuple]:
    """Every (column, rule, broken-when SQL, reason SQL) for the
    columns a batch has; a `when` refinement applies where its column
    holds that value, and the base rules everywhere else."""
    out = []
    for name, col in contract.columns.items():
        if name not in present:
            continue
        cases = list(col.cases())
        if not cases:
            out += [(name, r, w, why) for r, w, why in
                    _rules(name, col.spec())]
            continue
        by = cases[0][0]
        seen = []
        for _, value, spec in cases:
            q = value.replace("'", "''")
            seen.append(f"'{q}'")
            out += [(name, r, f'"{by}" = \'{q}\' AND ({w})', why)
                    for r, w, why in _rules(name, spec)]
        rest = f'"{by}" IS NULL OR "{by}" NOT IN ({", ".join(seen)})'
        out += [(name, r, f"({rest}) AND ({w})", why)
                for r, w, why in _rules(name, col.spec())]
    return out


def _text_table(rows) -> pa.Table:
    """Every column as text, plus _row, the 1-based position."""
    if isinstance(rows, pa.Table):
        t = rows
    else:
        names = list(dict.fromkeys(k for r in rows for k in r))
        t = pa.table({n: pa.array([None if r.get(n) is None else
                                   str(r.get(n)) for r in rows],
                                  pa.string()) for n in names})
    cols = {n: t.column(n).cast(pa.string()) for n in t.column_names}
    cols["_row"] = pa.array(range(1, t.num_rows + 1), pa.int64())
    return pa.table(cols)


_SCRATCH = []


def _scratch():
    """One in-memory DuckDB for checking batches: opening one costs
    more than checking a small file."""
    if not _SCRATCH:
        _SCRATCH.append(duckdb.connect())
    return _SCRATCH[0]


def _query(contract: Contract, names: list, table: str) -> str:
    """One pass over a batch: each rule is a CASE that names the rule
    the row broke, or is NULL; rows that broke none are left out."""
    cases = [f"CASE WHEN {w} THEN ['{c}', '{r}', '{c}: ' || {why}] END"
             for c, r, w, why in rules(contract, names)]
    keys = [k for k in contract.key if k in names]
    if keys and len(keys) == len(contract.key):
        k = ", ".join(f'"{x}"' for x in keys)
        cases.append(f"CASE WHEN count(*) OVER (PARTITION BY {k}) > 1 "
                     f"THEN ['{keys[-1]}', 'key', '{'+'.join(keys)}: "
                     "appears more than once'] END")
    if not cases:
        return ""
    return ("SELECT _row, unnest(found) FROM (SELECT _row, list_filter(["
            + ", ".join(cases) + "], x -> x IS NOT NULL) AS found "
            f"FROM {table}) WHERE len(found) > 0 ORDER BY _row")


def check_rows(contract: Contract, rows, con=None) -> list[Bad]:
    """Every rule every row breaks, in row order."""
    t = _text_table(rows)
    sql = _query(contract, t.column_names, "_batch")
    if t.num_rows == 0 or not sql:
        return []
    db = con or _scratch()
    db.register("_batch", t)
    try:
        found = db.execute(sql).fetchall()
    finally:
        db.unregister("_batch")
    out = [Bad(r, *f) for r, f in found]
    return sorted(out, key=lambda b: (b.row, b.column, b.rule))


# ------------------------------------------------------------ history
class History:
    """
    The last value each key had, by the order column: a supplier's
    cost for a product on the list before. A value more than
    max_ratio times the last one, or less than a max_ratio-th of it,
    is a bad row: a typing slip, or a unit that changed.
    """

    def __init__(self, contract: Contract):
        self.c = contract
        self.h = contract.history or {}
        self.last: dict = {}

    def _value(self, row) -> Decimal | None:
        raw = row.get(self.h["value"])
        scale = self.h.get("scale", {})
        try:
            v = Decimal(str(raw).strip())
        except (InvalidOperation, TypeError):
            return None
        if scale:
            factor = scale.get(row.get(scale["column"]))
            if factor is None:
                return None
            v *= Decimal(str(factor))
        return v

    def _order(self, row):
        col = self.c.columns[self.h["order"]]
        spec = col.spec()
        for by, value, rule in col.cases():
            if str(row.get(by)) == value:
                spec = rule
        try:
            return datetime.strptime(str(row.get(self.h["order"])),
                                     spec["format"])
        except (TypeError, ValueError):
            return None

    def _key(self, row):
        return tuple(row.get(k) for k in self.h["per"])

    def check(self, rows) -> list[Bad]:
        out, col = [], self.h["value"]
        ratio = Decimal(str(self.h.get("max_ratio", 3)))
        for i, row in enumerate(rows, 1):
            v, at = self._value(row), self._order(row)
            if v is None or at is None or v <= 0:
                continue
            before = [(t, p) for t, p in self.last.get(self._key(row),
                                                       []) if t < at]
            if not before:
                continue
            p = max(before)[1]
            r = v / p
            if r > ratio or r < 1 / ratio:
                out.append(Bad(i, col, "history",
                               f"{col}: {v.normalize():f} is {r:.1f} "
                               f"times the last list's "
                               f"{p.normalize():f}"))
        return out

    def add(self, rows) -> None:
        for row in rows:
            v, at = self._value(row), self._order(row)
            if v is not None and at is not None:
                self.last.setdefault(self._key(row), []).append((at, v))

    @classmethod
    def from_raw(cls, con, contract: Contract) -> "History":
        """What raw holds already, through the files in force."""
        h = cls(contract)
        view = f"raw.{contract.source}_{contract.name}"
        try:
            t = con.execute(
                f"SELECT r.* EXCLUDE (load_date) FROM {view} r JOIN "
                "audit.current_files f ON f.sha256 = r.file_sha256 "
                f"AND f.source = '{contract.source}' AND "
                f"f.table_name = '{contract.name}'").to_arrow_table()
        except duckdb.Error:
            return h
        h.add(t.to_pylist())
        return h


# ------------------------------------------------------------ verdict
def check(contract: Contract, rows, *, header=None, note=None,
          history: History | None = None, types: dict | None = None,
          con=None) -> Result:
    """Hold one batch (a file's rows, or a partition) to a contract."""
    n = rows.num_rows if isinstance(rows, pa.Table) else len(rows)
    res = Result(contract, n)
    if header is not None:
        res.format, res.breaking, res.notes = schema(contract, header,
                                                     note)
    cols = (rows.column_names if isinstance(rows, pa.Table)
            else list(dict.fromkeys(k for r in rows for k in r)))
    gone = [c for c, col in contract.columns.items()
            if c not in cols and not col.stamped]
    if n and gone:
        res.breaking.append(f"columns missing from the batch: "
                            f"{_names(gone)}")
    for c, kind in (types or {}).items():
        want = contract.columns.get(c)
        if want and not _fits(kind, want.type):
            res.breaking.append(f"{c}: arrived as {kind}, the contract "
                                f"says {want.type}")
    if res.breaking:
        return res
    res.bad = check_rows(contract, rows, con)
    if history is not None and contract.history:
        listed = rows.to_pylist() if isinstance(rows, pa.Table) else rows
        failed = {b.row for b in res.bad}
        res.bad += [b for b in history.check(listed)
                    if b.row not in failed]
        res.bad.sort(key=lambda b: (b.row, b.column, b.rule))
    k = len(res.failing)
    if k and contract.bad_rows == "fail":
        res.breaking.append(f"{k:,} of {n:,} rows break the contract, "
                            f"and its policy is to fail: "
                            f"{res.bad[0].reason}")
    elif n and k / n > contract.max_bad_share:
        common = Counter(f"{b.column}: {b.rule}" for b in res.bad)
        res.breaking.append(f"{k:,} of {n:,} rows fail (over "
                            f"{contract.max_bad_share:.0%}), most often "
                            f"{common.most_common(1)[0][0]}")
    return res


def _fits(physical: str, declared: str) -> bool:
    p = physical.lower()
    if p in ("string", "large_string", "varchar"):
        return True                      # text: the rows are checked
    if p.startswith(("int", "uint", "bigint")):
        return declared in ("integer", "decimal", "text")
    if p.startswith(("double", "float", "decimal")):
        return declared in ("decimal", "text")
    if p.startswith(("timestamp", "date")):
        return declared in ("date", "timestamp")
    if p in ("bool", "boolean"):
        return declared == "boolean"
    return False


# ------------------------------------------------------------ audit trail
def ensure(con) -> None:
    """Create audit.quarantine and audit.contract_runs. Safe twice."""
    con.execute(DDL)


def park(con, res: Result, rows, *, ref: str, at, file=None,
         load_id=None, load_date=None) -> int:
    """Put a batch's failing rows in audit.quarantine, each with every
    reason it failed; a rerun replaces, never doubles. Returns how many
    rows were parked."""
    ensure(con)
    listed = rows.to_pylist() if isinstance(rows, pa.Table) else rows
    c = res.contract
    out = []
    for i, found in sorted(res.failing.items()):
        out.append([c.source, c.name, ref, i,
                    f"{found[0].column}: {found[0].rule}",
                    "; ".join(b.reason for b in found),
                    json.dumps(listed[i - 1], ensure_ascii=False,
                               default=str), file, load_id, load_date,
                    c.version, at])
    if out:
        con.executemany("INSERT OR REPLACE INTO audit.quarantine "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        out)
    return len(out)


def record(con, res: Result, *, load_id: str, ref: str, at,
           name=None) -> None:
    """One row of audit.contract_runs: what the check found."""
    ensure(con)
    c = res.contract
    detail = "; ".join(res.breaking or res.notes) or None
    con.execute("INSERT OR REPLACE INTO audit.contract_runs VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [load_id, ref, c.source, c.name, name, at, c.version,
                 res.format, res.rows, res.parked, res.status, detail])


# ------------------------------------------------------------ the guard
class Guard:
    """
    A file reader from Chapter 9, held to a contract. Called as the
    reader is, with a file's bytes and its arrival:

    1.  the header is matched to a format the contract allows; a
        rename, a removal or a header no format knows is breaking;
    2.  the reader of Chapter 9 parses the file, unchanged;
    3.  every row is checked, and the batch judged as a whole.

    Breaking raises ContractBroken, which files.load records against
    the file (failed, tried again next run) and the load (failed).
    Otherwise the reader's result comes back with the failing rows
    taken out and its count of lines reduced by the same number, so
    files.load's reconciliation still holds; the rows taken out are
    kept on the guard until flush() parks them.
    """

    def __init__(self, read, contract: Contract, *, con=None,
                 history: History | None = None):
        self.read, self.contract = read, contract
        if history is None and contract.history:
            history = (History.from_raw(con, contract) if con
                       is not None else History(contract))
        self.history = history
        self.seen: list = []              # (arrival, sha, Result, rows)

    def __call__(self, data: bytes, arrival):
        from groundwork.ingest import files
        sha = hashlib.sha256(data).hexdigest()
        header = header_of(data)
        fmt, breaking, _ = schema(self.contract, header)
        if breaking:                     # do not even parse it
            res = Result(self.contract, files.lines(data),
                         breaking=breaking, format=fmt)
            self.seen.append((arrival, sha, res, []))
            raise ContractBroken(f"{arrival.name}: {breaking[0]}")
        p = self.read(data, arrival)
        res = check(self.contract, p.rows, header=header, note=p.note,
                    history=self.history)
        self.seen.append((arrival, sha, res, p.rows))
        if res.breaking:
            raise ContractBroken(f"{arrival.name}: {res.breaking[0]}")
        good = res.keep(p.rows)
        if self.history is not None:
            self.history.add(good)
        return files.Parsed(good, p.counted - res.parked,
                            p.logical_key, p.note)

    def flush(self, con, *, load_id: str, at, load_date=None) -> dict:
        """Park what was taken out, record each file's check, and
        say how it went: {'rows', 'parked', 'broken', 'files'}."""
        ensure(con)
        out = {"files": 0, "rows": 0, "parked": 0, "broken": []}
        latest = {}
        for a, sha, res, rows in self.seen:  # a rebuild re-reads files
            latest[(a.name, sha)] = (a, sha, res, rows)
        for a, sha, res, rows in latest.values():
            out["files"] += 1
            out["rows"] += res.rows
            if res.breaking:
                out["broken"].append(f"{a.name}: {res.breaking[0]}")
            else:
                out["parked"] += park(con, res, rows, ref=sha, at=at,
                                      file=a.name, load_id=load_id,
                                      load_date=load_date)
            record(con, res, load_id=load_id, ref=sha, at=at,
                   name=a.name)
        self.seen.clear()
        return out


# ------------------------------------------------------------ raw, after
def audit(con, table: str, *, load_date=None,
          archive=None) -> list[tuple[str, str, Result]]:
    """
    Hold what raw already holds to the contract, one file (or one
    load) at a time, oldest first: [(ref, name, Result)]. Headers come
    from the kept copies, where Groundwork has them.
    """
    from groundwork.ingest import files
    c = contracts.load(table)
    view = f"raw.{c.source}_{c.name}"
    cols = [r[0] for r in con.execute(f"DESCRIBE {view}").fetchall()]
    by_file = "file_sha256" in cols
    where = f"WHERE load_date = DATE '{load_date}'" if load_date else ""
    # A batch is one file in one load: the same bytes on two nights
    # are two batches, as Chapter 9 loaded them.
    group = ("file_sha256 || '|' || _load_id" if by_file
             else "_load_id")
    if by_file:
        refs = con.execute(
            f"SELECT b, any_value(file), min(f.arrived_at) AS t, "
            f"min(r.load_date) AS d FROM (SELECT *, {group} AS b "
            f"FROM {view} {where}) r LEFT JOIN audit.file_ledger f "
            "ON f.sha256 = r.file_sha256 GROUP BY 1 ORDER BY t, d, 2"
        ).fetchall()
    else:
        refs = con.execute(f"SELECT _load_id, _load_id, min(load_date) "
                           f"FROM {view} {where} GROUP BY 1 ORDER BY 3"
                           ).fetchall()
    keep = [x for x in cols if not x.startswith("_")
            and x not in ("load_date",)]
    history = History(c) if c.history else None
    out = []
    for ref, name, *_ in refs:
        t = con.execute(
            f"SELECT {', '.join(chr(34) + x + chr(34) for x in keep)} "
            f"FROM {view} WHERE {group} = ? "
            + (f"AND load_date = DATE '{load_date}' " if load_date
               else "") + "ORDER BY all", [ref]).to_arrow_table()
        header = None
        if by_file:
            try:
                kw = {"archive": archive} if archive else {}
                sha = ref.split("|")[0]
                header = header_of(files.read_kept(c.source, sha, **kw))
            except (OSError, ValueError):
                header = None
        res = check(c, t, header=header, history=history)
        if history is not None:
            history.add(res.keep(t.to_pylist()))
        out.append((ref, name, res))
    return out


def reopen(con, source: str, table: str, shas, reason: str) -> int:
    """Mark loaded files failed in the file ledger, with the reason, so
    the next run for their load date reads them again under today's
    contract and reader. `shas` may be audit()'s refs, sha|load_id.
    Returns how many were reopened."""
    shas = sorted({s.split("|")[0] for s in shas})
    if not shas:
        return 0
    marks = ", ".join("?" for _ in shas)
    con.execute(
        "UPDATE audit.file_ledger SET status = 'failed', note = ? "
        f"WHERE source = ? AND table_name = ? AND sha256 IN ({marks}) "
        "AND status = 'loaded'", [reason, source, table, *shas])
    (n,) = con.execute(
        "SELECT count(*) FROM audit.file_ledger WHERE source = ? AND "
        f"table_name = ? AND sha256 IN ({marks}) AND status = 'failed'",
        [source, table, *shas]).fetchone()
    return n


def main(argv=None) -> int:
    import argparse

    from groundwork.config import WAREHOUSE
    p = argparse.ArgumentParser(
        description="Hold what raw holds to the contracts.")
    p.add_argument("tables", nargs="*", help="default: every contract")
    p.add_argument("--load-date", help="only this load date")
    p.add_argument("--reopen", action="store_true",
                   help="mark files that break their contract failed, "
                        "so the next ingest reads them again")
    a = p.parse_args(argv)
    con = duckdb.connect(str(WAREHOUSE))
    status = 0
    try:
        views = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'raw'").fetchall()}
        for table in a.tables or contracts.names():
            if table.replace(".", "_") not in views:
                continue
            found = audit(con, table, load_date=a.load_date)
            rows = sum(r.rows for _, _, r in found)
            parked = sum(r.parked for _, _, r in found)
            broken = [(ref, n, r) for ref, n, r in found if r.breaking]
            print(f"{table:<26} {len(found):>5,} batches {rows:>9,} "
                  f"rows {parked:>5,} bad {len(broken):>3} broken")
            for _, n, r in broken[:5]:
                print(f"  {n}: {r.breaking[0]}")
            if len(broken) > 5:
                print(f"  ... and {len(broken) - 5} more")
            if broken:
                status = 1
            if a.reopen and broken:
                c = contracts.load(table)
                n = reopen(con, c.source, c.name,
                           [ref for ref, _, _ in broken],
                           f"reopened: {broken[0][2].breaking[0]}")
                print(f"  {n} files reopened for the next run")
    finally:
        con.close()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
