"""
A data diff: what a change does to every table, before it merges.

    python -m groundwork.diff A.duckdb B.duckdb [--schema S ...]
    python -m groundwork.diff --against main [--keep]

Tests check what a table promises: a key is unique, a total ties. A
diff checks what a change did. It takes two builds of the warehouse
(main's and the branch's, made from the same raw layer) and compares
them table by table:

    rows        how many each build holds
    keyed       a table with a key is joined on it: keys only in A are
                removed, keys only in B added, keys in both with any
                column different changed, and each column says on how
                many keyed rows it differs
    unkeyed     a table with no key is compared as a bag of rows
                (EXCEPT ALL both ways, Chapter 16's equality check)
    columns     a column only one build has, or whose type changed

`--against REF` makes both builds: the dbt project at REF (checked out
with `git worktree`) and the one in the working tree, each built with
`dbt build --full-refresh` into its own copy of the warehouse under
data/sandbox/diff/. The copies share the raw layer's Parquet files, so
the only thing that differs between them is the project. `make diff`
runs it against main.

The exit code is 0 when the diff is made, whatever it says: reading a
diff is a reviewer's job. `--strict` exits 1 if anything differs, for
a change that promises to change nothing (a refactor, a faster model).
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

from groundwork import ddl, testing
from groundwork.config import ROOT, SANDBOX, WAREHOUSE

PROJECT = ROOT / "dbt" / "groundwork"
HERE = SANDBOX / "diff"
SCHEMAS = ("staging", "intermediate", "marts")
NIGHT = "2026-01-04"                  # the last night raw holds

# Keys beyond the marts' (which come from marts.sql): the staging keys
# Chapter 15 tests, and the intermediate models' grains.
KEYS = {
    "staging.stg_tradeline__orders": ("order_id",),
    "staging.stg_tradeline__order_lines": ("order_id", "line_no"),
    "staging.stg_relay__accounts": ("account_id", "snapshot_date"),
    "staging.stg_relay__contracts": ("contract_id", "snapshot_date"),
    "staging.stg_frontline__tickets": ("ticket_id",),
    "staging.stg_catalogue__products": ("sku",),
    "intermediate.int_order_lines": ("order_id", "line_no"),
    "intermediate.int_account_identity": ("crm_id",),
}


def marts_keys() -> dict[str, tuple[str, ...]]:
    """The primary keys marts.sql declares, read by DuckDB itself."""
    con = duckdb.connect()
    ddl.create(con, "marts")
    rows = con.execute("""
        SELECT table_name, constraint_column_names
        FROM duckdb_constraints()
        WHERE schema_name = 'marts' AND constraint_type = 'PRIMARY KEY'
        ORDER BY table_name""").fetchall()
    con.close()
    return {f"marts.{t}": tuple(cols) for t, cols in rows}


def keys() -> dict[str, tuple[str, ...]]:
    return {**marts_keys(), **KEYS}


@dataclass
class TableDiff:
    """What one table did between build A and build B."""
    name: str
    rows_a: int | None              # None: the table is not in A
    rows_b: int | None
    key: tuple[str, ...] = ()
    added: int = 0                  # keys only in B (or rows, unkeyed)
    removed: int = 0                # keys only in A (or rows, unkeyed)
    changed: int = 0                # keys in both, some column differs
    columns: dict[str, int] = field(default_factory=dict)
    schema: list[str] = field(default_factory=list)  # column changes

    @property
    def same(self) -> bool:
        return (self.rows_a == self.rows_b and not self.added
                and not self.removed and not self.changed
                and not self.schema)


def _columns(con, name):
    schema, table = name.split(".")
    return dict(con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position", [schema, table]).fetchall())


def _tables(con, schemas):
    marks = ", ".join("?" for _ in schemas)
    return {f"{s}.{t}" for s, t in con.execute(
        f"SELECT table_schema, table_name FROM information_schema.tables"
        f" WHERE table_schema IN ({marks})", list(schemas)).fetchall()}


def _prints(con, name, key, cols, into, alias):
    """Table `name` reduced to its key and one hash per column, copied
    into the comparing connection as table `alias`. Each build is read
    through its own connection, so its views resolve in its own file."""
    sel = [f'"{k}"' for k in key] + [f'hash("{c}") AS "{c}"'
                                      for c in cols]
    t = con.execute(f"SELECT {', '.join(sel)} FROM {name}"
                    ).to_arrow_table()
    into.register("incoming", t)
    into.execute(f"CREATE OR REPLACE TABLE {alias} AS "
                 f"SELECT * FROM incoming")
    into.unregister("incoming")


def table(ca, cb, name, key=(), cmp=None) -> TableDiff:
    """Compare table (or view) `name` in build A, open on connection
    ca, with the same in build B, open on cb."""
    cola, colb = _columns(ca, name), _columns(cb, name)
    (na,) = ca.execute(f"SELECT count(*) FROM {name}").fetchone()
    (nb,) = cb.execute(f"SELECT count(*) FROM {name}").fetchone()
    d = TableDiff(name, na, nb, tuple(key))
    d.schema = ([f"-{c}" for c in cola if c not in colb]
                + [f"+{c}" for c in colb if c not in cola]
                + [f"~{c} {cola[c]} -> {colb[c]}" for c in cola
                   if c in colb and cola[c] != colb[c]])
    both = [c for c in cola if c in colb]
    cmp = cmp or duckdb.connect()
    if not (key and all(k in both for k in key)):
        d.key = key = ()
    rest = [c for c in both if c not in key]
    _prints(ca, name, key, rest, cmp, "x")
    _prints(cb, name, key, rest, cmp, "y")
    if key:
        on = " AND ".join(f'x."{k}" = y."{k}"' for k in key)
        k = f'"{key[0]}"'
        anyc = " OR ".join(f'x."{c}" <> y."{c}"' for c in rest) \
            or "false"
        per = "".join(f', count(*) FILTER (WHERE x.{k} IS NOT NULL '
                      f'AND y.{k} IS NOT NULL AND x."{c}" <> y."{c}")'
                      for c in rest)
        row = cmp.execute(f"""
            SELECT count(*) FILTER (WHERE x.{k} IS NULL),
                   count(*) FILTER (WHERE y.{k} IS NULL),
                   count(*) FILTER (WHERE x.{k} IS NOT NULL AND
                                    y.{k} IS NOT NULL AND ({anyc})){per}
            FROM x FULL JOIN y ON {on}""").fetchone()
        d.added, d.removed, d.changed = row[:3]
        d.columns = {c: n for c, n in zip(rest, row[3:]) if n}
    else:
        cols = ", ".join(f'"{c}"' for c in rest)
        (d.removed,) = cmp.execute(
            f"SELECT count(*) FROM (SELECT {cols} FROM x EXCEPT ALL "
            f"SELECT {cols} FROM y)").fetchone()
        (d.added,) = cmp.execute(
            f"SELECT count(*) FROM (SELECT {cols} FROM y EXCEPT ALL "
            f"SELECT {cols} FROM x)").fetchone()
    cmp.execute("DROP TABLE x; DROP TABLE y")
    return d


def compare(a, b, schemas=SCHEMAS, key_of=None) -> list[TableDiff]:
    """Every table and view in `schemas` of build a against build b."""
    key_of = keys() if key_of is None else key_of
    ca = duckdb.connect(str(a), read_only=True)
    cb = duckdb.connect(str(b), read_only=True)
    cmp = duckdb.connect()
    for c in (ca, cb, cmp):
        c.execute("SET enable_progress_bar = false")
    try:
        ta, tb = _tables(ca, schemas), _tables(cb, schemas)
        out = []
        for name in sorted(ta | tb):
            if name not in tb:
                (n,) = ca.execute(f"SELECT count(*) FROM {name}"
                                  ).fetchone()
                out.append(TableDiff(name, n, None, removed=n))
            elif name not in ta:
                (n,) = cb.execute(f"SELECT count(*) FROM {name}"
                                  ).fetchone()
                out.append(TableDiff(name, None, n, added=n))
            else:
                out.append(table(ca, cb, name, key_of.get(name, ()),
                                 cmp))
        return out
    finally:
        for c in (ca, cb, cmp):
            c.close()


def report(diffs, width=68) -> str:
    """The diff as a reviewer reads it: each table that changed, how,
    and in which columns; then how many did not change at all."""
    lines = []
    for d in diffs:
        if d.same:
            continue
        if d.rows_a is None or d.rows_b is None:
            side, n = ("B", d.rows_b) if d.rows_a is None else \
                ("A", d.rows_a)
            lines.append(f"{d.name}: only in build {side}, {n:,} rows")
            continue
        lines.append(f"{d.name}  {d.rows_a:,} -> {d.rows_b:,} rows")
        if d.key:
            lines.append(f"  by key: {d.changed:,} changed, "
                         f"{d.added:,} added, {d.removed:,} removed")
        else:
            lines.append(f"  no key: {d.removed:,} rows only in A, "
                         f"{d.added:,} only in B")
        if d.columns:
            cols = sorted(d.columns.items(), key=lambda kv: (-kv[1],
                                                             kv[0]))
            lines += _wrap("  columns: " + ", ".join(
                f"{c} {n:,}" for c, n in cols), width, "    ")
        lines += [f"  schema: {s}" for s in d.schema]
    same = sum(d.same for d in diffs)
    lines.append(f"{same} of {len(diffs)} tables and views identical")
    return "\n".join(lines)


def _wrap(text, width, indent):
    """Break a line of comma-separated items to fit the width."""
    out, line = [], ""
    for item in text.split(", "):
        if line and len(line) + 2 + len(item) > width:
            out.append(line + ",")
            line = indent + item
        else:
            line = f"{line}, {item}" if line else item
    return out + [line]


# ------------------------------------------------------------ builds

def project_at(ref: str, into: Path) -> Path:
    """The dbt project as it stands at git ref `ref`, checked out into
    `into` with git worktree; the project folder inside it."""
    if into.exists():
        subprocess.run(["git", "worktree", "remove", "--force",
                        str(into)], cwd=ROOT, capture_output=True)
        shutil.rmtree(into, ignore_errors=True)
    subprocess.run(["git", "worktree", "add", "--detach", str(into), ref],
                   cwd=ROOT, check=True, capture_output=True)
    return into / "dbt" / "groundwork"


def build(project: Path, warehouse: Path, base: Path = WAREHOUSE,
          night: str = NIGHT) -> dict[str, int]:
    """Copy `base` to `warehouse` and build `project` into it from
    scratch with `dbt build --full-refresh`; dbt's statuses, counted.
    The copy shares base's raw layer: its views read the same files."""
    warehouse = Path(warehouse).resolve()
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(base, warehouse)
    wal = Path(f"{base}.wal")
    if wal.exists():
        shutil.copy(wal, f"{warehouse}.wal")
    return testing.dbt_build(project, warehouse, night=night)


def against(ref: str, here: Path = HERE, base: Path = WAREHOUSE,
            project: Path = PROJECT, keep: bool = False):
    """Build the project at `ref` and the working tree's, and diff
    them: (statuses at ref, statuses here, diffs)."""
    here.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="ref-", dir=here))
    try:
        old = build(project_at(ref, tmp), here / "a" / "warehouse.duckdb",
                    base)
        new = build(project, here / "b" / "warehouse.duckdb", base)
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(tmp)],
                       cwd=ROOT, capture_output=True)
        shutil.rmtree(tmp, ignore_errors=True)
    diffs = compare(here / "a" / "warehouse.duckdb",
                    here / "b" / "warehouse.duckdb")
    if not keep:
        for side in ("a", "b"):
            shutil.rmtree(here / side, ignore_errors=True)
    return old, new, diffs


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m groundwork.diff",
                                description=__doc__.splitlines()[1])
    p.add_argument("a", nargs="?", help="build A (a warehouse file)")
    p.add_argument("b", nargs="?", help="build B")
    p.add_argument("--against", metavar="REF",
                   help="build REF's project and this one, then diff")
    p.add_argument("--schema", action="append", dest="schemas")
    p.add_argument("--json", type=Path, help="also write the diff here")
    p.add_argument("--keep", action="store_true",
                   help="keep both builds under data/sandbox/diff/")
    p.add_argument("--strict", action="store_true",
                   help="exit 1 if anything differs")
    args = p.parse_args(argv)
    schemas = tuple(args.schemas or SCHEMAS)
    if args.against:
        old, new, diffs = against(args.against, keep=args.keep)
        for who, got in (("A", old), ("B", new)):
            print(f"build {who}: " + ", ".join(
                f"{n} {s}" for s, n in sorted(got.items())))
        diffs = [d for d in diffs if d.name.split(".")[0] in schemas]
    elif args.a and args.b:
        diffs = compare(args.a, args.b, schemas)
    else:
        p.error("give two warehouses, or --against REF")
    print(report(diffs))
    if args.json:
        args.json.write_text(json.dumps([asdict(d) for d in diffs],
                                        indent=1))
    return 1 if args.strict and not all(d.same for d in diffs) else 0


if __name__ == "__main__":
    sys.exit(main())
