"""
Profile a table or a file: what a source holds, in one compact shape.

    import duckdb
    from groundwork.profile import profile
    con = duckdb.connect()
    print(profile(con.sql("SELECT * FROM 'catalogue.csv'"), "cat"))
    print(profile("data/.../accounts.csv"))       # a path works too

A profile answers the first questions to ask of any source: how many
rows; for each column its type, how many values are missing, how many
are distinct, the smallest and the largest, and the commonest. It also
notes habits worth a second look: text that holds timestamps or money,
codes in mixed case, values with stray spaces. It notes them and fixes
nothing. Fixing is a decision, and decisions belong to later chapters.

Every profile prints in the same shape, within 68 characters.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

WIDTH = 68       # the widest line a profile prints
TOP_WHEN = 50    # list the commonest values when a column has this few
TOP_N = 3        # and list this many of them
MONEY = r"^\s*-?[$£€]\s?-?[0-9,]+(\.[0-9]+)?\s*$"
SHORT = {"TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ"}


@dataclass
class Column:
    """One column's profile."""
    name: str
    type: str
    nulls: int
    distinct: int
    min: str | None = None
    max: str | None = None
    top: list[tuple[str, int]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class Profile:
    """A table's profile: its size and every column's."""
    name: str
    rows: int
    columns: list[Column]

    def notes(self) -> list[str]:
        """Every column's notes, each prefixed with its column."""
        return [f"{c.name}: {n}" for c in self.columns for n in c.notes]

    def to_dict(self) -> dict:
        return asdict(self)

    def __str__(self) -> str:
        out = [f"{self.name}: {self.rows:,} rows, "
               f"{len(self.columns)} columns",
               f"{'column':<20}{'type':<12}{'nulls':>10}"
               f"{'distinct':>12}"]
        for c in self.columns:
            out.append(f"{fit(c.name, 19):<20}{fit(c.type, 11):<12}"
                       f"{c.nulls:>10,}{c.distinct:>12,}")
            if c.min is not None:
                out.append(f"  range  {fit(c.min, 27)} .. "
                           f"{fit(c.max, 27)}")
            if c.top:
                out.append(_top_line(c.top))
        if self.notes():
            out.append("notes")
            out += [f"  {fit(n, WIDTH - 2)}" for n in self.notes()]
        return "\n".join(out)


def fit(s: str, n: int) -> str:
    """s, cut to n characters with '...' if it was longer."""
    s = str(s).replace("\n", " ")
    return s if len(s) <= n else s[:n - 3] + "..."


def _top_line(top: list[tuple[str, int]]) -> str:
    line = "  top    "
    for i, (value, count) in enumerate(top):
        item = f"{'' if i == 0 else ', '}{fit(value, 24)} ({count:,})"
        if len(line + item) > WIDTH:
            break
        line += item
    return line


def _q(name: str) -> str:
    """A column name quoted for SQL: some sources put spaces in."""
    return '"' + name.replace('"', '""') + '"'


def _short(t: str) -> str:
    """A type name short enough for the table: STRUCT(...) -> STRUCT."""
    t = SHORT.get(t, t)
    if "(" in t:
        t = t.split("(")[0] + ("[]" if t.endswith("[]") else "")
    return t


def _nested(t: str) -> bool:
    return t.startswith(("STRUCT", "MAP", "UNION")) or t.endswith("]")


def connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that prints every moment in UTC."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def _relation(source, con):
    """A DuckDB relation from a relation or a path to a file."""
    if isinstance(source, (str, Path)):
        path = str(source)
        con = con or connect()
        if path.endswith(".parquet"):
            return con.read_parquet(path)
        if path.endswith((".json", ".jsonl", ".jsonl.gz", ".ndjson")):
            return con.read_json(path)
        return con.read_csv(path)
    return source


def profile(source, name: str | None = None, con=None) -> Profile:
    """Profile a DuckDB relation, or a CSV, Parquet or JSON file."""
    rel = _relation(source, con)
    if name is None:
        name = Path(source).name if isinstance(source, (str, Path)) \
            else "relation"
    cols = [(c, str(t)) for c, t in zip(rel.columns, rel.types)]

    # One query for the counts and the extremes of every column.
    parts = ["COUNT(*)"]
    for c, t in cols:
        v = f"CAST({_q(c)} AS VARCHAR)" if _nested(t) else _q(c)
        parts += [f"COUNT({_q(c)})", f"COUNT(DISTINCT {v})"]
        if _nested(t):
            parts += ["NULL", "NULL"]
        else:
            parts += [f"CAST(MIN({v}) AS VARCHAR)",
                      f"CAST(MAX({v}) AS VARCHAR)"]
    row = rel.query("r", f"SELECT {', '.join(parts)} FROM r").fetchone()
    rows, out = row[0], []
    for i, (c, t) in enumerate(cols):
        filled, distinct, lo, hi = row[1 + 4 * i: 5 + 4 * i]
        col = Column(c, _short(t), rows - filled, distinct, lo, hi)
        if 0 < distinct <= TOP_WHEN and distinct < filled:
            col.top = _top(rel, c)
        if t == "VARCHAR" and filled:
            col.notes = _habits(rel, c, filled)
        out.append(col)
    return Profile(name, rows, out)


def _top(rel, c: str) -> list[tuple[str, int]]:
    v = f"CAST({_q(c)} AS VARCHAR)"
    return [(str(value), n) for value, n in rel.query("r", f"""
        SELECT {v} AS v, COUNT(*) AS n FROM r
        WHERE {_q(c)} IS NOT NULL
        GROUP BY v ORDER BY n DESC, v LIMIT {TOP_N}""").fetchall()]


def _habits(rel, c: str, filled: int) -> list[str]:
    """Things a text column's values suggest, worth a second look."""
    q = _q(c)
    ts, date_only, money, spaced, upper, lower = rel.query("r", f"""
        SELECT COUNT(TRY_CAST({q} AS TIMESTAMP)),
               COUNT(*) FILTER (WHERE LENGTH({q}) <= 10),
               COUNT(*) FILTER (WHERE regexp_matches({q}, '{MONEY}')),
               COUNT(*) FILTER (WHERE {q} <> TRIM({q})),
               COUNT(*) FILTER (WHERE {q} = UPPER({q})
                                  AND {q} <> LOWER({q})),
               COUNT(*) FILTER (WHERE {q} = LOWER({q})
                                  AND {q} <> UPPER({q}))
        FROM r WHERE {q} IS NOT NULL""").fetchone()
    notes = []
    if ts == filled:
        kind = "dates" if date_only == filled else "timestamps"
        notes.append(f"{kind} held as text")
    if money == filled:
        notes.append("money held as text")
    if upper and lower:
        notes.append(f"{lower:,} in lower case, "
                     f"{upper:,} in upper case")
    if spaced:
        notes.append(f"{spaced:,} with leading or trailing spaces")
    return notes


if __name__ == "__main__":
    import sys
    for arg in sys.argv[1:]:
        print(profile(arg), end="\n\n")
