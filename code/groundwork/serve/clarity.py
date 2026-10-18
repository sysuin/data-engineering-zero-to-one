"""
A description of the warehouse that a language model can read: the
marts and the metrics, and nothing else, in as few lines as say what
a careful analyst would need to know.

A model asked a question about Meridian needs three kinds of knowledge
(Book 1's Chapter 15): structure (tables, columns, types), values
(what a column's strings look like) and meaning (what a row is, which
rows are left out, which calendar a date is on). Structure comes from
the warehouse's catalogue; meaning comes from marts.sql's comments,
which say each table's grain and what the less obvious columns hold;
values are read from the data, for the few text columns with a short
list of them. The metrics come from metrics.yaml, because a question
that has a metric should be answered with the metric.

    describe(con, cat=None)     the description, as text
    columns_named(text)         every (table, column) it mentions
"""
from __future__ import annotations

import re
import textwrap

from groundwork import ddl
from groundwork.serve import metrics

RULES = """\
Rules for every question:
- Ask for a metric by name when one fits; write SQL only when
  none does, and say which.
- Money is DECIMAL dollars. *_date columns are the depot's
  calendar; *_at columns are UTC moments.
- Orders and order lines are placed and not cancelled; there is
  no status column to filter on.
- Join a fact to a dimension on its *_key. A fact's account_key
  is the account's version on the fact's day; for today's
  attributes use dim_account WHERE is_current.
- Ratios are ratios of sums, never averages of rows."""
TABLE = re.compile(
    r"CREATE TABLE IF NOT EXISTS (marts\.\w+) \((.*?)\n\);", re.S)
NOTE = re.compile(
    r"^\s+(\w+)\s+([A-Z]+(?:\(\d+,\d+\))?)[^-]*?(?:--\s*(.*))?$")
GRAIN = re.compile(r"COMMENT ON TABLE (marts\.\w+) IS\s+'(.*?)';", re.S)
MAX_VALUES = 6


# Columns added after marts.sql was written (Chapter 19's), with notes.
ADDED = {"account_id": "the portal's account",
         "is_bot": "a script, not a person: leave out",
         "late_events": "events that arrived after the row was written",
         "clock_offset_s": "seconds the device's clock was corrected"}


def _notes() -> dict[str, dict[str, str]]:
    """Each marts table's column comments, from marts.sql."""
    out = {}
    for table, body in TABLE.findall(ddl.sql("marts")):
        out[table] = {m.group(1): m.group(3) or ""
                      for m in map(NOTE.match, body.splitlines())
                      if m and m.group(1) not in ("PRIMARY", "UNIQUE")}
    return out


def _values(con, table: str, column: str) -> list[str] | None:
    """A text column's values, when there are few enough to list."""
    vals = con.execute(f"SELECT DISTINCT {column} FROM {table} "
                       f"WHERE {column} IS NOT NULL ORDER BY 1 "
                       f"LIMIT {MAX_VALUES + 1}").fetchall()
    return [v for (v,) in vals] if len(vals) <= MAX_VALUES else None


def describe(con, cat: metrics.Catalogue | None = None,
             width: int = 66) -> str:
    """The warehouse, for a model: rules, metrics, then the marts."""
    cat = cat or metrics.catalogue()
    grains = {t: " ".join(g.replace("''", "'").split())
              for t, g in GRAIN.findall(ddl.sql("marts"))}
    wrap = textwrap.TextWrapper(width=width, subsequent_indent="    ")
    out = [RULES, "", "Metrics (name: meaning; may be broken down by):"]
    for m in cat.metrics.values():
        what = " ".join(m.description.split())
        out += wrap.wrap(f"- {m.name}: {what} By: "
                         f"{', '.join(m.dimensions)}.")
    out += ["", "Tables (grain; columns, with notes where needed):"]
    notes = _notes()
    for table in notes:
        out += wrap.wrap(f"{table}: {grains[table]}")
        parts = []
        for name, typ in con.execute(
                "SELECT column_name, data_type FROM information_schema"
                ".columns WHERE table_schema || '.' || table_name = ? "
                "ORDER BY ordinal_position", [table]).fetchall():
            said = [notes[table].get(name) or ADDED.get(name, "")]
            if typ == "VARCHAR" and not name.endswith("_key"):
                said.append(" | ".join(_values(con, table, name) or []))
            said = "; ".join(x for x in said if x)
            parts.append(f"{name} {typ}"
                         + (f" ({said})" if said else ""))
        out += wrap.wrap("  " + ", ".join(parts))
    return "\n".join(out) + "\n"


def columns_named(text: str) -> set[tuple[str, str]]:
    """Every (table, column) a description mentions, to check it
    against the warehouse's own catalogue."""
    found, table = set(), None
    flat = re.sub(r"\n    ", " ", text)
    for line in flat.splitlines():
        if line.startswith("marts."):
            table = line.split(":")[0]
        elif table and line.startswith("  "):
            for part in re.sub(r"\([^)]*\)", "", line).split(","):
                found.add((table, part.split()[0]))
    return found
