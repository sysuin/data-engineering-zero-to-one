"""
Who read what: every query through a Session is checked against the
reader's role and written to audit.query_log, allowed or not.

    Session(con, user, role)     a reader, and the role they act in
    session.sql(query, at=...)   run it if the role may read every
                                 relation it names; log it either way
    sources(con, relation)       what a role's view reads underneath
    who_read(con, relations, since, until)
                                 the log's answer, by person

The log keeps the query's text with every quoted value replaced by '?',
because a support agent's search for a customer by email would
otherwise put the email in the log, and the log is kept for a year.
An engineer may read anything, as the person who holds the file can;
the log marks those reads, so that someone else can ask why.
"""
from __future__ import annotations

import re
from datetime import datetime

import duckdb

from groundwork.govern import roles

LOG = """
CREATE TABLE IF NOT EXISTS audit.query_log (
    query_no INTEGER, read_at TIMESTAMP, "user" VARCHAR, role VARCHAR,
    relations VARCHAR[], allowed BOOLEAN, break_glass BOOLEAN,
    rows BIGINT, query VARCHAR)"""
LITERAL = re.compile(r"'(?:[^']|'')*'")


class Denied(PermissionError):
    pass


def relations(query: str) -> list[str]:
    """The relations a query names, schema-qualified, sorted."""
    names = duckdb.get_table_names(query, qualified=True)
    return sorted({n.split(" AS ")[0].lower() for n in names})


def sources(con, relation: str) -> list[str]:
    """A role's view, followed down to what it reads."""
    schema, name = relation.split(".", 1)
    sql = con.execute(
        "SELECT sql FROM duckdb_views() WHERE schema_name = ?"
        " AND view_name = ?", [schema, name]).fetchone()
    if not sql or not schema.startswith("role_"):
        return [relation]
    return sorted(set(re.findall(r"FROM (\w+\.\w+)", sql[0])))


class Session:
    def __init__(self, con, user: str, role: str):
        self.con, self.user, self.role = con, user, role
        con.execute(LOG)

    def _may(self, rel: str) -> bool:
        if self.role == "engineer":
            return True
        return rel.startswith(roles.schema(self.role) + ".")

    def sql(self, query: str, at: datetime) -> list[tuple]:
        rels = relations(query)
        allowed = all(self._may(r) for r in rels)
        rows = None
        try:
            if not allowed:
                raise Denied(f"{self.user} ({self.role}) may not read "
                             + ", ".join(r for r in rels
                                         if not self._may(r)))
            rows = self.con.execute(query).fetchall()
            return rows
        finally:
            (n,) = self.con.execute(
                "SELECT count(*) + 1 FROM audit.query_log").fetchone()
            self.con.execute(
                "INSERT INTO audit.query_log VALUES (?, ?, ?, ?, ?, ?, ?,"
                " ?, ?)",
                [n, at, self.user, self.role, rels, allowed,
                 self.role == "engineer", None if rows is None
                 else len(rows), LITERAL.sub("'?'", " ".join(
                     query.split()))])


def who_read(con, wanted: set[str], since: datetime,
             until: datetime) -> list[tuple]:
    """(user, role, relation, reads, break-glass reads), allowed only."""
    out: dict[tuple, list[int]] = {}
    for user, role, rels, glass in con.execute(
            'SELECT "user", role, relations, break_glass FROM'
            " audit.query_log WHERE allowed AND read_at >= ?"
            " AND read_at < ?"
            " ORDER BY query_no", [since, until]).fetchall():
        for rel in rels:
            for src in sources(con, rel):
                if src in wanted:
                    k = (user, role, src)
                    n = out.setdefault(k, [0, 0])
                    n[0] += 1
                    n[1] += int(glass)
    return [(*k, *v) for k, v in sorted(out.items())]
