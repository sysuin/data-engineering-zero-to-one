"""
Roles, and what each may read.

DuckDB has no users, no GRANT and no row-level security: whoever can
open the file can read every table in it. So Groundwork writes access
down the way a warehouse would enforce it, and enforces what it can:

    create(con)      one schema per role, role_<name>, holding a view
                     for each relation the role may read: only the
                     columns it may see, only the rows it may see
    check(con)       every role's views classified, by name and by
                     content; a personal column a role is not cleared
                     for, or a row outside a role's region, is a
                     problem, returned as a sentence
    export(con, role, path)
                     the role's views copied as tables into a database
                     file of its own: the one boundary DuckDB can keep,
                     because the operating system decides who opens it

In a warehouse with users, the role schema is a GRANT: `GRANT USAGE ON
SCHEMA role_analyst TO ROLE analyst`, `GRANT SELECT ON ALL TABLES IN
SCHEMA role_analyst TO ROLE analyst`, and nothing on raw. The row
filters are row-level security policies and the omitted columns are
column grants or masking policies; the warehouse applies them to every
query, which a convention cannot do.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from groundwork.govern import classify

COLLEAGUES = {"owner", "owner_name", "assignee", "changed_by"}


@dataclass(frozen=True)
class Role:
    purpose: str
    reads: dict[str, tuple[str, ...]]      # schema -> relations ("*")
    allow: frozenset[str] = frozenset()    # personal columns allowed
    hide: dict[str, tuple[str, ...]] = field(default_factory=dict)
    region: str | None = None              # row filter on region_key


ROLES = {
    "analyst": Role(
        "questions about the business; never about one person",
        {"marts": ("*",), "masked": ("*",)},
        frozenset(COLLEAGUES | {"contact_email_ref", "requester_email_ref"}),
        hide={"fct_portal_sessions": ("session_id",)}),
    "finance": Role(
        "revenue, margin and the ledger",
        {"marts": ("dim_date", "dim_region", "dim_account", "dim_product",
                   "dim_supplier", "fct_order_lines", "fct_orders",
                   "fct_contracts", "fct_contract_month"),
         "staging": ("stg_ledger__gl_revenue",)},
        frozenset(COLLEAGUES)),
    "support": Role(
        "answering the customer on the phone",
        {"marts": ("dim_account", "fct_tickets"),
         "staging": ("stg_frontline__tickets",
                     "stg_frontline__ticket_comments",
                     "stg_relay__accounts")},
        frozenset(COLLEAGUES | {"requester_name", "requester_email",
                                "author", "body", "contact_name",
                                "contact_email", "contact_phone"})),
    "manager_southwest": Role(
        "the Southwest region's own accounts and orders",
        {"marts": ("dim_date", "dim_region", "dim_account", "dim_product",
                   "fct_order_lines", "fct_orders", "fct_tickets",
                   "fct_contracts", "fct_contract_month")},
        frozenset(COLLEAGUES), region="Southwest"),
}


def schema(role: str) -> str:
    return f"role_{role}"


def _relations(con, src: str, wanted: tuple[str, ...]) -> list[str]:
    have = [n for (n,) in con.execute(
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = ? ORDER BY 1", [src]).fetchall()]
    return have if wanted == ("*",) else [n for n in wanted if n in have]


def _columns(con, src: str, name: str) -> list[str]:
    return [c for (c,) in con.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = ? AND table_name = ?"
        " ORDER BY ordinal_position", [src, name]).fetchall()]


def _row_filter(con, src: str, name: str, region: str | None) -> str:
    if region is None or "region_key" not in _columns(con, src, name):
        return ""
    return (" WHERE region_key IN (SELECT region_key FROM"
            f" marts.dim_region WHERE region_name = '{region}')")


def create(con, roles: dict[str, Role] = ROLES) -> dict[str, int]:
    """One schema of views per role; views made for each."""
    made = {}
    for name, role in roles.items():
        target = schema(name)
        con.execute(f"DROP SCHEMA IF EXISTS {target} CASCADE")
        con.execute(f"CREATE SCHEMA {target}")
        made[name] = 0
        for src, wanted in role.reads.items():
            for rel in _relations(con, src, wanted):
                hidden = set(role.hide.get(rel, ()))
                cols = [f'"{c}"' for c in _columns(con, src, rel)
                        if c not in hidden]
                con.execute(
                    f"CREATE VIEW {target}.{rel} AS SELECT "
                    f"{', '.join(cols)} FROM {src}.{rel}"
                    f"{_row_filter(con, src, rel, role.region)}")
                made[name] += 1
    return made


def check(con, roles: dict[str, Role] = ROLES) -> list[str]:
    """What each role can see that it should not, as sentences."""
    problems = []
    for name, role in roles.items():
        target = schema(name)
        for col in classify.classify(con, (target,)):
            if col.cls == classify.PERSONAL and col.column not in role.allow:
                why = (f"{col.hits:,} rows hold contact details"
                       if col.hits else col.reason)
                problems.append(f"{name} can read {col.relation}."
                                f"{col.column}: {why}")
        if role.region is None:
            continue
        for rel in _relations(con, target, ("*",)):
            if "region_key" not in _columns(con, target, rel):
                continue
            (n,) = con.execute(
                f"SELECT count(*) FROM {target}.{rel} WHERE region_key "
                "NOT IN (SELECT region_key FROM marts.dim_region "
                f"WHERE region_name = '{role.region}')").fetchone()
            if n:
                problems.append(f"{name} sees {n:,} rows of {rel} outside "
                                f"{role.region}")
    return problems


def export(con, role: str, path) -> dict[str, int]:
    """The role's views as tables in a file of its own; rows in each."""
    path = Path(path)
    path.unlink(missing_ok=True)
    target = schema(role)
    out = {}
    con.execute(f"ATTACH '{path}' AS role_file")
    try:
        for rel in _relations(con, target, ("*",)):
            con.execute(f"CREATE TABLE role_file.main.{rel} AS "
                        f"SELECT * FROM {target}.{rel}")
            (out[rel],) = con.execute(
                f"SELECT count(*) FROM role_file.main.{rel}").fetchone()
    finally:
        con.execute("DETACH role_file")
    return out


def tables(path) -> list[str]:
    """What a role's file holds, opened as the role would open it."""
    with duckdb.connect(str(path), read_only=True) as con:
        return [f"{s}.{t}" for s, t in con.execute(
            "SELECT table_schema, table_name FROM information_schema"
            ".tables ORDER BY 1, 2").fetchall()]
