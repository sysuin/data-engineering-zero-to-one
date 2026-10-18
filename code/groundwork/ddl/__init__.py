"""
Groundwork's DDL: the SQL files that create the warehouse's tables, and one
function to run them.

    from groundwork.ddl import create
    create(con, "marts")             # runs ddl/marts.sql; safe to run twice
"""
from __future__ import annotations

from pathlib import Path

HERE = Path(__file__).resolve().parent


def sql(name: str) -> str:
    """The text of ddl/<name>.sql."""
    return (HERE / f"{name}.sql").read_text(encoding="utf-8")


def create(con, name: str = "marts") -> list[str]:
    """Run ddl/<name>.sql on a DuckDB connection; return the tables in that schema."""
    con.execute(sql(name))
    return [t for (t,) in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = ? ORDER BY table_name", [name]).fetchall()]
