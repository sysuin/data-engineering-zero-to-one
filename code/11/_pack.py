"""
Chapter 11's sandbox: a copy of the warehouse with nothing built on
raw yet, and a models folder holding Groundwork's staging models, its
seed, and the sales pack rewritten as models (code/11/pack/).
"""
import shutil
from pathlib import Path

import duckdb

from groundwork.config import SANDBOX, WAREHOUSE
from groundwork.transform.runner import MODELS

PACK = Path(__file__).with_name("pack")
CHAPTER_11_STAGING = (
    "stg_catalogue__products", "stg_frontline__ticket_fields",
    "stg_frontline__ticket_tags", "stg_frontline__tickets",
    "stg_ledger__gl_revenue", "stg_reference__price_events",
    "stg_reference__regions", "stg_reference__suppliers",
    "stg_relay__accounts", "stg_relay__contracts",
    "stg_relay__field_history", "stg_relay__migration_map",
    "stg_suppliers__price_lists", "stg_tradeline__order_lines",
    "stg_tradeline__orders",
)


def sandbox(name: str, pack: bool = True) -> tuple[Path, Path]:
    """(warehouse, models folder) for a listing, made afresh."""
    root = SANDBOX / "ch11" / name
    shutil.rmtree(root, ignore_errors=True)
    # Chapter 11's models only: later chapters add models and seeds
    # beside these (Chapter 13's ticket versions and comments among
    # them), so the fifteen it wrote are named rather than globbed.
    staging = root / "models" / "staging"
    staging.mkdir(parents=True)
    for model in CHAPTER_11_STAGING:
        shutil.copy(MODELS / "staging" / f"{model}.sql", staging)
    shutil.copytree(MODELS / "seeds", root / "models" / "seeds",
                    ignore=lambda d, names: [
                        n for n in names
                        if not n.startswith("supplier_names.")])
    if pack:
        shutil.copytree(PACK, root / "models" / "pack")
    shutil.copy(WAREHOUSE, root / "warehouse.duckdb")
    con = duckdb.connect(str(root / "warehouse.duckdb"))
    for schema in ("staging", "seeds", "intermediate", "pack"):
        con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    con.execute("DROP TABLE IF EXISTS audit.model_runs")
    con.close()
    return root / "warehouse.duckdb", root / "models"


def tidy(name: str) -> None:
    shutil.rmtree(SANDBOX / "ch11" / name, ignore_errors=True)
