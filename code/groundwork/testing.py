"""
Tiny warehouses for tests: rows a test chooses, run through the code
production runs.

A unit test of a SQL model runs the model's own file, the one dbt
builds every night, over a handful of rows the test writes:

    con = duckdb.connect()
    raw_table(con, "relay_contracts", [contract_row, ...])
    current_files(con, ("relay", "relay/n/contracts.csv", "sha"))
    rows = run_model(con, "stg_relay__contracts")

An integration test runs the pipeline, ingest then dbt, over a tiny
Meridian whose files and database a test writes in the real formats:

    s = Sources(tmp_path)
    s.account(10, segment="Enterprise")
    s.order(1, account=10, placed_at="2025-12-01 15:00:00",
            lines=[("MRD-CLE-001", 2, 1999)])
    warehouse = s.build()           # ingest, then dbt build

Every column a test does not mention takes a plain default, so a test
says only what it is about. The headers come from the contracts, so a
tiny source is held to the same promises as the real one.
"""
from __future__ import annotations

import csv
import re
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import jinja2
import yaml

from groundwork import contracts
from groundwork.config import ROOT

PROJECT = ROOT / "dbt" / "groundwork"
NIGHT = date(2026, 1, 4)


# ------------------------------------------------------- SQL models

def _schemas(project=None) -> dict[str, str]:
    """Each model's and seed's schema, from its folder, as dbt names
    them in Groundwork (generate_schema_name keeps the folder name)."""
    project = Path(project or PROJECT)
    out = {p.stem: p.parent.name
           for p in (project / "models").rglob("*.sql")}
    out.update({p.stem: "seeds" for p in (project / "seeds").rglob("*.csv")})
    out.update({p.stem: "snapshots"
                for p in (project / "snapshots").glob("*.sql")})
    return out


def render(name: str, project=None, refs=None, **variables) -> str:
    """Model `name`'s SQL as a full build compiles it: ref('m') is
    <schema>.m (or refs[m]), source('s', 't') is s.t, and var() takes
    the project's defaults unless given here."""
    project = Path(project or PROJECT)
    where = _schemas(project)
    refs = refs or {}
    conf = yaml.safe_load((project / "dbt_project.yml").read_text())
    values = {**conf.get("vars", {}), **variables}
    (path,) = (project / "models").rglob(f"{name}.sql")
    context = {
        "ref": lambda m: refs.get(m, f"{where[m]}.{m}"),
        "source": lambda s, t: f"{s}.{t}",
        "config": lambda **_: "",
        "is_incremental": lambda: False,
        "var": lambda k, default=None: values.get(k, default),
        "this": f"{where[name]}.{name}",
    }
    macros = "".join(p.read_text() for p in
                     sorted((project / "macros").glob("*.sql")))
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return env.from_string(macros + path.read_text()).render(
        **context).strip()


def run_model(con, name: str, select: str = "*", order_by=None,
              project=None, **variables) -> list[tuple]:
    """Run model `name` over the tables on `con`: its rows (only the
    columns in `select`), in `order_by`'s order or sorted."""
    sql = render(name, project, **variables)
    rows = con.execute(f"SELECT {select} FROM ({sql}) AS m "
                       + (f"ORDER BY {order_by}" if order_by else "")
                       ).fetchall()
    return rows if order_by else sorted(rows, key=repr)


def columns(con, name: str, project=None, **variables) -> list[str]:
    """The columns model `name` returns, by binding it, not running it."""
    sql = render(name, project, **variables)
    return [d[0] for d in con.execute(
        f"SELECT * FROM ({sql}) AS m LIMIT 0").description]


def raw_table(con, name: str, rows: list[dict],
              load_date: date = NIGHT) -> None:
    """raw.<name> from dicts, every value text as raw keeps a file's,
    plus the load_date raw's views add."""
    cols = list(rows[0])
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"CREATE OR REPLACE TABLE raw.{name} ("
                + ", ".join(f'"{c}" VARCHAR' for c in cols)
                + ", load_date DATE)")
    marks = ", ".join("?" for _ in cols)
    con.executemany(f"INSERT INTO raw.{name} VALUES ({marks}, ?)",
                    [[r[c] for c in cols] + [r.get("_day", load_date)]
                     for r in rows])


def table(con, name: str, rows: list[dict], types=None) -> None:
    """<schema>.<table> from dicts, typed from the Python values, or by
    `types` ({column: SQL type}) where every row holds None: an upstream
    model's output, written by hand."""
    import pyarrow as pa
    t = pa.Table.from_pylist(rows)
    types = types or {}
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {name.split('.')[0]}")
    cols = ", ".join(
        f'CAST("{f.name}" AS {types.get(f.name, "VARCHAR")}) '
        f'AS "{f.name}"' if pa.types.is_null(f.type) else f'"{f.name}"'
        for f in t.schema)
    con.register("rows_in", t)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS "
                f"SELECT {cols} FROM rows_in")
    con.unregister("rows_in")


def current_files(con, *files) -> None:
    """audit.current_files holding (source, name, sha256) per file."""
    con.execute("CREATE SCHEMA IF NOT EXISTS audit")
    con.execute("CREATE TABLE IF NOT EXISTS audit.current_files "
                "(source VARCHAR, name VARCHAR, sha256 VARCHAR)")
    if files:
        con.executemany("INSERT INTO audit.current_files VALUES "
                        "(?, ?, ?)", list(files))


def seed(con, name: str, project=None) -> None:
    """The project's seed `name` as seeds.<name>, all text, as dbt
    loads it (Chapter 14's _seeds.yml)."""
    project = Path(project or PROJECT)
    (path,) = (project / "seeds").rglob(f"{name}.csv")
    con.execute("CREATE SCHEMA IF NOT EXISTS seeds")
    con.execute(f"CREATE OR REPLACE TABLE seeds.{name} AS SELECT * "
                f"FROM read_csv('{path}', all_varchar = true)")


# ------------------------------------------------- a tiny Meridian

TRADELINE = """
CREATE TABLE channels (code TEXT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE orders (id INTEGER PRIMARY KEY, order_number INTEGER
  NOT NULL UNIQUE, account_id INTEGER NOT NULL, placed_at TEXT NOT
  NULL, channel TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT
  NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE order_lines (order_id INTEGER NOT NULL, line_no INTEGER
  NOT NULL, sku TEXT NOT NULL, qty INTEGER NOT NULL, unit_price_cents
  INTEGER NOT NULL, discount_pct INTEGER NOT NULL, created_at TEXT NOT
  NULL, updated_at TEXT NOT NULL, PRIMARY KEY (order_id, line_no));
CREATE TABLE replication_log (lsn INTEGER PRIMARY KEY, committed_at
  TEXT NOT NULL, table_name TEXT NOT NULL, op TEXT NOT NULL, row_json
  TEXT NOT NULL);
"""
REGIONS = [(1, "Northeast", "Newark", "America/New_York"),
           (2, "Southeast", "Atlanta", "America/New_York"),
           (3, "Midwest", "Columbus", "America/New_York"),
           (4, "West", "Sacramento", "America/Los_Angeles"),
           (5, "Southwest", "Dallas", "America/Chicago")]
SUPPLIERS = [(1, "Voss Industrial", "US", "Strategic"),
             (7, "Halvard Group", "US", "Preferred")]
SLUGS = {1: ("voss-industrial", "Voss Industrial"),   # folder, and the
         7: ("halvard-group", "Halvard Group")}       # catalogue's name


def _header(table: str, fmt: str | None = None) -> list[str]:
    """The header the contract says the producer sends (its first
    format unless another is named)."""
    formats = contracts.load(table).formats
    return list(formats[fmt] if fmt else next(iter(formats.values())))


class Sources:
    """A tiny Meridian under `root`: Tradeline's database, one night of
    Relay's export, the catalogue, a price list per supplier and month,
    and the reference files, as the real systems write them."""

    def __init__(self, root, night: date = NIGHT):
        self.root = Path(root)
        self.night = night
        self.run_at = datetime.combine(night + timedelta(days=1), time(6))
        self.accounts: dict[int, dict] = {}
        self.contracts: dict[int, dict] = {}
        self.orders: list[tuple[dict, list[dict]]] = []
        self.products: dict[str, dict] = {}
        self.costs: list[tuple[str, str, int]] = []   # sku, month, cents
        self.changes: list[dict] = []                 # Relay's audit
        self.legacy: dict[int, int] = {}              # old id -> Relay

    # what a test says ------------------------------------------------
    def account(self, account_id: int, **kw) -> dict:
        row = dict(account_id=account_id,
                   account_name=f"Account {account_id}",
                   region="Northeast", segment="Small business",
                   owner="Ada Kerr", customer_since="2022-01-01",
                   postcode="07001", key_account="false",
                   status="active", closed_on="",
                   contact_name="Sam Lee",
                   contact_email=f"sam@a{account_id}.example",
                   contact_phone="(973) 555-0100",
                   updated_at="2025-06-01T09:00:00Z")
        row.update(kw)
        self.accounts[account_id] = row
        return row

    def contract(self, contract_id: int, account: int, start: str,
                 end: str, **kw) -> dict:
        row = dict(contract_id=contract_id, account_id=account,
                   start_date=start, end_date=end, term_months=12,
                   legacy_terms="false", discount_pct=5, outcome="",
                   notice_date="", cancellation_reason="",
                   updated_at="2025-06-01T09:00:00Z")
        row.update(kw)
        self.contracts[contract_id] = row
        self.change("contract", contract_id, "created", "", "",
                    f"{start}T09:00:00Z")
        return row

    def change(self, record_type: str, record_id: int, field: str,
               old: str, new: str, at: str, by: str = "sales-ops"):
        """A row of Relay's field audit; creating a contract writes one."""
        self.changes.append(dict(changed_at=at, object=record_type,
                                 record_id=record_id, field=field,
                                 old_value=old, new_value=new,
                                 changed_by=by))

    def old_id(self, old_crm_id: int, account: int) -> None:
        """An old-CRM id that IT's migration map sends to `account`."""
        self.legacy[old_crm_id] = account

    def product(self, sku: str, supplier: int = 1, cost_cents: int = 1000,
                list_cents: int = 2000, category: str = "Cleaning",
                month: str = "2025-01") -> dict:
        row = {"SKU": sku, "Product Name": f"Item {sku[-3:]}",
               "Category": category, "Supplier": SLUGS[supplier][1],
               "List Price": f"${list_cents / 100:.2f}", "Launched": "",
               "Notes": "", "_supplier": supplier}
        self.products[sku] = row
        self.cost(sku, month, cost_cents)
        return row

    def cost(self, sku: str, month: str, cents: int) -> None:
        """The SKU's cost on its supplier's list for `month`, in force
        from the first of the month."""
        self.costs.append((sku, month, cents))

    def order(self, order_id: int, account: int, placed_at: str,
              lines=(("MRD-CLE-001", 1, 1999),), status="placed",
              channel="WEB", updated_at: str | None = None) -> dict:
        stamp = updated_at or placed_at
        o = dict(id=order_id, order_number=100000 + order_id,
                 account_id=account, placed_at=placed_at,
                 channel=channel, status=status, created_at=placed_at,
                 updated_at=stamp)
        ls = [dict(order_id=order_id, line_no=n, sku=sku, qty=qty,
                   unit_price_cents=cents, discount_pct=0,
                   created_at=placed_at, updated_at=stamp)
              for n, (sku, qty, cents) in enumerate(lines, start=1)]
        self.orders.append((o, ls))
        return o

    # the files, as the systems write them ----------------------------
    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def reference(self) -> Path:
        return self.root / "reference"

    @property
    def tradeline_db(self) -> Path:
        return self.root / "tradeline.db"

    def _csv(self, path: Path, header, rows, bom=False, arrived=None):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="",
                  encoding="utf-8-sig" if bom else "utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow(["" if r.get(h) is None else r.get(h, "")
                            for h in header])
        t = (arrived or self.run_at - timedelta(hours=2)).timestamp()
        os.utime(path, (t, t))

    def write(self) -> None:
        """Write every source; safe to call again after adding rows."""
        if self.root.exists():
            for p in ("landing", "reference", "tradeline.db"):
                q = self.root / p
                shutil.rmtree(q) if q.is_dir() else q.unlink(
                    missing_ok=True)
        night = self.landing / "relay" / self.night.isoformat()
        exported = datetime.combine(self.night + timedelta(days=1),
                                    time(4))
        self._csv(night / "accounts.csv", _header("relay.accounts"),
                  self.accounts.values(), bom=True, arrived=exported)
        self._csv(night / "contracts.csv", _header("relay.contracts"),
                  self.contracts.values(), bom=True, arrived=exported)
        months: dict[str, list] = {}
        for c in self.changes:
            months.setdefault(c["changed_at"][:7], []).append(c)
        for month, rows in sorted(months.items()):
            self._csv(self.landing / "relay" / "audit" /
                      f"field_history_{month}.csv",
                      _header("relay.field_history"),
                      sorted(rows, key=lambda r: r["changed_at"]))
        # IT's map always has rows; unless a test says otherwise, it
        # sends one old id, which no order uses, to the first account.
        legacy = self.legacy or {90001: min(self.accounts)}
        self._csv(self.landing / "relay" / "migration_map.csv",
                  _header("relay.migration_map"),
                  [dict(old_crm_id=o, relay_id=r, matched_by="name+postcode")
                   for o, r in sorted(legacy.items())])
        self._csv(self.landing / "catalogue" / "catalogue.csv",
                  _header("catalogue.products"), self.products.values())
        for sku, m, cents in self.costs:
            slug = SLUGS[self.products[sku]["_supplier"]][0]
            self._csv(self.landing / "suppliers" / slug /
                      f"{slug}_{m}.csv",
                      _header("suppliers.price_lists", "standard"),
                      [dict(sku=sku, description=f"Item {sku[-3:]}",
                            unit_cost=f"{cents / 100:.2f}",
                            currency="USD", effective_from=f"{m}-01")])
        for name, rows in (("regions", REGIONS),
                           ("suppliers", SUPPLIERS)):
            head = _header(f"reference.{name}")
            self._csv(self.reference / f"{name}.csv", head,
                      [dict(zip(head, r)) for r in rows])
        self._tradeline()

    def _tradeline(self) -> None:
        self.tradeline_db.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self.tradeline_db)
        con.executescript(TRADELINE)
        con.executemany("INSERT INTO channels VALUES (?, ?)",
                        [("WEB", "Web"), ("TEL", "Telesales"),
                         ("FLD", "Field sales"), ("PTR", "Partner")])
        for o, ls in sorted(self.orders, key=lambda x: x[0]["id"]):
            con.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
                        tuple(o.values()))
            con.execute("INSERT INTO replication_log (committed_at, "
                        "table_name, op, row_json) VALUES (?,?,?,?)",
                        (o["updated_at"], "orders", "I", json.dumps(o)))
            for line in ls:
                con.execute("INSERT INTO order_lines VALUES "
                            "(?,?,?,?,?,?,?,?)", tuple(line.values()))
                con.execute("INSERT INTO replication_log (committed_at,"
                            " table_name, op, row_json) VALUES "
                            "(?,?,?,?)", (line["updated_at"],
                                          "order_lines", "I",
                                          json.dumps(line)))
        con.commit()
        con.close()

    # the pipeline, as the nightly run does it ------------------------
    def ingest(self, warehouse: Path | None = None):
        """Groundwork's ingest for the night, every tiny source."""
        from groundwork.ingest.__main__ import run
        return run(self.night, run_at=self.run_at,
                   only=["tradeline", "relay", "suppliers", "catalogue",
                         "reference"],
                   landing=self.landing, reference_dir=self.reference,
                   tradeline_db=self.tradeline_db,
                   root=self.root / "raw",
                   warehouse=warehouse or self.warehouse,
                   archive=self.root / "landed")

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse.duckdb"

    SELECT = ("dim_date +fct_order_lines +fct_orders +fct_contracts "
              "+fct_contract_month")

    def transform(self, project=None, warehouse: Path | None = None,
                  select: str = SELECT) -> dict[str, int]:
        """dbt build of the order and contract marts, and every test
        whose every parent is built, into the tiny warehouse."""
        return dbt_build(project or PROJECT, warehouse or self.warehouse,
                         select=select, night=self.night)

    def build(self, project=None) -> Path:
        """Write the sources, ingest them, build the marts: the tiny
        warehouse. Raises if any load or any dbt node fails."""
        self.write()
        bad = [x for x in self.ingest() if x.status not in
               ("passed", "nothing new")]
        if bad:
            raise AssertionError(f"ingest: {bad}")
        got = self.transform(project or PROJECT)
        if set(got) - {"success", "pass"}:
            raise AssertionError(f"dbt build: {got}")
        return self.warehouse


def dbt_build(project, warehouse, select=None, night=NIGHT,
              full_refresh=True) -> dict[str, int]:
    """`dbt build` of `project` into `warehouse` (a file named
    warehouse.duckdb, as dbt names the database after it); dbt's
    statuses, counted."""
    warehouse = Path(warehouse).resolve()
    target = warehouse.parent / "target"
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               GROUNDWORK_DBT_PATH=str(warehouse))
    cmd = [str(Path(sys.executable).parent / "dbt"), "build",
           "--target", "dev", "--profiles-dir", ".",
           "--vars", json.dumps({"night": str(night)}),
           "--target-path", str(target),
           "--log-path", str(target / "logs"), "--quiet"]
    if full_refresh:
        cmd.append("--full-refresh")
    if select:
        cmd += ["--select", *select.split(),
                "--indirect-selection", "cautious"]
    done = subprocess.run(cmd, cwd=project, env=env, text=True,
                          capture_output=True)
    got: dict[str, int] = {}
    results = target / "run_results.json"
    if results.exists():
        for r in json.loads(results.read_text())["results"]:
            got[r["status"]] = got.get(r["status"], 0) + 1
    shutil.rmtree(target, ignore_errors=True)
    if not got:
        raise RuntimeError((done.stdout + done.stderr).strip()[-800:])
    return got


def example(root) -> Sources:
    """The tiny Meridian the suite's integration and diff tests share.
    Every row is there for a case the pipeline must get right."""
    s = Sources(root)
    s.account(10, account_name="Ashby Dental")             # Northeast
    s.account(20, account_name="Birch Logistics", region="West",
              segment="Enterprise")
    s.contract(1, 10, "2025-01-01", "2025-12-31", discount_pct=5)
    s.contract(2, 20, "2025-07-01", "2026-06-30", legacy_terms="true",
               discount_pct="")           # legacy terms: none recorded
    s.product("MRD-CLE-001", supplier=1, cost_cents=1200)
    s.cost("MRD-CLE-001", "2025-12", 1300)  # dearer from 1 December
    s.product("MRD-FAC-002", supplier=7, cost_cents=500)
    s.order(1, 10, "2025-12-01 15:00:00",
            lines=[("MRD-CLE-001", 2, 1999), ("MRD-FAC-002", 1, 999)])
    s.order(2, 20, "2025-12-02 03:30:00")   # 1 December in Sacramento
    s.order(3, 10, "2025-12-03 15:00:00", status="cancelled")
    s.order(4, 40, "2025-12-04 15:00:00")   # an id Relay never sent
    s.order(5, 10, "2025-12-01 04:30:00")   # 30 November in Newark
    return s


# ------------------------------------------- the project, unbuilt

# What the loaders keep beside the columns a source sends (Chapters 5,
# 6, 8 and 9), and what raw's views add: present in raw, never in a
# contract's list of what the producer sends.
KEPT = {"_loaded_at": "TIMESTAMP", "_load_id": "VARCHAR",
        "_source_file": "VARCHAR", "_source_query": "VARCHAR",
        "file": "VARCHAR", "file_sha256": "VARCHAR", "_page": "VARCHAR",
        "load_date": "DATE"}
SOURCE = re.compile(r"source\('(\w+)',\s*'(\w+)'\)")
REF = re.compile(r"ref\('(\w+)'\)")


def contract_for(raw: str, folder=contracts.HERE) -> str | None:
    """The contract behind raw table `raw` (relay_accounts, or
    tradeline_orders_current), or None if it has none."""
    have = set(contracts.names(folder))
    source, _, table = raw.removesuffix("_current").partition("_")
    return f"{source}.{table}" if f"{source}.{table}" in have else None


# Raw keeps what a source sent as the source typed it (DECISIONS §2): a
# database's and an API's integers are integers; a file's are text.
TYPED = {"tradeline", "frontline"}


def empty_raw(con, raw: str, folder=contracts.HERE) -> str | None:
    """raw.<raw> with no rows and the columns its contract promises,
    typed as raw keeps them, plus what the loaders keep; the contract's
    name, or None when there is no contract to build it from."""
    name = contract_for(raw, folder)
    if name is None:
        return None
    typed = name.split(".")[0] in TYPED
    cols = {c: "BIGINT" if typed and col.type == "integer" else "VARCHAR"
            for c, col in contracts.load(name, folder).columns.items()}
    cols.update({k: v for k, v in KEPT.items() if k not in cols})
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"CREATE OR REPLACE TABLE raw.{raw} ("
                + ", ".join(f'"{c}" {t}' for c, t in cols.items()) + ")")
    return name


def graph(project=None) -> dict[str, set[str]]:
    """Each model's refs, read from its file (the -- depends_on hints
    included), as dbt would find them."""
    project = Path(project or PROJECT)
    return {p.stem: set(REF.findall(p.read_text()))
            for p in (project / "models").rglob("*.sql")}


def bind_project(con, project=None, uncontracted=None,
                 folder=contracts.HERE) -> dict[str, str | None]:
    """Every model of the project created as a view, in dependency
    order, over empty raw tables built from the contracts and over the
    project's seeds: nothing is built and nothing is read, but every
    name every model uses must exist. A raw table with no contract is
    built from `uncontracted` ({table: {column: type}}) if it is named
    there. Model -> None if it bound, else the error (a model
    downstream of a failure is not tried)."""
    project = Path(project or PROJECT)
    for raw, cols in (uncontracted or {}).items():
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(f"CREATE OR REPLACE TABLE raw.{raw} ("
                    + ", ".join(f'"{c}" {t}' for c, t in cols.items())
                    + ")")
    where, refs = _schemas(project), graph(project)
    files = {p.stem: p for p in (project / "models").rglob("*.sql")}
    current_files(con)
    out: dict[str, str | None] = {}
    for p in (project / "seeds").rglob("*.csv"):
        seed(con, p.stem, project)
    done = {m for m, s in where.items() if s in ("seeds", "snapshots")}
    while len(out) < len(files):
        ready = sorted(m for m in files if m not in out
                       and refs[m] <= done | set(out))
        if not ready:
            for m in files:
                out.setdefault(m, "a cycle, or a ref to nothing")
            break
        for m in ready:
            if any(out.get(r) for r in refs[m]):
                out[m] = "upstream failed"
                continue
            missing = [f"raw.{t}" for s, t in
                       SOURCE.findall(files[m].read_text())
                       if s == "raw" and t not in (uncontracted or {})
                       and empty_raw(con, t, folder) is None]
            if missing:
                out[m] = "no contract for " + ", ".join(missing)
                continue
            try:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {where[m]}")
                con.execute(f"CREATE OR REPLACE VIEW {where[m]}.{m} AS "
                            + render(m, project))
                out[m] = None
            except Exception as e:                 # noqa: BLE001
                out[m] = str(e).splitlines()[0]
    return out
