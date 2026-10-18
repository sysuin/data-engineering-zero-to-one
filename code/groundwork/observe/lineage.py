"""
Where every table and column comes from, and what is built from it.

Two kinds of lineage, read from things Groundwork already has rather
than drawn by hand:

    table level    the orchestrator's DAG says which task reads which
                   system and writes which raw tables (Chapter 17);
                   dbt's manifest says which model reads which source,
                   seed or model, and which tests read each model
                   (Chapter 14). Joined, they run from a system's table
                   to a mart.
    column level   each model's compiled SQL, parsed with sqlglot, says
                   which upstream columns each of its columns is
                   computed from ("value"), and which upstream columns
                   decide which rows it has at all: a WHERE, a QUALIFY,
                   a HAVING or a semi join ("filter"). Raw keeps the
                   source's own column names (Chapter 5), so the last
                   hop, from raw back to the system, is read from the
                   contracts (Chapter 10).

Nodes are names: a system's table "tradeline.order_lines", a warehouse
relation "marts.fct_order_lines", a task "task:ingest_tradeline", a
test "test:<name>", and a column "<relation>.<column>".

    Graph                    nodes with a kind, edges with a how
    tables(dag, manifest)    the table-level graph
    columns(manifest, schema)            the column-level graph
    compile_project(warehouse, target)   dbt compile, into a sandbox
    schema_of(con)           {schema: {table: {column: type}}}
    impact(cols, tables, column)         what a change would reach
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage, to_node
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, traverse_scope

from groundwork import contracts
from groundwork.config import ROOT

PROJECT = ROOT / "dbt" / "groundwork"
DBT = Path(sys.executable).parent / "dbt"


# ------------------------------------------------------------ the graph
@dataclass
class Graph:
    """Nodes, each with a kind, and edges (upstream, downstream, how)."""
    kinds: dict = field(default_factory=dict)
    edges: set = field(default_factory=set)

    def add(self, up: str, down: str, how: str,
            up_kind: str | None = None, down_kind: str | None = None):
        if up_kind:
            self.kinds.setdefault(up, up_kind)
        if down_kind:
            self.kinds.setdefault(down, down_kind)
        self.edges.add((up, down, how))

    def parents(self, node: str, how=None) -> set[str]:
        return {a for a, b, h in self.edges
                if b == node and (how is None or h in how)}

    def children(self, node: str, how=None) -> set[str]:
        return {b for a, b, h in self.edges
                if a == node and (how is None or h in how)}

    def _walk(self, node, step, how) -> set[str]:
        seen, todo = set(), [node]
        while todo:
            for n in step(todo.pop(), how):
                if n not in seen:
                    seen.add(n)
                    todo.append(n)
        return seen

    def upstream(self, node: str, how=None) -> set[str]:
        """Everything `node` is built from, directly or not."""
        return self._walk(node, self.parents, how)

    def downstream(self, node: str, how=None) -> set[str]:
        """Everything built from `node`, directly or not."""
        return self._walk(node, self.children, how)

    def of_kind(self, nodes, *kinds) -> list[str]:
        return sorted(n for n in nodes if self.kinds.get(n) in kinds)


def relation(node: dict) -> str:
    """'schema.name' for a manifest node or source."""
    name = node.get("alias") or node.get("identifier") or node["name"]
    return f"{node['schema']}.{name}"


# ---------------------------------------------------------- table level
def _raw_tables(manifest: dict, source: str) -> list[str]:
    return sorted(relation(s) for s in manifest["sources"].values()
                  if s["schema"] == "raw"
                  and s["name"].startswith(f"{source}_"))


def tables(dag, manifest: dict) -> Graph:
    """
    One graph from a system's table to a mart and its tests: the DAG's
    ingest tasks (which system each reads, which raw tables it writes)
    joined to dbt's manifest (which model reads what).
    """
    g = Graph()
    for name in dag.tasks:
        if name.startswith("ingest_"):
            source = name.removeprefix("ingest_")
            writes = _raw_tables(manifest, source)
            if source == "frontline":     # comments come from their task
                writes = [w for w in writes if not w.endswith("comments")]
            for c in contracts.names():
                if c.split(".")[0] == source and not c.endswith("_log"):
                    g.add(c, f"task:{name}", "read by", "system", "task")
            for w in writes:
                g.add(f"task:{name}", w, "writes", "task", "raw")
    if "load_comments" in dag.tasks:
        g.add("frontline.tickets", "task:load_comments", "read by",
              "system", "task")
        g.add("task:load_comments", "raw.frontline_ticket_comments",
              "writes", "task", "raw")
    nodes = {**manifest["nodes"], **manifest["sources"]}
    kind = {"model": None, "seed": "seed", "snapshot": "snapshot",
            "source": "raw", "test": "test"}
    for child, parents in manifest["parent_map"].items():
        c = nodes[child]
        if c["resource_type"] == "test":
            down, dk = f"test:{c['name']}", "test"
        else:
            down = relation(c)
            dk = kind[c["resource_type"]] or c["schema"]
        for p in parents:
            if p not in nodes:
                continue
            n = nodes[p]
            up = relation(n)
            uk = kind[n["resource_type"]] or n["schema"]
            if n["resource_type"] == "source" and n["schema"] != "raw":
                uk = "audit"
            g.add(up, down, "tests" if dk == "test" else "ref", uk, dk)
    return g


# --------------------------------------------------------- column level
def compile_project(warehouse, target, vars: dict | None = None) -> Path:
    """
    `dbt compile` on Groundwork's project into `target`, against a copy
    of the warehouse, as a full refresh: the SQL each model would run on
    a rebuild. Returns the manifest's path.
    """
    target = Path(target)
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               GROUNDWORK_DBT_PATH=str(Path(warehouse).resolve()))
    cmd = [str(DBT), "compile", "--target", "dev", "--profiles-dir", ".",
           "--target-path", str(target), "--log-path",
           str(target / "logs"), "--full-refresh", "--quiet",
           "--vars", json.dumps(vars or {"night": "2026-01-04"})]
    done = subprocess.run(cmd, cwd=PROJECT, env=env, text=True,
                          capture_output=True)
    if done.returncode != 0:
        raise RuntimeError("dbt compile: " + (done.stdout + done.stderr)
                           .strip().splitlines()[-1])
    return target / "manifest.json"


def schema_of(con) -> dict:
    """Every relation's columns in the warehouse, as sqlglot wants
    them: {schema: {table: {column: type}}}."""
    out: dict = {}
    for s, t, c, ty in con.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = current_database()
            ORDER BY table_schema, table_name, ordinal_position
            """).fetchall():
        out.setdefault(s, {}).setdefault(t, {})[c] = ty
    return out


def _parse(sql: str) -> exp.Expr:
    """A model's compiled SQL, with the database name taken off every
    table, so that names read schema.table whatever the file is."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for t in tree.find_all(exp.Table):
        t.set("catalog", None)
    return tree


def _leaves(node) -> set[str]:
    """The table columns at the bottom of a sqlglot lineage tree."""
    out = set()
    for n in node.walk():
        t = n.source
        if isinstance(t, exp.Table) and t.name and not n.downstream:
            name = n.name.split(".")[-1].strip('"')
            out.add(f"{t.db}.{t.name}.{name}")
    return out


def _filters(tree: exp.Expr, schema: dict) -> set[str]:
    """
    The upstream columns that decide which rows a query has: those in
    a WHERE, a HAVING, a QUALIFY or a semi or anti join's condition,
    in the query, its CTEs and the subqueries it reads FROM, followed
    back to a table. A subquery that computes one value chooses no
    rows, and is left out.
    """
    q = qualify(tree.copy(), schema=schema, dialect="duckdb",
                validate_qualify_columns=False)
    out = set()
    for scope in traverse_scope(q):
        if scope.is_subquery:             # a value, not a set of rows
            continue
        sel = scope.expression
        preds = [sel.args.get(k) for k in ("where", "having", "qualify")]
        for j in sel.args.get("joins") or []:
            if (j.args.get("kind") or "").upper() in ("SEMI", "ANTI"):
                preds.append(j.args.get("on"))
        for pred in filter(None, preds):
            for col in pred.find_all(exp.Column):
                src = scope.sources.get(col.table)
                if isinstance(src, exp.Table) and src.name:
                    out.add(f"{src.db}.{src.name}.{col.name}")
                elif isinstance(src, Scope):
                    try:
                        out |= _leaves(to_node(col.name, src, "duckdb"))
                    except Exception:     # noqa: BLE001 a column sqlglot
                        continue          # cannot follow is left out
    return out


def models(manifest: dict) -> dict[str, str]:
    """{relation: compiled SQL} for every model and snapshot."""
    out = {}
    for n in manifest["nodes"].values():
        if n["resource_type"] in ("model", "snapshot") and \
                n.get("compiled_code"):
            out[relation(n)] = n["compiled_code"]
    return out


def column_edges(rel: str, sql: str, schema: dict
                 ) -> tuple[set[tuple[str, str, str]], list[str]]:
    """The edges into one model's columns, and any column sqlglot could
    not follow."""
    s, t = rel.split(".")
    outputs = list(schema.get(s, {}).get(t, {}))
    tree = _parse(sql)
    edges, lost = set(), []
    for col in outputs:
        try:
            ups = _leaves(lineage(col, tree, schema=schema,
                                  dialect="duckdb"))
        except Exception:                 # noqa: BLE001
            lost.append(f"{rel}.{col}")
            continue
        edges |= {(u, f"{rel}.{col}", "value") for u in ups}
    try:
        edges |= {(u, rel, "filter") for u in _filters(tree, schema)}
    except Exception:                     # noqa: BLE001
        lost.append(f"{rel} (filters)")
    return edges, lost


def columns(manifest: dict, schema: dict) -> Graph:
    """
    The column-level graph of every model, and the contracts' last hop
    from each raw column back to the system's own. A "filter" edge goes
    from a column to the relation whose rows it chooses.
    """
    g = Graph()
    g.lost = []
    for rel, sql in sorted(models(manifest).items()):
        edges, lost = column_edges(rel, sql, schema)
        g.lost += lost
        for up, down, how in edges:
            g.add(up, down, how, "column",
                  "column" if how == "value" else "relation")
    for name in contracts.names():
        c = contracts.load(name)
        for view in (f"{c.source}_{c.name}", f"{c.source}_{c.name}_current"):
            have = schema.get("raw", {}).get(view, {})
            for col in c.columns:
                if col in have and not c.columns[col].stamped:
                    g.add(f"{name}.{col}", f"raw.{view}.{col}", "value",
                          "system", "column")
    return g


# ------------------------------------------------------------- impact
def owners(column: str) -> dict:
    """Who sends a system's column and who answers for its contract."""
    table = ".".join(column.split(".")[:2])
    if table not in contracts.names():
        return {}
    c = contracts.load(table)
    return {"producer": c.producer.get("who", ""),
            "contact": c.producer.get("contact", ""), "owner": c.owner}


def tests_reading(manifest: dict, columns) -> list[str]:
    """
    The dbt tests that look at any of `columns`: a generic test on one
    of them, or a singular test over one of their relations whose SQL
    names one of them. A test that never reads a column cannot notice
    that it changed.
    """
    by_rel = defaultdict(set)
    for c in columns:
        r, name = c.rsplit(".", 1)
        by_rel[r].add(name)
    nodes = manifest["nodes"]
    out = set()
    for t in nodes.values():
        if t["resource_type"] != "test":
            continue
        rels = {relation(nodes[n]) for n in t["depends_on"]["nodes"]
                if n in nodes}
        hit = [r for r in rels if r in by_rel]
        if not hit:
            continue
        col = t.get("column_name")
        if col is not None:
            if any(col in by_rel[r] for r in hit):
                out.add(t["name"])
            continue
        sql = (t.get("compiled_code") or t.get("raw_code") or "").lower()
        words = set(_words(sql))
        if any(name in words for r in hit for name in by_rel[r]):
            out.add(t["name"])
    return sorted(out)


def _words(sql: str) -> list[str]:
    word, out = [], []
    for ch in sql + " ":
        if ch.isalnum() or ch == "_":
            word.append(ch)
        elif word:
            out.append("".join(word))
            word = []
    return out


def impact(cols: Graph, tbls: Graph, column: str,
           manifest: dict | None = None, rows: bool = True) -> dict:
    """
    What a change to `column` would reach: the columns computed from it
    and, with `rows`, every column of any relation whose rows it
    chooses (a filter changes which rows exist, so it changes all of
    them); the relations they are in; and the dbt tests that read them
    (every test on those relations, when no manifest is given to say
    which columns a test reads).
    """
    reached, todo = set(), [column]
    while todo:
        n = todo.pop()
        for c in cols.children(n):
            if cols.kinds.get(c) == "relation":     # it filters c's rows
                more = {b for a, b, h in cols.edges if h == "value"
                        and b.rsplit(".", 1)[0] == c} if rows else set()
            else:
                more = {c}
            for m in more - reached:
                reached.add(m)
                todo.append(m)
    rels = sorted({c.rsplit(".", 1)[0] for c in reached})
    if manifest is not None:
        tests = [f"test:{t}" for t in tests_reading(manifest, reached)]
    else:
        tests = sorted({t for r in rels
                        for t in tbls.children(r, ("tests",))})
    by_rel = defaultdict(list)
    for c in sorted(reached):
        r, name = c.rsplit(".", 1)
        by_rel[r].append(name)
    return {"column": column, "columns": sorted(reached),
            "relations": rels, "by_relation": dict(by_rel),
            "tests": tests, "owners": owners(column)}
