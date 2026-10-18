"""
Data contracts: what each raw table promises, written down.

One YAML file per raw table, named <source>.<table>.yaml, says who
produces the table and who owns it, what its rows are, which headers
its producer may send, and for every column its type, whether it may
be empty, the values it may take, its range and its unit. check.py
holds a batch to it.

    load("suppliers.price_lists")   the contract, as a Contract
    names()                         every table with a contract

A contract changes by a new version of its file, with the change
written in its `changes` list; compatible() says whether a new
version can break anyone who relied on the old one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
TYPES = ("text", "integer", "decimal", "date", "timestamp", "boolean")


@dataclass(frozen=True)
class Column:
    """One column's promises. `when` refines them by another column's
    value: {"cost_unit": {"cents": {...}, "dollars": {...}}}."""
    name: str
    type: str = "text"
    nullable: bool = False
    allowed: tuple | None = None
    pattern: str | None = None
    min: float | None = None
    max: float | None = None
    scale: int | None = None
    format: str | None = None
    unit: str | None = None
    stamped: bool = False             # added by the loader, not sent
    when: dict = field(default_factory=dict)

    def spec(self) -> dict:
        """The column's base rules as a plain dict (no `when`)."""
        keys = ("type", "nullable", "allowed", "pattern", "min", "max",
                "scale", "format")
        return {k: getattr(self, k) for k in keys}

    def cases(self):
        """(column, value, rules) for each refinement, rules merged
        over the base: the rules that hold where column = value."""
        for col, values in self.when.items():
            for value, extra in values.items():
                yield col, str(value), {**self.spec(), **extra}


@dataclass(frozen=True)
class Contract:
    table: str                        # "<source>.<table>"
    version: int
    description: str
    producer: dict
    owner: str
    grain: str
    columns: dict                     # name -> Column, in order
    key: tuple = ()
    formats: dict = field(default_factory=dict)  # name -> header
    history: dict | None = None
    policy: dict = field(default_factory=dict)
    changes: tuple = ()

    @property
    def source(self) -> str:
        return self.table.split(".")[0]

    @property
    def name(self) -> str:
        return self.table.split(".")[1]

    @property
    def bad_rows(self) -> str:
        """'quarantine' (park them, load the rest) or 'fail'."""
        return self.policy.get("bad_rows", "quarantine")

    @property
    def max_bad_share(self) -> float:
        """Above this share of failing rows, a batch is broken."""
        return float(self.policy.get("max_bad_share", 0.5))


def _column(name: str, spec: dict | None) -> Column:
    spec = dict(spec or {})
    if spec.get("type", "text") not in TYPES:
        raise ValueError(f"{name}: unknown type {spec['type']!r}")
    if "allowed" in spec:
        spec["allowed"] = tuple(str(v).lower() if isinstance(v, bool)
                                else str(v) for v in spec["allowed"])
    for col, values in spec.get("when", {}).items():
        spec["when"][col] = {str(k): v for k, v in values.items()}
    return Column(name, **spec)


def parse(doc: dict) -> Contract:
    """A Contract from a contract file's YAML, already loaded."""
    cols = {n: _column(n, s) for n, s in doc["columns"].items()}
    return Contract(
        table=doc["table"], version=int(doc["version"]),
        description=" ".join(doc.get("description", "").split()),
        producer=doc.get("producer", {}), owner=doc.get("owner", ""),
        grain=doc.get("grain", ""), columns=cols,
        key=tuple(doc.get("key", ())),
        formats={k: tuple(v) for k, v in
                 (doc.get("formats") or {}).items()},
        history=doc.get("history"), policy=doc.get("policy") or {},
        changes=tuple(doc.get("changes") or ()))


def path(table: str, folder: Path = HERE) -> Path:
    return Path(folder) / f"{table}.yaml"


def load(table: str, folder: Path = HERE) -> Contract:
    """The contract for '<source>.<table>'."""
    return parse(yaml.safe_load(path(table, folder).read_text()))


def names(folder: Path = HERE) -> list[str]:
    """Every raw table that has a contract, sorted."""
    return sorted(p.stem for p in Path(folder).glob("*.yaml"))


def compatible(old: Contract, new: Contract) -> list[tuple[str, str]]:
    """
    What changed from `old` to `new`, each as (who it can break,
    what changed). A change that lets more through (a new value, a
    wider range, a column that may now be empty) can break consumers,
    who may meet something they never handled; one that lets less
    through can break the producer, whose next batch may now fail. An
    additive change, an optional column or a new header that lands in
    the same columns, breaks nobody.
    """
    out = []
    for n, c in new.columns.items():
        if n not in old.columns:
            out.append(("nobody" if c.nullable else "producer",
                        f"column {n} added"
                        + ("" if c.nullable else ", required")))
    for n, c in old.columns.items():
        if n not in new.columns:
            out.append(("consumers", f"column {n} removed"))
            continue
        d = new.columns[n]
        if d.type != c.type:
            out.append(("both", f"{n}: {c.type} became {d.type}"))
        if c.nullable != d.nullable:
            out.append(("consumers" if d.nullable else "producer",
                        f"{n}: may {'now' if d.nullable else 'no longer'}"
                        " be empty"))
        if c.allowed and d.allowed and c.allowed != d.allowed:
            more = sorted(set(d.allowed) - set(c.allowed))
            fewer = sorted(set(c.allowed) - set(d.allowed))
            if more:
                out.append(("consumers", f"{n}: new values {more}"))
            if fewer:
                out.append(("producer", f"{n}: withdrawn {fewer}"))
        if (c.min, c.max) != (d.min, d.max):
            lo = d.min is None or (c.min is not None and d.min <= c.min)
            hi = d.max is None or (c.max is not None and d.max >= c.max)
            out.append(("consumers" if lo and hi else "producer",
                        f"{n}: range {c.min}..{c.max} became "
                        f"{d.min}..{d.max}"))
    for f, header in new.formats.items():
        if f not in old.formats:
            out.append(("nobody", f"format {f} added"))
        elif header != old.formats[f]:
            out.append(("producer", f"format {f} changed"))
    for f in old.formats:
        if f not in new.formats:
            out.append(("producer", f"format {f} withdrawn"))
    return out
