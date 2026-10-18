"""
Groundwork's semantic layer: every metric defined once, in
metrics.yaml, and compiled to SQL against the marts.

Nobody who reads a metric writes its SQL. A dashboard, the metrics
API, Clarity and the reverse-ETL jobs name a metric, the dimensions to
break it down by, the filters and the period; compile() writes the
query, with every value a bound parameter. A dimension a metric does
not allow is refused, not guessed.

    catalogue()                       the definitions, from the YAML
    compile(m, by=, where=, grain=, start=, end=)   (sql, params)
    query(con, m, ...)                rows, as dictionaries
    publish(con)                      metrics.definitions and one
                                      monthly view per metric
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import yaml

YAML = Path(__file__).with_name("metrics.yaml")
GRAINS = ("day", "month", "quarter", "year")


class MetricError(ValueError):
    """A question the layer will not answer, and why."""


@dataclass(frozen=True)
class Metric:
    name: str
    label: str
    owner: str
    description: str
    table: str
    time: str
    measure: str
    kind: str
    dimensions: tuple[str, ...]
    filter: str | None = None
    joins: tuple[str, ...] = ()
    loads: tuple[str, ...] = ()


@dataclass(frozen=True)
class Catalogue:
    metrics: dict[str, Metric]
    dimensions: dict[str, dict]
    joins: dict[str, str] = field(default_factory=dict)

    def __getitem__(self, name: str) -> Metric:
        if name not in self.metrics:
            raise MetricError(f"no metric called {name!r}; the "
                              f"metrics are {sorted(self.metrics)}")
        return self.metrics[name]


def catalogue(path: Path = YAML) -> Catalogue:
    """Every definition in the YAML file, checked as it is read."""
    doc = yaml.safe_load(path.read_text())
    metrics = {}
    for name, m in doc["metrics"].items():
        metric = Metric(name=name, **{
            k: tuple(v) if isinstance(v, list) else v
            for k, v in m.items()})
        for d in metric.dimensions:
            if d not in doc["dimensions"]:
                raise MetricError(f"{name}: no dimension {d!r}")
        if metric.kind not in ("sum", "ratio", "snapshot"):
            raise MetricError(f"{name}: unknown kind {metric.kind!r}")
        metrics[name] = metric
    return Catalogue(metrics, doc["dimensions"], doc["joins"])


def compile(metric: str, by=(), where=None, grain=None, start=None,
            end=None, cat: Catalogue | None = None):
    """The SQL and parameters for one question of one metric.

    by     dimensions to break the metric down by
    where  {dimension: value or [values]}, ANDed
    grain  day, month, quarter or year; None for the whole period
    start, end   the period, [start, end), on the metric's time
    """
    cat = cat or catalogue()
    m, where = cat[metric], dict(where or {})
    for d in (*by, *where):
        if d not in m.dimensions:
            raise MetricError(f"{metric} cannot be broken down by "
                              f"{d!r}; it allows {list(m.dimensions)}")
    if grain is not None and grain not in GRAINS:
        raise MetricError(f"grain must be one of {GRAINS}")
    joins = list(m.joins) + [cat.dimensions[d]["join"]
                             for d in (*by, *where)
                             if "join" in cat.dimensions[d]]
    select = [f"date_trunc('{grain}', {m.time})::DATE AS {grain}"
              ] if grain else []
    select += [f"{cat.dimensions[d]['sql']} AS {d}" for d in by]
    select.append(f"{m.measure} AS {metric}")
    span, params = [], []
    for op, day in ((">=", start), ("<", end)):
        if day is not None:
            span.append(f"{m.time} {op} ?")
            params.append(day)
    conds = [m.filter] if m.filter else []
    for d, v in where.items():
        values = list(v) if isinstance(v, (list, tuple)) else [v]
        marks = ", ".join("?" for _ in values)
        conds.append(f"{cat.dimensions[d]['sql']} IN ({marks})")
        params += values
    source = f"{m.table} AS f"
    if m.kind == "snapshot":        # each period's last snapshot only
        part = f"date_trunc('{grain}', {m.time})" if grain else "1"
        inner = f" WHERE {' AND '.join(span)}" if span else ""
        source = (f"(SELECT f.*, max({m.time}) OVER (PARTITION BY "
                  f"{part}) AS _last\n      FROM {m.table} AS f"
                  f"{inner}) AS f")
        span = [f"{m.time} = f._last"]
    sql = [f"SELECT {', '.join(select)}", f"FROM {source}"]
    sql += [cat.joins[j] for j in dict.fromkeys(joins)]
    if span + conds:
        sql.append("WHERE " + "\n  AND ".join(span + conds))
    if len(select) > 1:
        sql.append("GROUP BY ALL ORDER BY ALL")
    return "\n".join(sql), params


def query(con, metric: str, **question) -> list[dict]:
    """Ask one question of one metric; the rows as dictionaries."""
    sql, params = compile(metric, **question)
    cur = con.execute(sql, params)
    names = [c[0] for c in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


# metrics.sales: one wide row per order line, for a person with a
# pivot table. Each column's meaning is stored with it in the catalogue.
SALES = """
CREATE OR REPLACE VIEW metrics.sales AS
SELECT f.order_date, f.order_number, f.channel,
       r.region_name AS region, a.account_name AS account, a.segment,
       a.owner AS account_manager, p.sku, p.product_name AS product,
       p.category, s.supplier_name AS supplier, f.qty, f.unit_price,
       f.revenue, f.cost, f.gross_margin
FROM marts.fct_order_lines AS f
JOIN marts.dim_region AS r USING (region_key)
JOIN marts.dim_account AS a USING (account_key)
JOIN marts.dim_product AS p USING (product_key)
JOIN marts.dim_supplier AS s ON s.supplier_key = f.supplier_key
"""
SALES_DOCS = {
    "order_date": "the depot's date of the order",
    "order_number": "on the customer's paperwork",
    "channel": "how the order came in",
    "region": "the account's region today",
    "account": "the account's name today",
    "segment": "segment on the order's day",
    "account_manager": "manager on the order's day",
    "sku": "product code, upper case, trimmed",
    "product": "the catalogue's name",
    "category": "the catalogue's category",
    "supplier": "procurement's name for it",
    "qty": "units ordered",
    "unit_price": "dollars a unit, after discount",
    "revenue": "qty x unit_price",
    "cost": "qty x unit cost on the day",
    "gross_margin": "revenue - cost; NULL if no cost",
}


def publish(con, cat: Catalogue | None = None) -> int:
    """metrics.sales, metrics.definitions, and a monthly view of every
    metric, for tools that read tables rather than call the API."""
    cat = cat or catalogue()
    con.execute("CREATE SCHEMA IF NOT EXISTS metrics")
    con.execute(SALES)
    con.execute("COMMENT ON VIEW metrics.sales IS 'One row per line "
                "of a placed, not-cancelled order, every name joined "
                "in.'")
    for column, doc in SALES_DOCS.items():
        con.execute(f"COMMENT ON COLUMN metrics.sales.{column} IS "
                    f"'{doc.replace(chr(39), chr(39) * 2)}'")
    con.execute("""CREATE OR REPLACE TABLE metrics.definitions (
        name VARCHAR PRIMARY KEY, label VARCHAR, owner VARCHAR,
        kind VARCHAR, description VARCHAR, dimensions VARCHAR,
        sql VARCHAR)""")
    for m in cat.metrics.values():
        sql, _ = compile(m.name, grain="month", cat=cat)
        con.execute("INSERT INTO metrics.definitions VALUES "
                    "(?, ?, ?, ?, ?, ?, ?)",
                    [m.name, m.label, m.owner, m.kind, m.description,
                     ", ".join(m.dimensions), sql])
        con.execute(f"CREATE OR REPLACE VIEW metrics.{m.name}_monthly"
                    f" AS {sql}")
    return len(cat.metrics)


def period(year: int, month: int | None = None) -> tuple[date, date]:
    """[start, end) of a calendar year or month."""
    if month is None:
        return date(year, 1, 1), date(year + 1, 1, 1)
    end = date(year + month // 12, month % 12 + 1, 1)
    return date(year, month, 1), end
