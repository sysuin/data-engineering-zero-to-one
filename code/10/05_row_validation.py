# Checking every row of every price list two ways: a pydantic model
# in Python, and the same rules as SQL. They must agree.
from collections import Counter
from datetime import datetime
from decimal import Decimal
from typing import Literal

import duckdb
import pyarrow as pa
from pydantic import BaseModel, ValidationError, model_validator

from groundwork.config import LANDING
from groundwork.ingest import files, suppliers

rows = []
for a in files.discover(LANDING, "suppliers"):
    rows += suppliers.read(a.path.read_bytes(), a).rows
print(f"{len(rows):,} rows from every price list on disk")

FORMATS = {"standard": "%Y-%m-%d", "kestrel": "%Y-%m-%d",
           "pemberton": "%d/%m/%Y", "aldridge": "%d %B %Y"}
LIMITS = {"dollars": (Decimal("0.50"), Decimal("1000.00")),
          "cents": (Decimal(50), Decimal(100000))}


class PriceListRow(BaseModel):
    supplier: str
    sku: str
    cost: Decimal                      # required: None fails
    cost_unit: Literal["dollars", "cents"]
    layout: Literal["standard", "kestrel", "pemberton", "aldridge"]
    effective_from: str

    @model_validator(mode="after")
    def promises(self):
        lo, hi = LIMITS[self.cost_unit]
        if self.cost_unit == "cents" and self.cost != int(self.cost):
            raise ValueError("cents must be whole")
        if not lo <= self.cost <= hi:
            raise ValueError(f"cost {self.cost} outside {lo}..{hi}")
        datetime.strptime(self.effective_from, FORMATS[self.layout])
        return self


bad_py = {}
for i, row in enumerate(rows, 1):
    try:
        PriceListRow(**row)
    except ValidationError as e:
        bad_py[i] = e.errors()[0]["loc"][0] if e.errors()[0]["loc"] \
            else "row"
print(f"pydantic: {len(bad_py):,} rows fail; "
      f"{dict(Counter(bad_py.values()))}")

con = duckdb.connect()
t = pa.Table.from_pylist(rows)
con.register("batch", t)
bad_sql = dict(con.execute("""
    SELECT n, CASE WHEN cost IS NULL THEN 'cost'
                   WHEN NOT ok THEN 'cost'
                   ELSE 'effective_from' END
    FROM (SELECT row_number() OVER () AS n, *,
            CASE cost_unit
              WHEN 'cents' THEN regexp_full_match(cost, '[0-9]+')
                AND cost::BIGINT BETWEEN 50 AND 100000
              WHEN 'dollars' THEN TRY_CAST(cost AS DECIMAL(12, 2))
                BETWEEN 0.50 AND 1000.00 END AS ok,
            CASE layout
              WHEN 'pemberton' THEN try_strptime(effective_from,
                                                 '%d/%m/%Y')
              WHEN 'aldridge' THEN try_strptime(effective_from,
                                                '%d %B %Y')
              ELSE try_strptime(effective_from, '%Y-%m-%d') END AS d
          FROM batch)
    WHERE cost IS NULL OR NOT ok OR d IS NULL
    ORDER BY n""").fetchall())
print(f"SQL:      {len(bad_sql):,} rows fail; "
      f"{dict(Counter(bad_sql.values()))}")
print(f"the same rows: {sorted(bad_py) == sorted(bad_sql)}")
who = Counter(rows[i - 1]["supplier"] for i in bad_sql)
print(f"failing rows by supplier: {dict(who)}")

# The unit rule matters: Kestrel's cents read as dollars.
(n,) = con.execute("""SELECT count(*) FROM batch WHERE layout =
    'kestrel' AND cost::DECIMAL(12, 2) > 1000.00""").fetchone()
k = sum(1 for r in rows if r["layout"] == "kestrel")
print(f"Kestrel's rows above $1,000 if its cents were dollars: "
      f"{n} of {k}")
