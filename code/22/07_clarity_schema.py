# What Clarity is given: the marts and the metrics, described for a
# model, measured against handing it the whole catalogue.
# timeout: 300
import json
from pathlib import Path

import duckdb

from groundwork.serve import clarity

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()), read_only=True)
text = clarity.describe(con)

# The alternative: every column of every table and view, as DDL.
# The schemas Groundwork has by this chapter. Chapter 24 adds masked
# and the role_ schemas, Chapter 26 compat, metrics, features, govern
# and audit.book_reconciliations; they are left out here.
LAYERS = ("raw", "staging", "intermediate", "marts", "seeds",
          "snapshots", "audit")
everything, columns = [], 0
for schema, table, cols, n in con.execute("""
        SELECT table_schema, table_name,
               string_agg(column_name || ' ' || data_type, ', '
                          ORDER BY ordinal_position), count(*)
        FROM information_schema.columns
        WHERE list_contains(?, table_schema)
          AND table_name <> 'book_reconciliations'
        GROUP BY ALL ORDER BY ALL""", [list(LAYERS)]).fetchall():
    everything.append(f"CREATE TABLE {schema}.{table} ({cols});")
    columns += n
whole = "\n".join(everything)
print(f"{'':<26}{'relations':>10}{'columns':>9}{'characters':>12}")
print(f"{'the whole catalogue':<26}{len(everything):>10}"
      f"{columns:>9,}{len(whole):>12,}")
named = clarity.columns_named(text)
tables = {t for t, _ in named}
print(f"{'the description':<26}{len(tables):>10}{len(named):>9,}"
      f"{len(text):>12,}")
built = set(con.execute("""
    SELECT table_schema || '.' || table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'marts'""").fetchall())
print(f"marts columns {len(built)}, described {len(named)}: "
      f"{len(named - built)} described and absent,\n  "
      f"{len(built - named)} present and not described")

lines = text.splitlines()
start = next(i for i, x in enumerate(lines)
             if x.startswith("marts.fct_contracts"))
stop = next(i for i in range(start + 1, len(lines))
            if lines[i].startswith("marts."))
print("\n".join(lines[start:stop]))
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"text": text, "whole": len(whole)}))
con.close()
tidy("warehouse.duckdb")
