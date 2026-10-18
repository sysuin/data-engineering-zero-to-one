# Create Groundwork's marts, empty, and fill the one no source sends.
import duckdb

from groundwork import dates
from groundwork.config import SANDBOX
from groundwork.ddl import create

path = SANDBOX / "ch04" / "warehouse.duckdb"
path.unlink(missing_ok=True)
con = duckdb.connect(str(path))

tables = create(con, "marts")
again = create(con, "marts")            # a second run changes nothing
loaded = dates.load(con)
dates.load(con)                          # nor does a second load

print(f"{'table':<20}{'rows':>6}  one row per")
for name, comment in con.execute("""
    SELECT table_name, comment FROM duckdb_tables()
    WHERE schema_name = 'marts' ORDER BY table_name
""").fetchall():
    (rows,) = con.execute(f"SELECT COUNT(*) FROM marts.{name}"
                          ).fetchone()
    grain = comment.removeprefix("One row per ").rstrip(".")
    print(f"{name:<20}{rows:>6,}  {grain}")

print(f"\nsame tables after a second run: {tables == again}")
print(f"dim_date rows after two loads: {loaded:,}")
for dtype, n in con.execute("""
    SELECT data_type, COUNT(*) FROM information_schema.columns
    WHERE table_schema = 'marts'
      AND column_name IN ('revenue', 'cost', 'gross_margin',
                          'unit_price', 'unit_cost', 'list_price')
    GROUP BY data_type ORDER BY data_type
""").fetchall():
    print(f"money columns: {n}, all {dtype}")
