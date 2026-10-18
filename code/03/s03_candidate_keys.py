# Every column that could be a key: all values present and distinct.
from groundwork.config import TRADELINE_DB
from groundwork.profile import connect, profile

con = connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")


def candidate_keys(p) -> list[str]:
    """Columns with no missing value and a new value on every row."""
    return [c.name for c in p.columns
            if c.nulls == 0 and c.distinct == p.rows]


for table in ("orders", "order_lines"):
    p = profile(con.table(f"tradeline.{table}"), table)
    print(f"{table:<12}{p.rows:>11,} rows   candidate keys: "
          f"{', '.join(candidate_keys(p)) or 'none'}")
