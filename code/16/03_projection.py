# Reading less: the columns and row groups a query needs from raw.
import json
from pathlib import Path

from _plans import analyze, connect, name, operators

from groundwork.config import RAW

FILE = (RAW / "tradeline" / "order_lines" / "load_date=2026-01-04"
        / "part-0.parquet")
con = connect()
chunks = con.execute(f"""
    SELECT row_group_id, path_in_schema, stats_min_value,
           stats_max_value, total_compressed_size,
           total_uncompressed_size
    FROM parquet_metadata('{FILE}')""").fetchall()
groups = sorted({c[0] for c in chunks})
print(f"raw order lines: {len(groups)} row groups, "
      f"{len(chunks) // len(groups)} columns, "
      f"{sum(c[4] for c in chunks):,} bytes on disk")

SUM = ("SELECT sum(qty * unit_price_cents) "
       "FROM raw.tradeline_order_lines")
QUERIES = {
    "SELECT *": ("SELECT * FROM raw.tradeline_order_lines", None),
    "sum, all rows": (SUM, None),
    "sum, order_id >= 450000": (SUM + " WHERE order_id >= 450000",
                                ("order_id", "450000")),
    "sum, created_at >= Dec": (SUM + " WHERE created_at >= "
                               "'2025-12-01'",
                               ("created_at", "2025-12-01")),
    "sum, left(created_at,7)": (SUM + " WHERE left(created_at, 7) "
                                 "= '2025-12'", None),
}


def can_match(group, test):
    """Can this row group hold a match? Its footer statistics say."""
    if test is None:
        return True
    column, low = test
    (top,) = [c[3] for c in chunks if c[0] == group and c[1] == column]
    as_num = column == "order_id"
    return (int(top) >= int(low)) if as_num else (top >= low)


print(f"{'query':<24}{'cols':>5}{'groups':>7}{'bytes':>11}"
      f"{'share':>7}{'uncompressed':>13}")
units = {}
for label, (sql, test) in QUERIES.items():
    (scan,) = [n for _, n in operators(analyze(con, sql))
               if name(n) == "READ_PARQUET"]
    info = scan["extra_info"]
    cols = [c for c in info["Projections"] if c != "load_date"]
    cols += [c for c in ("order_id", "created_at")
             if c in info.get("Filters", "") and c not in cols]
    read = [g for g in groups if can_match(g, test)]
    size = sum(c[4] for c in chunks if c[0] in read and c[1] in cols)
    plain = sum(c[5] for c in chunks if c[0] in read and c[1] in cols)
    units[label] = {"bytes": size, "uncompressed": plain}
    print(f"{label:<24}{len(cols):>5}{len(read):>7}{size:>11,}"
          f"{size / units['SELECT *']['bytes']:>7.1%}"
          f"{plain / 1e6:>10.1f} MB")
(Path(__file__).parent / "03_projection.json").write_text(
    json.dumps(units, indent=1) + "\n")

print()
print("through the staging view, which keeps each line's latest row:")
VIEW = ("SELECT sum(qty * unit_price_cents) "
        "FROM staging.stg_tradeline__order_lines WHERE ")
for where in ("order_id >= 450000", "created_at >= '2025-12-01'"):
    window = [n for _, n in operators(analyze(con, VIEW + where))
              if name(n) == "WINDOW"][0]
    print(f"  {where:<28} window ranked "
          f"{window['operator_cardinality']:>9,} rows")
