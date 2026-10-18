# Inside a real Parquet file: magic bytes, row groups, column chunks,
# encodings and the statistics in its footer.
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from _order_lines import lines_parquet

path = lines_parquet()
raw = path.read_bytes()
pf = pq.ParquetFile(path)
meta = pf.metadata
footer = int.from_bytes(raw[-8:-4], "little")
print(f"{path.name}: {len(raw):,} bytes, {meta.num_rows:,} rows, "
      f"{meta.num_columns} columns")
print(f"starts {raw[:4]!r}, ends {raw[-4:]!r}, footer {footer:,} bytes")
last = meta.row_group(meta.num_row_groups - 1).num_rows
print(f"{meta.num_row_groups} row groups of 100,000 rows "
      f"(the last {last:,})")


def plain(col: pa.ChunkedArray) -> int:
    """Bytes the column would take written plainly, value by value."""
    if pa.types.is_string(col.type):
        return int(pc.sum(pc.binary_length(col)).as_py()) + 4 * len(col)
    return col.type.bit_width // 8 * len(col)


rg, first = meta.row_group(0), pf.read_row_group(0)
print(f"\nrow group 0: {rg.num_rows:,} rows"
      "     plain   encoded  on disk")
chunks = []
for i in range(rg.num_columns):
    c = rg.column(i)
    how = "dict" if "RLE_DICTIONARY" in c.encodings else "plain"
    p = plain(first.column(i))
    print(f"  {c.path_in_schema:<16} {c.physical_type:<10} {how:<4}"
          f"{p:>10,} {c.total_uncompressed_size:>9,}"
          f" {c.total_compressed_size:>8,}")
    chunks.append([c.path_in_schema, c.physical_type, how, p,
                   c.total_uncompressed_size, c.total_compressed_size])

print("\nfooter statistics    order_id          placed_at")
stats = []
for g in range(meta.num_row_groups):
    s_id = meta.row_group(g).column(0).statistics
    s_at = meta.row_group(g).column(3).statistics
    stats.append([g, s_id.min, s_id.max, str(s_at.min.date()),
                  str(s_at.max.date())])
    if g < 2 or g >= meta.num_row_groups - 2:
        print(f"  group {g:>2}   {s_id.min:>7} .. {s_id.max:<7}  "
              f"{s_at.min:%Y-%m-%d} .. {s_at.max:%Y-%m-%d}")
    elif g == 2:
        print("  ...")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "bytes": len(raw), "rows": meta.num_rows, "footer": footer,
    "groups": [[meta.row_group(g).num_rows,
                meta.row_group(g).total_byte_size,
                sum(meta.row_group(g).column(i).total_compressed_size
                    for i in range(meta.num_columns))]
               for g in range(meta.num_row_groups)],
    "chunks": chunks, "stats": stats}, indent=1))
