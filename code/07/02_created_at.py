# A high-water mark on created_at: thirty nights of copying only the
# rows created since the last run, then the copy set beside Tradeline.
from _watermark import Loader, compare, nights

loader = Loader(column="created_at")
for night, moment, src in nights():
    loader.run(night, src)
    if night == 0:
        print(f"snapshot at {moment:%Y-%m-%d %H:%M}: "
              f"{loader.rows_read:,} rows")
print(f"30 nights to {moment:%Y-%m-%d %H:%M}: "
      f"{loader.rows_read:,} rows read in all")

wrong, kinds = compare(loader)
print(f"orders that differ from Tradeline: {len(wrong):,}")
for kind, n in kinds.items():
    print(f"  {kind:<26} {n:>5,}")

# The last order whose lines the copy has wrong: copy, then Tradeline
oid = max(i for i, lines, ours, theirs, _ in wrong
          if lines and ours == theirs)
LINES = """SELECT line_no, qty, created_at, updated_at FROM {}
           WHERE order_id = ? ORDER BY line_no"""
for name, table in (("the copy", "copy_order_lines"),
                    ("Tradeline", "src.order_lines")):
    print(f"order {oid} in {name}:")
    for no, qty, created, updated in loader.db.execute(
            LINES.format(table), [oid]).fetchall():
        print(f"  line {no} qty {qty}  created {created[5:]}  "
              f"updated {updated[5:]}")
