# Strictly after the watermark, or at it too: > and >= on updated_at
# to the second, and on the same stamp cut to its date.
from _watermark import Loader, compare, nights

DAY = "substr(updated_at, 1, 10)"             # a date, not a moment
loaders = {("second", ">"): Loader("updated_at", ">"),
           ("second", ">="): Loader("updated_at", ">="),
           ("date", ">"): Loader(DAY, ">"),
           ("date", ">="): Loader(DAY, ">=")}
for night, moment, src in nights():
    for loader in loaders.values():
        loader.run(night, src)

print(f"{'watermark':<12} {'rows read':>11} {'orders wrong':>13} "
      f"{'rows read twice':>16}")
for (grain, op), loader in loaders.items():
    wrong, kinds = compare(loader)
    missed = len(wrong) - kinds["deleted in Tradeline"]
    twice = sum(loader.db.execute(
        f"SELECT (SELECT count(*) FROM got_{t}) - (SELECT count(*) "
        f"FROM (SELECT DISTINCT * EXCLUDE (night) FROM got_{t}))"
    ).fetchone()[0] for t in ("orders", "order_lines"))
    print(f"{grain + ' ' + op:<12} {loader.rows_read:>11,} "
          f"{missed:>13,} {twice:>16,}")
