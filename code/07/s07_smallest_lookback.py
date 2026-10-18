# Exercise 1: the >= loader over the thirty nights with lookbacks of
# a few minutes, to find the smallest that loses no amended order.
from datetime import timedelta

from _watermark import Loader, compare, nights

MINUTES = (0, 2, 4, 5, 6, 10)
loaders = {m: Loader("updated_at", ">=", timedelta(minutes=m))
           for m in MINUTES}
for night, moment, src in nights():
    for loader in loaders.values():
        loader.run(night, src)

print(f"{'lookback':>9} {'rows read':>11} {'amended orders wrong':>21}")
for m, loader in loaders.items():
    _, kinds = compare(loader)
    print(f"{m:>6} min {loader.rows_read:>11,} "
          f"{kinds['lines amended at 02:00']:>21,}")
