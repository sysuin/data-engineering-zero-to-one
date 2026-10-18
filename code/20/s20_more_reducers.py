# Exercise 20.2: the job from 03_map_reduce.py with 16 reducers.
from importlib import import_module

mr = import_module("03_map_reduce")

if __name__ == "__main__":
    for reducers in (4, 16):
        maps, reds = mr.run(4, reducers)
        files = sum(len(m["bytes"]) for m in maps)
        mb = sum(sum(m["bytes"]) for m in maps) / 1e6
        records = sum(r["records_in"] for r in reds)
        print(f"{reducers:>3} reducers: {files:>3} files, "
              f"{records:,} records, {mb:.1f} MB shuffled")
