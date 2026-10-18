# timeout: 3600
# The rerun drill on the backfilled quarter: twenty nights chosen by the
# book's seed, each rebuilt alone and compared with what is there; then
# one row spoiled in a copy, to show that the drill can fail.
import os

import pyarrow as pa
import pyarrow.parquet as pq
from _sandbox import HERE, copy_of_quarter, quarter, tidy

from groundwork.config import SEED
from groundwork.orchestrate import drill
from groundwork.storage import raw_path

if __name__ == "__main__":
    env = quarter()
    results = drill.drill(env, n=20, seed=SEED, parallel=2,
                          scratch=HERE / "work" / "07")
    print(drill.report(results))

    day = results[0].day
    copy = copy_of_quarter("07c")       # raw's files are links: copy
    path = raw_path("tradeline", "order_lines_log", day, copy.root)
    t = pq.read_table(path)             # one line's quantity, plus one
    qty = t.column("qty").to_pylist()
    qty[0] += 1
    t = t.set_column(t.schema.get_field_index("qty"), "qty",
                     pa.array(qty, t.schema.field("qty").type))
    pq.write_table(t, f"{path}.tmp", compression="zstd")
    os.replace(f"{path}.tmp", path)     # the quarter keeps its own
    spoiled = drill.drill(copy, [day], parallel=1,
                          scratch=HERE / "work" / "07d")
    print(f"\nthe same drill on a copy with one quantity changed on "
          f"{day}:")
    print(drill.report(spoiled))
    tidy("07c")
