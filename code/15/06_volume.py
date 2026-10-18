# Volume and partitions on a copy: the morning as it is, then a Relay
# export cut short, a Tradeline night of no changes, and a partition
# the load log wrote and the disk no longer has.
import shutil
from datetime import datetime

from groundwork import quality, storage
from groundwork.config import RAW

from _sandbox import HERE, connect, tidy, work

AT = datetime(2026, 1, 5, 7, 0)
path = work("volume")
con = connect(path, read_only=False)
print("\n".join(quality.report(quality.volume(con, AT))))

con.execute("""UPDATE audit.load_log SET rows_written = 3029
               WHERE load_id = 'relay.accounts.2026-01-04.1'""")
con.execute("""UPDATE audit.load_log SET rows_written = 0,
                   extra = '{"mode": "cdc", "snapshot": false}'
               WHERE source = 'tradeline' AND table_name = 'orders'
                 AND load_date = '2026-01-04'""")
print("\nafter:")
print("\n".join(quality.report(quality.volume(con, AT)[:3])))

raw = HERE / "volume" / "raw"
shutil.copytree(RAW, raw)
gone = storage.raw_path("relay", "contracts", "2025-12-28", raw).parent
shutil.rmtree(gone)
checks = quality.partitions(con, raw)
print(f"\npartitions: {sum(c.status == 'pass' for c in checks)} of "
      f"{len(checks)} tables whole")
print("\n".join(quality.report([c for c in checks
                                if c.status != "pass"])))
con.close()
tidy("volume")
