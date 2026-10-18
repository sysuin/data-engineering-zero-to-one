# Exercise 2: a delete detector that compares every key each night,
# priced in keys read against the log entries CDC reads for the same.
from datetime import timedelta

from _watermark import DB, KEYS, Loader, compare, nights

loader = Loader("updated_at", ">=", lookback=timedelta(hours=1))
gone, keys_read, log_read = set(), 0, 0
for night, moment, src in nights():
    if night:
        (n,) = src.execute("SELECT count(*) FROM replication_log WHERE "
                           "committed_at > ?", (str(moment - timedelta(
                               days=1)),)).fetchone()
        log_read += n
    loader.run(night, src)
    if night == 0:
        continue
    db = loader.db
    db.execute(f"ATTACH '{DB}' AS s (TYPE sqlite, READ_ONLY)")
    for t in KEYS:
        keys_read += db.execute(f"SELECT count(*) FROM s.{t}"
                                ).fetchone()[0]
    found = db.execute(
        f"SELECT id FROM {loader.copy('orders')} EXCEPT "
        "SELECT id FROM s.orders").fetchall()
    db.execute("DETACH s")
    for (oid,) in found:
        if oid not in gone:
            print(f"{moment:%Y-%m-%d}: order {oid} is gone from "
                  "Tradeline")
            gone.add(oid)
    ids = ", ".join(str(i) for i in gone) or "0"
    for t, key in KEYS.items():           # remove what has gone
        db.execute(f"DELETE FROM got_{t} WHERE "
                   f"{key.split(',')[0]} IN ({ids})")

wrong, kinds = compare(loader)
print(f"orders wrong after removing them: {len(wrong)}")
print(f"keys read to find them: {keys_read:,} "
      f"({keys_read // 30:,} a night)")
print(f"log entries CDC read instead: {log_read:,} "
      f"({log_read // 30:,} a night)")
