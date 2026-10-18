# The fix: read from an hour before the watermark, keep the newest
# version of each key, and reconcile the copy with Tradeline nightly.
from datetime import timedelta

from _watermark import Loader, compare, nights

loaders = {"no lookback": Loader("updated_at", ">"),
           "1 h lookback": Loader("updated_at", ">=",
                                  lookback=timedelta(hours=1))}
SUMS = {"orders": "count(*), sum(CASE WHEN status = 'cancelled' "
                  "THEN 1 ELSE 0 END)",
        "order_lines": "count(*), sum(qty * unit_price_cents)"}


def totals(sql_from):
    """Counts and sums for both tables; the same SQL on either side."""
    return [tuple(int(v) for v in sql_from(t, s)) for t, s in
            SUMS.items()]


def fingerprint(loader):
    copy = loader.copy("order_lines")
    return loader.db.execute(
        f"SELECT count(*), sum(hash(c)) FROM {copy} c").fetchone()


disagreed = {name: [] for name in loaders}
for night, moment, src in nights():
    theirs = totals(lambda t, s: src.execute(
        f"SELECT {s} FROM {t}").fetchone())
    for name, loader in loaders.items():
        loader.run(night, src)
        ours = totals(lambda t, s: loader.db.execute(
            f"SELECT {s} FROM {loader.copy(t)}").fetchone())
        if ours != theirs:
            disagreed[name].append(f"{moment:%Y-%m-%d}")
    if night == 30:                  # the last night, run once more
        fixed = loaders["1 h lookback"]
        before = fingerprint(fixed)
        fixed.run(night, src)
        same = fingerprint(fixed) == before
        rerun = "unchanged" if same else "CHANGED"

found = {name: compare(x)[1] for name, x in loaders.items()}
print(f"{'':<28} {'no lookback':>12} {'1 h lookback':>13}")
print(f"{'rows read in all':<28}", *(
    f"{x.rows_read:>12,}" for x in loaders.values()))
print("orders the copy has wrong")
for kind in ("deleted in Tradeline", "lines amended at 02:00"):
    print(f"{'  ' + kind:<28}", *(f"{k[kind]:>12,}"
                                  for k in found.values()))
print(f"{'nights it disagreed':<28}", *(
    f"{len(d):>12}" for d in disagreed.values()))
print(f"{'  the first of them':<28}", *(
    f"{d[0]:>12}" for d in disagreed.values()))
print(f"the last night run twice: the copy is {rerun}")
