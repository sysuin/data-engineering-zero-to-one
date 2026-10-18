# The quality suite in dbt: every test passing on a copy of the
# warehouse, then four promises broken, and which test notices each.
# timeout: 1200
from collections import Counter

from _sandbox import connect, dbt, save, tidy, work


def kind(node):
    meta = getattr(node, "test_metadata", None)
    return meta.name if meta else "singular"


path = work("assertions")
res = dbt("build", "--select", "tag:quality", path=path)
tests = [r for r in res.result.results
         if r.node.resource_type == "test"]
print(f"tagged quality: {len(tests)} tests and the seed "
      f"ledger_journals")
by = Counter((kind(r.node), str(r.status)) for r in tests)
for (k, status), n in sorted(by.items()):
    print(f"  {k:<16}{n:>3} {status}")
res = dbt("test", path=path)
everything = len(res.result.results)
print(f"the whole suite, with Chapter 14's: "
      f"{len(res.result.results)} tests, " + ", ".join(
          f"{n} {s}" for s, n in sorted(Counter(
              str(r.status) for r in res.result.results).items())))

con = connect(path, read_only=False)
# 1. Date the first hours of November by UTC, as the dashboard does.
moved = len(con.execute("""
    UPDATE marts.fct_order_lines
    SET order_date = CAST(placed_at AS DATE),
        date_key = CAST(strftime(placed_at, '%Y%m%d') AS INTEGER)
    WHERE placed_at >= '2025-11-01' AND order_date < '2025-11-01'
    RETURNING order_id""").fetchall())
# 2. A line of nothing: quantity 0, revenue left as it was.
con.execute("""UPDATE marts.fct_order_lines SET qty = 0
               WHERE order_number = 60603 AND line_no = 1""")
# 3. Two versions of one account that overlap by a day.
con.execute("""UPDATE marts.dim_account SET valid_to = valid_to + 1
               WHERE account_id = 1130 AND NOT is_current""")
# 4. Finance's register loses its November line.
con.execute("""DELETE FROM seeds.ledger_journals
               WHERE period = '2025-11'""")
con.close()

res = dbt("test", "--select", "tag:quality", path=path)
print(f"\nafter four changes ({moved} lines re-dated): "
      + ", ".join(f"{n} {s}" for s, n in sorted(Counter(
          str(r.status) for r in res.result.results).items())))
for r in sorted(res.result.results, key=lambda r: r.node.name):
    if str(r.status) != "pass":
        print(f"  {str(r.status):<5}{r.failures:>3}  {r.node.name}")
save("04_assertions", {
    "quality": len(tests), "all": everything,
    "kinds": {k: n for (k, _), n in by.items()},
    "broken": {r.node.name: [str(r.status), r.failures]
               for r in res.result.results if str(r.status) != "pass"}})
tidy("assertions")
