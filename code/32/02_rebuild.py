# The reference pipeline rebuilds Book 2's tables from the sources.
# timeout: 300
import sqlite3
import sys

from groundwork.config import BOOK2_WAREHOUSE, ROOT, SOURCES

sys.path.insert(0, str(ROOT / "code" / "meridian"))
import verify_sources as v  # noqa: E402

ours = v.rebuild(str(SOURCES))
b2 = sqlite3.connect(f"file:{BOOK2_WAREHOUSE}?mode=ro", uri=True)


def book2(sql):
    return b2.execute(sql).fetchall()


pairs = [
    ("orders", ours["orders"], book2(
        "SELECT order_id, account_id, order_date, channel "
        "FROM orders")),
    ("order_lines", v.rounded(ours["lines"], (4, 5)), v.rounded(
        book2("SELECT order_id, line_no, sku, qty, unit_price, "
              "unit_cost, discount_pct FROM order_lines"), (4, 5))),
    ("accounts", ours["accounts"], book2("SELECT * FROM accounts")),
    ("contracts", ours["contracts"], book2("SELECT * FROM contracts")),
    ("tickets", ours["tickets"], book2("SELECT * FROM tickets")),
    ("products", v.rounded(ours["products"], (4, 5)),
     v.rounded(book2("SELECT * FROM products"), (4, 5))),
]
print(f"{'table':<14}{'Book 2':>11}{'rebuilt':>11}{'differ':>9}")
for name, mine, theirs in pairs:
    a, b = set(map(tuple, mine)), set(map(tuple, theirs))
    print(f"{name:<14}{len(b):>11,}{len(a):>11,}{len(a ^ b):>9,}")
