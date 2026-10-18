# Exercise 20.3: exact averages with a combiner, and the wrong way.
import sqlite3
from collections import Counter
from fractions import Fraction
from importlib import import_module

from groundwork.config import TRADELINE_DB

mr = import_module("03_map_reduce")

if __name__ == "__main__":
    # The combiner already carries (cents, lines) per account.
    maps, reds = mr.run(4, 4, combine=True)
    cents, lines = Counter(), Counter()
    for red in reds:
        cents.update(red["cents"])
        lines.update(red["lines"])
    right = Fraction(cents[1], lines[1])        # Halloway, account 1

    # The wrong way: each mapper's average, then their average.
    con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
    (top,) = con.execute("SELECT max(id) FROM orders").fetchone()
    step = top // 4 + 1
    means = []
    for i in range(4):
        (c, n), = con.execute(mr.LINES.replace(
            "SELECT o.account_id, l.qty * l.unit_price_cents",
            "SELECT SUM(l.qty * l.unit_price_cents), COUNT(*)")
            + " AND o.account_id = 1", (i * step, (i + 1) * step - 1))
        if n:
            means.append(Fraction(c, n))
    wrong = sum(means) / len(means)
    (c, n), = con.execute(
        "SELECT SUM(l.qty * l.unit_price_cents), COUNT(*) "
        "FROM order_lines l JOIN orders o ON o.id = l.order_id "
        "WHERE o.status = 'placed' AND o.account_id = 1")
    print(f"Halloway's lines: {lines[1]:,} by hand, {n:,} in SQLite")
    print(f"average line, cents, (sum, count) combined: "
          f"{float(right):,.2f}")
    print(f"average line, cents, straight from SQLite:  "
          f"{float(Fraction(c, n)):,.2f}")
    print(f"average of the mappers' averages:           "
          f"{float(wrong):,.2f}")
    exact = right == Fraction(c, n)
    print(f"combined equals the truth exactly: {exact}")
