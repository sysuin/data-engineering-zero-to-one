# Whether unit_price_cents is before or after the line's discount.
import duckdb

from groundwork.config import LANDING, TRADELINE_DB

SKU = "MRD-CLE-001"
con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")

# The catalogue's list price, typed as text: '$88.64'.
catalogue = LANDING / "catalogue" / "catalogue.csv"
(price,) = con.execute(f"""
    SELECT "List Price" FROM read_csv('{catalogue}', all_varchar = true)
    WHERE SKU = ?""", [SKU]).fetchone()
list_cents = int(price.lstrip("$").replace(".", ""))
print(f"{SKU}: list price {price} in the catalogue")

print("\nlines placed in December 2025, by discount")
print("disc   lines   lowest paid   list less disc   below that")
for disc, lines, lowest, below in con.execute("""
        SELECT l.discount_pct, COUNT(*), MIN(l.unit_price_cents),
               COUNT(*) FILTER (WHERE l.unit_price_cents
                   < ROUND(? * (100 - l.discount_pct) / 100.0))
        FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
        WHERE l.sku = ? AND o.placed_at >= '2025-12-01'
        GROUP BY 1 ORDER BY 1""", [list_cents, SKU]).fetchall():
    expect = round(list_cents * (100 - disc) / 100)
    print(f"{disc:>3}%{lines:>8}{lowest:>14}{expect:>17}{below:>13}")

# What applying the discount a second time would do to a month.
stored, again = con.execute("""
    SELECT SUM(l.qty * l.unit_price_cents),
           SUM(l.qty * l.unit_price_cents * (100 - l.discount_pct))
    FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
    WHERE o.status = 'placed'
      AND o.placed_at >= '2025-11-01' AND o.placed_at < '2025-12-01'
""").fetchone()
again = again // 100            # whole cents, rounded down
print("\nNovember 2025, placed orders, SUM(qty * unit_price_cents)")
print(f"  as stored              {stored:>14,} cents")
print(f"  discount taken again   {again:>14,} cents")
print(f"  understated by         {(stored - again) / stored:>14.2%}")
