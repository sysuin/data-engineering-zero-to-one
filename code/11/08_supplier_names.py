# Three teams, three spellings: the catalogue joined to procurement's
# supplier list and to Tradeline's order lines, naively and then fixed.
import json
from pathlib import Path

import duckdb

from groundwork.config import WAREHOUSE
from groundwork.transform.runner import MODELS

seed = MODELS / "seeds" / "supplier_names.csv"
con = duckdb.connect(str(WAREHOUSE), read_only=True)
con.execute(f"""
    CREATE TEMP TABLE seed AS SELECT * FROM read_csv('{seed}',
        all_varchar = true);
    CREATE TEMP TABLE cat AS SELECT "SKU" AS sku, "Supplier" AS name
        FROM raw.catalogue_products;
    CREATE TEMP TABLE procurement AS
        SELECT CAST(supplier_id AS INTEGER) AS id, name
        FROM raw.reference_suppliers;
    CREATE TEMP TABLE lines AS
        SELECT sku, qty * unit_price_cents AS cents
        FROM raw.tradeline_order_lines_current""")

print("Catalogue spellings with no supplier of that name:")
missing = con.execute("""
    SELECT name, count(*) FROM cat ANTI JOIN procurement USING (name)
    GROUP BY name ORDER BY name""").fetchall()
for name, n in missing:
    print(f"  {name!r:<20} {n} products")
print("Catalogue SKUs Tradeline never writes:")
odd = con.execute("""SELECT sku FROM cat
    WHERE sku NOT IN (SELECT sku FROM lines) ORDER BY sku""").fetchall()
print("  " + ", ".join(repr(s) for (s,) in odd))

steps = [
    ("as the old job joins", "c.sku", "c.name", "p.name"),
    ("SKUs trimmed, upper", "upper(trim(c.sku))", "c.name", "p.name"),
    ("names trimmed too", "upper(trim(c.sku))", "trim(c.name)",
     "p.name"),
    ("through the seed", "upper(trim(c.sku))", "trim(c.name)", "seed"),
]
(all_lines, all_cents), = con.execute(
    "SELECT count(*), sum(cents) FROM lines").fetchall()
print(f"\n{'':<22}{'products':>9}{'lines':>12}{'sales':>21}")
ladder = []
for label, sku, name, how in steps:
    if how == "seed":
        supplier = (f"JOIN seed s ON s.source = 'catalogue' AND "
                    f"s.spelling = {name} JOIN procurement p "
                    "ON p.id = CAST(s.supplier_id AS INTEGER)")
    else:
        supplier = f"JOIN procurement p ON p.name = {name}"
    (products,) = con.execute(
        f"SELECT count(*) FROM cat c {supplier}").fetchone()
    n, cents = con.execute(f"""
        SELECT count(*), sum(l.cents) FROM lines l
        JOIN cat c ON {sku} = l.sku {supplier}""").fetchone()
    ladder.append([label, products, n, cents])
    usd = f"${cents // 100:,}.{cents % 100:02d}"
    print(f"{label:<22}{products:>6}/48{n:>12,}{usd:>21}")
usd = f"${all_cents // 100:,}.{all_cents % 100:02d}"
print(f"{'every order line':<22}{'':>9}{all_lines:>12,}{usd:>21}")

(unmapped,) = con.execute("""
    SELECT count(*) FROM (
        SELECT 'catalogue' AS source, trim("Supplier") AS spelling
        FROM raw.catalogue_products
        UNION SELECT 'suppliers', trim(supplier)
        FROM raw.suppliers_price_lists)
    ANTI JOIN seed USING (source, spelling)""").fetchone()
(rows,) = con.execute("SELECT count(*) FROM seed").fetchone()
print(f"\nseed: {rows} rows; spellings in raw it does not cover: "
      f"{unmapped}")

pairs = con.execute("""
    SELECT c.name, p.name, count(*), sum(c.name = p.name)
    FROM cat c JOIN seed s ON s.source = 'catalogue'
        AND s.spelling = trim(c.name)
    JOIN procurement p ON p.id = CAST(s.supplier_id AS INTEGER)
    GROUP BY ALL ORDER BY 1""").fetchall()
Path(__file__).with_suffix(".json").write_text(json.dumps(dict(
    pairs=pairs, ladder=ladder, odd=[s for (s,) in odd],
    total=[all_lines, all_cents]), indent=1))
