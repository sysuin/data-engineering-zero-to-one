# Exercise 3: a supplier spelling the seed does not know, the check that
# catches it, and the one-row change that mends it.
import duckdb

from groundwork.transform import runner

from _pack import sandbox, tidy

warehouse, folder = sandbox("spelling", pack=False)
runner.run(warehouse, folder=folder)
con = duckdb.connect(str(warehouse))
(sql,) = con.execute("SELECT sql FROM duckdb_views() WHERE schema_name "
                     "= 'raw' AND view_name = 'catalogue_products'"
                     ).fetchone()
body = sql.split(" AS ", 1)[1].rstrip(";")
con.execute(f"""CREATE OR REPLACE VIEW raw.catalogue_products AS
    {body} UNION ALL BY NAME
    (SELECT 'MRD-SAN-049' AS "SKU",
           'Sanitation item 11' AS "Product Name",
           'Sanitation' AS "Category", 'Thorne Chem.' AS "Supplier",
           '$61.20' AS "List Price", '' AS "Launched", '' AS "Notes",
           file, file_sha256, _loaded_at, _load_id, _source_file,
           load_date
    FROM ({body}) LIMIT 1)""")
CHECK = """SELECT sku, supplier_name
           FROM staging.stg_catalogue__products
           WHERE supplier_id IS NULL ORDER BY sku"""
print("the new product in staging:")
print(" ", con.execute("SELECT sku, supplier_name, supplier_id, "
                       "list_price_cents FROM staging.stg_catalogue__"
                       "products WHERE sku = 'MRD-SAN-049'").fetchone())
print("the check, products with no supplier:")
print(" ", con.execute(CHECK).fetchall())
con.close()

seed = folder / "seeds" / "supplier_names.csv"
seed.write_text(seed.read_text() + "catalogue,Thorne Chem.,5\n")
built = [r.model for r in runner.run(warehouse, folder=folder)
         if r.status == "built"]
print("after one row in the seed, rebuilt:")
print("  " + ", ".join(built[:2]) + ",\n  " + ", ".join(built[2:]))
con = duckdb.connect(str(warehouse))
print("the check:", con.execute(CHECK).fetchall())
con.close()
tidy("spelling")
