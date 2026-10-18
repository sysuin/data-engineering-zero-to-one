# Book 2's training table, built by Book 2's own rules (Chapter 22 wrote
# its builder as SQL) from meridian_ml.db and again from compat; then
# Groundwork's point-in-time feature table against both.
from _sandbox import connect

from groundwork.config import BOOK2_WAREHOUSE, ROOT

BUILDER = (ROOT / "code" / "22" / "book2_features.sql").read_text()
COLUMNS = {"account_id": "account", "since": "days since order",
           "n90": "orders, 90 days", "p90": "orders, 90 before",
           "spend": "spend, a year", "t90": "tickets, 90 days",
           "tenure": "tenure", "segment": "segment",
           "not_renewed": "label"}

con = connect()
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 "
            "(TYPE sqlite, READ_ONLY)")
con.execute(BUILDER)                           # reads b2.<table>
con.execute("ALTER TABLE book2 RENAME TO from_book2")
con.execute(BUILDER.replace("b2.", "compat."))  # the same, from compat
con.execute("ALTER TABLE book2 RENAME TO from_compat")

n_b2, n_gw = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
              for t in ("from_book2", "from_compat"))
print("Book 2's training table, by Book 2's rules")
print(f"  rows from meridian_ml.db {n_b2:,}, from compat {n_gw:,}")
print(f"  {'rows that differ in':<22}{'compat':>8}{'features':>10}")
renamed = {"since": "days_since_order", "n90": "orders_90d",
           "p90": "orders_prev_90d", "spend": "spend_365",
           "t90": "tickets_90d", "tenure": "tenure_days"}
for col, label in COLUMNS.items():
    (a,) = con.execute(f"""
        SELECT count(*) FROM from_book2 AS b JOIN from_compat AS c
        USING (contract_id) WHERE b.{col} IS DISTINCT FROM c.{col}"""
                       ).fetchone()
    ours = renamed.get(col, col)
    test = (f"abs(b.{col} - f.{ours}) >= 0.005" if col == "spend"
            else f"b.{col} IS DISTINCT FROM f.{ours}")
    (f,) = con.execute(f"""
        SELECT count(*) FROM from_book2 AS b
        JOIN features.renewal_snapshot AS f USING (contract_id)
        WHERE {test}""").fetchone()
    print(f"  {label:<22}{a:>8,}{f:>10,}")
total, labelled, leavers = con.execute("""
    SELECT count(*), count(not_renewed), count(*) FILTER (not_renewed)
    FROM features.renewal_snapshot""").fetchone()
(b2_leavers,) = con.execute("SELECT count(*) FILTER (not_renewed) FROM "
                            "from_book2").fetchone()
print(f"features.renewal_snapshot: {total:,} rows")
print(f"  labelled {labelled:,}, still open {total - labelled:,}")
print(f"  not renewed: {leavers:,} here, {b2_leavers:,} in Book 2's")
con.close()
