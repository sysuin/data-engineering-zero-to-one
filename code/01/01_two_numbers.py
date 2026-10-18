# November 2025's revenue, by the dashboard's query and by the ledger.
import duckdb

from groundwork.config import LANDING, TRADELINE_DB, THREADS

con = duckdb.connect(config={"threads": THREADS})
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")

# The dashboard's query: every order placed in November, straight
# from the orders application.
(dashboard,) = con.execute("""
    SELECT SUM(l.qty * l.unit_price_cents / 100.0)
    FROM tradeline.orders o
    JOIN tradeline.order_lines l ON l.order_id = o.id
    WHERE o.placed_at >= '2025-11-01'
      AND o.placed_at <  '2025-12-01'
""").fetchone()

# Finance's number: the month's revenue journal from the ledger.
(ledger,) = con.execute(f"""
    SELECT SUM(amount)
    FROM read_csv('{LANDING}/ledger/gl_revenue_2025-11.csv')
""").fetchone()

print(f"dashboard  {dashboard!r}")
print(f"ledger     {ledger:,.2f}")
print(f"gap        {dashboard - float(ledger):,.2f}")
