# Tradeline's orders table, profiled.
import json
from pathlib import Path

from groundwork.config import TRADELINE_DB
from groundwork.profile import connect, profile

con = connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")

orders = profile(con.table("tradeline.orders"), "tradeline.orders")
print(orders)

# Saved for the chapter's figure.
Path(__file__).with_suffix(".json").write_text(
    json.dumps(orders.to_dict(), indent=1) + "\n")
