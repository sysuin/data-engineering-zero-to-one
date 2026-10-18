# Saying what a file holds instead of letting the reader guess:
# what DuckDB's declarations check, what they do not, and a reader
# that checks the header itself and fails loudly.
import shutil

import duckdb

from groundwork.config import LANDING, SANDBOX

con = duckdb.connect()
here = SANDBOX / "ch10" / "declared"
here.mkdir(parents=True, exist_ok=True)
pem = LANDING / "suppliers" / "pemberton-mills"
june, july = (pem / "PEMBERTON-PRICES-202406.csv",
              pem / "PEMBERTON-PRICES-202407.csv")

# 1. One type declared by name, on the file with a stray newline.
lines = (LANDING / "relay/2026-01-04/accounts.csv").read_bytes()
lines = lines.split(b"\n")
lines[101] = lines[101].replace(b",", b"\n", 1)
stray = here / "accounts.csv"
stray.write_bytes(b"\n".join(lines))
try:
    con.execute(f"SELECT count(*) FROM read_csv('{stray}', "
                "types = {'updated_at': 'TIMESTAMP'})")
except duckdb.Error as e:
    msg = str(e).split("\n")[0].split(": ", 2)[-1]
    print(f"types= by name on {stray.name}:\n  {msg}")

# 2. Columns declared in full are matched by position, not name.
PEMBERTON = {"Item Code": "VARCHAR", "Item": "VARCHAR",
             "Cost (USD)": "DECIMAL(10, 2)", "Valid From": "VARCHAR"}
swapped = here / "swapped.csv"
rows = [ln.split(",") for ln in june.read_text().splitlines()]
swapped.write_text("\n".join(",".join([r[1], r[0], *r[2:]])
                             for r in rows) + "\n")
for f in (july, swapped):
    head = f.read_text().splitlines()[0]
    got = con.execute(f"SELECT * FROM read_csv('{f}', header = true, "
                      f"columns = {PEMBERTON}) LIMIT 1").fetchone()
    print(f"\ncolumns= on {f.name}\n  header {head}")
    print(f"  row    sku={got[0]!r}, cost={got[2]}")


# 3. Check the header by name, then read with the declared types.
class SchemaMismatch(ValueError):
    pass


def read_declared(path, declared: dict):
    with open(path, encoding="utf-8-sig") as f:
        header = f.readline().rstrip("\r\n").split(",")
    missing = [c for c in declared if c not in header]
    extra = [c for c in header if c not in declared]
    if missing or extra:
        raise SchemaMismatch(f"{path.name}: missing {missing}, "
                             f"unexpected {extra}")
    cols = {c: declared[c] for c in header}        # the file's order
    return con.sql(f"SELECT * FROM read_csv('{path}', header = true, "
                   f"columns = {cols}, auto_detect = false, "
                   "strict_mode = true)")


print()
for f in (june, july, swapped):
    try:
        r = read_declared(f, PEMBERTON)
        sku, cost = r.select('"Item Code", "Cost (USD)"').fetchone()
        print(f"{f.name}: {len(r.fetchall())} rows, "
              f"sku={sku!r}, cost={cost}")
    except SchemaMismatch as e:
        print(f"SchemaMismatch: {e}".replace(", unexpected",
                                             ",\n  unexpected"))

RELAY = dict.fromkeys(
    ["account_id", "account_name", "region", "segment", "owner",
     "customer_since", "postcode", "key_account", "status",
     "closed_on", "contact_name", "contact_email", "contact_phone",
     "updated_at"], "VARCHAR")
RELAY.update(account_id="BIGINT", customer_since="DATE",
             closed_on="DATE", key_account="BOOLEAN",
             updated_at="TIMESTAMP")
r = read_declared(LANDING / "relay/2026-01-04/accounts.csv", RELAY)
for zone in ("UTC", "Asia/Tokyo"):
    con.execute(f"SET TimeZone = '{zone}'")
    (at,) = r.filter("account_id = 1").select("updated_at").fetchone()
    print(f"account 1 updated_at, session in {zone:<11}{at}")
shutil.rmtree(here)
