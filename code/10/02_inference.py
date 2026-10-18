# What DuckDB and pandas decide about today's Relay export when
# nobody tells them, and what one stray newline does to each.
import io
import shutil

import duckdb
import pandas as pd

from groundwork.config import LANDING, SANDBOX

night = LANDING / "relay" / "2026-01-04"
con = duckdb.connect()


def duck(path):
    return dict(con.execute(
        f"SELECT column_name, column_type FROM "
        f"(DESCRIBE FROM read_csv('{path}'))").fetchall())


def written(path, column, row):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df[column][row]


print(f"{'column':<17}{'DuckDB':<13}{'pandas':<9}{'written':<12}"
      "pandas made")
for name, row, cols in (("accounts", 1, ["account_id", "postcode",
                                         "customer_since",
                                         "key_account"]),
                        ("contracts", 4, ["discount_pct",
                                          "notice_date"])):
    path = night / f"{name}.csv"
    d, df = duck(path), pd.read_csv(path)
    print(f"{name}.csv, row {row + 1}")
    for c in cols:
        w = written(path, c, row) or "(empty)"
        print(f"  {c:<15}{d[c]:<13}{str(df[c].dtype):<9}{w:<12}"
              f"{df[c][row]}")
    if name == "accounts":
        kind = d["updated_at"].replace(" WITH TIME ZONE", "TZ")
        print(f"  {'updated_at':<15}{kind:<13}"
              f"{str(df['updated_at'].dtype)}")

pd_post = pd.read_csv(night / "accounts.csv")["postcode"]
short = int((pd_post < 10000).sum())
print(f"\npandas postcodes now under five digits: {short:,}")

sql = (f"SELECT updated_at FROM read_csv('{night}/accounts.csv') "
       "WHERE account_id = 1")
print("account 1's updated_at, as DuckDB shows it:")
for zone in ("UTC", "America/New_York", "Asia/Tokyo"):
    con.execute(f"SET TimeZone = '{zone}'")
    print(f"  session in {zone:<17}{con.execute(sql).fetchone()[0]}")
con.execute("SET TimeZone = 'UTC'")

# One comma in account 101's line becomes a line break.
lines = (night / "accounts.csv").read_bytes().split(b"\n")
lines[101] = lines[101].replace(b",", b"\n", 1)
here = SANDBOX / "ch10" / "inference"
here.mkdir(parents=True, exist_ok=True)
bad = here / "accounts.csv"
bad.write_bytes(b"\n".join(lines))
d = duck(bad)
(n,) = con.execute(f"SELECT count(*) FROM read_csv('{bad}')").fetchone()
df = pd.read_csv(io.BytesIO(bad.read_bytes()))
print("\none comma made a newline, on line 101:")
print(f"  DuckDB  {n:,} rows, {len(d)} column, "
      f"{list(d.values())[0]}; no error")
kinds = sorted({str(t).split('(')[0] for t in df.dtypes})
print(f"  pandas  {len(df):,} rows, {df.shape[1]} columns, all "
      f"{', '.join(kinds)}; no error")
print(f"          account_id on row 102: {df['account_id'][101]!r}")
shutil.rmtree(here)
