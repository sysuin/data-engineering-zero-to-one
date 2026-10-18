# Two ways to mint a dimension's keys, each run twice over the same
# versions loaded in a different order.
import hashlib
import math

from _dims import built, tidy

con = built("keys")
versions = con.execute(
    "SELECT account_id, valid_from FROM marts.dim_account "
    "WHERE account_id > 0").fetchall()

def sequence(rows):
    """Keys 1, 2, 3 ... in the order the rows were loaded."""
    return {row: n for n, row in enumerate(rows, start=1)}

def digest(rows):
    """The first 16 hex characters of the MD5 of the natural key."""
    return {(a, d): hashlib.md5(f"{a}|{d:%Y-%m-%d}".encode())
            .hexdigest()[:16] for a, d in rows}

first = sorted(versions)                         # by account, date
second = sorted(versions, key=lambda r: (r[1], r[0]))   # by date
for name, mint in (("sequence", sequence), ("md5[:16]", digest)):
    a, b = mint(first), mint(second)
    moved = sum(a[r] != b[r] for r in versions)
    print(f"{name:<9} {len(a):,} keys; {moved:,} differ between "
          "the two loads")

ours = dict(con.execute(
    "SELECT account_id || '|' || strftime(valid_from, '%Y-%m-%d'), "
    "account_key FROM marts.dim_account").fetchall())
same = sum(ours[f"{a}|{d:%Y-%m-%d}"] == k
           for (a, d), k in digest(first).items())
print(f"dim_account's keys equal to the md5 rule: {same:,}")

stream = hashlib.md5(str(3).encode()).hexdigest()[:16]   # Chapter 19's
(region,) = con.execute("SELECT region_key FROM marts.dim_region "
                        "WHERE region_id = 3").fetchone()
print(f"region 3: dim_region {region}, the stream {stream}")

n = 0
for dim, key in (("dim_account", "account_key"), ("dim_product",
                 "product_key"), ("dim_region", "region_key"),
                 ("dim_supplier", "supplier_key")):
    rows, distinct = con.execute(f"SELECT count(*), count(DISTINCT "
                                 f"{key}) FROM marts.{dim}").fetchone()
    n += rows
    print(f"  {dim:<13} {rows:>7,} keys, {distinct:>7,} distinct")
odds = -math.expm1(-n * (n - 1) / 2 / 2 ** 64)
print(f"chance of a collision among {n:,} keys: about 1 in "
      f"{1 / odds:,.0f}")
tidy("keys")
