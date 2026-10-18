# Relay's accounts profiled on two nights, and what moved between them.
from groundwork.config import LANDING
from groundwork.profile import connect, profile

con = connect()
nights = ("2025-12-22", "2026-01-04")
a, b = (profile(con.read_csv(str(LANDING / "relay" / n /
                                "accounts.csv")), n) for n in nights)
print(f"rows: {a.rows:,} then {b.rows:,}")
print(f"{'column':<14}{'what':<9}{nights[0]:>20}{nights[1]:>20}")
for ca, cb in zip(a.columns, b.columns):
    for what in ("nulls", "distinct", "min", "max"):
        va, vb = getattr(ca, what), getattr(cb, what)
        if va != vb:
            print(f"{ca.name:<14}{what:<9}{str(va)[:19]:>20}"
                  f"{str(vb)[:19]:>20}")
