# The retention job on a copy of Groundwork, as of the book's today:
# what each policy removes, keeps and exempts, and what raw then knows.
from groundwork import storage
from groundwork.config import TODAY
from groundwork.govern import retention

from _govern import tidy, work

copy = work("retention")
con = copy.connect()
months = """
    SELECT strftime(CAST(load_date AS DATE), '%Y-%m') AS month,
           count(DISTINCT session_id)
    FROM raw.portal_events GROUP BY 1 ORDER BY 1"""
before = dict(con.execute(months).fetchall())

print(f"{'policy':<16}{'keep':>6}  owner")
for p in retention.POLICIES:
    print(f"{p.name:<16}{p.keep_days:>5}d  {p.owner}")

done = retention.run(con, copy.raw, copy.landed, TODAY)
print(f"\nrun as of {TODAY}:")
print(f"{'policy':<16}{'action':<9}{'targets':>8}{'rows':>10}{'MB':>8}")
summary = {}
for a in done:
    s = summary.setdefault((a.policy, a.action), [0, 0, 0])
    s[0], s[1], s[2] = s[0] + 1, s[1] + a.rows, s[2] + a.bytes
for (policy, action), (n, rows, size) in summary.items():
    print(f"{policy:<16}{action:<9}{n:>8}{rows:>10,}{size / 1e6:>8.1f}")
for a in done:
    if a.action == "exempt":
        print(f"  exempt {a.target}:\n    {a.note}")

storage.raw_views(con, root=copy.raw)     # fewer partitions now
after = dict(con.execute(months).fetchall())
print("\nportal sessions raw can still count, by month received:")
for month in before:
    gone = after.get(month, 0)
    print(f"  {month}  {before[month]:>7,} -> {gone:>7,}")
plan = retention.run(con, copy.raw, copy.landed, TODAY, dry_run=True)
second = [a for a in plan if a.action == "deleted"]
print(f"due on a second run the same day: {len(second)}")
con.close()
tidy("retention")
