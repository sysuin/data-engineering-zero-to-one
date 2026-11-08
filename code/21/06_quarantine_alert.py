# timeout: 900
# The watch, run at 07:00 UTC every morning of July 2024 over the
# replay: what fired on the quarantine's volume, who heard, and when.
import json
import textwrap
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
from _sandbox import base, copy_of, morning, replay, tidy

from groundwork.observe import alerts

NY = ZoneInfo("America/New_York")


def local(t: datetime) -> str:
    return (t.replace(tzinfo=ZoneInfo("UTC")).astimezone(NY)
            .strftime("%H:%M New York"))


con = duckdb.connect(str(copy_of("unwatched", "06")))
root = replay("unwatched") / "raw"
sent, quiet = [], 0
for i in range(31):
    day = date(2024, 7, 1) + timedelta(days=i)
    w = alerts.watch(con, morning(day), root)
    sent += w.sent
    if not (w.opened or w.resolved):
        quiet += 1
        continue
    print(f"{morning(day).day:>2} Jul 07:00  opened {len(w.opened)}, "
          f"still open {len(w.repeated)}, resolved {len(w.resolved)}")
    for a in w.opened:
        print(f"    {a.severity:<6} {a.rule}: {a.subject}")
print(f"{quiet} other mornings: nothing opened, nothing resolved")

print("\nwhat reached people on 3 July:")
for at, kind, to, texts in alerts.messages(sent):
    if at.date() == date(2024, 7, 3):
        print(f"  {at:%H:%M} UTC ({local(at)}): {kind} to {to}, "
              f"{len(texts)} alert{'s' * (len(texts) > 1)}")
held = next(s for s in sent if s.key.startswith("held"))
print(textwrap.fill(held.text, 64, initial_indent="  > ",
                    subsequent_indent="  > "))
first = {"watch": "3 July 2024",
         "local_watch": local(morning(date(2024, 7, 2))),
         "delivered": f"{held.at:%H:%M}", "local_delivered":
         local(held.at), "lines": textwrap.wrap(held.text, 76)}

print("\nover the month:")
for key, opened, times, resolved in con.execute("""
        SELECT key, opened_at, times, resolved_at FROM audit.alerts
        ORDER BY opened_at, key""").fetchall():
    days = (resolved - opened).days if resolved else None
    if key.startswith("held"):
        first["times"] = times
    print(f"  {key[:34]:<34} seen {times:>2}, " + (
        f"open {days} day{'s' * (days != 1)}" if resolved else
        "still open"))
kinds = {}
for s in sent:
    kinds[s.kind] = kinds.get(s.kind, 0) + 1
print("  notifications: " + ", ".join(f"{n} {k}" for k, n in
                                      sorted(kinds.items()))
      + f"; {len(alerts.messages(sent))} messages")
con.close()
tidy("06")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"first": first}, indent=1))

con = duckdb.connect(str(base() / "warehouse.duckdb"), read_only=True)
print("\ntoday's warehouse, judged at 07:00 on 5 January 2026:")
for a in alerts.held_rows(con, datetime(2026, 1, 5, 7)):
    print(f"  {a.severity} to {a.owner}, copied to "
          f"{', '.join(a.copy)}")
    print(textwrap.fill(a.text, 64, initial_indent="  > ",
                        subsequent_indent="  > "))
con.close()
