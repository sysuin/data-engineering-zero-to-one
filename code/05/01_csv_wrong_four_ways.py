# Relay's last export before the migration, read four wrong ways.
import csv
import json
from pathlib import Path

import duckdb

from groundwork.config import SANDBOX
from meridian.sources import relay

# The CRM's archive, restored for the night of 29 February 2024.
paths = relay.export("2024-02-29", SANDBOX / "ch05" / "relay")
path = Path(paths[0])                           # .../accounts.csv
data = path.read_bytes()
lines = data.split(b"\r\n")[1:-1]               # no header, no tail
print(f"{path.name}: {len(data):,} bytes, {len(lines):,} data rows")
line = next(ln for ln in lines if ln.startswith(b"2485;"))
text = line.decode("cp1252")
cut = [i + 1 for i, ch in enumerate(text) if ch == ";"]
for a, b in zip([0, cut[5], cut[9]], [cut[5], cut[9], len(text)]):
    print("  " + text[a:b])                     # one line, in three

# 1. A CSV is comma-separated, surely.
fields = next(csv.reader([line.decode("cp1252")]))
print(f"\n1 comma: {len(fields)} fields, split at "
      f"{fields[0][-8:]!r} | {fields[1][:5]!r}")

# 2. Text is UTF-8, surely.
try:
    line.decode("utf-8")
except UnicodeDecodeError as e:
    print(f"2 utf-8: byte {line[e.start]:#x} at {e.start}: {e.reason}")
con = duckdb.connect()
con.execute(f"""CREATE TABLE loaded AS
    SELECT * FROM read_csv('{path}', ignore_errors = true)""")
got = con.execute("SELECT count(*) FROM loaded").fetchone()[0]
print(f"  with ignore_errors: {got:,} rows loaded of {len(lines):,}")

# 3 and 4. The right delimiter and encoding, and types left to the
# reader's guesswork.
guessed = f"read_csv('{path}', encoding = 'cp1252')"
since, zip_code = con.execute(f"""SELECT "Customer Since", "Zip"
    FROM {guessed} WHERE "Acct ID" = 2485""").fetchone()
fmt = con.execute(f"SELECT DateFormat FROM sniff_csv('{path}', "
                  "encoding = 'cp1252')").fetchone()[0]
print(f"3 dates: 04/01/2022 read as {since} (format {fmt})")
kind = type(zip_code).__name__
print(f"4 zip:   read as {zip_code!r}, a Python {kind}")

wrong, short = con.execute(f"""SELECT
    count(*) FILTER (WHERE day("Customer Since") <> 1),
    count(*) FILTER (WHERE "Zip" < 10000)
    FROM {guessed}""").fetchone()
print(f"  dates wrong: {wrong:,}; zips under five digits: {short:,}")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "line": text, "comma_fields": fields, "loaded": got,
    "rows": len(lines), "since": str(since), "zip": zip_code,
    "utf8_at": text.index("ë")}, ensure_ascii=False, indent=1))
