#!/usr/bin/env python3
"""Check that every planted fact is visible in the warehouse and agrees with the documents."""
import json, os, re, sqlite3, sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.abspath(os.path.join(HERE, "..", "..", "data", "meridian"))
con = sqlite3.connect(os.path.join(DATA, "warehouse", "meridian.db"))
con.row_factory = sqlite3.Row
q = lambda s: [dict(r) for r in con.execute(s).fetchall()]
fails = []

def check(label, ok, detail=""):
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"  — {detail}" if detail else ""))
    if not ok:
        fails.append(label)

print("\nFact 1 — Halloway churn collapses Midwest revenue from 2024 Q3")
east = q("""SELECT year, quarter, ROUND(SUM(revenue)) rev FROM v_sales
            WHERE region='Midwest' GROUP BY year, quarter ORDER BY year, quarter""")
for r in east:
    mark = " <-- churn" if (r["year"], r["quarter"]) == (2024, 3) else ""
    print(f"    {r['year']} Q{r['quarter']}  ${r['rev']:>10,.0f}{mark}")
before = [r["rev"] for r in east if (r["year"], r["quarter"]) < (2024, 3)][-1]
after = [r["rev"] for r in east if (r["year"], r["quarter"]) == (2024, 3)][0]
drop = 100 * (before - after) / before
check("Midwest falls at least 20% at the churn", drop >= 20, f"fell {drop:.1f}%")

print("\nFact 2 — Voss price rise compresses margin from 2025 Q1")
voss = q("""SELECT year, quarter, ROUND(100.0*SUM(gross_profit)/SUM(revenue),1) margin
            FROM v_sales WHERE supplier='Voss Industrial'
            GROUP BY year, quarter ORDER BY year, quarter""")
for r in voss:
    mark = " <-- price rise" if (r["year"], r["quarter"]) == (2025, 1) else ""
    print(f"    {r['year']} Q{r['quarter']}  {r['margin']:>5.1f}%{mark}")
pre = [r["margin"] for r in voss if (r["year"], r["quarter"]) < (2025, 1)][-1]
post = [r["margin"] for r in voss if (r["year"], r["quarter"]) == (2025, 1)][0]
check("Voss margin drops at least 3 points", pre - post >= 3.0,
      f"{pre:.1f}% -> {post:.1f}%")

print("\nFact 3 — Sanitation exists only from 2024 Q2")
san = q("""SELECT MIN(year||' Q'||quarter) first FROM v_sales WHERE category='Sanitation'""")
check("first Sanitation sale is 2024 Q2", san[0]["first"] == "2024 Q2", san[0]["first"])

print("\nFact 4 — Pemberton quality spike in 2024 Q4")
tickets = [json.loads(l) for l in
           open(os.path.join(DATA, "documents", "tickets", "tickets.jsonl"))]
qual = {}
for t in tickets:
    if t["category"] == "Quality":
        qual[(t["year"], t["quarter"])] = qual.get((t["year"], t["quarter"]), 0) + 1
spike = qual.get((2024, 4), 0)
baseline = sum(v for k, v in qual.items() if k != (2024, 4)) / (len(qual) - 1)
print(f"    2024 Q4 quality tickets {spike}, baseline {baseline:.1f}/quarter")
check("spike is at least 3x baseline", spike >= 3 * baseline, f"{spike/baseline:.1f}x")

print("\nCross-check — do the quarterly reviews agree with SQL?")
mismatch = 0
for year in (2023, 2024, 2025):
    for quarter in (1, 2, 3, 4):
        path = os.path.join(DATA, "documents", "quarterly-reviews", f"qbr-{year}-Q{quarter}.md")
        text = open(path).read()
        stated = int(re.search(r"Revenue for .*? was \$([\d,]+)", text).group(1).replace(",", ""))
        actual = q(f"""SELECT ROUND(SUM(revenue)) r FROM v_sales
                       WHERE year={year} AND quarter={quarter}""")[0]["r"]
        if abs(stated - actual) > 1:
            print(f"    {year} Q{quarter}: document ${stated:,} vs SQL ${actual:,.0f}")
            mismatch += 1
check("all 12 reviews match the warehouse", mismatch == 0, f"{12 - mismatch}/12 agree")

print("\nConsistency — do the documents only mention regions that exist?")
import glob
real = {r["name"] for r in q("SELECT name FROM regions")}
# Any capitalised word immediately before "region", anywhere in the corpus.
bad = {}
for path in glob.glob(os.path.join(DATA, "documents", "**", "*.md"), recursive=True):
    for word in re.findall(r"\b([A-Z][a-z]+) region\b", open(path).read()):
        if word not in real:
            bad.setdefault(word, []).append(os.path.basename(path))
for word, files in sorted(bad.items()):
    print(f"    {word!r} appears in {len(files)} document(s), e.g. {files[0]}")
check("every region named in a document exists", not bad,
      f"regions are {', '.join(sorted(real))}")

print("\nCorpus shape")
for label, sql in [
    ("order lines", "SELECT COUNT(*) n FROM order_lines"),
    ("distinct SKUs", "SELECT COUNT(DISTINCT sku) n FROM order_lines"),
    ("date range", "SELECT MIN(order_date)||' to '||MAX(order_date) n FROM orders"),
]:
    print(f"    {label:16} {q(sql)[0]['n']}")

print(f"\n{'ALL CHECKS PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}\n")
sys.exit(1 if fails else 0)
