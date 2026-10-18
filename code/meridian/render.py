#!/usr/bin/env python3
"""
Render the Meridian Markdown sources into the formats a real corpus arrives in.

Most of the corpus is fine as Markdown. Three things are not, and they are the three that
break naive pipelines:

  * PDFs with real layout — the quarterly reviews and contracts
  * A two-column PDF, where reading order is not top-to-bottom
  * A scanned PDF with **no text layer at all**, produced by rendering a page and then
    rasterising it, which is exactly how a real scan reaches you

Requires pandoc, tectonic and pdftoppm (poppler). Skips gracefully if any is missing.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.abspath(os.path.join(HERE, "..", "..", "data", "meridian"))
DOCS = os.path.join(DATA, "documents")

NEEDED = ["pandoc", "tectonic", "pdftoppm"]


def have(tool: str) -> bool:
    return shutil.which(tool) is not None


def md_to_pdf(src: str, dst: str, extra: list[str] | None = None) -> None:
    subprocess.run(
        ["pandoc", src, "--pdf-engine=tectonic", "-V", "geometry:margin=1in",
         "-V", "fontsize=11pt", "-o", dst] + (extra or []),
        check=True, capture_output=True)


def make_scanned(src_md: str, dst_pdf: str) -> None:
    """
    Render, rasterise at 200 dpi, then wrap the images back into a PDF. The result has no
    extractable text — `pdftotext` on it returns nothing, which is the whole point.
    """
    tmp = os.path.join(DATA, "_tmp")
    os.makedirs(tmp, exist_ok=True)
    clean = os.path.join(tmp, "page.pdf")
    md_to_pdf(src_md, clean)
    subprocess.run(["pdftoppm", "-r", "200", "-gray", "-png", clean,
                    os.path.join(tmp, "scan")], check=True, capture_output=True)
    pages = sorted(f for f in os.listdir(tmp) if f.startswith("scan") and f.endswith(".png"))

    tex = os.path.join(tmp, "scanned.tex")
    with open(tex, "w") as f:
        f.write("\\documentclass{article}\n"
                "\\usepackage[margin=0pt,paperwidth=8.27in,paperheight=11.69in]{geometry}\n"
                "\\usepackage{graphicx}\n\\pagestyle{empty}\n\\begin{document}\n")
        for p in pages:
            f.write(f"\\noindent\\includegraphics[width=\\paperwidth]{{{p}}}\\newpage\n")
        f.write("\\end{document}\n")
    subprocess.run(["tectonic", "-o", tmp, tex], check=True, capture_output=True, cwd=tmp)
    shutil.move(os.path.join(tmp, "scanned.pdf"), dst_pdf)
    shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    missing = [t for t in NEEDED if not have(t)]
    if missing:
        print(f"skipping PDF render — not installed: {', '.join(missing)}")
        return 0

    for sub in ("quarterly-reviews", "contracts"):
        src_dir = os.path.join(DOCS, sub)
        out_dir = os.path.join(DOCS, f"{sub}-pdf")
        os.makedirs(out_dir, exist_ok=True)
        made = 0
        for name in sorted(os.listdir(src_dir)):
            if not name.endswith(".md"):
                continue
            dst = os.path.join(out_dir, name[:-3] + ".pdf")
            if not os.path.exists(dst):
                md_to_pdf(os.path.join(src_dir, name), dst)
            made += 1
        print(f"  {sub:20} {made} PDFs")

    awk = os.path.join(DOCS, "awkward")
    two_col = os.path.join(awk, "01-newsletter-two-column.pdf")
    if not os.path.exists(two_col):
        md_to_pdf(os.path.join(awk, "01-newsletter-two-column.md"), two_col,
                  ["-V", "classoption=twocolumn"])
    print("  awkward              two-column PDF")

    scanned = os.path.join(awk, "06-goods-received-note-SCANNED.pdf")
    if not os.path.exists(scanned):
        make_scanned(os.path.join(awk, "06-scanned-source.md"), scanned)

    # Prove the scan has no text layer — the book asserts this, so the build checks it.
    txt = subprocess.run(["pdftotext", scanned, "-"], capture_output=True, text=True).stdout
    extractable = len(txt.strip())
    print(f"  awkward              scanned PDF, {extractable} characters extractable "
          f"({'correct — needs a vision model' if extractable < 20 else 'PROBLEM: text layer present'})")
    return 0 if extractable < 20 else 1


if __name__ == "__main__":
    sys.exit(main())
