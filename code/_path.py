#!/usr/bin/env python3
"""Put this repository's code/ on the path of the Python that runs this file, for good.

    .venv/bin/python code/_path.py

It writes one line into the environment's site-packages, a `.pth` file, which Python reads
at start-up. After it, `from groundwork.config import WAREHOUSE` works from any folder.
"""
import pathlib
import site

code = pathlib.Path(__file__).resolve().parent
target = pathlib.Path(site.getsitepackages()[0], "zero-to-one-code.pth")
target.write_text(f"{code}\n")
print(f"{target} -> {code}")
