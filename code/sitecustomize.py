"""
Token accounting for the book's build, with no trace in the teaching code.

Python imports `sitecustomize` automatically at interpreter startup when it is on the
path. `code/_runner.py` puts `code/` on PYTHONPATH; nothing else does. So when the build
runs a listing, every model call is counted — and when a *reader* runs the same listing,
this file is never imported and the listing behaves exactly as printed in the book.

That property is the point. Instrumenting the listings themselves would mean the code in
the book is not the code that runs.

Almost nothing in this book calls a language model. Chapter 20 does, when it compares a
trained ticket classifier with a prompted model on cost per 1,000 tickets, and those are
the calls counted here. If the `openai` package is not installed, this does nothing.
"""
from __future__ import annotations

import atexit
import json
import os
import time

if os.getenv("BOOK_RUN") == "1":
    _records: list[dict] = []

    def _install() -> None:
        try:
            from openai.resources.chat import completions
        except Exception:                                   # noqa: BLE001
            return

        original = completions.Completions.create

        def counted(self, *args, **kwargs):
            started = time.time()
            response = original(self, *args, **kwargs)
            usage = getattr(response, "usage", None)
            if usage is not None:
                _records.append({
                    "model": kwargs.get("model", "?"),
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "seconds": round(time.time() - started, 3),
                })
            return response

        completions.Completions.create = counted

    def _flush() -> None:
        if not _records:
            return
        path = os.getenv("BOOK_USAGE_FILE", "code/_usage.jsonl")
        listing = os.getenv("BOOK_LISTING", "unknown")
        with open(path, "a") as f:
            for record in _records:
                f.write(json.dumps({"listing": listing, **record}) + "\n")

    # Patch when a listing first imports openai, not at startup: importing
    # it here would load it (and anyio) into every process the build runs.
    import importlib.abc
    import sys

    class _WhenImported(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path, target=None):
            if name != "openai.resources.chat.completions":
                return None
            sys.meta_path.remove(self)
            import importlib.util
            spec = importlib.util.find_spec(name)
            if spec is None or spec.loader is None:
                return spec
            run = spec.loader.exec_module

            def exec_module(module):
                run(module)
                _install()

            spec.loader.exec_module = exec_module
            return spec

    sys.meta_path.insert(0, _WhenImported())
    atexit.register(_flush)


# The book's code a listing imported, so the runner can rerun a listing
# when a module it depends on changes, not only when its own text does.
# Every process a listing starts appends its own line; the runner reads
# them all and hashes the files.
if os.getenv("BOOK_RUN") == "1" and os.getenv("BOOK_DEPS_FILE"):
    import sys

    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _MINE = (os.path.join(_ROOT, "code") + os.sep,
             os.path.join(_ROOT, "tests") + os.sep)

    def _imported() -> None:
        here = os.path.abspath(__file__)
        files = set()
        for module in list(sys.modules.values()):
            path = getattr(module, "__file__", None)
            if not path:
                continue
            path = os.path.abspath(path)
            if path.startswith(_MINE) and path != here:
                files.add(os.path.relpath(path, _ROOT))
        with open(os.environ["BOOK_DEPS_FILE"], "a") as f:
            f.write(json.dumps(sorted(files)) + "\n")

    atexit.register(_imported)
