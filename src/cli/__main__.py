# src/cli/__main__.py
from __future__ import annotations

from . import main as cli_main

if __name__ == "__main__":
    # Allow: python -m src.cli admin status
    raise SystemExit(cli_main())
