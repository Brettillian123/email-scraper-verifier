# src/ingest/rejects.py
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

_LOG_DIR = Path("logs")
_LOG_DIR.mkdir(exist_ok=True)
# one file per run; append mode so multiple files can share
_LOG_PATH = _LOG_DIR / f"ingest_rejects_{time.strftime('%Y%m%d_%H%M%S')}.log"


def log_reject(line_no: int, reason: str, row: dict[str, Any] | None = None) -> None:
    """
    Writes a TSV-ish line with JSON payload for the row.
    Example: 12\tmissing required fields\t{"company":"Acme","role":""}
    """
    payload = json.dumps(row or {}, ensure_ascii=False)
    with _LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{line_no}\t{reason}\t{payload}\n")


def current_rejects_file() -> str:
    return str(_LOG_PATH)
