from __future__ import annotations

import time
from dataclasses import asdict

from src.config import load_settings


def smoke_job(x: int, y: int) -> int:
    # Simulate a tiny bit of work + read config to ensure imports work.
    cfg = load_settings()
    _ = asdict(cfg.retry_timeout)  # touch config to prove it loads
    time.sleep(0.2)
    return x + y
