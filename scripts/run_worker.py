# scripts/run_worker.py
from __future__ import annotations

import logging
import os
import sys

from src.queueing.worker import run as run_worker

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)

if __name__ == "__main__":
    # Accept queue list via CLI: e.g. "verify_selftest" or "verify,verify_selftest"
    if len(sys.argv) > 1:
        os.environ["RQ_QUEUE"] = sys.argv[1]
    run_worker()
