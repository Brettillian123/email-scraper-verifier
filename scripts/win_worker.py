# scripts/win_worker.py
import os
import sys
from pathlib import Path

# Ensure repo root is importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Always use our Windows-safe runner (which selects SimpleWorker on Windows)
from src.queueing.worker import run  # noqa: E402

if __name__ == "__main__":
    # Accept queue names via argv (e.g., `python scripts\win_worker.py verify other`)
    if len(sys.argv) > 1:
        os.environ["RQ_QUEUE"] = ",".join(sys.argv[1:])
    # Ensure nothing overrides the worker class
    os.environ.pop("RQ_WORKER_CLASS", None)
    run()
