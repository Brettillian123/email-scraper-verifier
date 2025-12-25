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

# By default, listen on both verification + test-send queues.
# You can override this by passing queue names as arguments, e.g.:
#   python scripts\win_worker.py verify
#   python scripts\win_worker.py verify test_send mx
DEFAULT_QUEUES = ["verify", "test_send"]

if __name__ == "__main__":
    # If explicit queue names are provided on the command line, they win.
    if len(sys.argv) > 1:
        os.environ["RQ_QUEUE"] = ",".join(sys.argv[1:])
    else:
        # Otherwise, default to both verify and test_send, unless an external
        # RQ_QUEUE was already set (e.g., by a process manager).
        os.environ.setdefault("RQ_QUEUE", ",".join(DEFAULT_QUEUES))

    # Ensure nothing overrides the worker class (we want the Windows-safe one)
    os.environ.pop("RQ_WORKER_CLASS", None)

    run()
