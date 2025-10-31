import os
import pathlib
import sys

# Ensure repo root on sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from redis import Redis
from rq import Queue

try:
    # RQ >= 2.2 has SpawnWorker (Windows-friendly)
    from rq.worker import SpawnWorker as Worker
except Exception:
    # Fallback: SimpleWorker (dev only, no isolation/timeouts)
    from rq.worker import SimpleWorker as Worker


def main():
    r = Redis.from_url(os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    q = Queue("verify", connection=r)
    w = Worker([q], connection=r)
    print("Starting worker class:", w.__class__.__name__)
    w.work()  # blocks; Ctrl+C to stop


if __name__ == "__main__":
    main()
