# scripts/run_worker.py
import os
import pathlib
import sys

from redis import Redis
from rq import Queue, Worker

# Add repo root to sys.path (imports above keep E402 happy)
repo_root = pathlib.Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


def main() -> None:
    redis_url = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
    queue_name = os.getenv("QUEUE_NAME", "verify")

    r = Redis.from_url(redis_url)
    q = Queue(queue_name, connection=r)
    w = Worker([q], connection=r)
    w.work(with_scheduler=True)


if __name__ == "__main__":
    main()
