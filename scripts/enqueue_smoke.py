# scripts/enqueue_smoke.py
import os
import pathlib
import sys

from redis import Redis  # noqa: E402
from rq import Queue, Retry

# Choose one import below. If you didn't add always_fail, switch to verify_email_task.
from src.jobs import always_fail  # noqa: E402

# Put repo root on sys.path so "import src" works even if PYTHONPATH isn't set.
repo_root = pathlib.Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


def main() -> None:
    redis_url = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
    queue_name = os.getenv("QUEUE_NAME", "verify")

    r = Redis.from_url(redis_url)
    q = Queue(queue_name, connection=r)

    job = q.enqueue(
        always_fail,  # or: verify_email_task
        "nobody@example.com",
        retry=Retry(max=1, interval=[1]),  # quick retry to hit DLQ
    )
    print(f"Enqueued job: {job.id}")


if __name__ == "__main__":
    main()
