# scripts/enqueue_demo.py
import os

from redis import Redis
from rq import Queue, Retry

from src.queue import get_queue  # if you already wrap queue construction, prefer that

if __name__ == "__main__":
    # Either use your helper or construct directly:
    try:
        q = get_queue()
    except Exception:
        redis_url = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
        r = Redis.from_url(redis_url)
        q = Queue(os.getenv("QUEUE_NAME", "verify"), connection=r)

    # Import the real verify task
    from src.queueing.tasks import verify_email_task

    # Enqueue a job that will FAIL (use a bogus domain to force an exception),
    # with 2 retries (3 total attempts).
    job = q.enqueue(
        verify_email_task,
        "nobody@definitely-not-a-real-host.invalid",
        "definitely-not-a-real-host.invalid",
        retry=Retry(max=2, interval=[2, 5]),  # adjust to your needs
        job_timeout=60,
    )
    print("Enqueued job:", job.id)
