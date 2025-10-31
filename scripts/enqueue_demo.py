# scripts/enqueue_demo.py
import os

from redis import Redis
from rq import Queue, Retry

from src.queue import get_queue  # prefer this helper

if __name__ == "__main__":
    # Use your helper; if it fails, build a safe fallback
    try:
        q = get_queue()
    except Exception:
        redis_url = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
        r = Redis.from_url(redis_url, decode_responses=False)  # <-- important
        q = Queue(os.getenv("QUEUE_NAME", "verify"), connection=r)

    from src.queueing.tasks import verify_email_task

    # Option A: take 3 total attempts (1 + 2 retries) → DLQ after final attempt
    job = q.enqueue(
        verify_email_task,
        "nobody@definitely-not-a-real-host.invalid",
        None,  # company_id
        None,  # person_id
        retry=Retry(max=2, interval=[2, 5]),
        job_timeout=60,
    )

    # Option B (fast DLQ test): uncomment this block and comment Option A above
    # job = q.enqueue(
    #     verify_email_task,
    #     "nobody@definitely-not-a-real-host.invalid",
    #     None, None,
    #     retry=Retry(max=0),
    #     job_timeout=60,
    # )

    print("Enqueued job:", job.id)
