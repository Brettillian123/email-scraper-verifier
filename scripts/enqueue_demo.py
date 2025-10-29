import sys

from rq import Retry

from src.config import settings
from src.queueing.redis_conn import get_queue
from src.queueing.tasks import verify_email_task


def main():
    q = get_queue()
    for email in sys.argv[1:]:
        q.enqueue(
            verify_email_task,
            email,
            retry=Retry(max=settings.VERIFY_MAX_ATTEMPTS),
            job_timeout=600,
            result_ttl=0,
            failure_ttl=86400,
        )
    print(f"Enqueued {len(sys.argv) - 1} jobs")


if __name__ == "__main__":
    main()
