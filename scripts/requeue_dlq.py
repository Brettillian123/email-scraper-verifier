from rq import Queue

from src.config import load_settings
from src.jobs import get_queue
from src.queueing.redis_conn import get_redis

if __name__ == "__main__":
    cfg = load_settings()
    r = get_redis()
    q_dlq = Queue(cfg.queue.dlq_name, connection=r)
    q_main = get_queue()

    for job in q_dlq.jobs:
        # In real life, filter/triage by job.meta before requeueing
        q_main.enqueue(job.func, *job.args, **job.kwargs)
        print("requeued", job.id)
