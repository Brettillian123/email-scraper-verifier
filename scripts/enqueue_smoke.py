import os
import pathlib
import sys

# Ensure repo root (parent of scripts/) is on sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from redis import Redis
from rq import Queue

from src.queueing.tasks import verify_email_task

r = Redis.from_url(os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
q = Queue("verify", connection=r)
job = q.enqueue(verify_email_task, kwargs={"email": "check@example.com"})
print("enqueued", job.id)
