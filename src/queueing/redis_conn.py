from redis import Redis
from rq import Queue

from src.config import settings  # you already have this loader

_redis = None
_queue = None


def get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(settings.RQ_REDIS_URL, decode_responses=True)
    return _redis


def get_queue() -> Queue:
    global _queue
    if _queue is None:
        _queue = Queue(settings.QUEUE_NAME, connection=get_redis(), default_timeout=600)
    return _queue
