# src/queueing/redis_conn.py
import os
from functools import lru_cache

from redis import Redis


@lru_cache(maxsize=1)
def get_redis() -> Redis:
    url = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
    # IMPORTANT: RQ expects raw bytes; do NOT enable decode_responses.
    return Redis.from_url(url, decode_responses=False)
