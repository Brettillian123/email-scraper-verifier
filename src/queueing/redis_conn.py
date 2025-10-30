from __future__ import annotations

from functools import lru_cache

from redis import Redis

from src.config import load_settings


@lru_cache(maxsize=1)
def get_redis() -> Redis:
    """Return a cached Redis client using the RQ URL from structured config."""
    cfg = load_settings()
    return Redis.from_url(cfg.queue.rq_redis_url, decode_responses=True)
