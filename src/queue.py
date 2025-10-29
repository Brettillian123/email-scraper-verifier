from __future__ import annotations

import os

from redis import Redis
from rq import Queue, Worker

from src.config import load_settings


def get_redis() -> Redis:
    cfg = load_settings()
    url = os.getenv("RQ_REDIS_URL", cfg.queue.rq_redis_url)
    # decode_responses=False keeps bytes; RQ is fine either way.
    return Redis.from_url(url)


def get_queue() -> Queue:
    cfg = load_settings()
    r = get_redis()
    return Queue(cfg.queue.queue_name, connection=r)


def make_worker() -> tuple[Worker, Queue]:
    q = get_queue()
    w = Worker([q], connection=q.connection, name=f"worker:{q.name}")
    return w, q
