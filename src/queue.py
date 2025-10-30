# src/queue.py
from __future__ import annotations

import os

from redis import Redis
from rq import Queue, Worker

from src.queueing.redis_conn import get_redis


def get_queue() -> Queue:
    name = os.getenv("QUEUE_NAME", "verify")
    r: Redis = get_redis()
    return Queue(name, connection=r)


def make_worker() -> tuple[Worker, Queue]:
    q = get_queue()
    w = Worker([q], connection=q.connection, name=f"worker:{q.name}")
    return w, q
