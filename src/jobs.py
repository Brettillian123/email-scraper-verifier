# src/jobs.py
from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any  # <-- add

from rq import Queue, Retry

from src.config import load_settings
from src.queueing.redis_conn import get_redis

_cfg = load_settings()


# Reuse the same error taxonomy as tasks (kept local to avoid import cycles)
class TemporarySMTPError(Exception): ...


class PermanentSMTPError(Exception): ...


def get_queue(name: str | None = None) -> Queue:
    return Queue(name or _cfg.queue.queue_name, connection=get_redis())


def default_retry() -> Retry:
    """
    RQ retry policy using RETRY_SCHEDULE from .env.
    Number of retries = len(schedule). Backoffs follow the list values.
    """
    schedule = _cfg.retry_timeout.retry_schedule
    return Retry(max=len(schedule), interval=schedule)


def enqueue_verify(func, *args, **kwargs):
    """
    Helper to ensure all jobs get our standard retry policy unless overridden.
    """
    q = get_queue()
    kwargs.setdefault("retry", default_retry())
    return q.enqueue(func, *args, **kwargs)


# ---- add this: tests may import src.jobs.enqueue ----
def enqueue(job_name: str, **payload: Any) -> None:
    """
    Present for symmetry with src.queue.enqueue so tests can monkey-patch either.
    Default is a no-op in R07.
    """
    return None


# ---- Demo/test jobs ----
def smoke_job(x: int, y: int) -> int:
    cfg = load_settings()
    _ = asdict(cfg.retry_timeout)
    time.sleep(0.2)
    return x + y


def demo_temp_fail():
    raise TemporarySMTPError("450 greylisted; try again later")


def demo_perm_fail():
    raise PermanentSMTPError("550 user unknown")
