from __future__ import annotations

import logging

from rq import Queue, Worker

from src.config import load_settings
from src.queueing.dlq import push_to_dlq
from src.queueing.redis_conn import get_redis

log = logging.getLogger(__name__)
_cfg = load_settings()

# Use a string check to avoid import coupling
_PERM_ERR_SUFFIX = ".PermanentSMTPError"


def _to_exc_name(exc_type) -> str:
    return f"{exc_type.__module__}.{exc_type.__name__}"


def _dlq_exception_handler(job, exc_type, exc_value, tb):
    """
    On PermanentSMTPError or when retries are exhausted, copy the job into DLQ.
    """
    try:
        if getattr(job, "origin", "") == _cfg.queue.dlq_name:
            return  # never DLQ a DLQ job

        exc_name = _to_exc_name(exc_type)
        exhausted = getattr(job, "retries_left", 0) == 0
        is_perm = exc_name.endswith(_PERM_ERR_SUFFIX)

        if is_perm or exhausted:
            push_to_dlq(job, err=exc_value)
    except Exception:  # noqa: BLE001
        log.exception("DLQ exception handler failed")


def run():
    r = get_redis()
    q_main = Queue(_cfg.queue.queue_name, connection=r)
    w = Worker([q_main], connection=r, exception_handlers=[_dlq_exception_handler])
    w.work(with_scheduler=True)


if __name__ == "__main__":
    run()
