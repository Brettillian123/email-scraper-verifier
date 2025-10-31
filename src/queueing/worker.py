# src/queueing/worker.py
from __future__ import annotations

import importlib
import logging
import os

from rq import Queue
from rq import SimpleWorker as RQSimpleWorker
from rq import Worker as RQWorker

from src.config import load_settings
from src.queueing import tasks as _tasks  # noqa: F401
from src.queueing.dlq import push_to_dlq
from src.queueing.redis_conn import get_redis

log = logging.getLogger(__name__)
_cfg = load_settings()

_PERM_ERR_SUFFIX = ".PermanentSMTPError"


def _to_exc_name(exc_type) -> str:
    return f"{exc_type.__module__}.{exc_type.__name__}"


def _dlq_exception_handler(job, exc_type, exc_value, tb):
    try:
        if getattr(job, "origin", "") == _cfg.queue.dlq_name:
            return
        exhausted = getattr(job, "retries_left", 0) == 0
        is_perm = _to_exc_name(exc_type).endswith(_PERM_ERR_SUFFIX)
        if is_perm or exhausted:
            push_to_dlq(job, err=exc_value)
    except Exception:  # noqa: BLE001
        log.exception("DLQ exception handler failed")


def _queue_names_from_env_or_cfg() -> list[str]:
    raw = os.getenv("RQ_QUEUE", "")
    if raw.strip():
        return [q.strip() for q in raw.split(",") if q.strip()]
    return [_cfg.queue.queue_name]


def _select_worker_cls():
    """
    Windows: always SimpleWorker. If RQ_WORKER_CLASS is set to a non-SimpleWorker,
    ignore it and warn. Non-Windows: honor RQ_WORKER_CLASS if provided, else Worker.
    """
    if os.name == "nt":
        env_cls = os.getenv("RQ_WORKER_CLASS", "").strip()
        if env_cls and not env_cls.endswith("SimpleWorker"):
            logging.warning(
                "Ignoring RQ_WORKER_CLASS=%s on Windows; using rq.SimpleWorker", env_cls
            )
        return RQSimpleWorker

    env_cls = os.getenv("RQ_WORKER_CLASS", "").strip()
    if env_cls:
        mod, name = env_cls.rsplit(".", 1)
        return getattr(importlib.import_module(mod), name)
    return RQWorker


def run():
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    r = get_redis()
    queue_names = _queue_names_from_env_or_cfg()
    queues = [Queue(name, connection=r) for name in queue_names]

    worker_cls = _select_worker_cls()
    qnames = ", ".join(queue_names)
    print(f"*** Starting {worker_cls.__module__}.{worker_cls.__name__} on queues: {qnames}")
    log.info("Worker class: %s.%s", worker_cls.__module__, worker_cls.__name__)
    log.info("Queues: %s", qnames)

    w = worker_cls(queues, connection=r, exception_handlers=[_dlq_exception_handler])

    # Only the forking Worker supports with_scheduler
    if worker_cls is RQWorker:
        w.work(with_scheduler=True)
    else:
        w.work()


if __name__ == "__main__":
    run()
