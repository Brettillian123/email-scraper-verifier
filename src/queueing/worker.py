# src/queueing/worker.py
from __future__ import annotations

import importlib
import logging
import os
import threading
import time

from rq import Queue
from rq import SimpleWorker as RQSimpleWorker
from rq import Worker as RQWorker
from rq.scheduler import RQScheduler

from src.config import load_settings
from src.queueing import tasks as _tasks  # noqa: F401  (ensure task module is imported)
from src.queueing.dlq import push_to_dlq
from src.queueing.redis_conn import get_redis

log = logging.getLogger(__name__)
_cfg = load_settings()

_PERM_ERR_SUFFIX = ".PermanentSMTPError"


# -----------------------------
# Windows-safe death penalty
# -----------------------------
class _NoOpPenalty:
    """
    A no-op death penalty context manager.

    RQ uses a "death penalty" (signal/SIGALRM on POSIX) to enforce job timeouts.
    On Windows, SIGALRM does not exist, so we must disable that mechanism.

    When set as worker.death_penalty_class, job timeouts will be ignored.
    """

    def __init__(self, timeout, exception, **_kwargs) -> None:  # noqa: D401
        self.timeout = timeout
        self.exception = exception

    def __enter__(self):  # noqa: D401
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: D401
        # Do not suppress exceptions raised by the job
        return False


def _to_exc_name(exc_type) -> str:
    return f"{exc_type.__module__}.{exc_type.__name__}"


def _dlq_exception_handler(job, exc_type, exc_value, tb):
    try:
        # Never DLQ jobs already running on the DLQ queue
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
    Windows: always SimpleWorker (forking Worker uses os.wait4 which doesn't exist on Windows).
    If RQ_WORKER_CLASS is set to a non-SimpleWorker on Windows, ignore it with a warning.
    Non-Windows: honor RQ_WORKER_CLASS if provided; else use Worker.
    """
    if os.name == "nt":
        env_cls = os.getenv("RQ_WORKER_CLASS", "").strip()
        if env_cls and not env_cls.endswith("SimpleWorker"):
            logging.warning(
                "Ignoring RQ_WORKER_CLASS=%s on Windows; using rq.SimpleWorker",
                env_cls,
            )
        return RQSimpleWorker

    env_cls = os.getenv("RQ_WORKER_CLASS", "").strip()
    if env_cls:
        mod, name = env_cls.rsplit(".", 1)
        return getattr(importlib.import_module(mod), name)
    return RQWorker


def _start_inline_scheduler(queues: list[Queue], connection, interval: int = 15) -> None:
    """
    Promote due scheduled jobs from a thread, for the SimpleWorker path.

    SimpleWorker never gets with_scheduler=True (see run() below), so RQ's
    forking scheduler subprocess never starts and anything delayed --
    Retry(interval=...), enqueue_in, enqueue_at -- sits in ScheduledJobRegistry
    forever. It enqueues without error and never fires, which is the exact
    class of invisible failure this deployment already has too much of.

    enqueue_scheduled_jobs() is a plain synchronous Redis call, no fork, so it
    is safe here. Do NOT call scheduler.work() instead: it installs signal
    handlers, which raises off the main thread.
    """
    scheduler = RQScheduler(queues, connection=connection, interval=interval)
    if not scheduler.acquire_locks():
        log.info("inline scheduler: locks held by another worker, skipping")
        return

    def _loop() -> None:
        while True:
            try:
                scheduler.enqueue_scheduled_jobs()
                scheduler.heartbeat()
            except Exception:
                log.exception("inline scheduler: promotion pass failed")
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True, name="inline-scheduler").start()
    log.info("inline scheduler started (interval=%ss)", interval)


def run():
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    r = get_redis()
    queue_names = _queue_names_from_env_or_cfg()
    queues = [Queue(name, connection=r) for name in queue_names]

    worker_cls = _select_worker_cls()
    qnames = ", ".join(queue_names)
    mod, cls = worker_cls.__module__, worker_cls.__name__

    # E501-safe banner
    print(f"*** Starting {mod}.{cls}")
    print(f"Queues: {qnames}")
    log.info("Worker class: %s.%s", mod, cls)
    log.info("Queues: %s", qnames)

    # Prefer RQ 2.x API to register exception handler; keep constructor fallback
    w = worker_cls(queues, connection=r, exception_handlers=[_dlq_exception_handler])
    try:
        w.push_exc_handler(_dlq_exception_handler)  # RQ 2.x+
    except Exception:
        pass  # constructor handler covers older/newer versions

    # ---- Windows adjustments ----
    if os.name == "nt":
        # Disable death penalty (SIGALRM not available on Windows)
        try:
            # type: ignore[attr-defined]
            w.death_penalty_class = _NoOpPenalty  # noqa: SLF001
            log.info("Windows: disabled job timeouts (death_penalty_class->NoOp)")
        except Exception:
            log.warning("Windows: failed to set death_penalty_class NoOp; continuing")

        # Avoid worker TTL semantics that may rely on signals
        try:
            # internal attr; safe to relax in our controlled worker
            w._default_worker_ttl = 0  # noqa: SLF001
        except Exception:
            pass

    # Only the forking Worker supports with_scheduler
    if worker_cls is RQWorker:
        w.work(with_scheduler=True)
    else:
        # SimpleWorker path (Windows): no forking scheduler, so run the
        # promotion pass inline or delayed jobs never leave the registry.
        _start_inline_scheduler(queues, r)
        w.work()


if __name__ == "__main__":
    run()
