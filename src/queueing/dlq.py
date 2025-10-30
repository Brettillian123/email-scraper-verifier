from __future__ import annotations

import logging

from rq import Queue
from rq.job import Job

from src.config import load_settings

log = logging.getLogger(__name__)
_cfg = load_settings()


def push_to_dlq(job: Job, *, err: BaseException | str, **extra_meta) -> str | None:
    """
    Copy the failed job to the DLQ with the same function/args/kwargs.
    Returns new DLQ job id, or None if skipped (already in DLQ).
    """
    try:
        if getattr(job, "origin", "") == _cfg.queue.dlq_name:
            # Avoid DLQ loops
            return None

        conn = job.connection
        q_dlq = Queue(_cfg.queue.dlq_name, connection=conn)

        meta = dict(job.meta or {})
        meta.update(
            {
                "dlq_reason": str(err),
                "failed_job_id": job.id,
                "origin": job.origin,
                "exc_type": f"{type(err).__module__}.{type(err).__name__}",
            }
        )
        if extra_meta:
            meta.update(extra_meta)

        new_job = q_dlq.enqueue(
            job.func,
            *job.args,
            **job.kwargs,
            job_timeout=job.timeout,
            meta=meta,
            # no retry policy in DLQ
        )
        log.warning("Moved job %s to DLQ %s as %s", job.id, _cfg.queue.dlq_name, new_job.id)
        return new_job.id
    except Exception:  # noqa: BLE001
        log.exception("Failed to push job %s to DLQ", getattr(job, "id", "<?>"))
        return None
