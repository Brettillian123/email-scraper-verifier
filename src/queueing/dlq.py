# src/queueing/dlq.py
from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime

from src.queueing.redis_conn import get_redis

DLQ_KEY = "dlq:verify"
DLQ_MAX = 1000  # keep the newest 1000 failures


def push_to_dlq(
    job,
    *,
    email: str | None = None,
    mx_host: str | None = None,
    err: Exception | None = None,
    extra: dict | None = None,
) -> None:
    """
    Mirror a final-attempt failure into a Redis list for easy inspection.
    Call this only on the last retry (job.retries_left == 0), then re-raise in the caller.
    """
    r = get_redis()

    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "job_id": getattr(job, "id", None),
        "queue": getattr(job, "origin", None),
        "retries_left": getattr(job, "retries_left", None),
        "email": email,
        "mx_host": mx_host,
        "error_type": type(err).__name__ if err else None,
        "error_message": str(err) if err else None,
        "traceback": traceback.format_exc() if err else None,
        "meta": getattr(job, "meta", None),
    }
    if extra:
        payload.update(extra)

    with r.pipeline() as p:
        p.lpush(DLQ_KEY, json.dumps(payload))
        p.ltrim(DLQ_KEY, 0, DLQ_MAX - 1)
        p.execute()
