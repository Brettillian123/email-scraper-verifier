from __future__ import annotations

import time
from dataclasses import asdict

from rq import Queue, Retry

from src.config import load_settings
from src.exceptions import PermanentSMTPError, TemporarySMTPError
from src.queueing.redis_conn import get_redis
from src.queueing.tasks import task_generate_emails, task_send_test_email

_cfg = load_settings()


def _queue_name_for(kind: str | None = None) -> str:
    """
    Resolve a queue name from config.

    Prefers _cfg.queues.<kind> if present (new style), otherwise falls back to
    _cfg.queue.queue_name (legacy single-queue config). If nothing is configured,
    returns 'default'.
    """
    qnames = getattr(_cfg, "queues", None)
    if kind and qnames and hasattr(qnames, kind):
        try:
            name = getattr(qnames, kind)
            if isinstance(name, str) and name.strip():
                return name
        except Exception:
            pass
    legacy = getattr(getattr(_cfg, "queue", object()), "queue_name", None)
    return legacy or "default"


def get_queue(name: str | None = None) -> Queue:
    return Queue(name or _queue_name_for(None), connection=get_redis())


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


# -----------------------
# R12: generation helpers
# -----------------------


def enqueue_generate_emails(person_id: int, first: str, last: str, domain: str):
    """
    Enqueue the R12 generation job for a person@domain.
    Uses the 'generate' queue if configured, otherwise the default queue.
    """
    q = get_queue(_queue_name_for("generate"))
    return q.enqueue(
        task_generate_emails,
        person_id,
        first,
        last,
        domain,
        retry=default_retry(),
    )


def maybe_enqueue_generation(
    *,
    person_id: int,
    first: str | None,
    last: str | None,
    domain: str | None,
    email: str | None,
):
    """
    Convenience wrapper for ingest pipelines:
      If we have names + domain but no email, enqueue generation.
      Otherwise no-op and return None.
    """
    have_email = bool((email or "").strip())
    if have_email:
        return None
    if not ((first or "").strip() or (last or "").strip()):
        return None
    if not (domain or "").strip():
        return None
    return enqueue_generate_emails(person_id, first or "", last or "", domain or "")


# ------------------------------
# O26: test-send / bounce helpers
# ------------------------------


def enqueue_test_send_email(
    verification_result_id: int,
    email: str,
    token: str,
):
    """
    Enqueue the O26 test-send job for bounce-based verification.

    The worker task is responsible for:
      - Sending a minimal test email to `email`,
      - Using an envelope sender like bounce+{token}@yourdomain.com,
      - Calling src.verify.test_send.mark_test_send_sent(...) on success.

    Queue selection:
      - Uses the 'test_send' queue if configured in settings.queues.test_send,
      - Otherwise falls back to the default queue.
    """
    q = get_queue(_queue_name_for("test_send"))
    return q.enqueue(
        task_send_test_email,
        verification_result_id,
        email,
        token,
        retry=default_retry(),
    )


# ---- Demo/test jobs ----
def smoke_job(x: int, y: int) -> int:
    # Simulate a tiny bit of work + read config to ensure imports work.
    cfg = load_settings()
    _ = asdict(cfg.retry_timeout)  # touch config to prove it loads
    time.sleep(0.2)
    return x + y


def demo_temp_fail():
    raise TemporarySMTPError("450 greylisted; try again later")


def demo_perm_fail():
    raise PermanentSMTPError("550 user unknown")
