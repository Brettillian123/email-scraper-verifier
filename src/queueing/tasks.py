# src/queueing/tasks.py
import logging

import dns.resolver
from redis import Redis
from rq import get_current_job
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.config import settings
from src.queueing.dlq import push_to_dlq
from src.queueing.rate_limit import (
    GLOBAL_SEM,
    MX_SEM,
    RPS_KEY_GLOBAL,
    RPS_KEY_MX,
    can_consume_rps,
    full_jitter_delay,
    release,
    try_acquire,
)
from src.queueing.redis_conn import get_redis

# Temporary stub until Step 9 wires the real DB upsert.
try:
    from src.db import upsert_verification_result  # type: ignore
except Exception:

    def upsert_verification_result(**kwargs):  # type: ignore[unused-argument]
        return None


log = logging.getLogger(__name__)


class TemporarySMTPError(Exception): ...


class PermanentSMTPError(Exception): ...


def lookup_mx(domain: str) -> tuple[str, int]:
    """Return (hostname, pref) for lowest-preference MX; fallback to domain."""
    try:
        answers = dns.resolver.resolve(domain, "MX")
        pairs = sorted(
            [(r.exchange.to_text(omit_final_dot=True), r.preference) for r in answers],
            key=lambda x: x[1],
        )
        return pairs[0]
    except Exception:
        return (domain, 0)


@retry(
    reraise=True,
    retry=retry_if_exception_type(TemporarySMTPError),
    stop=stop_after_attempt(settings.VERIFY_MAX_ATTEMPTS),
    wait=wait_random_exponential(
        multiplier=settings.VERIFY_BASE_BACKOFF_SECONDS,
        max=settings.VERIFY_MAX_BACKOFF_SECONDS,
    ),
)
def smtp_probe(email: str, helo_domain: str) -> tuple[str, str]:
    """
    Do minimal SMTP (connect, HELO/EHLO, MAIL FROM, RCPT TO).
    Return (verify_status, reason).
    Raise TemporarySMTPError for 4xx/timeouts, PermanentSMTPError for 5xx.
    NOTE: Implement fully in R16; this is a placeholder.
    """
    # TODO: replace with real implementation in R16
    raise TemporarySMTPError("placeholder until R16")


def verify_email_task(  # noqa: C901 - acceptable complexity for orchestrator function
    email: str,
    company_id: int | None = None,
    person_id: int | None = None,
):
    """
    Queue entrypoint. Enforces global + per-MX caps and RPS, then performs the
    probe and persists the result idempotently.
    """
    job = get_current_job()
    redis: Redis = get_redis()

    domain = email.split("@")[-1].lower()
    mx_host, _pref = lookup_mx(domain)

    # Acquire semaphores
    global_lim = int(settings.GLOBAL_MAX_CONCURRENCY)
    mx_lim = int(settings.PER_MX_MAX_CONCURRENCY_DEFAULT)

    got_global = try_acquire(redis, GLOBAL_SEM, global_lim)
    if not got_global:
        # Soft requeue after jitter (no semaphores were acquired yet)
        delay = full_jitter_delay(
            settings.VERIFY_BASE_BACKOFF_SECONDS,
            getattr(job, "retries_left", 0) ^ 1,
            settings.VERIFY_MAX_BACKOFF_SECONDS,
        )
        if job:
            job.requeue(delay=delay)
            return
        raise TemporarySMTPError("global concurrency cap; no job to requeue")

    mx_key = MX_SEM.format(mx=mx_host)
    got_mx = try_acquire(redis, mx_key, mx_lim)
    if not got_mx:
        # Release only the global semaphore we hold
        release(redis, GLOBAL_SEM)
        delay = full_jitter_delay(
            settings.VERIFY_BASE_BACKOFF_SECONDS,
            getattr(job, "retries_left", 0) ^ 1,
            settings.VERIFY_MAX_BACKOFF_SECONDS,
        )
        if job:
            job.requeue(delay=delay)
            return
        raise TemporarySMTPError("per-MX concurrency cap; no job to requeue")

    try:
        # Optional RPS smoothing (global)
        if settings.GLOBAL_RPS and not can_consume_rps(
            redis,
            RPS_KEY_GLOBAL,
            int(settings.GLOBAL_RPS),
        ):
            delay = full_jitter_delay(1.0, 1, 2.0)
            if job:
                job.requeue(delay=delay)
                return
            raise TemporarySMTPError("global RPS throttle; no job to requeue")

        # Optional RPS smoothing (per MX)
        if settings.PER_MX_RPS_DEFAULT and not can_consume_rps(
            redis,
            RPS_KEY_MX.format(mx=mx_host),
            int(settings.PER_MX_RPS_DEFAULT),
        ):
            delay = full_jitter_delay(1.0, 1, 2.0)
            if job:
                job.requeue(delay=delay)
                return
            raise TemporarySMTPError("MX RPS throttle; no job to requeue")

        # ---- Probe (Tenacity handles its own internal retries) ----
        verify_status, reason = smtp_probe(email, settings.SMTP_HELO_DOMAIN)

        # ---- Idempotent upsert on success ----
        upsert_verification_result(
            email=email,
            verify_status=verify_status,
            reason=reason,
            mx_host=mx_host,
            verified_at=None,  # let DB default to CURRENT_TIMESTAMP if desired
            company_id=company_id,
            person_id=person_id,
        )

        log.info(
            "verified email",
            extra={
                "email": email,
                "status": verify_status,
                "reason": reason,
                "mx": mx_host,
            },
        )

        return {
            "email": email,
            "verify_status": verify_status,
            "reason": reason,
            "mx_host": mx_host,
        }

    except PermanentSMTPError as e:
        # Permanent → treat as handled business outcome, not a task failure
        upsert_verification_result(
            email=email,
            verify_status="invalid",
            reason=str(e),
            mx_host=mx_host,
            verified_at=None,
            company_id=company_id,
            person_id=person_id,
        )
        log.warning(
            "permanent failure",
            extra={"email": email, "err": str(e), "mx": mx_host},
        )
        return {
            "email": email,
            "verify_status": "invalid",
            "reason": str(e),
            "mx_host": mx_host,
        }

    except TemporarySMTPError as e:
        # Will raise to let RQ retry; if this is the FINAL attempt, mirror to DLQ first
        if job and getattr(job, "retries_left", 0) == 0:
            push_to_dlq(job, email=email, mx_host=mx_host, err=e)
        raise

    except Exception as e:  # noqa: BLE001
        # Unknown error → retry via RQ; mirror to DLQ if retries exhausted
        if job and getattr(job, "retries_left", 0) == 0:
            push_to_dlq(job, email=email, mx_host=mx_host, err=e)
        log.exception("unexpected error", extra={"email": email, "mx": mx_host})
        raise

    finally:
        # Always release semaphores we acquired
        release(redis, mx_key)
        release(redis, GLOBAL_SEM)
