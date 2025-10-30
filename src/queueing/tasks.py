from __future__ import annotations

import json
import logging
import time
import traceback

import dns.resolver
from redis import Redis
from rq import get_current_job
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.config import load_settings
from src.queueing.rate_limit import (
    GLOBAL_SEM,
    MX_SEM,
    RPS_KEY_GLOBAL,
    RPS_KEY_MX,
    can_consume_rps,
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
_cfg = load_settings()


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
    stop=stop_after_attempt(_cfg.retry_timeout.verify_max_attempts),
    wait=wait_random_exponential(
        multiplier=_cfg.retry_timeout.verify_base_backoff_seconds,
        max=_cfg.retry_timeout.verify_max_backoff_seconds,
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


def verify_email_task(  # noqa: C901
    email: str,
    company_id: int | None = None,
    person_id: int | None = None,
):
    """
    Queue entrypoint. Enforces global + per-MX caps and RPS, then performs the
    probe and persists the result idempotently.

    IMPORTANT: If any cap/throttle is hit, we raise TemporarySMTPError and let
    the RQ job-level retry policy handle re-enqueue/backoff (see enqueue helper).

    DLQ behavior: if an exception escapes and the job has no retries left,
    mirror a payload to Redis list 'dlq:verify'.
    """
    redis: Redis = get_redis()
    job = get_current_job()  # may be None if called outside RQ

    domain = email.split("@")[-1].lower()
    mx_host, _pref = lookup_mx(domain)

    # Acquire semaphores
    got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
    if not got_global:
        raise TemporarySMTPError("global concurrency cap reached")

    mx_key = MX_SEM.format(mx=mx_host)
    got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
    if not got_mx:
        release(redis, GLOBAL_SEM)
        raise TemporarySMTPError("per-MX concurrency cap reached")

    try:
        # Optional RPS smoothing (global)
        if _cfg.rate.global_rps and not can_consume_rps(
            redis,
            RPS_KEY_GLOBAL,
            int(_cfg.rate.global_rps),
        ):
            raise TemporarySMTPError("global RPS throttle")

        # Optional RPS smoothing (per MX)
        if _cfg.rate.per_mx_rps_default and not can_consume_rps(
            redis,
            RPS_KEY_MX.format(mx=mx_host),
            int(_cfg.rate.per_mx_rps_default),
        ):
            raise TemporarySMTPError("MX RPS throttle")

        # ---- Probe (Tenacity handles its own internal retries) ----
        verify_status, reason = smtp_probe(email, _cfg.smtp_identity.helo_domain)

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

    except Exception as e:
        # Mirror to DLQ only when we've exhausted job retries.
        retries_left = 0
        try:
            if job is not None and getattr(job, "retries_left", None) is not None:
                retries_left = int(job.retries_left)  # type: ignore[arg-type]
        except Exception:
            # Be defensive; if anything goes wrong reading retries, treat as last try.
            retries_left = 0

        if retries_left == 0:
            payload = {
                "job_id": getattr(job, "id", None),
                "queue": getattr(job, "origin", None),
                "email": email,
                "domain": domain,
                "mx_host": mx_host,
                "error": str(e),
                "exc_type": e.__class__.__name__,
                "traceback": traceback.format_exc(),
                "enqueued_at": (
                    getattr(job, "enqueued_at", None).isoformat()
                    if getattr(job, "enqueued_at", None)
                    else None
                ),
                "attempts_used": (
                    (job.meta.get("retry_count", 0) + 1) if getattr(job, "meta", None) else None
                ),
                "ts": int(time.time()),
            }
            redis.lpush("dlq:verify", json.dumps(payload))

        # Re-raise so RQ marks the job as failed (and/or retries if any left).
        raise

    finally:
        # Always release semaphores we acquired
        release(redis, mx_key)
        release(redis, GLOBAL_SEM)
