# src/queueing/tasks.py
from __future__ import annotations

import logging
import os
import sqlite3
import time

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
from src.db import upsert_verification_result, write_domain_resolution
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
from src.resolve.domain import resolve

log = logging.getLogger(__name__)
_cfg = load_settings()


def _conn() -> sqlite3.Connection:
    """
    Lightweight SQLite connection helper for tasks that need direct DB access.
    Uses DATABASE_PATH if set, otherwise falls back to 'dev.db'.
    """
    return sqlite3.connect(os.getenv("DATABASE_PATH", "dev.db"))


def resolve_company_domain(
    company_id: int, company_name: str, user_hint: str | None = None
) -> dict:
    """
    RQ task: Resolve the official domain for a company, persist the decision, and
    return a structured dict for logs/metrics.

    Returns:
        dict: {"company_id", "chosen", "method", "confidence"}
    """
    dec = resolve(company_name, user_hint)
    log.info(
        "resolve_domain company_id=%s name=%r hint=%r chosen=%r method=%s confidence=%s",
        company_id,
        company_name,
        user_hint,
        getattr(dec, "chosen", None),
        getattr(dec, "method", "unknown"),
        getattr(dec, "confidence", 0),
    )
    with _conn() as con:
        write_domain_resolution(con, company_id, company_name, dec, user_hint)
    return {
        "company_id": company_id,
        "chosen": dec.chosen,
        "method": dec.method,
        "confidence": dec.confidence,
    }


def _retries_left(job) -> int:
    try:
        if job is not None and getattr(job, "retries_left", None) is not None:
            return int(job.retries_left)  # type: ignore[arg-type]
    except Exception:
        pass
    return 0


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
    Self-test behavior:
      - ok5@crestwellpartners.com      -> success ("valid", "selftest-ok")
      - willfail@crestwellpartners.com -> TemporarySMTPError (retries then handled)
      - permfail@...                   -> PermanentSMTPError

    Fallback: allow TEST_PROBE to force modes when testing manually.
    """
    e = email.lower()

    # Deterministic per-address behavior for the self-test
    if e == "ok5@crestwellpartners.com":
        return ("valid", "selftest-ok")
    if e == "willfail@crestwellpartners.com":
        raise TemporarySMTPError("selftest temporary failure")
    if e.startswith("permfail@"):
        raise PermanentSMTPError("selftest permanent failure")

    # Optional env overrides for ad-hoc manual testing
    mode = os.getenv("TEST_PROBE")
    if mode == "success":
        return ("valid", "ok_test")
    if mode == "temp":
        raise TemporarySMTPError("test_temp_error")
    if mode == "perm":
        raise PermanentSMTPError("test_perm_550_user_unknown")
    if mode == "crash":
        raise RuntimeError("test_unexpected_exception")

    # Default: treat others as valid in the stub
    return ("valid", "stub")


def _bool_env(name: str) -> str | None:
    """
    Return '1' or '0' if explicitly set, otherwise None.
    Keeps three-state behavior: True/False/Unset.
    """
    v = os.getenv(name)
    if v in {"0", "1"}:
        return v
    return None


def verify_email_task(  # noqa: C901
    email: str,
    company_id: int | None = None,
    person_id: int | None = None,
):
    """
    Queue entrypoint. Enforces global + per-MX caps and RPS, then performs the
    probe and persists the result idempotently.

    Behavior:
      - TemporarySMTPError and PermanentSMTPError are mapped to terminal statuses.
      - On the 'verify_selftest' queue, the 'willfail@…' job *must* fail:
        we propagate PermanentSMTPError so RQ records a Failed/DLQ job.
      - TEMP errors are **not** re-raised by default (set SELFTEST_RAISE_TEMP=1 to re-raise).
    """
    redis: Redis = get_redis()
    job = get_current_job()  # may be None if called outside RQ

    # Env toggles (optional)
    env_raise_perm = _bool_env("SELFTEST_RAISE_PERM")  # '1', '0', or None
    env_raise_temp = _bool_env("SELFTEST_RAISE_TEMP")  # '1', '0', or None

    raise_perm_env = (env_raise_perm == "1") if env_raise_perm is not None else False
    raise_temp = (env_raise_temp == "1") if env_raise_temp is not None else False

    # Detect self-test queue and whether we must force a *failed* job
    on_selftest_queue = bool(getattr(job, "origin", "") == "verify_selftest")
    willfail_addr = email.lower().startswith("willfail@")
    force_selftest_perm = on_selftest_queue and willfail_addr and env_raise_perm != "0"

    domain = email.split("@")[-1].lower()
    mx_host, _pref = lookup_mx(domain)
    mx_key = MX_SEM.format(mx=mx_host)

    start = time.perf_counter()
    attempt = 1

    status: str = "unknown"
    reason: str | None = "unstarted"

    got_global = False
    got_mx = False

    try:
        # ---- Force the self-test's one failed job ASAP (before throttling paths) ----
        if force_selftest_perm:
            raise PermanentSMTPError("R06 selftest: simulated 550 user unknown")

        # Also allow manual forcing of permanent failure via env outside self-test.
        if raise_perm_env and willfail_addr:
            raise PermanentSMTPError("R06 selftest (env): simulated 550 user unknown")

        # ---- Acquire concurrency semaphores ----
        got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
        if not got_global:
            raise TemporarySMTPError("global concurrency cap reached")

        got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
        if not got_mx:
            raise TemporarySMTPError("per-MX concurrency cap reached")

        # Record/advance job attempt counter
        if job:
            attempt = int(job.meta.get("attempt", 0)) + 1
            job.meta["attempt"] = attempt
            job.save_meta()

        # 1-second RPS buckets
        sec = int(time.time())
        key_global_rps = RPS_KEY_GLOBAL.format(sec=sec)
        key_mx_rps = RPS_KEY_MX.format(mx=mx_host, sec=sec)

        # Optional RPS smoothing (global)
        if _cfg.rate.global_rps and not can_consume_rps(
            redis,
            key_global_rps,
            int(_cfg.rate.global_rps),
        ):
            raise TemporarySMTPError("global RPS throttle")

        # Optional RPS smoothing (per MX)
        if _cfg.rate.per_mx_rps_default and not can_consume_rps(
            redis,
            key_mx_rps,
            int(_cfg.rate.per_mx_rps_default),
        ):
            raise TemporarySMTPError("MX RPS throttle")

        # ---- Probe (Tenacity handles retries on TemporarySMTPError) ----
        verify_status, probe_reason = smtp_probe(
            email,
            _cfg.smtp_identity.helo_domain,
        )
        status, reason = verify_status, probe_reason

        latency_ms = int((time.perf_counter() - start) * 1000)
        log.info(
            "verified email",
            extra={
                "email": email,
                "status": status,
                "reason": reason,
                "mx": mx_host,
                "attempt": attempt,
                "latency_ms": latency_ms,
            },
        )

        return {
            "email": email,
            "verify_status": status,
            "reason": reason,
            "mx_host": mx_host,
        }

    except PermanentSMTPError as e:
        # Terminal hard failure; *re-raise* if this is the self-test's forced failure
        status, reason = "invalid", str(e)
        latency_ms = int((time.perf_counter() - start) * 1000)
        log.info(
            "permanent failure handled"
            if not (force_selftest_perm or raise_perm_env)
            else "permanent failure (propagating)",
            extra={
                "email": email,
                "mx": mx_host,
                "status": status,
                "reason": reason,
                "attempt": attempt,
                "latency_ms": latency_ms,
            },
        )
        if force_selftest_perm or raise_perm_env:
            # Propagate to RQ -> Failed/DLQ
            raise

    except TemporarySMTPError as e:
        # Throttling / soft failure; do not re-raise by default.
        status, reason = "unknown_timeout", (str(e) or "temp_error")
        latency_ms = int((time.perf_counter() - start) * 1000)
        log.warning(
            "temporary failure handled" if not raise_temp else "temporary failure (re-raising)",
            extra={
                "email": email,
                "mx": mx_host,
                "status": status,
                "reason": reason,
                "attempt": attempt,
                "latency_ms": latency_ms,
            },
        )
        if raise_temp:
            raise  # only if explicitly requested via env

    except Exception as e:
        # Unexpected exceptions: record and finish.
        status, reason = ("error", f"{type(e).__name__}: {e}")
        latency_ms = int((time.perf_counter() - start) * 1000)
        retries_left = _retries_left(job)
        log.exception(
            "unexpected exception handled",
            extra={
                "email": email,
                "mx": mx_host,
                "status": status if retries_left == 0 else "error",
                "reason": reason,
                "attempt": attempt,
                "latency_ms": latency_ms,
            },
        )

    finally:
        # Idempotent UPSERT on every outcome (success, temp/perm error, crash)
        try:
            upsert_verification_result(
                email=email,
                verify_status=status,
                reason=reason,
                mx_host=mx_host,
                verified_at=None,
                company_id=company_id,
                person_id=person_id,
            )
        except Exception:
            # Never block semaphore release on DB issues
            log.exception(
                "upsert_verification_result failed",
                extra={
                    "email": email,
                    "status": status,
                    "reason": reason,
                    "mx": mx_host,
                },
            )

        # Only release what we actually acquired
        if got_mx:
            release(redis, mx_key)
        if got_global:
            release(redis, GLOBAL_SEM)
