# src/queueing/tasks.py
from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import dns.resolver
from redis import Redis
from rq import Queue, get_current_job
from rq.decorators import job
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.autodiscovery_result import AutodiscoveryResult
from src.config import (
    SMTP_COMMAND_TIMEOUT,
    SMTP_CONNECT_TIMEOUT,
    SMTP_HELO_DOMAIN,
    SMTP_MAIL_FROM,
    load_settings,
)
from src.crawl.runner import crawl_domain
from src.db import (
    get_conn,
    upsert_generated_email,
    upsert_verification_result,
    write_domain_resolution,
)
from src.db_pages import save_pages
from src.extract.candidates import ROLE_ALIASES
from src.extract.candidates import Candidate as ExtractCandidate
from src.extract.candidates import extract_candidates as extract_html_candidates

# Task A: Robots explainability imports
try:
    from src.fetch.robots import RobotsBlockInfo, explain_block
    from src.fetch.robots import is_allowed as robots_is_allowed

    _HAS_ROBOTS_EXPLAINABILITY = True
except ImportError:
    _HAS_ROBOTS_EXPLAINABILITY = False
    RobotsBlockInfo = None  # type: ignore[assignment,misc]
    explain_block = None  # type: ignore[assignment]
    robots_is_allowed = None  # type: ignore[assignment]

# O27 AI wrapper (preferred) + metrics plumb-back
try:
    from src.extract.ai_candidates_wrapper import (
        refine_candidates_with_ai,
        update_result_from_metrics,
    )

    _HAS_AI_WRAPPER = True
except Exception:  # pragma: hidden
    refine_candidates_with_ai = None  # type: ignore[assignment]
    update_result_from_metrics = None  # type: ignore[assignment]
    _HAS_AI_WRAPPER = False

# Keep AI_PEOPLE_ENABLED as a lightweight "global enable" hint (best-effort).
# The wrapper enforces the real contract and handles optional OpenAI deps.
try:
    from src.extract.ai_candidates import AI_PEOPLE_ENABLED  # type: ignore
except Exception:  # pragma: no cover
    AI_PEOPLE_ENABLED = True  # type: ignore[assignment]

# O26: role/placeholder handling
from src.emails.classify import is_role_or_placeholder_email
from src.generate.patterns import (
    PATTERNS as CANON_PATTERNS,  # keys of canonical patterns (e.g., "first.last")
)
from src.generate.patterns import (
    generate_candidate_emails_for_person,  # O26 canonical generator
    infer_domain_pattern,  # O01 canonical inference
)
from src.ingest.normalize import (
    normalize_row,  # R13 lightweight full-row normalization
    normalize_split_parts,  # O09 normalization for generation (ASCII locals)
)
from src.ingest.persist import (
    upsert_row as persist_upsert_row,  # R13: persist normalized rows
)
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
from src.resolve.mx import resolve_mx as _resolve_mx  # R15
from src.verify.catchall import check_catchall_for_domain  # R17 domain-level catch-all
from src.verify.smtp import probe_rcpt  # R16 SMTP probe core
from src.verify.status import (
    VerificationSignals,
    classify,  # R18 classifier
    should_escalate_to_test_send,  # O26 escalation helper
)
from src.verify.test_send import (
    mark_test_send_sent,
    request_test_send,
)  # O26 bounce-based verification

log = logging.getLogger(__name__)
_cfg = load_settings()


def _conn() -> Any:
    """
    Lightweight connection helper for tasks that need direct DB access.

    Delegate to src.db.get_conn() so we always use the same DATABASE_URL / schema
    as the rest of the application (including domain_resolutions, R17, etc.).

    Returns a CompatConnection (works with both SQLite and PostgreSQL).
    """
    return get_conn()


def _store_result_in_job_meta(result: AutodiscoveryResult) -> None:
    """
    Task E: Store autodiscovery result in RQ job meta if running inside a worker.

    This enables inspection of results via job.meta["autodiscovery_result"].
    """
    try:
        job = get_current_job()
        if job is not None:
            job.meta["autodiscovery_result"] = result.to_dict()
            job.save_meta()
    except Exception as e:
        log.debug("Could not store result in job meta: %s", e)


def _enqueue_company_task(task_name: str, company_id: int) -> None:
    """
    Best-effort helper to enqueue a company-scoped task on the 'verify' queue.

    Uses the legacy envelope format consumed by handle_task() so we do not need
    separate RQ job decorators for each new function.
    """
    try:
        q = Queue(name="verify", connection=get_redis())
        envelope = {
            "task": task_name,
            "payload": {"company_id": int(company_id)},
        }
        q.enqueue(handle_task, envelope, job_timeout=600)
    except Exception as exc:  # pragma: no cover - logging-only path
        log.warning(
            "auto-discovery enqueue failed",
            extra={"task": task_name, "company_id": company_id, "exc": str(exc)},
        )


def resolve_company_domain(
    company_id: int,
    company_name: str,
    user_hint: str | None = None,
    user_supplied_domain: str | None = None,
    **_: Any,
) -> dict:
    """
    RQ task: Resolve the official domain for a company, persist the decision, and
    return a structured dict for logs/metrics.

    Arguments:
        company_id: DB primary key of the company.
        company_name: Raw company name.
        user_hint: Optional free-form hint (legacy).
        user_supplied_domain: Optional explicit domain supplied by the user
            (newer callers). When present and user_hint is not provided,
            this will be used as the effective hint.

    Returns:
        dict: {"company_id", "chosen", "method", "confidence"}
    """
    # Prefer explicit user_hint; otherwise fall back to user_supplied_domain.
    hint = user_hint or user_supplied_domain

    dec = resolve(company_name, hint)
    log.info(
        "resolve_domain company_id=%s name=%r hint=%r chosen=%r method=%s confidence=%s",
        company_id,
        company_name,
        hint,
        getattr(dec, "chosen", None),
        getattr(dec, "method", "unknown"),
        getattr(dec, "confidence", 0),
    )
    with _conn():
        # write_domain_resolution already expects a hint-like string in the last param.
        write_domain_resolution(
            company_id=company_id,
            company_name=company_name,
            user_hint=hint,
            chosen_domain=getattr(dec, "chosen", None),
            method=getattr(dec, "method", None),
            confidence=getattr(dec, "confidence", None),
            reason=getattr(dec, "reason", None),
            resolver_version=getattr(dec, "resolver_version", None) or "1.0",
        )

    # After we have a chosen canonical domain, kick off the crawl as a follow-up
    # task. We keep this best-effort; failures to enqueue should not break R08.
    if getattr(dec, "chosen", None):
        _enqueue_company_task("crawl_company_site", company_id=company_id)

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


# Reuse the same error taxonomy as jobs (kept local to avoid import cycles)
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
    Legacy queue entrypoint used by earlier stages.

    Enforces global + per-MX caps and RPS, then performs a *stub* probe (smtp_probe)
    and persists the result idempotently. R18 will supersede the persistence path.

    NOTE: New R16 probes should prefer task_probe_email() which returns a structured
    result and defers persistence to a later release.
    """
    redis: Redis = get_redis()
    job = get_current_job()  # may be None if called outside RQ

    # Env toggles (optional)
    env_raise_perm = _bool_env("SELFTEST_RAISE_PERM")  # '1', '0', or None
    env_raise_temp = _bool_env("SELFTEST_RAISE_TEMP")  # '1', '0', or None

    raise_perm_env = (env_raise_perm == "1") if env_raise_perm is not None else False
    raise_temp_env = (env_raise_temp == "1") if env_raise_temp is not None else False

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

        # Allow manual forcing of temporary failure via env outside self-test.
        if raise_temp_env and willfail_addr:
            raise TemporarySMTPError("R06 selftest (env): simulated temp failure")

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
            if not force_selftest_perm
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
        if force_selftest_perm:
            # Propagate to RQ -> Failed/DLQ
            raise

    except TemporarySMTPError as e:
        # Throttling / soft failure; do not re-raise by default.
        status, reason = "unknown", (str(e) or "temp_error")
        latency_ms = int((time.perf_counter() - start) * 1000)
        log.warning(
            "temporary failure handled",
            extra={
                "email": email,
                "mx": mx_host,
                "status": status,
                "reason": reason,
                "attempt": attempt,
                "latency_ms": latency_ms,
            },
        )

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
                extra={"email": email, "status": status, "reason": reason, "mx": mx_host},
            )

        # Only release what we actually acquired
        try:
            redis2 = get_redis()
        except Exception:
            redis2 = None  # type: ignore[assignment]

        if redis2 is not None:
            if got_mx:
                try:
                    release(redis2, MX_SEM.format(mx=mx_host))
                except Exception:
                    pass
            if got_global:
                try:
                    release(redis2, GLOBAL_SEM)
                except Exception:
                    pass


# -------------------------------#
# R15: MX resolution queue task
# -------------------------------


@job("mx", timeout=10)
def task_resolve_mx(company_id: int, domain: str, force: bool = False) -> dict:
    """
    R15 queue task: Resolve MX for a domain and persist to domain_resolutions.

    Behavior:
      - If Redis is available, enforce R06 concurrency/RPS caps (global + per-domain pre-MX).
      - If Redis is NOT available (e.g., direct call / smoke), gracefully bypass throttling
        and run inline (no Redis dependency).

    Returns:
      {"ok", "company_id", "domain", "lowest_mx", "mx_hosts", "preference_map",
       "cached", "failure", "row_id"}
    """
    import time as _time

    dom = (domain or "").strip().lower()
    if not dom:
        return {"ok": False, "error": "empty_domain", "company_id": company_id, "domain": dom}

    # Detect Redis availability; if unreachable, run inline without throttling.
    redis_ok = False
    try:
        redis: Redis = get_redis()
        try:
            redis.ping()
            redis_ok = True
        except Exception:
            redis_ok = False
    except Exception:
        redis_ok = False

    start = _time.perf_counter()
    got_global = False
    got_key = False
    sem_key = MX_SEM.format(mx=dom)  # per-domain key pre-MX to avoid herd

    try:
        # --------------------------
        # Optional throttling (R06)
        # --------------------------
        if redis_ok:
            # Concurrency caps
            got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
            if not got_global:
                return {
                    "ok": False,
                    "error": "global concurrency cap reached",
                    "company_id": company_id,
                    "domain": dom,
                }

            got_key = try_acquire(redis, sem_key, _cfg.rate.per_mx_max_concurrency_default)
            if not got_key:
                return {
                    "ok": False,
                    "error": "per-MX concurrency cap reached",
                    "company_id": company_id,
                    "domain": dom,
                }

            # RPS smoothing (optional)
            sec = int(_time.time())
            key_global_rps = RPS_KEY_GLOBAL.format(sec=sec)
            key_dom_rps = RPS_KEY_MX.format(mx=dom, sec=sec)

            if _cfg.rate.global_rps and not can_consume_rps(
                redis, key_global_rps, int(_cfg.rate.global_rps)
            ):
                return {
                    "ok": False,
                    "error": "global RPS throttle",
                    "company_id": company_id,
                    "domain": dom,
                }

            if _cfg.rate.per_mx_rps_default and not can_consume_rps(
                redis, key_dom_rps, int(_cfg.rate.per_mx_rps_default)
            ):
                return {
                    "ok": False,
                    "error": "MX RPS throttle",
                    "company_id": company_id,
                    "domain": dom,
                }

        # --------------------------
        # Resolve & persist (R15)
        # --------------------------
        db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
        res = _resolve_mx(company_id=company_id, domain=dom, force=force, db_path=db_path)

        latency_ms = int((_time.perf_counter() - start) * 1000)
        log.info(
            "R15 resolved MX",
            extra={
                "company_id": company_id,
                "domain": dom,
                "lowest_mx": res.lowest_mx,
                "cached": res.cached,
                "failure": res.failure,
                "latency_ms": latency_ms,
                "row_id": res.row_id,
                "count_hosts": len(res.mx_hosts or []),
                "throttled": redis_ok,
            },
        )

        return {
            "ok": True,
            "company_id": company_id,
            "domain": dom,
            "lowest_mx": res.lowest_mx,
            "mx_hosts": res.mx_hosts,
            "preference_map": res.preference_map,
            "cached": res.cached,
            "failure": res.failure,
            "row_id": res.row_id,
        }

    except Exception as e:
        log.exception(
            "R15 task_resolve_mx failed",
            extra={"company_id": company_id, "domain": dom, "exc": str(e)},
        )
        return {
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
            "company_id": company_id,
            "domain": dom,
        }
    finally:
        # Only release if we actually acquired and Redis was usable
        if redis_ok:
            if got_key:
                release(redis, sem_key)
            if got_global:
                release(redis, GLOBAL_SEM)


# -------------------------------
# R17: domain-level catch-all task
# -------------------------------


def _task_check_catchall(domain: str, force: bool = False) -> dict:
    """
    Core implementation for R17 catch-all detection.

    Adds a fast TCP/25 preflight so we don't sit in long timeouts when
    outbound 25 is blocked (common on local ISPs/VPNs).
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return {"ok": False, "error": "empty_domain"}

    # Fast preflight against lowest MX (or domain fallback).
    mx_host, _pref = lookup_mx(dom)
    redis_obj, redis_ok = _init_redis_for_probe()
    pre = _smtp_tcp25_preflight_mx(
        mx_host,
        timeout_s=float(os.getenv("TCP25_PROBE_TIMEOUT_SECONDS", "1.5")),
        redis=redis_obj if redis_ok else None,
    )
    if not pre.get("ok") and not bool(force):
        return {
            "ok": False,
            "error": "tcp25_blocked",
            "domain": dom,
            "mx_host": mx_host,
            "preflight": pre,
        }

    try:
        res = check_catchall_for_domain(dom, force=force)
    except Exception as e:
        log.exception("R17 task_check_catchall failed", extra={"domain": dom, "exc": str(e)})
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "domain": dom}

    return {
        "ok": True,
        "domain": res.domain,
        "status": res.status,
        "rcpt_code": res.rcpt_code,
        "cached": res.cached,
        "mx_host": res.mx_host,
        "elapsed_ms": int(res.elapsed_ms),
        "error": res.error,
    }


@job("mx", timeout=10)
def task_check_catchall(domain: str, force: bool = False) -> dict:
    """
    R17 queue task: thin RQ wrapper around _task_check_catchall, so tests can
    call __wrapped__ to exercise the core logic inline.
    """
    return _task_check_catchall(domain, force=force)


# Expose core implementation for tests via __wrapped__ (pytest expects this)
task_check_catchall.__wrapped__ = _task_check_catchall  # type: ignore[attr-defined]


# -------------------------------
# R16: SMTP RCPT probe queue task (+ O07 fallback, R18 classification, O26 escalation)
# -------------------------------


def _mx_info(domain: str, *, force: bool, db_path: str | None) -> tuple[str, dict[str, Any] | None]:
    """
    Resolve MX for a domain using R15 helpers and return (lowest_mx, behavior_hint).
    Falls back to naive DNS and no behavior if R15 helper is unavailable.
    """
    try:
        # Prefer get_or_resolve_mx if present
        from src.resolve.mx import get_or_resolve_mx as _gomx  # type: ignore

        res = _gomx(domain, force=force, db_path=db_path)
        if isinstance(res, dict):
            mxh = res.get("lowest_mx") or domain
            beh = res.get("behavior") or res.get("mx_behavior")
        else:
            mxh = getattr(res, "lowest_mx", None) or domain
            beh = getattr(res, "behavior", None) or getattr(res, "mx_behavior", None)
        return (mxh, beh)
    except Exception:
        pass
    try:
        # Fall back to resolve_mx (R15) dataclass
        res = _resolve_mx(company_id=0, domain=domain, force=force, db_path=db_path)
        mxh = getattr(res, "lowest_mx", None) or domain
        beh = getattr(res, "behavior", None) or getattr(res, "mx_behavior", None)
        return (mxh, beh)
    except Exception:
        # Last resort: bare DNS
        mxh, _pref = lookup_mx(domain)
        return (mxh, None)


def _normalize_probe_inputs(
    email_id: int,
    email: str,
    domain: str,
) -> tuple[str, str] | dict[str, Any]:
    """
    Normalize email/domain inputs and return either (email_str, domain)
    or an error payload suitable for early return.
    """
    email_str = (email or "").strip()
    dom = (domain or "").strip().lower() or (
        email_str.split("@", 1)[1].strip().lower() if "@" in email_str else ""
    )
    if not email_str or "@" not in email_str or not dom:
        return {
            "ok": False,
            "error": "bad_input",
            "category": "unknown",
            "code": None,
            "mx_host": None,
            "domain": dom,
            "email_id": email_id,
            "email": email_str,
            "elapsed_ms": 0,
        }
    return email_str, dom


def _throttle_error_result(
    *,
    error: str,
    mx_host: str | None,
    dom: str,
    email_id: int,
    email_str: str,
    start: float,
) -> dict[str, Any]:
    """Build a standardized throttle/error result dict."""
    return {
        "ok": False,
        "error": error,
        "category": "unknown",
        "code": None,
        "mx_host": mx_host,
        "domain": dom,
        "email_id": email_id,
        "email": email_str,
        "elapsed_ms": int((time.perf_counter() - start) * 1000),
    }


def _acquire_throttles(
    *,
    redis_ok: bool,
    redis: Redis | None,
    mx_host: str,
    mx_key: str,
    dom: str,
    email_id: int,
    email_str: str,
    start: float,
) -> tuple[bool, bool, dict[str, Any] | None]:
    """
    Acquire global + per-MX concurrency and RPS slots.

    Returns (got_global, got_mx, error_payload). error_payload is None on success.
    """
    got_global = False
    got_mx = False

    if not redis_ok or redis is None:
        return got_global, got_mx, None

    got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
    if not got_global:
        err = _throttle_error_result(
            error="global concurrency cap reached",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )
        return got_global, got_mx, err

    got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
    if not got_mx:
        err = _throttle_error_result(
            error="per-MX concurrency cap reached",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )
        return got_global, got_mx, err

    sec = int(time.time())
    key_global_rps = RPS_KEY_GLOBAL.format(sec=sec)
    key_mx_rps = RPS_KEY_MX.format(mx=mx_host, sec=sec)

    if _cfg.rate.global_rps and not can_consume_rps(
        redis,
        key_global_rps,
        int(_cfg.rate.global_rps),
    ):
        err = _throttle_error_result(
            error="global RPS throttle",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )
        return got_global, got_mx, err

    if _cfg.rate.per_mx_rps_default and not can_consume_rps(
        redis,
        key_mx_rps,
        int(_cfg.rate.per_mx_rps_default),
    ):
        err = _throttle_error_result(
            error="MX RPS throttle",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )
        return got_global, got_mx, err

    return got_global, got_mx, None


def _maybe_run_fallback(email_str: str, category: str) -> tuple[str | None, Any | None]:
    """
    O07: invoke verify_with_fallback(email) for ambiguous classifications.

    Returns (fallback_status, fallback_raw), both None when not applicable.
    """
    if category not in {"unknown", "temp_fail"}:
        return None, None

    try:
        fb = globals().get("verify_with_fallback")
        if callable(fb):
            fb_res = fb(email_str)  # type: ignore[misc]
            status = getattr(fb_res, "status", None)
            raw = getattr(fb_res, "raw", None)
            return status, raw
    except Exception as e:
        log.exception("O07 verify_with_fallback failed", extra={"email": email_str, "exc": str(e)})
    return None, None


def _utcnow_iso() -> str:
    """Return an ISO-8601 UTC timestamp like '2025-11-20T19:45:03Z'."""
    dt = datetime.utcnow().replace(microsecond=0)
    return dt.isoformat() + "Z"


def _load_catchall_status_for_domain(db_path: str, domain: str) -> str | None:
    """
    Best-effort helper for R18: load the latest catch_all_status from
    domain_resolutions for a given domain. Swallows errors so R16/R06
    tests can run against minimal schemas without R17 applied.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return None

    try:
        con = get_conn()
        try:
            row = con.execute(
                """
                SELECT catch_all_status
                FROM domain_resolutions
                WHERE chosen_domain = ? OR user_hint = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (dom, dom),
            ).fetchone()
            if not row:
                return None
            # Handle both dict-like (DictCursor) and tuple-like rows
            if hasattr(row, "get"):
                val = row.get("catch_all_status")
            elif hasattr(row, "__getitem__"):
                val = row[0] if isinstance(row, (list, tuple)) else row["catch_all_status"]
            else:
                val = row[0]
            return str(val) if val is not None else None
        finally:
            con.close()
    except Exception:
        log.debug(
            "R18: failed to load catch_all_status for domain", exc_info=True, extra={"domain": dom}
        )
        return None


def _parse_rcpt_code(code: Any) -> int | None:
    """
    Normalize an arbitrary code value into an int or None.
    """
    if isinstance(code, int):
        return code
    try:
        return int(code) if code is not None else None
    except Exception:
        return None


def _probe_hostile_from_behavior(behavior: Any) -> bool:
    """
    Extract a 'probe-hostile' flag from behavior/mx_behavior hints.

    Supports both dict-like and attribute-style objects.
    """
    if behavior is None:
        return False

    # Attribute-based (dataclass / SimpleNamespace-style)
    for attr in ("probing_hostile", "probe_hostile", "probe_hostile_mx"):
        if hasattr(behavior, attr):
            try:
                return bool(getattr(behavior, attr))
            except Exception:
                pass

    # Dict-like
    if isinstance(behavior, dict):
        for key in ("probing_hostile", "probe_hostile", "probe_hostile_mx"):
            if key in behavior:
                try:
                    return bool(behavior[key])
                except Exception:
                    return False

    return False


def _persist_probe_result_r18(
    *,
    db_path: str,
    email_id: int,
    email: str,
    domain: str,
    mx_host: str | None,
    category: str | None,
    code: Any,
    error: str | None,
    fallback_status: str | None,
    fallback_raw: Any,
    tcp25_ok: bool | None = None,
) -> tuple[str | None, str | None, str | None, str | None, int | None]:
    """
    R18: Best-effort classification + persistence of a verification attempt.

    Adjustment:
      - Prefer cached catch-all status from DB.
      - Skip *active* catch-all probing when tcp25_ok is False to avoid long hangs.
      - Persist email/domain when columns exist.
      - Use src.db.get_conn() for consistency with the rest of the app.
    """
    dom = (domain or "").strip().lower()
    try:
        cat_norm = (category or "").strip().lower() or None
        rcpt_code = _parse_rcpt_code(code)

        # --- Catch-all status (cached first; probe only if safe) -------------
        ca_status_db = _load_catchall_status_for_domain(db_path, dom)
        if ca_status_db in {"catch_all", "not_catch_all"}:
            catch_all_status: str | None = ca_status_db
        elif tcp25_ok is False:
            catch_all_status = None
        else:
            try:
                ca_result = check_catchall_for_domain(dom)
                ca_status = (ca_result.status or "").strip().lower()
                catch_all_status = (
                    ca_status if ca_status in {"catch_all", "not_catch_all"} else None
                )
            except Exception:
                log.exception(
                    "R18: failed to obtain catch-all status via check_catchall_for_domain",
                    extra={"domain": dom},
                )
                catch_all_status = None

        ts_iso = _utcnow_iso()

        signals = VerificationSignals(
            rcpt_category=cat_norm,
            rcpt_code=rcpt_code,
            rcpt_msg=None,
            catch_all_status=catch_all_status,
            fallback_status=(fallback_status or None),
            mx_host=mx_host,
            verified_at=ts_iso,
        )

        verify_status, verify_reason = classify(signals, now=datetime.utcnow())

        raw_status = cat_norm or "unknown"
        raw_reason = error or None

        if fallback_raw is None:
            fallback_raw_text: str | None = None
        elif isinstance(fallback_raw, str):
            fallback_raw_text = fallback_raw
        else:
            try:
                fallback_raw_text = json.dumps(fallback_raw, default=str)
            except Exception:
                fallback_raw_text = str(fallback_raw)

        # Normalize email_id: store NULL instead of 0.
        email_id_val: int | None = int(email_id) if int(email_id or 0) > 0 else None

        # Build a flexible insert that adapts to schema drift.
        desired_order = [
            "email_id",
            "email",
            "domain",
            "mx_host",
            "status",
            "reason",
            "checked_at",
            "fallback_status",
            "fallback_raw",
            "fallback_checked_at",
            "verify_status",
            "verify_reason",
            "verified_mx",
            "verified_at",
            "test_send_status",
        ]
        values: dict[str, Any] = {
            "email_id": email_id_val,
            "email": (email or "").strip() or None,
            "domain": dom or None,
            "mx_host": (mx_host or "").strip().lower() or None,
            "status": raw_status,
            "reason": raw_reason,
            "checked_at": ts_iso,
            "fallback_status": fallback_status,
            "fallback_raw": fallback_raw_text,
            "fallback_checked_at": ts_iso if fallback_status is not None else None,
            "verify_status": verify_status,
            "verify_reason": verify_reason,
            "verified_mx": (mx_host or "").strip().lower() or None,
            "verified_at": ts_iso,
            "test_send_status": "not_requested",
        }

        con = get_conn()
        try:
            # Figure out available columns via PRAGMA emulation (works on both SQLite and Postgres)
            cols: set[str] = set()
            try:
                rows = con.execute("PRAGMA table_info(verification_results)").fetchall()
                cols = {r[1] for r in rows if len(r) > 1}
            except Exception:
                cols = set()

            attempts: list[list[str]] = []
            if cols:
                attempts.append([c for c in desired_order if c in cols])
                # If email/domain are absent in schema, the above automatically drops them.
            else:
                # Unknown schema: try full, then reduced.
                attempts.append(desired_order)
                attempts.append(
                    [
                        "email_id",
                        "mx_host",
                        "status",
                        "reason",
                        "checked_at",
                        "fallback_status",
                        "fallback_raw",
                        "fallback_checked_at",
                        "verify_status",
                        "verify_reason",
                        "verified_mx",
                        "verified_at",
                    ]
                )

            last_exc: Exception | None = None
            verification_result_id: int | None = None

            for cols_try in attempts:
                cols_try = [c for c in cols_try if c in values]
                if not cols_try:
                    continue
                placeholders = ", ".join(["?"] * len(cols_try))
                cols_sql = ", ".join(cols_try)
                params = [values[c] for c in cols_try]
                try:
                    cur = con.execute(
                        f"INSERT INTO verification_results ({cols_sql}) VALUES ({placeholders})",
                        params,
                    )
                    try:
                        verification_result_id = int(cur.lastrowid)  # type: ignore[attr-defined]
                    except Exception:
                        verification_result_id = None
                    con.commit()
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    continue

            if last_exc is not None:
                raise last_exc

        finally:
            try:
                con.close()
            except Exception:
                pass

    except Exception:
        log.exception(
            "R18: failed to persist verification_results row",
            extra={"email_id": email_id, "email": email, "domain": dom, "mx_host": mx_host},
        )
        return None, None, None, None, None

    return verify_status, verify_reason, mx_host, ts_iso, verification_result_id


def _init_redis_for_probe() -> tuple[Redis | None, bool]:
    """
    Initialize Redis connection for probe tasks and indicate availability.
    """
    try:
        redis_obj: Redis = get_redis()
    except Exception:
        return None, False
    try:
        redis_obj.ping()
    except Exception:
        return redis_obj, False
    return redis_obj, True


def _smtp_tcp25_preflight_mx(
    mx_host: str,
    *,
    timeout_s: float = 1.5,
    redis: Redis | None = None,
    ttl_s: int = 300,
) -> dict[str, Any]:
    """
    Fast TCP/25 reachability preflight for the resolved MX host.
    Caches results in Redis (best-effort) to avoid repeated socket attempts.

    Returns:
      {"ok": bool, "mx_host": str, "cached": bool, "error": str|None}
    """
    host = (mx_host or "").strip().lower()
    if not host:
        return {"ok": False, "mx_host": mx_host, "cached": False, "error": "empty_mx_host"}

    cache_key = f"tcp25_preflight:{host}"
    if redis is not None:
        try:
            cached = redis.get(cache_key)
            if cached in (b"1", b"0"):
                return {
                    "ok": cached == b"1",
                    "mx_host": host,
                    "cached": True,
                    "error": None if cached == b"1" else "tcp25_blocked",
                }
        except Exception:
            pass

    ok = False
    err: str | None = None
    try:
        with closing(socket.create_connection((host, 25), timeout=timeout_s)):
            ok = True
            err = None
    except Exception as exc:
        ok = False
        err = f"{type(exc).__name__}: {exc}"

    if redis is not None:
        try:
            redis.setex(cache_key, int(ttl_s), b"1" if ok else b"0")
        except Exception:
            pass

    return {"ok": ok, "mx_host": host, "cached": False, "error": err}


@job("test_send", timeout=30)
def task_send_test_email(verification_result_id: int, email: str, token: str) -> dict:
    """
    O26: RQ job that sends a minimal test email and marks the row as 'sent'.

    This is intentionally stubby: production deployments should plug in a
    real SMTP/ESP integration here while keeping the DB updates intact.
    """
    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    sent_at = _utcnow_iso()

    try:
        # TODO: implement real outbound send; for now we just mark as sent.
        mark_test_send_sent(db_path, verification_result_id, sent_at=sent_at)
        log.info(
            "O26 test_send marked sent",
            extra={
                "verification_result_id": verification_result_id,
                "email": email,
                "token": token,
            },
        )
        return {
            "ok": True,
            "verification_result_id": verification_result_id,
            "email": email,
            "token": token,
            "sent_at": sent_at,
        }
    except Exception as exc:
        log.exception(
            "O26 task_send_test_email failed",
            extra={
                "verification_result_id": verification_result_id,
                "email": email,
                "token": token,
                "exc": str(exc),
            },
        )
        return {
            "ok": False,
            "verification_result_id": verification_result_id,
            "email": email,
            "token": token,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _enqueue_test_send_email(
    verification_result_id: int,
    email: str,
    token: str,
) -> None:
    """
    Best-effort helper: enqueue the O26 test-send job on the 'test_send' queue.
    """
    try:
        q = Queue(name="test_send", connection=get_redis())
        q.enqueue(
            task_send_test_email, verification_result_id, email, token, job_timeout=30, retry=None
        )
    except Exception as exc:
        log.warning(
            "O26 enqueue test_send failed",
            extra={
                "verification_result_id": verification_result_id,
                "email": email,
                "token": token,
                "exc": str(exc),
            },
        )


def _maybe_escalate_to_test_send(
    *,
    db_path: str,
    email_id: int,
    email: str,
    domain: str,
    mx_host: str | None,
    category: str | None,
    code: Any,
    fallback_status: str | None,
    behavior_hint: Any,
    verify_status: str | None,
    verify_reason: str | None,
    verified_at: str | None,
    verification_result_id: int | None,
) -> None:
    """
    O26: Decide whether to escalate this verification attempt to a
    bounce-based test-send path and, if so, enqueue the test-send job.

    Escalation rules:
      - Only when verify_status == "unknown".
      - Only when the MX is classified as probe-hostile by behavior hints.
      - Only for tempfail/timeout/blocked RCPT-style outcomes.
      - Only when no test-send has been requested yet (new row ->
        test_send_status='not_requested').
    """
    if verification_result_id is None or verify_status is None:
        return

    probe_hostile = _probe_hostile_from_behavior(behavior_hint)
    if not probe_hostile:
        return

    # Build signals consistent with R18 so the helper can reuse
    # normalization + RCPT flag logic.
    catch_all_status = _load_catchall_status_for_domain(db_path, domain)
    signals = VerificationSignals(
        rcpt_category=(category or None),
        rcpt_code=_parse_rcpt_code(code),
        rcpt_msg=None,
        catch_all_status=catch_all_status,
        fallback_status=(fallback_status or None),
        mx_host=mx_host,
        verified_at=verified_at,
    )

    # Newly inserted rows always start at 'not_requested'.
    test_send_status = "not_requested"

    if not should_escalate_to_test_send(
        signals,
        verify_status=verify_status,  # type: ignore[arg-type]
        probe_hostile=probe_hostile,
        test_send_status=test_send_status,
    ):
        return

    try:
        token = request_test_send(
            db_path=db_path, verification_result_id=verification_result_id, email=email
        )
    except Exception as exc:
        log.exception(
            "O26 request_test_send failed",
            extra={
                "verification_result_id": verification_result_id,
                "email": email,
                "domain": domain,
                "mx_host": mx_host,
                "verify_status": verify_status,
                "verify_reason": verify_reason,
                "exc": str(exc),
            },
        )
        return

    _enqueue_test_send_email(verification_result_id, email, token)
    log.info(
        "O26 test_send escalation enqueued",
        extra={
            "verification_result_id": verification_result_id,
            "email": email,
            "domain": domain,
            "mx_host": mx_host,
            "verify_status": verify_status,
            "verify_reason": verify_reason,
            "probe_hostile": probe_hostile,
        },
    )


def _task_probe_email_impl(email_id: int, email: str, domain: str, force: bool = False) -> dict:
    """
    Adjustments:
      - Add fast TCP/25 preflight (cached) and short-circuit when blocked (unless force=True).
      - Clamp connect/command timeouts so a single probe can't chew huge wall time.
      - Ensure R18 persistence does NOT trigger long catch-all probes when tcp25 is blocked.
    """
    normalized = _normalize_probe_inputs(email_id, email, domain)
    if isinstance(normalized, dict):
        return normalized
    email_str, dom = normalized

    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    start = time.perf_counter()
    mx_host, behavior_hint = _mx_info(dom, force=bool(force), db_path=db_path)

    redis_obj, redis_ok = _init_redis_for_probe()

    # --- NEW: fast TCP/25 preflight BEFORE throttles/probing -----------------
    pre = _smtp_tcp25_preflight_mx(
        mx_host,
        timeout_s=float(os.getenv("TCP25_PROBE_TIMEOUT_SECONDS", "1.5")),
        redis=redis_obj if redis_ok else None,
    )
    tcp25_ok = bool(pre.get("ok"))
    if not tcp25_ok and not bool(force):
        payload: dict[str, Any] = {
            "ok": False,
            "category": "unknown",
            "code": None,
            "mx_host": mx_host,
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
            "error": "tcp25_blocked",
            "preflight": pre,
        }

        v_status, v_reason, v_mx, v_at, _vr_id = _persist_probe_result_r18(
            db_path=db_path,
            email_id=int(email_id),
            email=email_str,
            domain=dom,
            mx_host=mx_host,
            category=payload["category"],
            code=None,
            error=payload["error"],
            fallback_status=None,
            fallback_raw=None,
            tcp25_ok=False,
        )
        if v_status is not None:
            payload["verify_status"] = v_status
            payload["verify_reason"] = v_reason
            payload["verified_mx"] = v_mx
            payload["verified_at"] = v_at

        return payload

    mx_key = MX_SEM.format(mx=mx_host)
    got_global = False
    got_mx = False

    try:
        got_global, got_mx, throttle_error = _acquire_throttles(
            redis_ok=redis_ok,
            redis=redis_obj,
            mx_host=mx_host,
            mx_key=mx_key,
            dom=dom,
            email_id=int(email_id),
            email_str=email_str,
            start=start,
        )
        if throttle_error is not None:
            v_status, v_reason, v_mx, v_at, _vr_id = _persist_probe_result_r18(
                db_path=db_path,
                email_id=int(email_id),
                email=email_str,
                domain=dom,
                mx_host=mx_host,
                category=throttle_error.get("category"),
                code=throttle_error.get("code"),
                error=throttle_error.get("error"),
                fallback_status=None,
                fallback_raw=None,
                tcp25_ok=tcp25_ok,
            )
            if v_status is not None:
                throttle_error["verify_status"] = v_status
                throttle_error["verify_reason"] = v_reason
                throttle_error["verified_mx"] = v_mx
                throttle_error["verified_at"] = v_at
            return throttle_error

        # --- NEW: clamp probe timeouts so the 20s job timeout remains realistic
        connect_timeout = float(SMTP_CONNECT_TIMEOUT)
        command_timeout = float(SMTP_COMMAND_TIMEOUT)
        connect_timeout = min(
            connect_timeout, float(os.getenv("SMTP_CONNECT_TIMEOUT_CLAMP", "6.0"))
        )
        command_timeout = min(
            command_timeout, float(os.getenv("SMTP_COMMAND_TIMEOUT_CLAMP", "10.0"))
        )

        result = probe_rcpt(
            email_str,
            mx_host,
            helo_domain=SMTP_HELO_DOMAIN,
            mail_from=SMTP_MAIL_FROM,
            connect_timeout=connect_timeout,
            command_timeout=command_timeout,
            behavior_hint=behavior_hint,
        )

        category = result.get("category", "unknown")
        code = result.get("code")
        error_val = result.get("error")

        fallback_status, fallback_raw = _maybe_run_fallback(email_str, category)

        base: dict[str, Any] = {
            "ok": bool(result.get("ok", True)),
            "category": category,
            "code": code,
            "mx_host": mx_host,
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
            "error": error_val,
        }

        if fallback_status is not None:
            base["fallback_status"] = fallback_status
            base["fallback_raw"] = fallback_raw

        v_status, v_reason, v_mx, v_at, vr_id = _persist_probe_result_r18(
            db_path=db_path,
            email_id=int(email_id),
            email=email_str,
            domain=dom,
            mx_host=mx_host,
            category=category,
            code=code,
            error=error_val,
            fallback_status=fallback_status,
            fallback_raw=fallback_raw,
            tcp25_ok=tcp25_ok,
        )
        if v_status is not None:
            base["verify_status"] = v_status
            base["verify_reason"] = v_reason
            base["verified_mx"] = v_mx
            base["verified_at"] = v_at

        _maybe_escalate_to_test_send(
            db_path=db_path,
            email_id=int(email_id),
            email=email_str,
            domain=dom,
            mx_host=mx_host,
            category=category,
            code=code,
            fallback_status=fallback_status,
            behavior_hint=behavior_hint,
            verify_status=v_status,
            verify_reason=v_reason,
            verified_at=v_at,
            verification_result_id=vr_id,
        )

        return base

    except Exception as exc:
        log.exception(
            "R16 task_probe_email failed",
            extra={
                "email_id": email_id,
                "email": email_str,
                "domain": dom,
                "mx_host": mx_host,
                "exc": str(exc),
            },
        )
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "category": "unknown",
            "code": None,
            "mx_host": mx_host,
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
        }
    finally:
        if redis_ok and redis_obj is not None:
            if got_mx:
                try:
                    release(redis_obj, mx_key)
                except Exception:
                    pass
            if got_global:
                try:
                    release(redis_obj, GLOBAL_SEM)
                except Exception:
                    pass


@job("verify", timeout=20)
def task_probe_email(email_id: int, email: str, domain: str, force: bool = False) -> dict:
    """RQ entrypoint for R16/R18 probes.

    This thin wrapper exists so we can expose a stable synchronous callable for
    local debugging/tests via ``task_probe_email.__wrapped__(...)``.
    """
    return _task_probe_email_impl(email_id=email_id, email=email, domain=domain, force=force)


# Expose core implementation for direct synchronous invocation (pytest/CLI)
task_probe_email.__wrapped__ = _task_probe_email_impl  # type: ignore[attr-defined]


# ---------------------------------------------
# Helpers for O01 domain pattern inference/cache
# ---------------------------------------------


def _has_table(con: Any, name: str) -> bool:
    """
    Check if a table exists in the database.
    Works with both SQLite (via compat layer emulation) and PostgreSQL.
    """
    try:
        # The compat layer emulates sqlite_master queries for Postgres
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (name,))
        return cur.fetchone() is not None
    except Exception:
        return False


def _examples_for_domain(con: Any, domain: str) -> list[tuple[str, str, str]]:
    """
    Build [(first, last, localpart)] examples for a domain using 'published' emails.

    We:
      - Prefer a join from emails → people when both tables/columns exist.
      - Fall back to names stored directly on emails when available.
      - Filter out obvious role/placeholder locals (info@, support@, etc.).
    """
    examples: list[tuple[str, str, str]] = []
    dom = (domain or "").strip().lower()
    if not dom:
        return examples

    # 1) Preferred: join emails → people, using the email's domain.
    try:
        rows = con.execute(
            """
            SELECT p.first_name, p.last_name, e.email
            FROM emails e
            JOIN people p ON p.id = e.person_id
            WHERE lower(substr(e.email, instr(e.email, '@') + 1)) = ?
              AND e.is_published = 1
            """,
            (dom,),
        ).fetchall()
    except Exception:
        # Fallback without is_published filter.
        try:
            rows = con.execute(
                """
                SELECT p.first_name, p.last_name, e.email
                FROM emails e
                JOIN people p ON p.id = e.person_id
                WHERE lower(substr(e.email, instr(e.email, '@') + 1)) = ?
                """,
                (dom,),
            ).fetchall()
        except Exception:
            rows = []

    for fn, ln, em in rows:
        if not em or "@" not in em or not fn or not ln:
            continue
        local = em.split("@", 1)[0].lower()
        if local in ROLE_ALIASES:
            continue
        examples.append((str(fn), str(ln), local))

    if examples:
        return examples

    # 2) Fallback: names stored directly on emails table.
    try:
        rows = con.execute(
            """
            SELECT first_name, last_name, email
            FROM emails
            WHERE lower(substr(email, instr(email, '@') + 1)) = ?
              AND is_published = 1
            """,
            (dom,),
        ).fetchall()
    except Exception:
        try:
            rows = con.execute(
                """
                SELECT first_name, last_name, email
                FROM emails
                WHERE lower(substr(email, instr(email, '@') + 1)) = ?
                """,
                (dom,),
            ).fetchall()
        except Exception:
            rows = []

    for fn, ln, em in rows:
        if not em or "@" not in em or not fn or not ln:
            continue
        local = em.split("@", 1)[0].lower()
        if local in ROLE_ALIASES:
            continue
        examples.append((str(fn), str(ln), local))

    return examples


def _load_cached_pattern(con: Any, domain: str) -> str | None:
    """Read a cached canonical pattern key for a domain if the table exists."""
    if not _has_table(con, "domain_patterns"):
        return None
    try:
        row = con.execute(
            "SELECT pattern FROM domain_patterns WHERE domain = ?", (domain,)
        ).fetchone()
        pat = row[0] if row and row[0] else None
        if pat in CANON_PATTERNS:
            return pat
    except Exception:
        pass
    return None


def _save_inferred_pattern(
    con: Any, domain: str, pattern: str, confidence: float, samples: int
) -> None:
    """Upsert the inferred pattern if the table exists."""
    if not _has_table(con, "domain_patterns"):
        return
    try:
        # Use CURRENT_TIMESTAMP for cross-database compatibility
        con.execute(
            """
            INSERT INTO domain_patterns (domain, pattern, confidence, samples)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
              pattern=excluded.pattern,
              confidence=excluded.confidence,
              samples=excluded.samples,
              inferred_at=CURRENT_TIMESTAMP
            """,
            (domain, pattern, float(confidence), int(samples)),
        )
    except Exception:
        # Non-fatal; skip caching errors
        log.exception(
            "failed to upsert domain_patterns", extra={"domain": domain, "pattern": pattern}
        )


def _company_id_for_person(con: Any, person_id: int) -> int | None:
    """
    Helper: resolve company_id for a person, if available.
    """
    try:
        row = con.execute("SELECT company_id FROM people WHERE id = ?", (person_id,)).fetchone()
        if not row:
            return None
        val = row[0]
        return int(val) if val is not None else None
    except Exception:
        log.debug(
            "failed to load company_id for person", exc_info=True, extra={"person_id": person_id}
        )
        return None


def _load_company_email_pattern(con: Any, company_id: int | None) -> str | None:
    """
    O26: read a per-company email pattern from companies.attrs["email_pattern"]
    when present and valid.

    The value must be one of the canonical pattern keys from src.generate.patterns.
    """
    if not company_id:
        return None
    try:
        # Guard against schemas without attrs.
        if not _has_table(con, "companies"):
            return None
        row = con.execute("SELECT attrs FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not row:
            return None
        raw = row[0]
        if not raw:
            return None

        try:
            attrs = json.loads(raw)
        except Exception:
            log.debug(
                "O26: failed to parse companies.attrs JSON",
                exc_info=True,
                extra={"company_id": company_id},
            )
            return None

        if not isinstance(attrs, dict):
            return None

        pat = attrs.get("email_pattern")
        if isinstance(pat, str) and pat in CANON_PATTERNS:
            return pat
    except Exception:
        log.debug(
            "O26: failed to load company email_pattern",
            exc_info=True,
            extra={"company_id": company_id},
        )
    return None


# ---------------------------------------------
# R12 wiring: email generation + verify enqueue (→ R16)
# ---------------------------------------------


def _email_row_id(con: Any, email: str) -> int | None:
    """
    Try to fetch the primary key for an email row.
    Falls back to rowid if 'id' column is absent (SQLite only).
    """
    email = (email or "").strip().lower()
    if not email:
        return None
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(emails)").fetchall()}
        if "id" in cols:
            row = con.execute("SELECT id FROM emails WHERE email = ?", (email,)).fetchone()
            return int(row[0]) if row else None
        # rowid fallback only works for SQLite
        row = con.execute("SELECT rowid FROM emails WHERE email = ?", (email,)).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None


def _enqueue_r16_probe(email_id: int | None, email: str, domain: str) -> None:
    """
    Enqueue the R16 probe task explicitly. Best-effort (swallows Redis errors).
    """
    try:
        q = Queue(name="verify", connection=get_redis())
        q.enqueue(
            task_probe_email,
            email_id=int(email_id or 0),
            email=email,
            domain=domain,
            force=False,
            job_timeout=20,
            retry=None,
        )
    except Exception as e:
        log.warning("R16 enqueue skipped (best-effort): %s", e)


def task_generate_emails(person_id: int, first: str, last: str, domain: str) -> dict:
    """
    Adjustment:
      - Persists generated candidates (now capped to MAX_PROBES_PER_PERSON, default 6)
      - Enqueues up to MAX_PROBES_PER_PERSON probes
      - Stores pattern_used + pattern_rank with each generated email
    """
    con = get_conn()
    dom = (domain or "").lower().strip()
    if not dom:
        return {
            "count": 0,
            "enqueued": 0,
            "max_probes_per_person": 0,
            "only_pattern": None,
            "domain": dom,
            "person_id": person_id,
        }

    try:
        max_probes = max(0, int(os.getenv("MAX_PROBES_PER_PERSON", "6")))
    except Exception:
        max_probes = 6

    company_id = _company_id_for_person(con, person_id)
    company_pattern = _load_company_email_pattern(con, company_id)

    nf, nl = normalize_split_parts(first, last)
    if not (nf or nl):
        log.info(
            "R12 skipped generation due to empty normalized name",
            extra={
                "person_id": person_id,
                "domain": dom,
                "first": first,
                "last": last,
                "company_id": company_id,
                "company_pattern": company_pattern,
            },
        )
        return {
            "count": 0,
            "enqueued": 0,
            "max_probes_per_person": max_probes,
            "only_pattern": None,
            "domain": dom,
            "person_id": person_id,
        }

    cached_pattern = _load_cached_pattern(con, dom)
    examples = _examples_for_domain(con, dom)

    domain_pattern: str | None = None
    inf_conf: float = 0.0
    inf_samples: int = 0

    if cached_pattern in CANON_PATTERNS:
        domain_pattern = cached_pattern
    elif examples:
        inf_result = infer_domain_pattern(examples)
        if inf_result.pattern in CANON_PATTERNS and inf_result.confidence >= 0.5:
            domain_pattern = inf_result.pattern
            inf_conf = inf_result.confidence
            inf_samples = inf_result.sample_count
            _save_inferred_pattern(con, dom, inf_result.pattern, inf_conf, inf_samples)

    effective_pattern = company_pattern or domain_pattern

    ranked_candidates = generate_candidate_emails_for_person(
        first=nf,
        last=nl,
        domain=dom,
        only_pattern=effective_pattern,
    )

    if max_probes > 0:
        ranked_candidates = ranked_candidates[:max_probes]

    inserted = 0
    enqueued = 0
    for rank, (email_addr, pattern_key) in enumerate(ranked_candidates, 1):
        try:
            upsert_generated_email(
                conn=con,
                person_id=person_id,
                email=email_addr,
                pattern_used=pattern_key,
                pattern_rank=rank,
            )
            inserted += 1
        except Exception:
            log.debug(
                "R12 upsert_generated_email failed",
                exc_info=True,
                extra={
                    "person_id": person_id,
                    "email": email_addr,
                    "pattern": pattern_key,
                    "rank": rank,
                },
            )
            continue

        email_id = _email_row_id(con, email_addr)
        try:
            _enqueue_r16_probe(email_id, email_addr, dom)
            enqueued += 1
        except Exception:
            log.debug(
                "R12 enqueue probe failed",
                exc_info=True,
                extra={"person_id": person_id, "email": email_addr, "email_id": email_id},
            )

    try:
        con.commit()
    except Exception:
        pass

    log.info(
        "R12 generated emails",
        extra={
            "person_id": person_id,
            "company_id": company_id,
            "domain": dom,
            "first_norm": nf,
            "last_norm": nl,
            "only_pattern": effective_pattern,
            "company_pattern": company_pattern,
            "domain_pattern": domain_pattern,
            "inference_confidence": inf_conf,
            "inference_samples": inf_samples,
            "count": inserted,
            "enqueued": enqueued,
            "max_probes_per_person": max_probes,
        },
    )
    return {
        "count": inserted,
        "enqueued": enqueued,
        "max_probes_per_person": max_probes,
        "only_pattern": effective_pattern,
        "inference_confidence": inf_conf,
        "inference_samples": inf_samples,
        "domain": dom,
        "person_id": person_id,
    }


# ---------------------------
# Auto-discovery: company-scoped crawl + extract
# ---------------------------


def _sources_has_company_id(con: Any) -> bool:
    try:
        rows = con.execute("PRAGMA table_info(sources)").fetchall()
    except Exception:
        return False
    for r in rows:
        if len(r) > 1 and r[1] == "company_id":
            return True
    return False


def crawl_company_site(company_id: int) -> dict:
    """
    Crawl the canonical domain for a single company and persist pages into 'sources'.

    This is the core "company → crawl" building block used by auto-discovery.
    It is side-effectful (DB writes) but does not itself depend on RQ; callers
    may invoke it directly or via handle_task().

    Task E: Now tracks metrics via AutodiscoveryResult and stores in job.meta.
    """
    con = _conn()
    pages: list[Any] = []

    result_obj = AutodiscoveryResult(company_id=company_id)

    try:
        cur = con.execute(
            "SELECT name, official_domain, domain FROM companies WHERE id = ?", (company_id,)
        )
        row = cur.fetchone()
        if not row:
            result_obj.add_error("company_not_found")
            _store_result_in_job_meta(result_obj)
            return {
                "ok": False,
                "error": "company_not_found",
                "company_id": company_id,
                "autodiscovery_result": result_obj.to_dict(),
            }

        company_name = row[0]
        official = (row[1] or "").strip() if row[1] is not None else ""
        fallback = (row[2] or "").strip() if row[2] is not None else ""
        dom = official or fallback
        if not dom:
            result_obj.add_error("no_domain_for_company")
            _store_result_in_job_meta(result_obj)
            return {
                "ok": False,
                "error": "no_domain_for_company",
                "company_id": company_id,
                "company_name": company_name,
                "autodiscovery_result": result_obj.to_dict(),
            }

        result_obj.domain = dom

        pages = crawl_domain(dom, result=result_obj)
        result_obj.pages_fetched = len(pages)

        if pages:
            try:
                save_pages(con, pages, company_id=company_id)  # type: ignore[call-arg]
            except TypeError:
                save_pages(con, pages)  # type: ignore[call-arg]
            con.commit()

        _store_result_in_job_meta(result_obj)

        result: dict[str, Any] = {
            "ok": True,
            "company_id": company_id,
            "company_name": company_name,
            "domain": dom,
            "page_count": len(pages),
            "autodiscovery_result": result_obj.to_dict(),
        }

        _enqueue_company_task("extract_candidates_for_company", company_id=company_id)

        return result
    except Exception as exc:
        log.exception(
            "crawl_company_site failed", extra={"company_id": company_id, "exc": str(exc)}
        )
        result_obj.add_error(f"{type(exc).__name__}: {exc}")
        _store_result_in_job_meta(result_obj)
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "company_id": company_id,
            "autodiscovery_result": result_obj.to_dict(),
        }


def autodiscover_company(company_id: int) -> dict:
    """
    Auto-discover a company in one pass:
      - crawl domain (robots-aware; records robots blocks into result if enabled in runner)
      - persist pages into sources
      - extract candidates for company (includes optional AI refinement path already inside)
      - store AutodiscoveryResult in RQ job meta (if running under RQ)
      - return result.to_dict() for queue propagation

    Task E: Full metrics tracking with AutodiscoveryResult.
    """
    con = _conn()
    result_obj = AutodiscoveryResult(company_id=company_id)

    try:
        company = _load_company_name_and_domain(con, company_id)
        if company is None:
            result_obj.add_error("company_not_found")
            _store_result_in_job_meta(result_obj)
            return result_obj.to_dict()

        company_name, dom, _fallback_domain = company
        result_obj.domain = dom
        # Best-effort global hint; wrapper provides the true per-run metrics later.
        result_obj.ai_enabled = bool(AI_PEOPLE_ENABLED)

        pages = crawl_domain(dom, result=result_obj)
        result_obj.pages_fetched = len(pages)

        if pages:
            try:
                save_pages(con, pages, company_id=company_id)  # type: ignore[call-arg]
            except TypeError:
                save_pages(con, pages)  # type: ignore[call-arg]
            con.commit()

        # Pass result through so AI wrapper metrics are recorded on the same object.
        extract_payload = extract_candidates_for_company(company_id, result=result_obj)

        # Update extraction counters (best-effort; tolerate schema drift)
        try:
            if hasattr(result_obj, "candidates_with_email"):
                result_obj.candidates_with_email = int(
                    extract_payload.get("found_candidates_email") or 0
                )
            if hasattr(result_obj, "candidates_no_email"):
                result_obj.candidates_no_email = int(
                    extract_payload.get("found_candidates_no_email") or 0
                )
            if hasattr(result_obj, "people_upserted"):
                result_obj.people_upserted = int(extract_payload.get("inserted_people") or 0)
            if hasattr(result_obj, "emails_upserted"):
                result_obj.emails_upserted = int(extract_payload.get("inserted_emails") or 0)
        except Exception:
            pass

        _store_result_in_job_meta(result_obj)
        return result_obj.to_dict()

    except Exception as exc:
        log.exception(
            "autodiscover_company failed", extra={"company_id": company_id, "exc": str(exc)}
        )
        result_obj.add_error(f"{type(exc).__name__}: {exc}")
        _store_result_in_job_meta(result_obj)
        return result_obj.to_dict()

    finally:
        try:
            con.close()
        except Exception:
            pass


def _get_company_attrs(con: Any, company_id: int) -> dict[str, Any]:
    """
    Best-effort loader for companies.attrs (JSON); returns {} on any error.
    """
    attrs: dict[str, Any] = {}
    try:
        if not _has_table(con, "companies"):
            return attrs
        row = con.execute("SELECT attrs FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not row:
            return attrs
        raw = row[0]
        if not raw:
            return attrs
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        if isinstance(raw, str):
            obj = json.loads(raw)
            if isinstance(obj, dict):
                attrs = obj
    except Exception:
        log.debug(
            "O27: failed to load companies.attrs", exc_info=True, extra={"company_id": company_id}
        )
    return attrs


def _set_company_attrs(con: Any, company_id: int, attrs: dict[str, Any]) -> None:
    """
    Best-effort writer for companies.attrs (JSON). Does not commit.
    """
    try:
        raw = json.dumps(attrs, separators=(",", ":"), sort_keys=True)
        con.execute("UPDATE companies SET attrs = ? WHERE id = ?", (raw, int(company_id)))
    except Exception:
        log.debug(
            "O27: failed to update companies.attrs", exc_info=True, extra={"company_id": company_id}
        )


def _should_run_ai_for_company(con: Any, company_id: int) -> bool:
    """
    Return True if AI people extraction should run for this company.

    We run at most once per company, keyed by attrs['ai_people_extracted'].
    """
    attrs = _get_company_attrs(con, company_id)
    flag = attrs.get("ai_people_extracted")
    return not bool(flag)


def _mark_ai_people_extracted(con: Any, company_id: int) -> None:
    """
    Mark that AI people extraction has been attempted for this company.
    """
    try:
        attrs = _get_company_attrs(con, company_id)
        if attrs.get("ai_people_extracted"):
            return
        attrs["ai_people_extracted"] = True
        _set_company_attrs(con, company_id, attrs)
    except Exception:
        log.debug(
            "O27: failed to mark ai_people_extracted flag",
            exc_info=True,
            extra={"company_id": company_id},
        )


def _load_company_name_and_domain(con: Any, company_id: int) -> tuple[str, str, str] | None:
    """
    Return (company_name, dom, fallback_domain) or None if company is missing.
    """
    row = con.execute(
        "SELECT name, official_domain, domain FROM companies WHERE id = ?", (company_id,)
    ).fetchone()
    if not row:
        return None

    company_name = str(row[0])
    official = (row[1] or "").strip() if row[1] is not None else ""
    fallback = (row[2] or "").strip() if row[2] is not None else ""
    dom = (official or fallback).lower()
    return company_name, dom, fallback


def _load_company_sources(con: Any, company_id: int, dom: str) -> list[tuple[str, bytes | str]]:
    """
    Load (source_url, html) rows from sources, scoping by company_id if supported,
    otherwise filtering by domain match against source_url host.
    """
    pages_rows: list[tuple[str, bytes | str]] = []

    has_company_id = _sources_has_company_id(con)
    if has_company_id:
        cur_src = con.execute(
            "SELECT source_url, html FROM sources WHERE company_id = ?", (company_id,)
        )
        return [(r[0], r[1]) for r in cur_src.fetchall()]

    cur_src = con.execute("SELECT source_url, html FROM sources")
    raw_rows = cur_src.fetchall()
    if not dom:
        return [(r[0], r[1]) for r in raw_rows]

    for r in raw_rows:
        src_url = (r[0] or "").strip()
        host = (urlparse(src_url).netloc or "").lower()
        if not host:
            continue
        if host == dom or host.endswith("." + dom):
            pages_rows.append((src_url, r[1]))

    return pages_rows


def _extract_raw_candidates_from_pages(
    pages_rows: list[tuple[str, bytes | str]],
    dom: str,
) -> list[ExtractCandidate]:
    """
    Run the HTML-level extractor over all pages and return a flat list of candidates.

    P1/P2 polish (page classifier gate):
      - Skip extraction entirely on pages that are very likely to mention
        third-party people (job boards, press releases, testimonials, etc.).
    """
    try:
        from src.extract.page_classifier import classify_page_type
    except Exception:  # pragma: no cover
        classify_page_type = None  # type: ignore[assignment]

    raw_candidates: list[ExtractCandidate] = []

    for src_url, html_raw in pages_rows:
        if isinstance(html_raw, bytes):
            try:
                html_str = html_raw.decode("utf-8", "ignore")
            except Exception:
                continue
        else:
            html_str = str(html_raw or "")

        # Page classifier gate: skip extraction on pages unlikely to have employees.
        if classify_page_type is not None:
            try:
                page_type = classify_page_type(html_str, url=src_url)  # type: ignore[misc]
                if page_type in {"job_board", "press_release", "testimonial", "news", "careers"}:
                    log.debug(
                        "P1/P2 skipping extraction for page_type=%s url=%s",
                        page_type,
                        src_url,
                    )
                    continue
            except Exception:
                pass

        try:
            cands = extract_html_candidates(html_str, source_url=src_url, domain=dom)
            raw_candidates.extend(cands)
        except Exception:
            log.debug("extract_candidates failed for %s", src_url, exc_info=True)
            continue

    return raw_candidates


def _split_role_and_personish_candidates(
    raw_candidates: list[ExtractCandidate],
) -> tuple[list[ExtractCandidate], list[ExtractCandidate]]:
    """
    Split candidates into (role_candidates, personish_candidates).

    - Role: role/placeholder emails (info@, support@, etc.)
    - Personish: non-role emails + no-email candidates (people-cards, etc.)
    """
    role_candidates: list[ExtractCandidate] = []
    personish_candidates: list[ExtractCandidate] = []

    for cand in raw_candidates:
        email_norm = (getattr(cand, "email", "") or "").strip().lower()
        if not email_norm:
            # Future-proof: allow no-email candidates to be refined/persisted as people.
            personish_candidates.append(cand)
            continue
        if is_role_or_placeholder_email(email_norm):
            role_candidates.append(cand)
        else:
            personish_candidates.append(cand)

    return role_candidates, personish_candidates


def _decide_ai_allowed_for_company(con: Any, company_id: int) -> bool:
    """
    Determine whether AI refinement is permitted for this company.
    """
    if not AI_PEOPLE_ENABLED:
        return False

    try:
        return _should_run_ai_for_company(con, company_id)
    except Exception:
        log.debug(
            "O27: failed to evaluate ai_people_extracted flag; defaulting to allowed",
            exc_info=True,
            extra={"company_id": company_id},
        )
        return True


def _ai_attempted_from_metrics(metrics: Any) -> bool:
    """
    Best-effort detection of whether an AI call was actually attempted.
    Supports dict-like metrics or dataclass-like objects.
    """
    if metrics is None:
        return False

    # Dict-like
    if isinstance(metrics, dict):
        for k in ("ai_called", "attempted", "ai_attempted", "called"):
            if k in metrics:
                try:
                    return bool(metrics[k])
                except Exception:
                    pass
        # Some wrappers track status strings (e.g., "ok_nonempty", "ok_empty", "failed")
        st = str(metrics.get("status") or metrics.get("ai_status") or "").strip().lower()
        if st in {"ok_nonempty", "ok_empty", "failed"}:
            return True
        return False

    # Attribute-like
    for attr in ("ai_called", "attempted", "ai_attempted", "called"):
        if hasattr(metrics, attr):
            try:
                return bool(getattr(metrics, attr))
            except Exception:
                pass
    st = ""
    for attr in ("status", "ai_status"):
        if hasattr(metrics, attr):
            try:
                st = str(getattr(metrics, attr) or "").strip().lower()
            except Exception:
                st = ""
            break
    return st in {"ok_nonempty", "ok_empty", "failed"}


def _maybe_refine_people_with_ai(
    *,
    company_name: str,
    dom: str,
    fallback_domain: str,
    personish_candidates: list[ExtractCandidate],
    ai_allowed_for_company: bool,
    result: AutodiscoveryResult | None = None,
) -> tuple[list[ExtractCandidate], bool]:
    """
    Optionally run AI refinement once per company.

    Uses the O27 wrapper (src.extract.ai_candidates_wrapper) to enforce:
      - AI enabled + call succeeds but returns 0 => return [] (NO fallback)
      - AI disabled => smart fallback
      - AI failed => smart fallback

    Returns (refined_people, ai_attempted).
    """
    if not personish_candidates or not ai_allowed_for_company:
        return personish_candidates, False

    if not _HAS_AI_WRAPPER or refine_candidates_with_ai is None:
        # Wrapper missing: keep legacy behavior (no AI), but consider this "not attempted"
        return personish_candidates, False

    try:
        refined, metrics = refine_candidates_with_ai(
            company_name=company_name,
            domain=(dom or fallback_domain),
            raw_candidates=personish_candidates,
        )
        if result is not None and update_result_from_metrics is not None:
            try:
                update_result_from_metrics(result, metrics)
            except Exception:
                pass
        return list(refined), _ai_attempted_from_metrics(metrics)
    except Exception as exc:
        # If the wrapper itself fails unexpectedly, do not block extraction.
        log.debug(
            "AI wrapper failed; continuing with heuristic-only candidates",
            exc_info=True,
            extra={"domain": dom, "exc": str(exc)},
        )
        return personish_candidates, True


def _candidate_has_any_name(c: ExtractCandidate) -> bool:
    return bool(
        getattr(c, "first_name", None)
        or getattr(c, "last_name", None)
        or getattr(c, "raw_name", None)
    )


def _merge_candidates_by_email(
    refined_people: list[ExtractCandidate],
    role_candidates: list[ExtractCandidate],
) -> dict[str, ExtractCandidate]:
    """
    Merge refined people + role candidates by email, preferring candidates with
    better name information when duplicates exist.
    """
    candidates_by_email: dict[str, ExtractCandidate] = {}

    def merge_one(c: ExtractCandidate) -> None:
        email_norm = (getattr(c, "email", "") or "").strip().lower()
        if not email_norm:
            return

        existing = candidates_by_email.get(email_norm)
        if existing is None:
            candidates_by_email[email_norm] = c
            return

        if _candidate_has_any_name(c) and not _candidate_has_any_name(existing):
            candidates_by_email[email_norm] = c

    for cand in refined_people:
        merge_one(cand)
    for cand in role_candidates:
        merge_one(cand)

    return candidates_by_email


def _candidate_full_name(cand: ExtractCandidate) -> str | None:
    raw = getattr(cand, "raw_name", None)
    if raw:
        return str(raw)

    parts = [p for p in (getattr(cand, "first_name", None), getattr(cand, "last_name", None)) if p]
    return " ".join(parts) if parts else None


def _upsert_person_from_candidate(
    con: Any,
    *,
    company_id: int,
    cand: ExtractCandidate,
    full_name: str,
) -> tuple[int, bool]:
    """
    Upsert a person row for (company_id, full_name).

    Returns (person_id, inserted_new).
    """
    row_p = con.execute(
        "SELECT id FROM people WHERE company_id = ? AND full_name = ?", (company_id, full_name)
    ).fetchone()
    if row_p:
        return int(row_p[0]), False

    title_val = getattr(cand, "title", None) or "Auto-discovered"

    cur_p = con.execute(
        """
        INSERT INTO people (
          company_id,
          first_name,
          last_name,
          full_name,
          title,
          source_url
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            getattr(cand, "first_name", None),
            getattr(cand, "last_name", None),
            full_name,
            title_val,
            getattr(cand, "source_url", None),
        ),
    )
    return int(cur_p.lastrowid), True


def _upsert_email_from_candidate(
    con: Any,
    *,
    company_id: int,
    email_norm: str,
    person_id_for_email: int | None,
    source_url: str | None,
) -> tuple[int, bool]:
    """
    Upsert an email row by email address.

    Returns (email_id, inserted_new).
    """
    row_e = con.execute("SELECT id FROM emails WHERE email = ?", (email_norm,)).fetchone()
    if row_e:
        email_id = int(row_e[0])
        con.execute(
            """
            UPDATE emails
               SET company_id = COALESCE(company_id, ?),
                   person_id = COALESCE(person_id, ?),
                   source_url = COALESCE(source_url, ?),
                   is_published = COALESCE(is_published, 1)
             WHERE id = ?
            """,
            (company_id, person_id_for_email, source_url, email_id),
        )
        return email_id, False

    cur_e = con.execute(
        """
        INSERT INTO emails (person_id, company_id, email, is_published, source_url)
        VALUES (?, ?, ?, ?, ?)
        """,
        (person_id_for_email, company_id, email_norm, 1, source_url),
    )
    return int(cur_e.lastrowid), True


def _persist_candidates_for_company(  # noqa: C901
    con: Any,
    *,
    company_id: int,
    dom: str,
    candidates_by_email: dict[str, ExtractCandidate],
    candidates_no_email: Sequence[ExtractCandidate] | None = None,
) -> tuple[int, int, int, int]:
    """
    Persist people + emails, and enqueue R16 probes.

    IMPORTANT:
      - Email-bearing candidates are keyed by normalized email (existing behavior).
      - No-email candidates MUST be persisted as people and MUST NOT be collapsed
        by an email-keyed dict. We dedupe no-email candidates by a person signature.

    Returns (inserted_people, updated_people, inserted_emails, updated_emails).
    """
    inserted_people = 0
    updated_people = 0
    inserted_emails = 0
    updated_emails = 0

    def _norm_sig(s: str | None) -> str:
        if not s:
            return ""
        s2 = str(s).replace("\u00a0", " ").strip().lower()
        s2 = " ".join(s2.split())
        return s2

    def _looks_like_valid_person_name(name: str | None) -> bool:
        if not name:
            return False
        n = name.strip()
        if len(n) < 3:
            return False
        parts = [p for p in n.split() if p]
        if len(parts) < 2 or len(parts) > 5:
            return False

        for p in parts:
            clean = p.replace("-", "").replace("'", "").replace(".", "")
            if not clean:
                continue
            if not clean[0].isupper():
                return False
            if not all(ch.isalpha() for ch in clean):
                return False

        try:
            vp = globals().get("validate_person_name")
            if callable(vp):
                res = vp(n)
                if hasattr(res, "is_valid"):
                    return bool(res.is_valid)
                if isinstance(res, tuple) and res:
                    return bool(res[0])
                if isinstance(res, bool):
                    return res
        except Exception:
            pass

        return True

    def _clean_or_drop_title(title: str | None) -> str | None:
        if not title:
            return None
        t = str(title).strip()
        if not t:
            return None

        try:
            ct = globals().get("clean_title_if_invalid")
            if callable(ct):
                cleaned = ct(t)
                return cleaned.strip() if cleaned else None
        except Exception:
            pass

        return t

    # ----------------------------
    # Bucket 1: NO-EMAIL people
    # ----------------------------
    approved_no_email = list(candidates_no_email or [])
    no_email_deduped = 0
    no_email_rejected = 0
    seen_no_email_sigs: set[str] = set()

    for cand in approved_no_email:
        full_name = _candidate_full_name(cand)
        if not _looks_like_valid_person_name(full_name):
            no_email_rejected += 1
            continue

        title = _clean_or_drop_title(getattr(cand, "title", None))
        try:
            cand.title = title
        except Exception:
            pass

        source_url = getattr(cand, "source_url", None)

        sig = f"{_norm_sig(full_name)}|{_norm_sig(title)}|{_norm_sig(source_url)}"
        if sig in seen_no_email_sigs:
            no_email_deduped += 1
            continue
        seen_no_email_sigs.add(sig)

        _person_id, inserted_new = _upsert_person_from_candidate(
            con,
            company_id=company_id,
            cand=cand,
            full_name=full_name,
        )
        if inserted_new:
            inserted_people += 1
        else:
            updated_people += 1

    # ----------------------------
    # Bucket 2: EMAIL-bearing candidates
    # ----------------------------
    for email_norm, cand in sorted(candidates_by_email.items()):
        email_norm = (email_norm or "").strip().lower()
        if not email_norm:
            continue

        is_placeholder = is_role_or_placeholder_email(email_norm)
        full_name = _candidate_full_name(cand)

        person_id_for_person: int | None = None
        if full_name and _looks_like_valid_person_name(full_name):
            person_id_for_person, inserted_new = _upsert_person_from_candidate(
                con,
                company_id=company_id,
                cand=cand,
                full_name=full_name,
            )
            if inserted_new:
                inserted_people += 1
            else:
                updated_people += 1

        person_id_for_email: int | None = None
        if not is_placeholder and person_id_for_person is not None:
            person_id_for_email = person_id_for_person

        email_id, inserted_new_email = _upsert_email_from_candidate(
            con,
            company_id=company_id,
            email_norm=email_norm,
            person_id_for_email=person_id_for_email,
            source_url=getattr(cand, "source_url", None),
        )
        if inserted_new_email:
            inserted_emails += 1
        else:
            updated_emails += 1

        try:
            domain_for_email = email_norm.split("@", 1)[1].lower() if "@" in email_norm else dom
            _enqueue_r16_probe(email_id, email_norm, domain_for_email or dom)
        except Exception as e:
            log.debug(
                "R16 enqueue from extract_candidates_for_company failed",
                exc_info=True,
                extra={"email": email_norm, "company_id": company_id, "exc": str(e)},
            )

    try:
        log.info(
            "Persisted candidates for company_id=%s dom=%s "
            "approved_email=%d approved_no_email=%d "
            "no_email_rejected=%d no_email_deduped=%d "
            "people_inserted=%d people_updated=%d "
            "emails_inserted=%d emails_updated=%d",
            company_id,
            dom,
            len(candidates_by_email),
            len(approved_no_email),
            no_email_rejected,
            no_email_deduped,
            inserted_people,
            updated_people,
            inserted_emails,
            updated_emails,
        )
    except Exception:
        pass

    return inserted_people, updated_people, inserted_emails, updated_emails


def _empty_extract_result(
    *,
    company_id: int,
    company_name: str,
    dom: str,
    found_candidates: int = 0,
    inserted_people: int = 0,
    inserted_emails: int = 0,
    emails_total: int = 0,
) -> dict[str, Any]:
    return {
        "ok": True,
        "company_id": company_id,
        "company_name": company_name,
        "domain": dom,
        "found_candidates": found_candidates,
        "inserted_people": inserted_people,
        "inserted_emails": inserted_emails,
        "emails_total": emails_total,
    }


def extract_candidates_for_company(  # noqa: C901
    company_id: int, result: AutodiscoveryResult | None = None
) -> dict:
    """
    R11+ glue: Pull HTML pages for a company from 'sources', run the HTML-level
    extractor, optionally refine those candidates with AI, and upsert people/emails
    into the core tables. For newly created emails, enqueue R16 verification probes.

    O26 behavior alignment:
      - Role/placeholder emails (info@, support@, example@, noreply@, etc.) are
        stored at the company level (person_id = NULL) even when we have a name.
        We still create a person row when we have a name so that permutations
        can be generated later, but we do NOT attach these role emails to that
        person.

    O27 enhancement (refiner mode):
      - Broad heuristic extraction across pages.
      - Split role vs personish.
      - Run the AI refiner once per company (wrapper enforces no-fallback-on-ok-empty).
      - Persist email-bearing + no-email buckets.
      - AI runs at most once per company, controlled by companies.attrs["ai_people_extracted"].

    P1/P2 polish:
      - Block candidates originating from third-party/non-employee pages BEFORE AI/persist.
    """
    try:
        from src.extract.source_filters import is_blocked_source_url
    except Exception:  # pragma: no cover
        is_blocked_source_url = None  # type: ignore[assignment]

    def _candidate_source_url(c: Any) -> str:
        return (
            (getattr(c, "source_url", None) or "")
            or (getattr(c, "page_url", None) or "")
            or (getattr(c, "url", None) or "")
        ).strip()

    def _filter_blocked_sources(cands: list[Any], *, label: str) -> tuple[list[Any], int]:
        if not cands or is_blocked_source_url is None:
            return cands, 0

        out: list[Any] = []
        blocked = 0
        for cand in cands:
            url = _candidate_source_url(cand)
            if url:
                try:
                    blocked_flag, reason = is_blocked_source_url(url)  # type: ignore[misc]
                except Exception:
                    blocked_flag, reason = False, None
                if blocked_flag:
                    blocked += 1
                    log.info(
                        "Source-filter blocked %s candidate: name=%r email=%r url=%s reason=%s",
                        label,
                        getattr(cand, "raw_name", None),
                        getattr(cand, "email", None),
                        url,
                        reason,
                    )
                    continue
            out.append(cand)

        if blocked:
            log.info("Source-filter summary: blocked=%d kept=%d label=%s", blocked, len(out), label)
        return out, blocked

    con = _conn()
    try:
        company = _load_company_name_and_domain(con, company_id)
        if company is None:
            return {"ok": False, "error": "company_not_found", "company_id": company_id}

        company_name, dom, fallback_domain = company

        if not _has_table(con, "sources"):
            return {
                "ok": False,
                "error": "sources_table_missing",
                "company_id": company_id,
                "company_name": company_name,
            }

        pages_rows = _load_company_sources(con, company_id, dom)
        if not pages_rows:
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        ai_allowed_for_company = _decide_ai_allowed_for_company(con, company_id)

        raw_candidates = _extract_raw_candidates_from_pages(pages_rows, dom)
        if not raw_candidates:
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        role_candidates, personish_candidates = _split_role_and_personish_candidates(raw_candidates)

        role_candidates, blocked_role = _filter_blocked_sources(role_candidates, label="role")
        personish_candidates, blocked_personish = _filter_blocked_sources(
            personish_candidates, label="personish"
        )

        if not role_candidates and not personish_candidates:
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        refined_people, ai_attempted = _maybe_refine_people_with_ai(
            company_name=company_name,
            dom=dom,
            fallback_domain=fallback_domain,
            personish_candidates=personish_candidates,
            ai_allowed_for_company=ai_allowed_for_company,
            result=result,
        )

        candidates_by_email = _merge_candidates_by_email(refined_people, role_candidates)

        candidates_no_email: list[ExtractCandidate] = []
        for cand in refined_people:
            email_val = getattr(cand, "email", None)
            if email_val is None or not str(email_val).strip():
                candidates_no_email.append(cand)

        if not candidates_by_email and not candidates_no_email:
            if ai_allowed_for_company and ai_attempted:
                _mark_ai_people_extracted(con, company_id)
                con.commit()
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        inserted_people, updated_people, inserted_emails, updated_emails = (
            _persist_candidates_for_company(
                con,
                company_id=company_id,
                dom=dom,
                candidates_by_email=candidates_by_email,
                candidates_no_email=candidates_no_email,
            )
        )

        if ai_allowed_for_company and ai_attempted:
            _mark_ai_people_extracted(con, company_id)

        con.commit()

        emails_total_row = con.execute(
            "SELECT COUNT(*) FROM emails WHERE company_id = ?", (company_id,)
        ).fetchone()
        emails_total = int(emails_total_row[0]) if emails_total_row else 0

        found_email = len(candidates_by_email)
        found_no_email = len(candidates_no_email)

        return {
            "ok": True,
            "company_id": company_id,
            "company_name": company_name,
            "domain": dom,
            "found_candidates": found_email + found_no_email,
            "found_candidates_email": found_email,
            "found_candidates_no_email": found_no_email,
            "inserted_people": inserted_people,
            "updated_people": updated_people,
            "inserted_emails": inserted_emails,
            "updated_emails": updated_emails,
            "emails_total": emails_total,
            "blocked_candidates_role": int(blocked_role),
            "blocked_candidates_personish": int(blocked_personish),
        }
    except Exception as exc:
        log.exception(
            "extract_candidates_for_company failed",
            extra={"company_id": company_id, "exc": str(exc)},
        )
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "company_id": company_id}


# ---------------------------
# R10 wiring: thin crawl task
# ---------------------------


def crawl_approved_domains(db: str | None = None, limit: int | None = None) -> int:
    """
    R10: Read approved official domains written by R08 and run the crawler for each.
    Returns the number of domains crawled.

    This is intentionally thin and calls the same CLI used in acceptance:
    `scripts/crawl_domain.py <domain> --db <db>`

    Args:
        db: Path to database (legacy parameter, ignored for Postgres).
        limit: Optional cap on how many domains to process (useful for smoke runs).
    """
    con = get_conn()
    try:
        cur = con.execute(
            """
            SELECT DISTINCT TRIM(official_domain)
            FROM companies
            WHERE official_domain IS NOT NULL
              AND TRIM(official_domain) <> ''
            ORDER BY 1
            """
        )
        domains = [row[0] for row in cur.fetchall()]
    finally:
        try:
            con.close()
        except Exception:
            pass

    if limit is not None:
        domains = domains[:limit]

    if not domains:
        log.info("crawl_approved_domains: no official domains found in companies table")
        return 0

    script = Path(__file__).resolve().parents[2] / "scripts" / "crawl_domain.py"
    if not script.exists():
        raise FileNotFoundError(f"crawl_domain.py not found at {script}")

    # Get db_path for CLI compatibility (falls back to DATABASE_PATH or default)
    db_path = db or os.getenv("DATABASE_PATH", "data/dev.db")

    count = 0
    for domain in domains:
        cmd = [sys.executable, str(script), domain, "--db", str(db_path)]
        log.info(
            "R10 crawl start", extra={"domain": domain, "db": str(db_path), "cmd": " ".join(cmd)}
        )
        subprocess.run(cmd, check=True)
        count += 1
        log.info("R10 crawl done", extra={"domain": domain})

    log.info("R10 crawled domains total", extra={"count": count})
    return count


# -----------------------------------------------------------
# R13 helper task: upsert a person with lightweight normalization
# -----------------------------------------------------------


def upsert_person_task(row: dict) -> dict:
    """
    R13: Queueable task to upsert a person/company from arbitrary *raw* input.

    - Runs normalize_row(row) (name/title/company normalization; preserves source_url)
    - Persists via src.ingest.persist.upsert_row()
    - Returns a small echo payload (without IDs; DB layer remains the source of truth)

    Use this from other stages that discover people (e.g., extractors) so that all
    entries pass through the same normalization guardrails as CLI ingest.
    """
    try:
        normalized, errors = normalize_row(row or {})
        persist_upsert_row(normalized)
        log.info(
            "R13 upsert_person_task persisted",
            extra={
                "company": normalized.get("company"),
                "domain": normalized.get("domain"),
                "first_name": normalized.get("first_name"),
                "last_name": normalized.get("last_name"),
                "title_norm": normalized.get("title_norm"),
                "company_norm_key": normalized.get("company_norm_key"),
                "err_count": len(errors),
            },
        )
        return {
            "ok": True,
            "errors": errors,
            "company": normalized.get("company"),
            "domain": normalized.get("domain"),
            "first_name": normalized.get("first_name"),
            "last_name": normalized.get("last_name"),
            "title_norm": normalized.get("title_norm"),
            "company_norm_key": normalized.get("company_norm_key"),
        }
    except Exception as e:
        log.exception("R13 upsert_person_task failed", extra={"exc": str(e)})
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ---------------------------------------------------------------------------
# Control plane orchestration (Runs API fan-out)
# ---------------------------------------------------------------------------


def _utc_now_iso_z() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


class _UserSuppliedDecision:
    """Minimal Decision-like object for write_domain_resolution()."""

    def __init__(self, chosen: str):
        self.chosen = chosen
        self.method = "user_supplied"
        self.confidence = 100
        self.reason = "pipeline_start"
        self.resolver_version = "pipeline_start"


def _table_cols(con: Any, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
        return {str(r[1]) for r in rows if len(r) > 1 and r[1]}
    except Exception:
        return set()


def _update_run_row(
    con: Any,
    *,
    tenant_id: str,
    run_id: str,
    status: str | None = None,
    progress: dict[str, Any] | None = None,
    error: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    """Best-effort runs table update (works across schema drift)."""
    if not _has_table(con, "runs"):
        return

    cols = _table_cols(con, "runs")
    if not cols:
        return

    now = _utc_now_iso_z()

    sets: list[str] = []
    vals: list[Any] = []

    def set_if(col: str, val: Any) -> None:
        if col in cols:
            sets.append(f"{col} = ?")
            vals.append(val)

    if status is not None:
        set_if("status", status)
    if error is not None:
        set_if("error", error)
    if started_at is not None:
        set_if("started_at", started_at)
    if finished_at is not None:
        set_if("finished_at", finished_at)
    if progress is not None and "progress_json" in cols:
        set_if("progress_json", json.dumps(progress, separators=(",", ":")))

    set_if("updated_at", now)

    if not sets:
        return

    if "tenant_id" in cols:
        vals.extend([tenant_id, run_id])
        con.execute(
            f"UPDATE runs SET {', '.join(sets)} WHERE tenant_id = ? AND id = ?", tuple(vals)
        )
    else:
        vals.append(run_id)
        con.execute(f"UPDATE runs SET {', '.join(sets)} WHERE id = ?", tuple(vals))

    try:
        con.commit()
    except Exception:
        pass


def _ensure_company_for_domain(con: Any, *, tenant_id: str, domain: str) -> tuple[int, str]:  # noqa: C901
    """Ensure a companies row exists for the tenant/domain and return (company_id, company_name)."""
    dom = (domain or "").strip().lower()
    if not dom:
        raise ValueError("domain is empty")

    if not _has_table(con, "companies"):
        raise RuntimeError("companies table not found")

    cols = _table_cols(con, "companies")

    # Try to find an existing row for this domain.
    where: list[str] = []
    params: list[Any] = []

    if "tenant_id" in cols:
        where.append("tenant_id = ?")
        params.append(tenant_id)

    dom_conds: list[str] = []
    for c in ("official_domain", "user_supplied_domain", "domain"):
        if c in cols:
            dom_conds.append(f"lower({c}) = ?")
            params.append(dom)

    if dom_conds:
        where.append("(" + " OR ".join(dom_conds) + ")")

    sql_sel = "SELECT id, name FROM companies"
    if where:
        sql_sel += " WHERE " + " AND ".join(where)
    sql_sel += " ORDER BY id LIMIT 1"

    try:
        row = con.execute(sql_sel, tuple(params)).fetchone()
    except Exception:
        row = None

    if row:
        return int(row[0]), str(row[1] or dom)

    # Insert new.
    insert_cols: list[str] = []
    insert_vals: list[Any] = []

    def add(c: str, v: Any) -> None:
        if c in cols:
            insert_cols.append(c)
            insert_vals.append(v)

    add("tenant_id", tenant_id)
    add("name", dom)
    add("domain", dom)
    add("user_supplied_domain", dom)

    if not insert_cols:
        raise RuntimeError("companies table has no insertable columns")

    placeholders = ", ".join(["?"] * len(insert_cols))
    cols_sql = ", ".join(insert_cols)

    is_pg = bool(getattr(con, "is_postgres", False))

    if is_pg:
        cur = con.execute(
            f"INSERT INTO companies ({cols_sql}) VALUES ({placeholders}) RETURNING id",
            tuple(insert_vals),
        )
        new_row = cur.fetchone()
        company_id = int(new_row[0]) if new_row else 0
    else:
        cur = con.execute(
            f"INSERT INTO companies ({cols_sql}) VALUES ({placeholders})", tuple(insert_vals)
        )
        # Best-effort: sqlite3 lastrowid lives on the underlying cursor
        company_id = int(getattr(getattr(cur, "_cursor", None), "lastrowid", 0) or 0)

    try:
        con.commit()
    except Exception:
        pass

    if not company_id:
        # Fallback: re-select
        try:
            row2 = con.execute(sql_sel, tuple(params)).fetchone()
            if row2:
                company_id = int(row2[0])
        except Exception:
            pass

    if not company_id:
        raise RuntimeError("failed to insert company")

    return company_id, dom


def pipeline_start(*, run_id: str, tenant_id: str) -> dict[str, Any]:
    """Orchestrator entrypoint for Runs API.

    Responsibilities (MVP):
      - Mark the run as running.
      - Ensure a companies row exists per input domain (tenant-scoped when supported).
      - Persist a 'user_supplied' domain resolution so downstream stages can treat
        the domain as canonical.
      - Enqueue autodiscovery jobs for each company on the discovery queue.

    This function is intentionally conservative: it does not attempt to compute a
    full DAG across crawl/extract/generate/smtp/finalize yet.
    """

    con = get_conn()
    now = _utc_now_iso_z()

    # Load run payload
    try:
        if not _has_table(con, "runs"):
            raise RuntimeError("runs table not found")

        cols = _table_cols(con, "runs")
        if "tenant_id" in cols:
            row = con.execute(
                "SELECT domains_json, options_json FROM runs WHERE tenant_id = ? AND id = ?",
                (tenant_id, run_id),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT domains_json, options_json FROM runs WHERE id = ?", (run_id,)
            ).fetchone()

        if not row:
            raise ValueError("run_not_found")

        domains_json = row[0] or "[]"
        options_json = row[1] or "{}"

        domains = json.loads(domains_json) if isinstance(domains_json, str) else domains_json
        if not isinstance(domains, list):
            raise ValueError("runs.domains_json is not a list")

        options = json.loads(options_json) if isinstance(options_json, str) else options_json
        if not isinstance(options, dict):
            options = {}

        discovery_queue = str(
            options.get("discovery_queue") or os.getenv("DISCOVERY_QUEUE_NAME", "crawl")
        )
        job_timeout = int(
            options.get("discovery_job_timeout") or os.getenv("DISCOVERY_JOB_TIMEOUT", "1800")
        )

        progress: dict[str, Any] = {
            "phase": "start",
            "queued_at": now,
            "discovery_queue": discovery_queue,
            "domains": [],
        }

        _update_run_row(
            con,
            tenant_id=tenant_id,
            run_id=run_id,
            status="running",
            started_at=now,
            progress=progress,
        )

        # Enqueue per-domain autodiscovery
        enqueued: list[dict[str, Any]] = []

        try:
            q = Queue(name=discovery_queue, connection=get_redis())
        except Exception as exc:
            msg = f"failed to init discovery queue {discovery_queue!r}: {exc}"
            raise RuntimeError(msg) from exc
        for d in domains:
            dom = str(d or "").strip().lower()
            if not dom:
                continue

            company_id, company_name = _ensure_company_for_domain(
                con, tenant_id=tenant_id, domain=dom
            )

            # Persist a canonical resolution from the user input so autodiscovery uses
            # official_domain.
            try:
                write_domain_resolution(
                    con,
                    company_id=company_id,
                    company_name=company_name,
                    decision=_UserSuppliedDecision(dom),
                    user_hint=dom,
                    tenant_id=tenant_id,
                )
                con.commit()
            except Exception:
                # Non-fatal; company row still exists.
                log.debug(
                    "pipeline_start: write_domain_resolution failed",
                    exc_info=True,
                    extra={"domain": dom},
                )

            job = q.enqueue(autodiscover_company, company_id=company_id, job_timeout=job_timeout)

            enqueued.append(
                {"domain": dom, "company_id": company_id, "job_id": getattr(job, "id", None)}
            )

            progress["domains"].append(
                {"domain": dom, "company_id": company_id, "state": "enqueued"}
            )
            _update_run_row(con, tenant_id=tenant_id, run_id=run_id, progress=progress)

        progress["phase"] = "fanout_enqueued"
        progress["enqueued"] = len(enqueued)
        progress["enqueued_items"] = enqueued
        _update_run_row(con, tenant_id=tenant_id, run_id=run_id, progress=progress)

        return {"ok": True, "run_id": run_id, "tenant_id": tenant_id, "enqueued": enqueued}

    except Exception as exc:
        try:
            _update_run_row(
                con,
                tenant_id=tenant_id,
                run_id=run_id,
                status="error",
                error=f"{type(exc).__name__}: {exc}",
                finished_at=_utc_now_iso_z(),
            )
        except Exception:
            pass
        raise

    finally:
        try:
            con.close()
        except Exception:
            pass


def handle_task(envelope: Any) -> Any:
    """
    Backwards-compatible RQ entrypoint for R06-style task envelopes.

    Expected envelope shape (minimum):
        {"task": "<task_name>", "payload": {...}}

    The "task" field selects which function in this module to call. The
    "payload" dict is expanded as keyword arguments to that function.

    This is intentionally thin so tests can call it directly and RQ workers
    can import it by dotted path "src.queueing.tasks.handle_task".
    """
    if envelope is None:
        raise ValueError("handle_task expected an envelope, got None")

    if isinstance(envelope, str):
        try:
            envelope_obj = json.loads(envelope)
        except Exception as exc:  # pragma: no cover
            raise ValueError(f"handle_task could not decode JSON envelope: {exc}") from exc
    else:
        envelope_obj = envelope

    if not isinstance(envelope_obj, dict):
        raise TypeError("handle_task expects a dict-like envelope with 'task' and 'payload' keys")

    task_name = (
        envelope_obj.get("task")
        or envelope_obj.get("name")
        or envelope_obj.get("kind")
        or envelope_obj.get("type")
    )
    if not task_name or not isinstance(task_name, str):
        raise ValueError("handle_task envelope is missing a string 'task' field")

    payload = envelope_obj.get("payload") or {}
    if not isinstance(payload, dict):
        raise TypeError("handle_task envelope.payload must be a dict")

    base_task_resolve_mx = (
        task_resolve_mx.__wrapped__ if hasattr(task_resolve_mx, "__wrapped__") else task_resolve_mx
    )
    base_task_check_catchall = (
        task_check_catchall.__wrapped__
        if hasattr(task_check_catchall, "__wrapped__")
        else task_check_catchall
    )
    base_task_probe_email = (
        task_probe_email.__wrapped__
        if hasattr(task_probe_email, "__wrapped__")
        else task_probe_email
    )

    task_map: dict[str, Any] = {
        "resolve_company_domain": resolve_company_domain,
        "verify_email": verify_email_task,
        "verify_email_task": verify_email_task,
        "task_resolve_mx": base_task_resolve_mx,
        "resolve_mx": base_task_resolve_mx,
        "task_check_catchall": base_task_check_catchall,
        "check_catchall": base_task_check_catchall,
        "task_probe_email": base_task_probe_email,
        "probe_email": base_task_probe_email,
        "task_send_test_email": task_send_test_email,
        "send_test_email": task_send_test_email,
        "task_generate_emails": task_generate_emails,
        "generate_emails": task_generate_emails,
        "upsert_person_task": upsert_person_task,
        "crawl_approved_domains": crawl_approved_domains,
        "crawl_company_site": crawl_company_site,
        "extract_candidates_for_company": extract_candidates_for_company,
        "autodiscover_company": autodiscover_company,
    }

    func = task_map.get(task_name)
    if func is None:
        raise ValueError(f"Unknown task '{task_name}'")

    return func(**payload)
