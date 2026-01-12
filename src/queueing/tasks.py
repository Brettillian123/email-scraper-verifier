# src/queueing/tasks.py
from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
import subprocess
import sys
import time
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

# O27 AI extractor (with optional global enable flag)
try:
    from src.extract.ai_candidates import (
        AI_PEOPLE_ENABLED,
        extract_ai_candidates,
    )
except ImportError:
    from src.extract.ai_candidates import extract_ai_candidates  # type: ignore

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


# ---------------------------
# Queue naming / operating model
# ---------------------------


def _autodiscovery_queue_name() -> str:
    """
    Queue used for local-only, HTTP-based work (crawl/extract/generate).

    Target operating model:
      - Local machine: crawl/extract/generate/ingest (HTTP-based).
      - VPS: MX/catch-all/SMTP RCPT probes (TCP/25) + writing verification results.
    """
    return (
        os.getenv("RQ_QUEUE_AUTODISCOVERY")
        or os.getenv("AUTODISCOVERY_QUEUE")
        or "autodiscovery"
    )


def _conn() -> sqlite3.Connection:
    """
    Lightweight SQLite connection helper for tasks that need direct DB access.

    Delegate to src.db.get_conn() so we always use the same DB_URL / schema
    as the rest of the application (including domain_resolutions, R17, etc.).
    """
    return get_conn()


def _enqueue_company_task(task_name: str, company_id: int) -> None:
    """
    Best-effort helper to enqueue a company-scoped task on the local-only queue.

    IMPORTANT:
      - This MUST NOT enqueue to the SMTP 'verify' queue; that queue should be
        reserved for port-25 verification work consumed by the VPS worker.
    """
    try:
        q = Queue(name=_autodiscovery_queue_name(), connection=get_redis())
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
    with _conn() as con:
        write_domain_resolution(con, company_id, company_name, dec, hint)

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

    if e == "ok5@crestwellpartners.com":
        return ("valid", "selftest-ok")
    if e == "willfail@crestwellpartners.com":
        raise TemporarySMTPError("selftest temporary failure")
    if e.startswith("permfail@"):
        raise PermanentSMTPError("selftest permanent failure")

    mode = os.getenv("TEST_PROBE")
    if mode == "success":
        return ("valid", "ok_test")
    if mode == "temp":
        raise TemporarySMTPError("test_temp_error")
    if mode == "perm":
        raise PermanentSMTPError("test_perm_550_user_unknown")
    if mode == "crash":
        raise RuntimeError("test_unexpected_exception")

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
    job = get_current_job()

    env_raise_perm = _bool_env("SELFTEST_RAISE_PERM")
    env_raise_temp = _bool_env("SELFTEST_RAISE_TEMP")

    raise_perm_env = (env_raise_perm == "1") if env_raise_perm is not None else False
    raise_temp_env = (env_raise_temp == "1") if env_raise_temp is not None else False

    domain = email.split("@")[-1].lower()
    mx_host, _pref = lookup_mx(domain)
    mx_key = MX_SEM.format(mx=mx_host)

    start = time.perf_counter()
    attempt = 1

    status: str = "unknown"
    reason: str | None = "unstarted"

    got_global = False
    got_mx = False

    on_selftest_queue = bool(getattr(job, "origin", "") == "verify_selftest")
    willfail_addr = email.lower().startswith("willfail@")
    force_selftest_perm = on_selftest_queue and willfail_addr and env_raise_perm != "0"

    try:
        if force_selftest_perm:
            raise PermanentSMTPError("R06 selftest: simulated 550 user unknown")

        if raise_perm_env and willfail_addr:
            raise PermanentSMTPError("R06 selftest (env): simulated 550 user unknown")

        if raise_temp_env and willfail_addr:
            raise TemporarySMTPError("R06 selftest (env): simulated temp failure")

        got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
        if not got_global:
            raise TemporarySMTPError("global concurrency cap reached")

        got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
        if not got_mx:
            raise TemporarySMTPError("per-MX concurrency cap reached")

        if job:
            attempt = int(job.meta.get("attempt", 0)) + 1
            job.meta["attempt"] = attempt
            job.save_meta()

        sec = int(time.time())
        key_global_rps = RPS_KEY_GLOBAL.format(sec=sec)
        key_mx_rps = RPS_KEY_MX.format(mx=mx_host, sec=sec)

        if _cfg.rate.global_rps and not can_consume_rps(
            redis,
            key_global_rps,
            int(_cfg.rate.global_rps),
        ):
            raise TemporarySMTPError("global RPS throttle")

        if _cfg.rate.per_mx_rps_default and not can_consume_rps(
            redis,
            key_mx_rps,
            int(_cfg.rate.per_mx_rps_default),
        ):
            raise TemporarySMTPError("MX RPS throttle")

        verify_status, probe_reason = smtp_probe(email, _cfg.smtp_identity.helo_domain)
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
            raise

    except TemporarySMTPError as e:
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
            log.exception(
                "upsert_verification_result failed",
                extra={
                    "email": email,
                    "status": status,
                    "reason": reason,
                    "mx": mx_host,
                },
            )

        redis = get_redis()
        if redis:
            try:
                release(redis, MX_SEM.format(mx=mx_host))
            except Exception:
                pass
            try:
                release(redis, GLOBAL_SEM)
            except Exception:
                pass


# -------------------------------#
# R15: MX resolution queue task
# -------------------------------


def _task_resolve_mx(company_id: int, domain: str, force: bool = False) -> dict:
    """
    Core R15 implementation: Resolve MX for a domain and persist to domain_resolutions.

    Behavior:
      - If Redis is available, enforce R06 concurrency/RPS caps (global + per-domain pre-MX).
      - If Redis is NOT available (e.g., direct call / smoke), gracefully bypass throttling
        and run inline (no Redis dependency).
    """
    import time as _time

    dom = (domain or "").strip().lower()
    if not dom:
        return {"ok": False, "error": "empty_domain", "company_id": company_id, "domain": dom}

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
    sem_key = MX_SEM.format(mx=dom)

    try:
        if redis_ok:
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
        if redis_ok:
            if got_key:
                release(redis, sem_key)
            if got_global:
                release(redis, GLOBAL_SEM)


@job("mx", timeout=10)
def task_resolve_mx(company_id: int, domain: str, force: bool = False) -> dict:
    """
    R15 queue task: thin RQ wrapper around _task_resolve_mx.

    Manual calls may use task_resolve_mx.__wrapped__(...) to run inline.
    """
    return _task_resolve_mx(company_id, domain, force=force)


# Ensure __wrapped__ exists even when rq.job decorator doesn't set it.
task_resolve_mx.__wrapped__ = _task_resolve_mx  # type: ignore[attr-defined]


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
        res = _resolve_mx(company_id=0, domain=domain, force=force, db_path=db_path)
        mxh = getattr(res, "lowest_mx", None) or domain
        beh = getattr(res, "behavior", None) or getattr(res, "mx_behavior", None)
        return (mxh, beh)
    except Exception:
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
    got_global = False
    got_mx = False

    if not redis_ok or redis is None:
        return got_global, got_mx, None

    got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
    if not got_global:
        return got_global, got_mx, _throttle_error_result(
            error="global concurrency cap reached",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )

    got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
    if not got_mx:
        return got_global, got_mx, _throttle_error_result(
            error="per-MX concurrency cap reached",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )

    sec = int(time.time())
    key_global_rps = RPS_KEY_GLOBAL.format(sec=sec)
    key_mx_rps = RPS_KEY_MX.format(mx=mx_host, sec=sec)

    if _cfg.rate.global_rps and not can_consume_rps(
        redis,
        key_global_rps,
        int(_cfg.rate.global_rps),
    ):
        return got_global, got_mx, _throttle_error_result(
            error="global RPS throttle",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )

    if _cfg.rate.per_mx_rps_default and not can_consume_rps(
        redis,
        key_mx_rps,
        int(_cfg.rate.per_mx_rps_default),
    ):
        return got_global, got_mx, _throttle_error_result(
            error="MX RPS throttle",
            mx_host=mx_host,
            dom=dom,
            email_id=email_id,
            email_str=email_str,
            start=start,
        )

    return got_global, got_mx, None


def _maybe_run_fallback(email_str: str, category: str) -> tuple[str | None, Any | None]:
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
    dt = datetime.utcnow().replace(microsecond=0)
    return dt.isoformat() + "Z"


def _load_catchall_status_for_domain(db_path: str, domain: str) -> str | None:
    dom = (domain or "").strip().lower()
    if not dom:
        return None
    try:
        con = get_conn()
        con.row_factory = sqlite3.Row
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
            val = row["catch_all_status"]
            return str(val) if val is not None else None
        finally:
            con.close()
    except Exception:
        log.debug(
            "R18: failed to load catch_all_status for domain",
            exc_info=True,
            extra={"domain": dom},
        )
        return None


def _parse_rcpt_code(code: Any) -> int | None:
    if isinstance(code, int):
        return code
    try:
        return int(code) if code is not None else None
    except Exception:
        return None


def _probe_hostile_from_behavior(behavior: Any) -> bool:
    if behavior is None:
        return False

    for attr in ("probing_hostile", "probe_hostile", "probe_hostile_mx"):
        if hasattr(behavior, attr):
            try:
                return bool(getattr(behavior, attr))
            except Exception:
                pass

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
    """
    dom = (domain or "").strip().lower()
    try:
        cat_norm = (category or "").strip().lower() or None
        rcpt_code = _parse_rcpt_code(code)

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

        con = get_conn()
        try:
            cur = con.execute(
                """
                INSERT INTO verification_results (
                  email_id,
                  email,
                  domain,
                  mx_host,
                  status,
                  reason,
                  checked_at,
                  fallback_status,
                  fallback_raw,
                  fallback_checked_at,
                  verify_status,
                  verify_reason,
                  verified_mx,
                  verified_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(email_id) if email_id else None,
                    email,
                    dom,
                    mx_host,
                    raw_status,
                    raw_reason,
                    ts_iso,
                    fallback_status,
                    fallback_raw_text,
                    ts_iso if fallback_status is not None else None,
                    verify_status,
                    verify_reason,
                    mx_host,
                    ts_iso,
                ),
            )
            verification_result_id = int(cur.lastrowid)
            con.commit()
        finally:
            con.close()
    except Exception:
        log.exception(
            "R18: failed to persist verification_results row",
            extra={"email_id": email_id, "email": email, "domain": dom, "mx_host": mx_host},
        )
        return None, None, None, None, None

    return verify_status, verify_reason, mx_host, ts_iso, verification_result_id


def _init_redis_for_probe() -> tuple[Redis | None, bool]:
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
    """
    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    sent_at = _utcnow_iso()

    try:
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


def _enqueue_test_send_email(verification_result_id: int, email: str, token: str) -> None:
    try:
        q = Queue(name="test_send", connection=get_redis())
        q.enqueue(
            task_send_test_email,
            verification_result_id,
            email,
            token,
            job_timeout=30,
            retry=None,
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
    if verification_result_id is None or verify_status is None:
        return

    probe_hostile = _probe_hostile_from_behavior(behavior_hint)
    if not probe_hostile:
        return

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
            db_path=db_path,
            verification_result_id=verification_result_id,
            email=email,
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


def _task_probe_email(email_id: int, email: str, domain: str, force: bool = False) -> dict:
    """
    Core implementation for R16 probe task.

    Fix:
      - Provide a stable __wrapped__ target for inline/manual calling.
    """
    normalized = _normalize_probe_inputs(email_id, email, domain)
    if isinstance(normalized, dict):
        return normalized
    email_str, dom = normalized

    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    start = time.perf_counter()
    mx_host, behavior_hint = _mx_info(dom, force=bool(force), db_path=db_path)

    redis_obj, redis_ok = _init_redis_for_probe()

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

        connect_timeout = min(
            float(SMTP_CONNECT_TIMEOUT), float(os.getenv("SMTP_CONNECT_TIMEOUT_CLAMP", "6.0"))
        )
        command_timeout = min(
            float(SMTP_COMMAND_TIMEOUT), float(os.getenv("SMTP_COMMAND_TIMEOUT_CLAMP", "10.0"))
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
        elapsed_res = result.get("elapsed_ms")
        error_val = result.get("error")

        fallback_status, fallback_raw = _maybe_run_fallback(email_str, category)

        base: dict[str, Any] = {
            "ok": bool(result.get("ok", True)),
            "category": category,
            "code": code,
            "mx_host": result.get("mx_host", mx_host),
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int(elapsed_res or int((time.perf_counter() - start) * 1000)),
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
            mx_host=base["mx_host"],
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

            try:
                _maybe_escalate_to_test_send(
                    db_path=db_path,
                    email_id=int(email_id),
                    email=email_str,
                    domain=dom,
                    mx_host=base["mx_host"],
                    category=category,
                    code=code,
                    fallback_status=fallback_status,
                    behavior_hint=behavior_hint,
                    verify_status=v_status,
                    verify_reason=v_reason,
                    verified_at=v_at,
                    verification_result_id=vr_id,
                )
            except Exception:
                log.exception(
                    "O26 test_send escalation failed",
                    extra={
                        "email_id": int(email_id),
                        "email": email_str,
                        "domain": dom,
                        "mx_host": base["mx_host"],
                    },
                )

        return base

    except Exception as e:
        error_payload: dict[str, Any] = {
            "ok": False,
            "category": "unknown",
            "code": None,
            "mx_host": mx_host,
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
            "error": f"{type(e).__name__}: {e}",
        }

        v_status, v_reason, v_mx, v_at, _vr_id = _persist_probe_result_r18(
            db_path=db_path,
            email_id=int(email_id),
            email=email_str,
            domain=dom,
            mx_host=mx_host,
            category=error_payload["category"],
            code=error_payload["code"],
            error=error_payload["error"],
            fallback_status=None,
            fallback_raw=None,
            tcp25_ok=tcp25_ok,
        )
        if v_status is not None:
            error_payload["verify_status"] = v_status
            error_payload["verify_reason"] = v_reason
            error_payload["verified_mx"] = v_mx
            error_payload["verified_at"] = v_at

        return error_payload
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
    """
    R16 queue task: thin RQ wrapper around _task_probe_email.

    Manual calls may use task_probe_email.__wrapped__(...) to run inline.
    """
    return _task_probe_email(email_id, email, domain, force=force)


# Fix for your traceback: ensure __wrapped__ always exists for inline/manual calling.
task_probe_email.__wrapped__ = _task_probe_email  # type: ignore[attr-defined]


# ---------------------------------------------
# O01 domain pattern inference/cache
# ---------------------------------------------


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    try:
        cur = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (name,),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _examples_for_domain(con: sqlite3.Connection, domain: str) -> list[tuple[str, str, str]]:
    examples: list[tuple[str, str, str]] = []
    dom = (domain or "").strip().lower()
    if not dom:
        return examples

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


def _load_cached_pattern(con: sqlite3.Connection, domain: str) -> str | None:
    if not _has_table(con, "domain_patterns"):
        return None
    try:
        row = con.execute(
            "SELECT pattern FROM domain_patterns WHERE domain = ?",
            (domain,),
        ).fetchone()
        pat = row[0] if row and row[0] else None
        if pat in CANON_PATTERNS:
            return pat
    except Exception:
        pass
    return None


def _save_inferred_pattern(
    con: sqlite3.Connection,
    domain: str,
    pattern: str,
    confidence: float,
    samples: int,
) -> None:
    if not _has_table(con, "domain_patterns"):
        return
    try:
        con.execute(
            """
            INSERT INTO domain_patterns (domain, pattern, confidence, samples)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
              pattern=excluded.pattern,
              confidence=excluded.confidence,
              samples=excluded.samples,
              inferred_at=datetime('now')
            """,
            (domain, pattern, float(confidence), int(samples)),
        )
    except Exception:
        log.exception(
            "failed to upsert domain_patterns",
            extra={"domain": domain, "pattern": pattern},
        )


def _company_id_for_person(con: sqlite3.Connection, person_id: int) -> int | None:
    try:
        row = con.execute(
            "SELECT company_id FROM people WHERE id = ?",
            (person_id,),
        ).fetchone()
        if not row:
            return None
        val = row[0]
        return int(val) if val is not None else None
    except Exception:
        log.debug(
            "failed to load company_id for person",
            exc_info=True,
            extra={"person_id": person_id},
        )
        return None


def _load_company_email_pattern(con: sqlite3.Connection, company_id: int | None) -> str | None:
    if not company_id:
        return None
    try:
        if not _has_table(con, "companies"):
            return None
        row = con.execute(
            "SELECT attrs FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()
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


def _email_row_id(con: sqlite3.Connection, email: str) -> int | None:
    email = (email or "").strip().lower()
    if not email:
        return None
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(emails)").fetchall()}
        if "id" in cols:
            row = con.execute("SELECT id FROM emails WHERE email = ?", (email,)).fetchone()
            return int(row[0]) if row else None
        row = con.execute("SELECT rowid FROM emails WHERE email = ?", (email,)).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None


def _enqueue_r16_probe(email_id: int | None, email: str, domain: str) -> None:
    """
    Enqueue the R16 probe task explicitly. Best-effort (swallows Redis errors).

    NOTE: This targets the 'verify' queue (TCP/25 work) intended for the VPS worker.
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
    con = get_conn()
    dom = (domain or "").lower().strip()
    if not dom:
        return {"count": 0, "only_pattern": None, "domain": dom, "person_id": person_id}

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
        return {"count": 0, "only_pattern": None, "domain": dom, "person_id": person_id}

    cached_pattern = _load_cached_pattern(con, dom)
    examples = _examples_for_domain(con, dom)

    domain_pattern: str | None = None
    inf_conf = 0.0
    inf_samples = 0

    if cached_pattern:
        domain_pattern = cached_pattern
    else:
        inf = infer_domain_pattern(examples)
        domain_pattern = inf.pattern
        inf_conf = float(inf.confidence)
        inf_samples = int(inf.samples)
        if domain_pattern:
            _save_inferred_pattern(con, dom, domain_pattern, inf_conf, inf_samples)

    effective_pattern = company_pattern or domain_pattern

    canonical_emails: list[str] = (
        generate_candidate_emails_for_person(
            first_name=nf,
            last_name=nl,
            domain=dom,
            company_pattern=effective_pattern,
        )
        or []
    )

    legacy_emails: list[str] = []
    try:
        from src.generate.permutations import PATTERNS as LEGACY_TEMPLATES  # type: ignore
        from src.generate.permutations import normalize_name_parts as _legacy_norm  # type: ignore

        first_n, last_n, f_initial, l_initial = _legacy_norm(nf, nl)
        ctx = {"first": first_n, "last": last_n, "f": f_initial, "l": l_initial}

        for pattern in LEGACY_TEMPLATES:
            try:
                local = pattern.format(**ctx)
            except Exception:
                continue
            if not local:
                continue
            legacy_emails.append(f"{local}@{dom}")
    except Exception:
        legacy_emails = []

    seen: set[str] = set()
    ordered_candidates: list[str] = []
    for email in canonical_emails + legacy_emails:
        if not email or "@" not in email:
            continue
        local = email.split("@", 1)[0].lower()
        if not local or local in ROLE_ALIASES:
            continue
        if email in seen:
            continue
        seen.add(email)
        ordered_candidates.append(email)

    max_probes = int(os.getenv("MAX_PROBES_PER_PERSON", "6"))
    inserted = 0
    enqueued = 0

    for e in ordered_candidates:
        upsert_generated_email(con, person_id, e, dom, source_note="r12")
        inserted += 1

        if enqueued < max_probes:
            try:
                email_id = _email_row_id(con, e)
                _enqueue_r16_probe(email_id, e, dom)
                enqueued += 1
            except Exception as ee:
                log.debug("R16 enqueue failed (best-effort): %s", ee)

    con.commit()

    log.info(
        "R12 generated emails",
        extra={
            "person_id": person_id,
            "company_id": company_id,
            "domain": dom,
            "first": first,
            "last": last,
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


def _sources_has_company_id(con: sqlite3.Connection) -> bool:
    try:
        rows = con.execute("PRAGMA table_info(sources)").fetchall()
    except sqlite3.OperationalError:
        return False
    for r in rows:
        if len(r) > 1 and r[1] == "company_id":
            return True
    return False


def crawl_company_site(company_id: int) -> dict:
    con = _conn()
    pages: list[Any] = []
    try:
        cur = con.execute(
            "SELECT name, official_domain, domain FROM companies WHERE id = ?",
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"ok": False, "error": "company_not_found", "company_id": company_id}

        company_name = row[0]
        official = (row[1] or "").strip() if row[1] is not None else ""
        fallback = (row[2] or "").strip() if row[2] is not None else ""
        dom = official or fallback
        if not dom:
            return {
                "ok": False,
                "error": "no_domain_for_company",
                "company_id": company_id,
                "company_name": company_name,
            }

        pages = crawl_domain(dom)
        if pages:
            try:
                save_pages(con, pages, company_id=company_id)  # type: ignore[call-arg]
            except TypeError:
                save_pages(con, pages)  # type: ignore[call-arg]
            con.commit()

        result: dict[str, Any] = {
            "ok": True,
            "company_id": company_id,
            "company_name": company_name,
            "domain": dom,
            "page_count": len(pages),
        }

        _enqueue_company_task("extract_candidates_for_company", company_id=company_id)
        return result

    except Exception as exc:
        log.exception("crawl_company_site failed", extra={"company_id": company_id, "exc": str(exc)})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "company_id": company_id}


def _get_company_attrs(con: sqlite3.Connection, company_id: int) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    try:
        if not _has_table(con, "companies"):
            return attrs
        row = con.execute(
            "SELECT attrs FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()
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
        log.debug("O27: failed to load companies.attrs", exc_info=True, extra={"company_id": company_id})
    return attrs


def _set_company_attrs(con: sqlite3.Connection, company_id: int, attrs: dict[str, Any]) -> None:
    try:
        raw = json.dumps(attrs, separators=(",", ":"), sort_keys=True)
        con.execute("UPDATE companies SET attrs = ? WHERE id = ?", (raw, int(company_id)))
    except Exception:
        log.debug("O27: failed to update companies.attrs", exc_info=True, extra={"company_id": company_id})


def _should_run_ai_for_company(con: sqlite3.Connection, company_id: int) -> bool:
    attrs = _get_company_attrs(con, company_id)
    return not bool(attrs.get("ai_people_extracted"))


def _mark_ai_people_extracted(con: sqlite3.Connection, company_id: int) -> None:
    try:
        attrs = _get_company_attrs(con, company_id)
        if attrs.get("ai_people_extracted"):
            return
        attrs["ai_people_extracted"] = True
        _set_company_attrs(con, company_id, attrs)
    except Exception:
        log.debug("O27: failed to mark ai_people_extracted flag", exc_info=True, extra={"company_id": company_id})


def _load_company_name_and_domain(con: sqlite3.Connection, company_id: int) -> tuple[str, str, str] | None:
    row = con.execute(
        "SELECT name, official_domain, domain FROM companies WHERE id = ?",
        (company_id,),
    ).fetchone()
    if not row:
        return None
    company_name = str(row[0])
    official = (row[1] or "").strip() if row[1] is not None else ""
    fallback = (row[2] or "").strip() if row[2] is not None else ""
    dom = (official or fallback).lower()
    return company_name, dom, fallback


def _load_company_sources(con: sqlite3.Connection, company_id: int, dom: str) -> list[tuple[str, bytes | str]]:
    has_company_id = _sources_has_company_id(con)
    if has_company_id:
        cur_src = con.execute(
            "SELECT source_url, html FROM sources WHERE company_id = ?",
            (company_id,),
        )
        return [(r[0], r[1]) for r in cur_src.fetchall()]

    cur_src = con.execute("SELECT source_url, html FROM sources")
    raw_rows = cur_src.fetchall()
    if not dom:
        return [(r[0], r[1]) for r in raw_rows]

    pages_rows: list[tuple[str, bytes | str]] = []
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
    raw_candidates: list[ExtractCandidate] = []
    for source_url, html_blob in pages_rows:
        if isinstance(html_blob, (bytes, bytearray)):
            html_str = html_blob.decode("utf-8", "ignore")
        else:
            html_str = str(html_blob or "")

        for cand in extract_html_candidates(html_str, source_url=source_url, official_domain=dom or None):
            raw_candidates.append(cand)

    return raw_candidates


def _split_role_and_personish_candidates(
    raw_candidates: list[ExtractCandidate],
) -> tuple[list[ExtractCandidate], list[ExtractCandidate]]:
    role_candidates: list[ExtractCandidate] = []
    personish_candidates: list[ExtractCandidate] = []
    for cand in raw_candidates:
        email_norm = (getattr(cand, "email", "") or "").strip().lower()
        if not email_norm:
            continue
        if is_role_or_placeholder_email(email_norm):
            role_candidates.append(cand)
        else:
            personish_candidates.append(cand)
    return role_candidates, personish_candidates


def _decide_ai_allowed_for_company(con: sqlite3.Connection, company_id: int) -> bool:
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


def _maybe_refine_people_with_ai(
    *,
    company_name: str,
    dom: str,
    fallback_domain: str,
    personish_candidates: list[ExtractCandidate],
    ai_allowed_for_company: bool,
) -> tuple[list[ExtractCandidate], bool]:
    if not personish_candidates or not ai_allowed_for_company:
        return personish_candidates, False

    try:
        refined = extract_ai_candidates(
            company_name=company_name,
            domain=dom or fallback_domain,
            raw_candidates=personish_candidates,
        )
        return refined, True
    except Exception as exc:
        log.debug(
            "AI people refinement failed; continuing with heuristic-only candidates",
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
    con: sqlite3.Connection,
    *,
    company_id: int,
    cand: ExtractCandidate,
    full_name: str,
) -> tuple[int, bool]:
    row_p = con.execute(
        "SELECT id FROM people WHERE company_id = ? AND full_name = ?",
        (company_id, full_name),
    ).fetchone()
    if row_p:
        return int(row_p[0]), False

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
            "Auto-discovered",
            getattr(cand, "source_url", None),
        ),
    )
    return int(cur_p.lastrowid), True


def _upsert_email_from_candidate(
    con: sqlite3.Connection,
    *,
    company_id: int,
    email_norm: str,
    person_id_for_email: int | None,
    source_url: str | None,
) -> tuple[int, bool]:
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


def _persist_candidates_for_company(
    con: sqlite3.Connection,
    *,
    company_id: int,
    dom: str,
    candidates_by_email: dict[str, ExtractCandidate],
) -> tuple[int, int]:
    inserted_people = 0
    inserted_emails = 0

    for email_norm, cand in sorted(candidates_by_email.items()):
        email_norm = (email_norm or "").strip().lower()
        if not email_norm:
            continue

        is_placeholder = is_role_or_placeholder_email(email_norm)
        full_name = _candidate_full_name(cand)

        person_id_for_person: int | None = None
        if full_name:
            person_id_for_person, inserted_new = _upsert_person_from_candidate(
                con, company_id=company_id, cand=cand, full_name=full_name
            )
            if inserted_new:
                inserted_people += 1

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

        try:
            domain_for_email = email_norm.split("@", 1)[1].lower() if "@" in email_norm else dom
            _enqueue_r16_probe(email_id, email_norm, domain_for_email or dom)
        except Exception as e:
            log.debug(
                "R16 enqueue from extract_candidates_for_company failed",
                exc_info=True,
                extra={"email": email_norm, "company_id": company_id, "exc": str(e)},
            )

    return inserted_people, inserted_emails


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


def extract_candidates_for_company(company_id: int) -> dict:
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

        refined_people, ai_attempted = _maybe_refine_people_with_ai(
            company_name=company_name,
            dom=dom,
            fallback_domain=fallback_domain,
            personish_candidates=personish_candidates,
            ai_allowed_for_company=ai_allowed_for_company,
        )

        candidates_by_email = _merge_candidates_by_email(refined_people, role_candidates)
        if not candidates_by_email:
            if ai_allowed_for_company and ai_attempted:
                _mark_ai_people_extracted(con, company_id)
                con.commit()
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        inserted_people, inserted_emails = _persist_candidates_for_company(
            con, company_id=company_id, dom=dom, candidates_by_email=candidates_by_email
        )

        if ai_allowed_for_company and ai_attempted:
            _mark_ai_people_extracted(con, company_id)

        con.commit()

        emails_total_row = con.execute(
            "SELECT COUNT(*) FROM emails WHERE company_id = ?",
            (company_id,),
        ).fetchone()
        emails_total = int(emails_total_row[0]) if emails_total_row else 0

        return {
            "ok": True,
            "company_id": company_id,
            "company_name": company_name,
            "domain": dom,
            "found_candidates": len(candidates_by_email),
            "inserted_people": inserted_people,
            "inserted_emails": inserted_emails,
            "emails_total": emails_total,
        }

    except Exception as exc:
        log.exception("extract_candidates_for_company failed", extra={"company_id": company_id, "exc": str(exc)})
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "company_id": company_id}


# ---------------------------
# R10 wiring: thin crawl task
# ---------------------------


def crawl_approved_domains(db: str | None = None, limit: int | None = None) -> int:
    db_path = Path(db or os.getenv("DATABASE_PATH", "dev.db")).resolve()

    with get_conn() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT DISTINCT TRIM(official_domain)
            FROM companies
            WHERE official_domain IS NOT NULL
              AND TRIM(official_domain) <> ''
            ORDER BY 1
            """
        )
        domains = [row[0] for row in cur.fetchall()]

    if limit is not None:
        domains = domains[:limit]

    if not domains:
        log.info("crawl_approved_domains: no official domains found in companies table", extra={"db": str(db_path)})
        return 0

    script = Path(__file__).resolve().parents[2] / "scripts" / "crawl_domain.py"
    if not script.exists():
        raise FileNotFoundError(f"crawl_domain.py not found at {script}")

    count = 0
    for domain in domains:
        cmd = [sys.executable, str(script), domain, "--db", str(db_path)]
        log.info("R10 crawl start", extra={"domain": domain, "db": str(db_path), "cmd": " ".join(cmd)})
        subprocess.run(cmd, check=True)
        count += 1
        log.info("R10 crawl done", extra={"domain": domain})

    log.info("R10 crawled domains total", extra={"count": count})
    return count


def upsert_person_task(row: dict) -> dict:
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


def handle_task(envelope: Any) -> Any:
    if envelope is None:
        raise ValueError("handle_task expected an envelope, got None")

    if isinstance(envelope, str):
        try:
            envelope_obj = json.loads(envelope)
        except Exception as exc:
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
        task_check_catchall.__wrapped__ if hasattr(task_check_catchall, "__wrapped__") else task_check_catchall
    )
    base_task_probe_email = (
        task_probe_email.__wrapped__ if hasattr(task_probe_email, "__wrapped__") else task_probe_email
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
    }

    func = task_map.get(task_name)
    if func is None:
        raise ValueError(f"Unknown task '{task_name}'")

    return func(**payload)
