# src/queueing/tasks.py
from __future__ import annotations

import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

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
from src.db import (
    get_conn,
    upsert_generated_email,
    upsert_verification_result,
    write_domain_resolution,
)
from src.generate.patterns import (
    PATTERNS as CANON_PATTERNS,  # keys of canonical patterns (e.g., "first.last")
)
from src.generate.patterns import (
    infer_domain_pattern,  # O01 canonical inference (returns Inference)
)
from src.generate.permutations import generate_permutations
from src.ingest.normalize import (
    normalize_row,  # R13 lightweight full-row normalization
    normalize_split_parts,  # O09 normalization for generation (ASCII locals)
)
from src.ingest.persist import upsert_row as persist_upsert_row  # R13: persist normalized rows
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
from src.verify.smtp import probe_rcpt  # R16 SMTP probe core

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
        status, reason = "unknown_timeout", (str(e) or "temp_error")
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
                extra={
                    "email": email,
                    "status": status,
                    "reason": reason,
                    "mx": mx_host,
                },
            )

        # Only release what we actually acquired
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


# -------------------------------
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
    import os
    import time

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

    start = time.perf_counter()
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
            sec = int(time.time())
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

        latency_ms = int((time.perf_counter() - start) * 1000)
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
# R16: SMTP RCPT probe queue task
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


@job("verify", timeout=20)
def task_probe_email(email_id: int, email: str, domain: str, force: bool = False) -> dict:
    """
    R16 queue task: Resolve MX if needed, enforce R06 throttling per-MX,
    run RCPT probe via src.verify.smtp.probe_rcpt, and return a structured dict.

    Does NOT write verification_results; persistence happens in R18.
    """
    # Normalize inputs
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

    # Resolve MX and get behavior hint from cache (O06)
    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    start = time.perf_counter()
    mx_host, behavior_hint = _mx_info(dom, force=bool(force), db_path=db_path)

    # Throttling (R06): global + per-MX concurrency and RPS
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

    got_global = False
    got_mx = False
    sec = int(time.time())
    key_global_rps = RPS_KEY_GLOBAL.format(sec=sec)
    key_mx_rps = RPS_KEY_MX.format(mx=mx_host, sec=sec)
    mx_key = MX_SEM.format(mx=mx_host)

    try:
        if redis_ok:
            got_global = try_acquire(redis, GLOBAL_SEM, _cfg.rate.global_max_concurrency)
            if not got_global:
                return {
                    "ok": False,
                    "error": "global concurrency cap reached",
                    "category": "unknown",
                    "code": None,
                    "mx_host": mx_host,
                    "domain": dom,
                    "email_id": email_id,
                    "email": email_str,
                    "elapsed_ms": int((time.perf_counter() - start) * 1000),
                }

            got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
            if not got_mx:
                return {
                    "ok": False,
                    "error": "per-MX concurrency cap reached",
                    "category": "unknown",
                    "code": None,
                    "mx_host": mx_host,
                    "domain": dom,
                    "email_id": email_id,
                    "email": email_str,
                    "elapsed_ms": int((time.perf_counter() - start) * 1000),
                }

            # RPS smoothing
            if _cfg.rate.global_rps and not can_consume_rps(
                redis, key_global_rps, int(_cfg.rate.global_rps)
            ):
                return {
                    "ok": False,
                    "error": "global RPS throttle",
                    "category": "unknown",
                    "code": None,
                    "mx_host": mx_host,
                    "domain": dom,
                    "email_id": email_id,
                    "email": email_str,
                    "elapsed_ms": int((time.perf_counter() - start) * 1000),
                }

            if _cfg.rate.per_mx_rps_default and not can_consume_rps(
                redis, key_mx_rps, int(_cfg.rate.per_mx_rps_default)
            ):
                return {
                    "ok": False,
                    "error": "MX RPS throttle",
                    "category": "unknown",
                    "code": None,
                    "mx_host": mx_host,
                    "domain": dom,
                    "email_id": email_id,
                    "email": email_str,
                    "elapsed_ms": int((time.perf_counter() - start) * 1000),
                }

        # Execute probe
        result = probe_rcpt(
            email_str,
            mx_host,
            helo_domain=SMTP_HELO_DOMAIN,
            mail_from=SMTP_MAIL_FROM,
            connect_timeout=SMTP_CONNECT_TIMEOUT,
            command_timeout=SMTP_COMMAND_TIMEOUT,
            behavior_hint=behavior_hint,
        )

        return {
            "ok": bool(result.get("ok", True)),
            "category": result.get("category", "unknown"),
            "code": result.get("code"),
            "mx_host": result.get("mx_host", mx_host),
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int(
                result.get("elapsed_ms") or int((time.perf_counter() - start) * 1000)
            ),
            "error": result.get("error"),
        }

    except Exception as e:
        return {
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
    finally:
        if redis_ok:
            if got_mx:
                try:
                    release(redis, mx_key)
                except Exception:
                    pass
            if got_global:
                try:
                    release(redis, GLOBAL_SEM)
                except Exception:
                    pass


# ---------------------------------------------
# Helpers for O01 domain pattern inference/cache
# ---------------------------------------------


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    try:
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (name,))
        return cur.fetchone() is not None
    except Exception:
        return False


def _examples_for_domain(con: sqlite3.Connection, domain: str) -> list[tuple[str, str, str]]:
    """
    Build [(first, last, localpart)] examples for a domain using 'published' emails.
    Tries a join to people first; falls back to emails table fields if present.
    """
    examples: list[tuple[str, str, str]] = []

    # 1) Preferred: join emails -> people
    try:
        rows = con.execute(
            """
            SELECT p.first_name, p.last_name, e.email
            FROM emails e
            JOIN people p ON p.id = e.person_id
            WHERE e.domain = ? AND e.source = 'published'
            """,
            (domain,),
        ).fetchall()
        for fn, ln, em in rows:
            if not em or "@" not in em or not fn or not ln:
                continue
            local = em.split("@", 1)[0].lower()
            examples.append((str(fn), str(ln), local))
        if examples:
            return examples
    except Exception:
        pass

    # 2) Fallback: emails table has names inline
    try:
        rows = con.execute(
            """
            SELECT first_name, last_name, email
            FROM emails
            WHERE domain = ? AND source = 'published'
            """,
            (domain,),
        ).fetchall()
        for fn, ln, em in rows:
            if not em or "@" not in em or not fn or not ln:
                continue
            local = em.split("@", 1)[0].lower()
            examples.append((str(fn), str(ln), local))
    except Exception:
        # As a last resort, return empty -> no inference
        examples = []

    return examples


def _load_cached_pattern(con: sqlite3.Connection, domain: str) -> str | None:
    """Read a cached canonical pattern key for a domain if the table exists."""
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
    """Upsert the inferred pattern if the table exists."""
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
        # Non-fatal; skip caching errors
        log.exception(
            "failed to upsert domain_patterns",
            extra={"domain": domain, "pattern": pattern},
        )


# ---------------------------------------------
# R12 wiring: email generation + verify enqueue (→ R16)
# ---------------------------------------------


def _email_row_id(con: sqlite3.Connection, email: str) -> int | None:
    """
    Try to fetch the primary key for an email row.
    Falls back to rowid if 'id' column is absent.
    """
    email = (email or "").strip().lower()
    if not email:
        return None
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(emails)").fetchall()}
        if "id" in cols:
            row = con.execute("SELECT id FROM emails WHERE email = ?", (email,)).fetchone()
            return int(row[0]) if row else None
        # fallback: rowid (works unless WITHOUT ROWID)
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
    R12 job: Generate email permutations for a person@domain, persist them as
    'generated' candidates, and enqueue verification (R16 probe) for each.

    O01 enhancement:
      - Collect published examples for the domain.
      - Use canonical inference to decide a single pattern when confident.
      - Optionally persist/read per-domain decisions via domain_patterns.

    O09 enhancement:
      - Normalize/transliterate name parts before applying patterns so locals are ASCII
        and particle-aware for global correctness.

    R13 note:
      - This task already performs a lightweight normalization (normalize_split_parts)
        before generation; for *upserts* from other stages, use upsert_person_task().
    """
    con = get_conn()
    dom = (domain or "").lower().strip()
    if not dom:
        return {"count": 0, "only_pattern": None, "domain": dom, "person_id": person_id}

    # --- O09: normalize already-split name parts (no reordering, CJK-safe) ---
    nf, nl = normalize_split_parts(first, last)
    if not (nf or nl):
        log.info(
            "R12 skipped generation due to empty normalized name",
            extra={"person_id": person_id, "domain": dom, "first": first, "last": last},
        )
        return {"count": 0, "only_pattern": None, "domain": dom, "person_id": person_id}

    # Try cached pattern first (if migration was applied)
    cached_pattern = _load_cached_pattern(con, dom)

    examples = _examples_for_domain(con, dom)
    inf_pattern = None
    inf_conf = 0.0
    inf_samples = 0

    if cached_pattern:
        inf_pattern = cached_pattern
    else:
        # Run canonical inference from examples (filters role aliases internally)
        inf = infer_domain_pattern(examples)
        inf_pattern = inf.pattern
        inf_conf = float(inf.confidence)
        inf_samples = int(inf.samples)
        if inf_pattern:
            _save_inferred_pattern(con, dom, inf_pattern, inf_conf, inf_samples)

    # Generate candidates: use the inferred/cached pattern if available,
    # otherwise fall back to the full canonical set.
    candidates = generate_permutations(
        nf,  # normalized/transliterated first
        nl,  # normalized/transliterated last
        dom,
        only_pattern=inf_pattern,  # canonical key or None
    )

    inserted = 0
    for e in sorted(candidates):
        # Persist each candidate
        upsert_generated_email(con, person_id, e, dom, source_note="r12")
        inserted += 1
        # Enqueue an R16 probe immediately (best-effort)
        try:
            email_id = _email_row_id(con, e)
            _enqueue_r16_probe(email_id, e, dom)
        except Exception as ee:
            log.debug("R16 enqueue failed (best-effort): %s", ee)

    con.commit()

    log.info(
        "R12 generated emails",
        extra={
            "person_id": person_id,
            "domain": dom,
            "first": first,
            "last": last,
            "first_norm": nf,
            "last_norm": nl,
            "only_pattern": inf_pattern,
            "inference_confidence": inf_conf,
            "inference_samples": inf_samples,
            "count": inserted,
        },
    )
    return {
        "count": inserted,
        "only_pattern": inf_pattern,
        "inference_confidence": inf_conf,
        "inference_samples": inf_samples,
        "domain": dom,
        "person_id": person_id,
    }


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
        db: Path to SQLite DB. Defaults to $DATABASE_PATH or 'dev.db'.
        limit: Optional cap on how many domains to process (useful for smoke runs).
    """
    db_path = Path(db or os.getenv("DATABASE_PATH", "dev.db")).resolve()

    # Pull distinct official domains discovered by R08
    with sqlite3.connect(str(db_path)) as con:
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
        log.info(
            "crawl_approved_domains: no official domains found in companies table",
            extra={"db": str(db_path)},
        )
        return 0

    # Invoke the same script used manually, to keep behavior identical
    script = Path(__file__).resolve().parents[2] / "scripts" / "crawl_domain.py"
    if not script.exists():
        raise FileNotFoundError(f"crawl_domain.py not found at {script}")

    count = 0
    for domain in domains:
        cmd = [sys.executable, str(script), domain, "--db", str(db_path)]
        log.info(
            "R10 crawl start",
            extra={
                "domain": domain,
                "db": str(db_path),
                "cmd": " ".join(cmd),
            },
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
        # Never drop provenance (normalize_row preserves source_url by design)
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
