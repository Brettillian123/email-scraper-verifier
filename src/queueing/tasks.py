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
from datetime import datetime, timedelta
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
from src.exceptions import PermanentSMTPError, TemporarySMTPError
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
    RENDERABLE_PATTERNS,  # canonical pattern names that can be built into an address
    first_name_dictionary,  # cached folded first names, for shape inference
    record_inferred_patterns,  # what a company's PUBLISHED addresses imply
    record_pattern_outcome,  # per-company evidence: remembers hits AND misses
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
    daily_probe_budget,
    release,
    try_acquire,
)
from src.queueing.redis_conn import get_redis
from src.resolve.domain import resolve
from src.resolve.mx import resolve_mx as _resolve_mx  # R15
from src.verify.candidates import (  # evidence-ranked probe planning
    catchall_confidence,
    company_employee_count,
    company_pattern_evidence,
    has_trusted_pattern,
    max_probes_per_person,
    rank_candidates,
)
from src.verify.catchall import check_catchall_for_domain  # R17 domain-level catch-all
from src.verify.smtp import (  # R16 SMTP probe core
    is_greylist_deferral,
    probe_rcpt,
    probe_rcpt_batch,  # one session, several RCPTs
)
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


def _safe_int(val: Any) -> int | None:
    try:
        if val is None:
            return None
        return int(val)
    except Exception:
        return None


def _count_run_companies(con: Any, *, tenant_id: str, run_id: str) -> int | None:
    """
    Best-effort: determine the total number of companies for a run.




    Supports schema drift by checking the common association patterns:
    - run_companies (preferred)
    - companies.run_id (fallback)
    """
    try:
        if _has_table(con, "run_companies"):
            cols = _table_cols(con, "run_companies")
            if cols and "run_id" in cols:
                where: list[str] = ["run_id = ?"]
                params: list[Any] = [run_id]
                if "tenant_id" in cols:
                    where.insert(0, "tenant_id = ?")
                    params.insert(0, tenant_id)
                row = con.execute(
                    "SELECT COUNT(*) FROM run_companies WHERE " + " AND ".join(where),
                    tuple(params),
                ).fetchone()
                return int(row[0] or 0) if row else 0

        if _has_table(con, "companies"):
            cols2 = _table_cols(con, "companies")
            if cols2 and "run_id" in cols2:
                where2: list[str] = ["run_id = ?"]
                params2: list[Any] = [run_id]
                if "tenant_id" in cols2:
                    where2.insert(0, "tenant_id = ?")
                    params2.insert(0, tenant_id)
                row2 = con.execute(
                    "SELECT COUNT(*) FROM companies WHERE " + " AND ".join(where2),
                    tuple(params2),
                ).fetchone()
                return int(row2[0] or 0) if row2 else 0
    except Exception:
        return None

    return None


def _track_company_completion(*, run_id: str, tenant_id: str, company_id: int, total: int) -> bool:
    """
    Mark a company as completed (idempotent across retries) and return True if run is complete.




    Uses a Redis set keyed by (tenant_id, run_id) so retries don't over-count.
    """
    try:
        redis: Redis = get_redis()
    except Exception:
        return False

    if total <= 0:
        return False

    set_key = f"tenant:{tenant_id}:run:{run_id}:completed_companies"
    ttl_s = 86400  # 24h

    lua = """
    local setkey = KEYS[1]
    local ttl = tonumber(ARGV[1])
    local cid = ARGV[2]
    redis.call('SADD', setkey, cid)
    redis.call('EXPIRE', setkey, ttl)
    return redis.call('SCARD', setkey)
    """

    try:
        completed = int(redis.eval(lua, 1, set_key, str(ttl_s), str(company_id)) or 0)
    except Exception:
        return False

    if completed < total:
        return False

    # Completed: trigger callback (best-effort) and clear state.
    try:
        from src.queueing.pipeline_v2 import run_completion_callback  # type: ignore

        try:
            run_completion_callback(run_id=run_id, tenant_id=tenant_id)
        except Exception:
            log.exception(
                "run_completion_callback failed", extra={"tenant_id": tenant_id, "run_id": run_id}
            )
    finally:
        try:
            redis.delete(set_key)
        except Exception:
            pass

    return True


def _touch_run_heartbeat(*, run_id: str) -> None:
    """Bump runs.updated_at so the reaper can tell a working run from a dead one.

    The reaper judges liveness from runs.updated_at, but fan-out finishes in
    seconds while the real work runs for hours in separate per-company RQ jobs.
    This is the worker-side heartbeat: one tiny UPDATE at each per-company job
    boundary, on every stage (not just the terminal one), so updated_at keeps
    moving for as long as companies keep completing.

    Best-effort by contract: it must never fail the job it rides on.
    """
    con = None
    try:
        con = _conn()
        con.execute(
            "UPDATE runs SET updated_at = ? WHERE id = ? AND status IN ('queued','running')",
            (_utc_now_iso_z(), run_id),
        )
        con.commit()
    except Exception:
        log.debug("run heartbeat failed", exc_info=True, extra={"run_id": run_id})
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass


def _check_run_completion(
    *,
    run_id: str,
    tenant_id: str,
    company_id: int,
    total: int | None = None,
) -> None:
    """Check if a run has fully completed and, if so, trigger aggregation/callback."""
    con = None
    try:
        total_n = total
        if total_n is None:
            con = _conn()
            total_n = _count_run_companies(con, tenant_id=tenant_id, run_id=run_id)

        if not total_n:
            return

        done = _track_company_completion(
            run_id=run_id,
            tenant_id=tenant_id,
            company_id=company_id,
            total=int(total_n),
        )
        if not done:
            return

        # Best-effort: mark run finished in DB
        # (if your callback already does this, this is harmless).
        try:
            if con is None:
                con = _conn()
            _update_run_row(
                con,
                tenant_id=tenant_id,
                run_id=run_id,
                status="completed",
                finished_at=_utc_now_iso_z(),
            )
        except Exception:
            pass

    except Exception:
        # Never let completion tracking fail the company job
        return
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass


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


# SMTP error classes are now imported from src.exceptions


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

    # Default: STUB mode - this should NOT be used in production!
    # If this code path is hit, it means the legacy verify_email_task was called
    # instead of the real task_probe_email. Log a warning and return unknown.
    log.warning(
        "smtp_probe STUB called - this should not happen in production",
        extra={"email": email, "helo_domain": helo_domain},
    )
    return ("unknown", "stub_not_verified")


class ThrottledError(TemporarySMTPError):
    """We hit our own concurrency/RPS cap and never opened the connection.

    Distinct from TemporarySMTPError, which means the *MX* asked us to come
    back later -- that is a (weak) signal about the address.  A throttle is a
    signal about us, and must never be written to verification_results as a
    verdict: doing so freezes "we did not get to ask" into a terminal row that
    no sweep will revisit.
    """


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
    email_id: int | None = None,
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

    # A throttle means we never asked; nothing gets written in that case.
    persist_result = True

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
            raise ThrottledError("global concurrency cap reached")

        got_mx = try_acquire(redis, mx_key, _cfg.rate.per_mx_max_concurrency_default)
        if not got_mx:
            raise ThrottledError("per-MX concurrency cap reached")

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
            raise ThrottledError("global RPS throttle")

        # Optional RPS smoothing (per MX)
        if _cfg.rate.per_mx_rps_default and not can_consume_rps(
            redis,
            key_mx_rps,
            int(_cfg.rate.per_mx_rps_default),
        ):
            raise ThrottledError("MX RPS throttle")

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

    except ThrottledError as e:
        # We hit our own cap and never opened a connection, so we learned
        # nothing about this address. Recording "unknown" here would freeze a
        # non-answer into a terminal row.
        persist_result = False
        status, reason = "unknown", (str(e) or "throttled")

    except TemporarySMTPError as e:
        # The MX asked us to come back later; do not re-raise by default.
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
            resolved_email_id = email_id
            if resolved_email_id is None and persist_result:
                con_db = None
                try:
                    con_db = _conn()
                    cols = _table_cols(con_db, "emails")
                    has_tenant = "tenant_id" in cols
                    has_company = "company_id" in cols
                    tenant_id = None
                    if job is not None:
                        tenant_id = job.meta.get("tenant_id")
                    where = ["LOWER(email) = ?"]
                    params = [email.lower()]
                    if company_id is not None and has_company:
                        where.append("company_id = ?")
                        params.append(int(company_id))
                    if tenant_id and has_tenant:
                        where.append("tenant_id = ?")
                        params.append(str(tenant_id))
                    try:
                        sql = (
                            "SELECT id FROM emails WHERE "
                            + " AND ".join(where)
                            + " ORDER BY id DESC LIMIT 1"
                        )
                        row = con_db.execute(sql, tuple(params)).fetchone()
                        if row:
                            resolved_email_id = int(row[0])
                    except Exception:
                        # Best-effort fallback without tenant/company predicates
                        try:
                            row = con_db.execute(
                                "SELECT id FROM emails"
                                " WHERE LOWER(email) = ?"
                                " ORDER BY id DESC LIMIT 1",
                                (email.lower(),),
                            ).fetchone()
                            if row:
                                resolved_email_id = int(row[0])
                        except Exception:
                            resolved_email_id = None

                    if resolved_email_id is None and person_id is not None:
                        try:
                            resolved_email_id = upsert_generated_email(
                                conn=con_db,
                                person_id=int(person_id),
                                email=email,
                                domain=domain,
                                source_note="verify:autoinsert",
                            )
                        except Exception:
                            resolved_email_id = None

                    try:
                        con_db.commit()
                    except Exception:
                        pass
                finally:
                    try:
                        if con_db is not None:
                            con_db.close()
                    except Exception:
                        pass

            ts_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            if not persist_result:
                # Leaving verification_results untouched is exactly what makes
                # this retryable: the verify sweep selects emails that have no
                # verification row, so the address is picked up again.
                log.info(
                    "throttled before probe: no verification result persisted",
                    extra={"email": email, "mx": mx_host, "reason": reason},
                )
            elif resolved_email_id is None:
                log.warning(
                    "skip upsert_verification_result: missing email_id",
                    extra={
                        "email": email,
                        "status": status,
                        "reason": reason,
                        "mx": mx_host,
                        "company_id": company_id,
                        "person_id": person_id,
                    },
                )
            else:
                upsert_verification_result(
                    email_id=int(resolved_email_id),
                    domain=domain,
                    email=email,
                    verify_status=status,
                    reason=reason,
                    mx_host=mx_host,
                    verified_at=ts_iso,
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
        timeout_s=float(os.getenv("TCP25_PROBE_TIMEOUT_SECONDS", "3.0")),
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
        res = _resolve_mx(company_id=0, company_domain=domain, force=force, db_path=db_path)
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

    If email is empty but email_id is provided, looks up email from DB.
    """
    email_str = (email or "").strip()

    # If email is empty but we have email_id, look it up from DB
    if (not email_str or "@" not in email_str) and email_id:
        try:
            con = get_conn()
            try:
                row = con.execute(
                    "SELECT email FROM emails WHERE id = ?", (int(email_id),)
                ).fetchone()
                if row:
                    email_str = (row[0] if isinstance(row, tuple) else row["email"] or "").strip()
            finally:
                try:
                    con.close()
                except Exception:
                    pass
        except Exception:
            log.debug(
                "_normalize_probe_inputs: failed to lookup email from email_id",
                exc_info=True,
                extra={"email_id": email_id},
            )

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


def _job_meta_get(key: str) -> str | None:
    """Best-effort helper to read a value from the current RQ job meta."""
    try:
        job = get_current_job()
        if job is None:
            return None
        meta = getattr(job, "meta", None)
        if not isinstance(meta, dict):
            return None
        val = meta.get(key)
        if val is None:
            return None
        s = str(val).strip()
        return s or None
    except Exception:
        return None


def _infer_tenant_id(*, con: Any | None = None, company_id: int | None = None) -> str | None:
    """Infer tenant_id from job meta or companies table (best-effort)."""
    tid = _job_meta_get("tenant_id")
    if tid:
        return tid
    if company_id is None:
        return None

    close_con = False
    try:
        if con is None:
            con = get_conn()
            close_con = True

        cols = _table_cols(con, "companies")
        if "tenant_id" not in cols:
            return None

        row = con.execute(
            "SELECT tenant_id FROM companies WHERE id = ? ORDER BY id DESC LIMIT 1",
            (int(company_id),),
        ).fetchone()
        if not row:
            return None
        val = row.get("tenant_id") if hasattr(row, "get") else row[0]
        s = str(val).strip() if val is not None else ""
        return s or None
    except Exception:
        return None
    finally:
        if close_con:
            try:
                con.close()
            except Exception:
                pass


def _ensure_domain_resolution_row_for_domain(domain: str, *, tenant_id: str | None = None) -> None:
    """Ensure a domain_resolutions row exists (best-effort, tenant-scoped when possible).

    This protects catch-all caching: if no row exists, R17 catch-all probes cannot persist
    results, and downstream classification can incorrectly treat accept as valid.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return

    tid = (tenant_id or _job_meta_get("tenant_id") or "").strip() or None

    try:
        con = get_conn()
        try:
            if not _has_table(con, "domain_resolutions") or not _has_table(con, "companies"):
                return

            dr_cols = _table_cols(con, "domain_resolutions")
            if "chosen_domain" not in dr_cols or "company_id" not in dr_cols:
                return

            comp_cols = _table_cols(con, "companies")
            where = ["LOWER(official_domain) = ?"]
            params: list[Any] = [dom]
            if tid and "tenant_id" in comp_cols:
                where.insert(0, "tenant_id = ?")
                params.insert(0, tid)

            row = con.execute(
                f"SELECT id FROM companies WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT 1",
                tuple(params),
            ).fetchone()
            if not row:
                return

            company_id = int(row.get("id") if hasattr(row, "get") else row[0])

            insert_data: dict[str, Any] = {
                "company_id": company_id,
                "chosen_domain": dom,
            }
            if "user_hint" in dr_cols:
                insert_data["user_hint"] = dom
            if tid and "tenant_id" in dr_cols:
                insert_data["tenant_id"] = tid

            # Keep only existing columns (for minimal test schemas)
            insert_data = {k: v for k, v in insert_data.items() if k in dr_cols}
            if not insert_data:
                return

            cols_sql = ", ".join(insert_data.keys())
            placeholders = ", ".join(["?"] * len(insert_data))
            params2 = list(insert_data.values())

            try:
                con.execute(
                    f"INSERT INTO domain_resolutions ({cols_sql}) VALUES ({placeholders})",
                    tuple(params2),
                )
                try:
                    con.commit()
                except Exception:
                    pass
            except Exception:
                # Duplicate row or constraint mismatch; ignore (best-effort).
                try:
                    con.rollback()
                except Exception:
                    pass
                return
        finally:
            try:
                con.close()
            except Exception:
                pass
    except Exception:
        return


def _load_catchall_status_for_domain(
    db_path: str, domain: str, *, tenant_id: str | None = None
) -> str | None:
    """Load cached catch-all status for a domain (best-effort).

    - Tenant-scoped when tenant_id is available (passed in or from job meta).
    - Swallows errors so tests can run against minimal schemas without R17 applied.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return None

    tid = (tenant_id or _job_meta_get("tenant_id") or "").strip() or None

    try:
        con = get_conn()
        try:
            if not _has_table(con, "domain_resolutions"):
                return None

            cols = _table_cols(con, "domain_resolutions")
            if "catch_all_status" not in cols:
                return None

            where: list[str] = []
            params: list[Any] = []

            if tid and "tenant_id" in cols:
                where.append("tenant_id = ?")
                params.append(tid)

            if "chosen_domain" in cols and "user_hint" in cols:
                where.append("(chosen_domain = ? OR user_hint = ?)")
                params.extend([dom, dom])
            elif "chosen_domain" in cols:
                where.append("chosen_domain = ?")
                params.append(dom)
            elif "domain" in cols:
                where.append("domain = ?")
                params.append(dom)
            else:
                return None

            order: list[str] = []
            if "created_at" in cols:
                order.append("created_at DESC")
            if "id" in cols:
                order.append("id DESC")
            order_sql = ", ".join(order) if order else "1"

            sql = (
                "SELECT catch_all_status FROM domain_resolutions "
                f"WHERE {' AND '.join(where)} "
                f"ORDER BY {order_sql} LIMIT 1"
            )
            row = con.execute(sql, tuple(params)).fetchone()
            if not row:
                return None

            val = row.get("catch_all_status") if hasattr(row, "get") else row[0]
            return str(val) if val is not None else None
        finally:
            try:
                con.close()
            except Exception:
                pass
    except Exception:
        log.debug(
            "R17/R18: failed to load catch_all_status for domain",
            exc_info=True,
            extra={"domain": dom, "tenant_id": tid},
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


def _persist_probe_result_r18(  # noqa: C901
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
        tenant_id = _job_meta_get("tenant_id")
        ca_status_db = _load_catchall_status_for_domain(db_path, dom, tenant_id=tenant_id)
        if ca_status_db in {"catch_all", "not_catch_all"}:
            catch_all_status: str | None = ca_status_db
        elif tcp25_ok is False:
            catch_all_status = None
        else:
            try:
                _ensure_domain_resolution_row_for_domain(dom, tenant_id=tenant_id)
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

        # Canonical persistence (preferred): use the DB helper that works on Postgres in prod.
        try:
            if email_id_val is not None:
                upsert_verification_result(
                    email_id=int(email_id_val),
                    domain=dom,
                    email=(email or "").strip(),
                    verify_status=verify_status,
                    reason=verify_reason,
                    mx_host=(mx_host or "").strip().lower() or None,
                    verified_at=ts_iso,
                )
        except Exception:
            log.exception(
                "R18: upsert_verification_result failed",
                extra={
                    "email_id": email_id,
                    "email": email,
                    "domain": dom,
                    "mx_host": mx_host,
                    "verify_status": verify_status,
                    "verify_reason": verify_reason,
                },
            )

        verification_result_id: int | None = None

        # Best-effort enrichment of verification_results with raw probe/fallback fields.
        try:
            con = get_conn()
            try:
                cols = _table_cols(con, "verification_results")
                if cols and email_id_val is not None and "email_id" in cols:
                    # Find the latest row id if available
                    if "id" in cols:
                        row = con.execute(
                            "SELECT id FROM verification_results"
                            " WHERE email_id = ?"
                            " ORDER BY id DESC LIMIT 1",
                            (int(email_id_val),),
                        ).fetchone()
                        if row and row[0] is not None:
                            verification_result_id = int(row[0])

                    upd_cols = [c for c in values.keys() if c in cols and c not in {"email_id"}]
                    if upd_cols:
                        set_sql = ", ".join([f"{c} = ?" for c in upd_cols])
                        params = [values[c] for c in upd_cols]
                        if verification_result_id is not None and "id" in cols:
                            con.execute(
                                f"UPDATE verification_results SET {set_sql} WHERE id = ?",
                                tuple(params + [verification_result_id]),
                            )
                        else:
                            con.execute(
                                f"UPDATE verification_results SET {set_sql} WHERE email_id = ?",
                                tuple(params + [int(email_id_val)]),
                            )
                        con.commit()
            finally:
                try:
                    con.close()
                except Exception:
                    pass
        except Exception:
            # Never fail the probe task due to optional enrichment.
            pass
    except Exception:
        log.exception(
            "R18: failed to persist verification_results row",
            extra={"email_id": email_id, "email": email, "domain": dom, "mx_host": mx_host},
        )
        return None, None, None, None, None

    # Cleanup invalid generated emails immediately to reduce DB clutter.
    # This only deletes emails that were generated (not scraped from web sources).
    if verify_status == "invalid" and email_id_val is not None:
        _cleanup_invalid_generated_email(int(email_id_val), verify_status)

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
    timeout_s: float = 3.0,  # Increased from 1.5 to reduce false negatives
    redis: Redis | None = None,
    success_ttl_s: int = 300,  # Cache successes for 5 minutes
    failure_ttl_s: int = 10,  # Cache failures for only 10 seconds (allows quick retry)
    max_attempts: int = 2,  # Try twice before declaring blocked
    ttl_s: int = 60,  # Kept for backward compatibility, ignored
) -> dict[str, Any]:
    """
    Fast TCP/25 reachability preflight for the resolved MX host.




    FIXED:
    - Only cache successes long-term (5 min)
    - Cache failures for very short time (10 sec) to allow quick retry
    - Attempt multiple connections before declaring blocked
    - Increased timeout to reduce false negatives on slow networks




    Returns:
      {"ok": bool, "mx_host": str, "cached": bool, "error": str|None, "attempts": int}
    """
    host = (mx_host or "").strip().lower()
    if not host:
        return {
            "ok": False,
            "mx_host": mx_host,
            "cached": False,
            "error": "empty_mx_host",
            "attempts": 0,
        }

    cache_key = f"tcp25_preflight:{host}"

    # Check cache first - only trust cached SUCCESSES
    if redis is not None:
        try:
            cached = redis.get(cache_key)
            if cached == b"1":
                # Only trust cached successes
                return {
                    "ok": True,
                    "mx_host": host,
                    "cached": True,
                    "error": None,
                    "attempts": 0,
                }
            # Note: Intentionally ignore cached failures (b"0") to allow retry
        except Exception:
            pass

    # Attempt connection with retries
    ok = False
    last_err: str | None = None
    attempts_made = 0

    for attempt in range(max_attempts):
        attempts_made += 1
        try:
            with closing(socket.create_connection((host, 25), timeout=timeout_s)):
                ok = True
                last_err = None
                break
        except TimeoutError:
            last_err = f"timeout after {timeout_s}s (attempt {attempts_made})"
        except ConnectionRefusedError:
            last_err = f"connection refused (attempt {attempts_made})"
            # Don't retry on explicit refusal - it's definitive
            break
        except OSError as exc:
            last_err = f"{type(exc).__name__}: {exc} (attempt {attempts_made})"
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc} (attempt {attempts_made})"

        # Small delay between retries
        if attempt < max_attempts - 1:
            time.sleep(0.5)

    # Cache result with different TTLs
    if redis is not None:
        try:
            if ok:
                # Cache success for longer
                redis.setex(cache_key, success_ttl_s, b"1")
            else:
                # Cache failure for very short time to allow quick retry
                redis.setex(cache_key, failure_ttl_s, b"0")
        except Exception:
            pass

    return {
        "ok": ok,
        "mx_host": host,
        "cached": False,
        "error": last_err,
        "attempts": attempts_made,
    }


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
    tenant_id = _job_meta_get("tenant_id")
    catch_all_status = _load_catchall_status_for_domain(db_path, domain, tenant_id=tenant_id)
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


_DEFINITIVE_VERIFY_STATUSES = frozenset({"valid", "invalid", "risky_catch_all"})


def _existing_definitive_status(email_id: int) -> str | None:
    """Best-effort lookup of an already-decided verify_status for email_id."""
    try:
        con = get_conn()
    except Exception:
        return None
    try:
        row = con.execute(
            "SELECT verify_status FROM verification_results WHERE email_id = ?",
            (int(email_id),),
        ).fetchone()
    except Exception:
        return None
    finally:
        try:
            con.close()
        except Exception:
            pass
    if not row:
        return None
    try:
        val = row["verify_status"]
    except Exception:
        val = row[0]
    return val.strip().lower() if isinstance(val, str) else None


def _maybe_defer_greylist_retry(
    *,
    email_id: int,
    email: str,
    domain: str,
    mx_host: str | None,
    code: int | None,
    message: str | None,
    defer_attempt: int,
) -> bool:
    """
    Requeue this probe after the next greylist interval instead of sleeping.

    Production threw 20,443 temp_fails away (32% of all probes) after two inline
    sleeps of 1s and 2s. Greylisting needs 300s+, so none of those probes could
    ever have succeeded. Only a 4xx the server actually SPOKE (code AND message
    present) is deferred; a timeout or a dropped socket returns False, because
    waiting fifteen minutes for a dead socket buys nothing and the caller
    already has the only answer it is going to get.

    Returns True if a retry was scheduled, in which case an interim temp_fail
    row is persisted and the caller must not write a verdict.
    """
    if code is None or not message:
        return False

    schedule = _cfg.retry_timeout.greylist_retry_schedule
    if defer_attempt >= len(schedule):
        return False

    delay_s = schedule[defer_attempt]
    reason = (
        "greylist_deferred" if is_greylist_deferral(code, message) else "policy_temp_fail_deferred"
    )

    try:
        q = Queue(name="verify", connection=get_redis())
        q.enqueue_in(
            timedelta(seconds=delay_s),
            task_probe_email,
            email_id=int(email_id),
            email=email,
            company_domain=domain,
            force=False,
            defer_attempt=defer_attempt + 1,
            job_timeout=30,
            retry=None,
        )
    except Exception:
        log.warning(
            "greylist retry enqueue failed; treating as exhausted",
            exc_info=True,
            extra={"email_id": email_id, "email": email},
        )
        return False

    # verification_results has no attempt column; the reason string carries the
    # progress, matching this file's existing colon-delimited reason idiom.
    _persist_probe_result_r18(
        db_path=os.getenv("DATABASE_PATH") or "data/dev.db",
        email_id=int(email_id),
        email=email,
        domain=domain,
        mx_host=mx_host,
        category="temp_fail",
        code=code,
        error=f"{reason}:attempt={defer_attempt + 1}/{len(schedule)}:retry_in={delay_s}s",
        fallback_status=None,
        fallback_raw=None,
        tcp25_ok=True,
    )
    log.info(
        "verify probe deferred",
        extra={
            "email_id": email_id,
            "email": email,
            "domain": domain,
            "reason": reason,
            "defer_attempt": defer_attempt + 1,
            "delay_s": delay_s,
        },
    )
    return True


_SELECT_REFUTATION_CONTEXT = """
    SELECT p.company_id, p.first_name, p.last_name, e.source_note
    FROM emails e JOIN people p ON p.id = e.person_id
    WHERE e.id = ?
"""


def _record_probe_refutation(*, email_id: int, email: str, domain: str) -> None:
    """
    File a hard 5xx as a REFUTATION of the format that produced it.

    36,320 hard failures produced exactly one stored row in production: the
    async probe path never touched the pattern evidence, so a dead format was
    re-guessed for every person at the company. The pattern is read from the
    source_note the generator wrote ("generated:pattern=flast:rank=1") --
    re-deriving it from the person's name lands on `other` whenever the name is
    messy, and a refutation filed under `other` stops nothing.
    """
    try:
        con = get_conn()
    except Exception:
        return
    try:
        row = con.execute(_SELECT_REFUTATION_CONTEXT, (int(email_id),)).fetchone()
        if not row:
            return
        pattern = None
        for tok in str(row[3] or "").split(":"):
            if tok.startswith("pattern="):
                pattern = tok.split("=", 1)[1] or None
        _record_pattern_outcome_safe(
            con,
            company_id=row[0],
            domain=domain,
            email=email,
            first=row[1] or "",
            last=row[2] or "",
            ok=False,
            pattern=pattern,
        )
    except Exception:
        log.debug(
            "failed to record probe refutation",
            exc_info=True,
            extra={"email_id": email_id, "email": email},
        )
    finally:
        try:
            con.close()
        except Exception:
            pass


def _task_probe_email_impl(  # noqa: C901
    email_id: int,
    email: str,
    domain: str,
    force: bool = False,
    defer_attempt: int = 0,
) -> dict:
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

    # A deferred retry can race the sequential verifier, which may already have
    # decided this address. Re-probing a settled mailbox is pure waste.
    if not force:
        already = _existing_definitive_status(int(email_id))
        if already in _DEFINITIVE_VERIFY_STATUSES:
            return {
                "ok": True,
                "skipped": "already_decided",
                "verify_status": already,
                "email_id": int(email_id),
                "email": email_str,
                "domain": dom,
            }

    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    start = time.perf_counter()
    mx_host, behavior_hint = _mx_info(dom, force=bool(force), db_path=db_path)

    redis_obj, redis_ok = _init_redis_for_probe()

    # --- NEW: fast TCP/25 preflight BEFORE throttles/probing -----------------
    pre = _smtp_tcp25_preflight_mx(
        mx_host,
        timeout_s=float(os.getenv("TCP25_PROBE_TIMEOUT_SECONDS", "3.0")),
        redis=redis_obj if redis_ok else None,
    )
    tcp25_ok = bool(pre.get("ok"))
    if not tcp25_ok and not bool(force):
        fallback_status, fallback_raw = _maybe_run_fallback(email_str, "unknown")

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

        if fallback_status is not None:
            payload["fallback_status"] = fallback_status
            payload["fallback_raw"] = fallback_raw

        v_status, v_reason, v_mx, v_at, _vr_id = _persist_probe_result_r18(
            db_path=db_path,
            email_id=int(email_id),
            email=email_str,
            domain=dom,
            mx_host=mx_host,
            category=payload["category"],
            code=None,
            error=payload["error"],
            fallback_status=fallback_status,
            fallback_raw=fallback_raw,
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
            log.info(
                "R16: throttled, skipping probe (no result persisted)",
                extra={
                    "email_id": email_id,
                    "email": email_str,
                    "domain": dom,
                    "mx_host": mx_host,
                    "reason": throttle_error.get("error"),
                },
            )
            return throttle_error

        # --- NEW: clamp probe timeouts so the 20s job timeout remains realistic
        connect_timeout = float(SMTP_CONNECT_TIMEOUT)
        command_timeout = float(SMTP_COMMAND_TIMEOUT)
        connect_timeout = min(
            connect_timeout, float(os.getenv("SMTP_CONNECT_TIMEOUT_CLAMP", "10.0"))
        )
        command_timeout = min(
            command_timeout, float(os.getenv("SMTP_COMMAND_TIMEOUT_CLAMP", "20.0"))
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

        # A server-issued 4xx says "come back later", not "no". Requeue past the
        # greylisting window rather than burning the fallback API and writing a
        # verdict we never got.
        #
        # The 4xx has to be real. _classify_exc() buckets timeouts AND every
        # unexpected exception as temp_fail with code=None, so gating on the
        # category alone would put a socket timeout -- or a TypeError in our own
        # code -- on the 60/300/900s greylist schedule. Those 319 timeout probes
        # already burned 16.5% of all SMTP wall-time at one attempt each;
        # retrying them three more times is the opposite of the point. No code
        # means no server reply, which means nothing to come back to.
        if category == "temp_fail" and code is not None and 400 <= code < 500:
            if _maybe_defer_greylist_retry(
                email_id=int(email_id),
                email=email_str,
                domain=dom,
                mx_host=mx_host,
                code=code,
                message=result.get("message"),
                defer_attempt=defer_attempt,
            ):
                return {
                    "ok": False,
                    "status": "deferred_retry_scheduled",
                    "category": category,
                    "code": code,
                    "mx_host": mx_host,
                    "domain": dom,
                    "email_id": int(email_id),
                    "email": email_str,
                    "elapsed_ms": int((time.perf_counter() - start) * 1000),
                    "error": error_val,
                    "defer_attempt": defer_attempt + 1,
                }
            if defer_attempt > 0:
                error_val = (
                    f"{error_val or 'temp_fail'}:greylist_exhausted:attempts={defer_attempt}"
                )

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

        if category == "hard_fail":
            _record_probe_refutation(email_id=int(email_id), email=email_str, domain=dom)

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
        err = f"{type(exc).__name__}: {exc}"
        log.exception(
            "R16 task_probe_email failed",
            extra={
                "email_id": email_id,
                "email": email_str,
                "domain": dom,
                "mx_host": mx_host,
                "exc": err,
            },
        )

        payload: dict[str, Any] = {
            "ok": False,
            "error": err,
            "category": "unknown",
            "code": None,
            "mx_host": mx_host,
            "domain": dom,
            "email_id": int(email_id),
            "email": email_str,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
        }

        # Persist an "unknown" result so the UI doesn't show unlabeled rows.
        try:
            v_status, v_reason, _v_mx, v_at, vr_id = _persist_probe_result_r18(
                db_path=db_path,
                email_id=int(email_id),
                email=email_str,
                domain=dom,
                mx_host=mx_host,
                category="unknown",
                code=None,
                error=err,
                fallback_status=None,
                fallback_raw=None,
                tcp25_ok=tcp25_ok,
            )
            if v_status is not None:
                payload["verify_status"] = v_status
            if v_reason is not None:
                payload["verify_reason"] = v_reason
            if v_at is not None:
                payload["verified_at"] = v_at
            if vr_id is not None:
                payload["verification_result_id"] = vr_id
        except Exception:
            pass

        return payload
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
def task_probe_email(
    email_id: int,
    email: str,
    domain: str | None = None,
    company_domain: str | None = None,
    force: bool = False,
    defer_attempt: int = 0,
) -> dict:
    """RQ entrypoint for R16/R18 probes.




    This thin wrapper exists so we can expose a stable synchronous callable for
    local debugging/tests via ``task_probe_email.__wrapped__(...)``.
    """
    dom = (domain or company_domain or "").strip().lower()
    return _task_probe_email_impl(
        email_id=email_id,
        email=email,
        domain=dom,
        force=force,
        defer_attempt=defer_attempt,
    )


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
    Operator override: a pattern pinned on companies.attrs["email_pattern"].

    Outranks the statistical base rate but not confirmed evidence -- see
    src.verify.candidates.rank_patterns(). The value must be a renderable
    pattern name from src.generate.patterns.
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
        if isinstance(pat, str) and pat in RENDERABLE_PATTERNS:
            return pat
    except Exception:
        log.debug(
            "failed to load company email_pattern override",
            exc_info=True,
            extra={"company_id": company_id},
        )
    return None


# ---------------------------------------------
# R12 wiring: email generation + verify enqueue (Ã¢â€ â€™ R16)
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
    if email_id is None or int(email_id) <= 0:
        log.warning(
            "R16 enqueue skipped: missing email_id",
            extra={"email": email, "domain": domain},
        )
        return

    try:
        q = Queue(name="verify", connection=get_redis())
        job = q.enqueue(
            task_probe_email,
            email_id=int(email_id),
            email=email,
            company_domain=domain,
            force=False,
            job_timeout=30,  # Increased from 20 to allow for slower servers
            retry=None,
        )
        log.info(
            "R16 probe enqueued",
            extra={
                "email": email,
                "email_id": email_id,
                "domain": domain,
                "job_id": job.id if job else None,
            },
        )
    except Exception as e:
        log.warning("R16 enqueue failed: %s", e, extra={"email": email, "domain": domain})


def task_generate_emails(person_id: int, first: str, last: str, domain: str) -> dict:
    """Generate and verify candidate addresses for one person.

    Thin wrapper so the company's outstanding-people counter is decremented on
    EVERY exit path, including exceptions. The body has many returns; a finally
    here is the only place that catches all of them.

    The last person job to finish is what declares the company complete. The
    fan-out cannot: it only enqueues and returns in seconds, which is why run
    a661ca92 reported `succeeded` with 655 jobs still queued.
    """
    try:
        return _generate_emails_for_person(person_id, first, last, domain)
    finally:
        _signal_person_job_done()


def _signal_person_job_done() -> None:
    """Decrement this company's counter; the one that empties it completes it.

    Best-effort throughout. A missing counter means "do not complete from here",
    so a lost key delays a run's terminal status rather than fabricating one --
    which is the safe direction, because a run marked complete while its work
    continues is what this whole change exists to stop.
    """
    try:
        from rq import get_current_job

        from src.queueing.pipeline_v2 import company_pending_done

        job = get_current_job()
        meta = (job.meta or {}) if job is not None else {}
        run_id = meta.get("run_id")
        company_id = meta.get("company_id")
        tenant_id = str(meta.get("tenant_id") or "dev")
        if not run_id or company_id is None:
            return

        remaining = company_pending_done(
            tenant_id=tenant_id, run_id=str(run_id), company_id=int(company_id)
        )
        if remaining != 0:
            return

        # Last person at this company. NOW the company is genuinely done.
        _touch_run_heartbeat(run_id=str(run_id))
        if not meta.get("is_terminal_stage"):
            return
        total = meta.get("total_companies")
        if not total:
            return
        _check_run_completion(
            run_id=str(run_id),
            tenant_id=tenant_id,
            company_id=int(company_id),
            total=int(total),
        )
    except Exception:
        log.debug("person-job completion signal failed", exc_info=True)


def _generate_emails_for_person(  # noqa: C901
    person_id: int,
    first: str,
    last: str,
    domain: str,
) -> dict:
    """
    Generate and verify candidate addresses for one person.

    Candidates are ORDERED BY EVIDENCE, not guessed once and abandoned:
    patterns this company has already confirmed come first, then an operator
    override, then the size-adjusted global base rate. Patterns this company has
    only ever failed are skipped. See src.verify.candidates.rank_patterns().

    Modes (controlled by SEQUENTIAL_VERIFICATION env):
    - SEQUENTIAL_VERIFICATION=1 (default): probe the list in order, stop at the
      first `valid`, and record every decided outcome back into the per-company
      pattern evidence.
    - SEQUENTIAL_VERIFICATION=0: parallel enqueue (legacy behaviour).

    MAX_PROBES_PER_PERSON caps the list (default 3). It is a budget, not a
    target: walking many addresses at one domain is the directory-harvest
    pattern that gets a probing IP throttled.
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

    max_probes = max_probes_per_person()

    # Check for sequential verification mode
    seq_env = os.getenv("SEQUENTIAL_VERIFICATION", "1").strip().lower()
    sequential_mode = seq_env in ("1", "true", "yes")

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

    evidence = company_pattern_evidence(con, company_id)
    ranked_candidates = rank_candidates(
        evidence,
        nf,
        nl,
        dom,
        employee_count=company_employee_count(con, company_id),
        override=company_pattern,
        limit=max_probes if max_probes > 0 else None,
    )
    effective_pattern = ranked_candidates[0][1] if ranked_candidates else None

    # ----- SEQUENTIAL VERIFICATION MODE -----
    if sequential_mode:
        result = _generate_emails_sequential(
            con=con,
            person_id=person_id,
            domain=dom,
            ranked_candidates=ranked_candidates,
            effective_pattern=effective_pattern,
            company_pattern=company_pattern,
            evidence=evidence,
            max_probes=max_probes,
            nf=nf,
            nl=nl,
            company_id=company_id,
        )
        _record_generation_outcome(con, person_id=person_id, result=result)
        return result

    # ----- PARALLEL MODE (LEGACY) -----
    inserted = 0
    enqueued = 0
    for rank, (email_addr, pattern_key) in enumerate(ranked_candidates, 1):
        try:
            upsert_generated_email(
                conn=con,
                person_id=person_id,
                email=email_addr,
                domain=dom,
                source_note=f"generated:pattern={pattern_key}:rank={rank}",
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

    # Log summary with clear indication of success/failure
    if inserted == 0:
        log.warning(
            "R12 generated NO emails - check permutation generation",
            extra={
                "person_id": person_id,
                "domain": dom,
                "first_norm": nf,
                "last_norm": nl,
                "only_pattern": effective_pattern,
                "candidates_attempted": len(ranked_candidates) if ranked_candidates else 0,
            },
        )
    else:
        log.info(
            "R12 generated emails",
            extra={
                "person_id": person_id,
                "domain": dom,
                "first_norm": nf,
                "last_norm": nl,
                "only_pattern": effective_pattern,
                "company_pattern": company_pattern,
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
        "domain": dom,
        "person_id": person_id,
    }


def _persist_sequential_verification_result(
    *,
    con: Any,
    email_id: int,
    email: str,
    domain: str,
    mx_host: str | None,
    status: str | None,
    reason: str | None,
    code: int | None,
    catch_all_status: str | None,
    company_id: int | None = None,
    person_id: int | None = None,
    confidence: float | None = None,
    confidence_grade: str | None = None,
) -> None:
    """
    Persist verification result from sequential verification.

    IMPORTANT: must work on Postgres in production, so avoid raw INSERTs
    with SQLite-only PRAGMA introspection. Use the canonical DB upsert helper.

    `confidence`/`confidence_grade` carry the pattern-evidence grade for an
    address SMTP could not decide. They annotate the row; they never change the
    verdict, which stays risky_catch_all.
    """
    from datetime import datetime as dt

    ts_iso = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    verify_status_map = {
        "valid": "valid",
        "invalid": "invalid",
        "risky_catch_all": "risky_catch_all",
        "temp_fail": "unknown_timeout",
        "unknown": "unknown_timeout",
    }

    st = (status or "unknown").strip().lower()
    verify_status = verify_status_map.get(st, "unknown_timeout")
    verify_reason = (reason or "").strip() or "sequential_verification"

    # NOTE: code/catch_all_status are captured in the logs; the canonical upsert
    # interface stores the normalized verify_status/reason used by the app/UI.
    upsert_verification_result(
        email_id=int(email_id),
        domain=(domain or "").strip().lower(),
        email=(email or "").strip(),
        verify_status=verify_status,
        reason=verify_reason,
        mx_host=(mx_host or "").strip().lower() or None,
        verified_at=ts_iso,
        company_id=company_id,
        person_id=person_id,
    )

    if confidence is None and confidence_grade is None:
        return

    # Additive columns (db/migrations/2026_08_19_verification_pattern_confidence.sql).
    # upsert_verification_result() only writes columns it knows about, so the
    # grade is applied here; skip silently on a database without the migration.
    try:
        cols = _table_cols(con, "verification_results")
        if "pattern_confidence" not in cols or "pattern_confidence_grade" not in cols:
            return
        con.execute(
            """
            UPDATE verification_results
               SET pattern_confidence = ?, pattern_confidence_grade = ?
             WHERE email_id = ?
            """,
            (confidence, confidence_grade, int(email_id)),
        )
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        log.debug(
            "failed to persist pattern confidence",
            exc_info=True,
            extra={"email_id": email_id, "grade": confidence_grade},
        )


def _delete_email_and_verification_results(con: Any, email_id: int) -> None:
    """Best-effort cleanup helper used to reduce DB clutter from generated permutations.


    Deletes:
      - verification_results rows that reference the email_id (if the table exists)
      - the email row itself (id first; rowid fallback for legacy SQLite)
    """
    vr_deleted = False
    email_deleted = False

    try:
        if _has_table(con, "verification_results"):
            con.execute("DELETE FROM verification_results WHERE email_id = ?", (int(email_id),))
            vr_deleted = True
    except Exception:
        log.debug(
            "Failed to delete verification_results",
            exc_info=True,
            extra={"email_id": email_id},
        )

    # Prefer 'id' column, fall back to SQLite rowid if needed
    try:
        con.execute("DELETE FROM emails WHERE id = ?", (int(email_id),))
        email_deleted = True
    except Exception:
        try:
            con.execute("DELETE FROM emails WHERE rowid = ?", (int(email_id),))
            email_deleted = True
        except Exception:
            log.debug("Failed to delete email row", exc_info=True, extra={"email_id": email_id})

    log.debug(
        "_delete_email_and_verification_results completed",
        extra={"email_id": email_id, "vr_deleted": vr_deleted, "email_deleted": email_deleted},
    )


def _is_generated_email(con: Any, email_id: int) -> bool:
    """
    Check if an email is a generated/permutation email (not scraped from a source).

    Generated emails are identified by:
      - source = 'generated' column (if exists), OR
      - source_note patterns like 'generated:', 'sequential_candidate:', etc. (if exists), OR
      - Empty/null source_url (no web source) - fallback when other columns don't exist

    Returns True if the email appears to be generated (not scraped from web).
    """
    if email_id is None or int(email_id) <= 0:
        return False

    try:
        cols = _table_cols(con, "emails")
        has_source = "source" in cols
        has_source_note = "source_note" in cols
        has_source_url = "source_url" in cols

        # Build SELECT based on available columns
        select_cols = []
        if has_source:
            select_cols.append("source")
        if has_source_note:
            select_cols.append("source_note")
        if has_source_url:
            select_cols.append("source_url")

        if not select_cols:
            # No way to determine - assume generated to be safe (will delete)
            log.debug("_is_generated_email: no source columns found, assuming generated")
            return True

        row = con.execute(
            f"SELECT {', '.join(select_cols)} FROM emails WHERE id = ?", (int(email_id),)
        ).fetchone()

        if not row:
            return False

        # Parse results based on column order
        idx = 0
        source_val = ""
        source_note_val = ""
        source_url_val = ""

        if has_source:
            source_val = (row[idx] or "").strip().lower()
            idx += 1
        if has_source_note:
            source_note_val = (row[idx] or "").strip().lower()
            idx += 1
        if has_source_url:
            source_url_val = (row[idx] or "").strip()
            idx += 1

        # If has source_url with content, it's scraped not generated
        if source_url_val:
            return False

        # If we have source column, check for 'generated'
        if has_source and source_val == "generated":
            return True

        # If we have source_note, check for generated patterns
        if has_source_note:
            generated_prefixes = (
                "generated:",
                "sequential_candidate:",
                "sequential_",
                "permutation:",
                "unverified:",
                "invalid:",
            )
            if any(source_note_val.startswith(p) for p in generated_prefixes):
                return True

        # Fallback: if source_url is empty/null and we don't have source/source_note,
        # treat as generated (better to clean up than leave orphans)
        if not has_source and not has_source_note and has_source_url and not source_url_val:
            return True

        return False
    except Exception:
        log.debug("_is_generated_email check failed", exc_info=True, extra={"email_id": email_id})
        return False


def _cleanup_invalid_generated_email(email_id: int, verify_status: str) -> bool:
    """
    Clean up an invalid generated email from the database.

    Only deletes if:
      - verify_status is 'invalid'
      - The email is a generated/permutation email (not scraped)
      - CLEANUP_INVALID_GENERATED env var is not explicitly disabled

    Returns True if deleted, False otherwise.
    """
    # Normalize verify_status
    vs = (verify_status or "").strip().lower()

    if vs != "invalid":
        return False

    if email_id is None:
        return False

    try:
        eid = int(email_id)
        if eid <= 0:
            return False
    except (ValueError, TypeError):
        return False

    # Check if cleanup is enabled (default: yes)
    _cleanup_env = os.getenv("CLEANUP_INVALID_GENERATED", "1").strip().lower()
    cleanup_enabled = _cleanup_env in ("1", "true", "yes")
    if not cleanup_enabled:
        log.debug("Cleanup disabled via CLEANUP_INVALID_GENERATED", extra={"email_id": eid})
        return False

    try:
        con = get_conn()
        try:
            # Only delete if it's a generated email
            is_generated = _is_generated_email(con, eid)
            if not is_generated:
                log.debug(
                    "Skipping cleanup: not a generated email",
                    extra={"email_id": eid, "verify_status": vs},
                )
                return False

            # An address we once CONFIRMED is never deleted by a later 550.
            # See _was_ever_valid(): a throttling provider rejects rather than
            # defers, and that is indistinguishable from "no such user" here.
            if _was_ever_valid(con, eid):
                log.info(
                    "Skipping cleanup: address was previously confirmed valid",
                    extra={"email_id": eid, "verify_status": vs},
                )
                return False

            _delete_email_and_verification_results(con, eid)
            con.commit()

            log.info(
                "Cleaned up invalid generated email",
                extra={"email_id": eid, "verify_status": vs},
            )
            return True
        finally:
            try:
                con.close()
            except Exception:
                pass
    except Exception:
        log.warning(
            "Failed to cleanup invalid generated email",
            exc_info=True,
            extra={"email_id": eid, "verify_status": vs},
        )
        return False


def _record_pattern_outcome_safe(
    con: Any,
    *,
    company_id: int | None,
    domain: str,
    email: str,
    first: str,
    last: str,
    ok: bool,
    pattern: str | None = None,
) -> str | None:
    """
    Remember what this guess did, so the next person at this company benefits.

    Pass `pattern` whenever the caller GENERATED the address: re-deriving the
    format from the person's name lands on `other` when the name is messy, and
    a refutation filed under `other` never stops the format that actually
    missed from being tried again.

    Only `valid` and `invalid` are evidence. A catch-all 250 and a timeout say
    nothing about the format, so callers must not route them here.

    This is the ONLY memory of a failed guess: the invalid address row itself is
    deleted by CLEANUP_INVALID_GENERATED, by design.
    """
    if not company_id:
        return None
    try:
        pattern = record_pattern_outcome(
            con,
            company_id=int(company_id),
            domain=domain,
            email=email,
            first_name=first,
            last_name=last,
            ok=ok,
            pattern=pattern,
        )
        con.commit()
        return pattern
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        log.debug(
            "failed to record pattern outcome",
            exc_info=True,
            extra={"company_id": company_id, "email": email, "ok": ok},
        )
        return None


def _record_inferred_patterns_safe(con: Any, *, company_id: int, dom: str, emails) -> None:
    """
    Teach the pattern store from the addresses the site PUBLISHED.

    Shape inference needs no matching person row, which is why it is the path
    from 35 companies with a known format to 128. Deliberately kept out of
    confirmed_count: a published address proves what the website says, not that
    SMTP accepts it.
    """
    want = (dom or "").strip().lower()
    local_parts = [
        e.split("@", 1)[0]
        for e in emails
        if "@" in e and e.split("@", 1)[1].strip().lower() == want
    ]
    if not (company_id and local_parts):
        return
    try:
        counts = record_inferred_patterns(
            con,
            company_id=int(company_id),
            domain=want,
            local_parts=local_parts,
            first_names=first_name_dictionary(con),
        )
        con.commit()
        if counts:
            log.info(
                "pattern shapes inferred from published addresses",
                extra={"company_id": company_id, "domain": want, "counts": counts},
            )
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        log.debug(
            "shape inference unavailable",
            exc_info=True,
            extra={"company_id": company_id, "domain": want},
        )


def _set_email_source_note(con: Any, email_id: int | None, note: str) -> None:
    """Best-effort source_note update; the column is absent on older schemas."""
    if email_id is None:
        return
    try:
        if "source_note" not in _table_cols(con, "emails"):
            return
        con.execute("UPDATE emails SET source_note = ? WHERE id = ?", (note, int(email_id)))
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        log.debug("failed to update source_note", exc_info=True, extra={"email_id": email_id})


def _was_ever_valid(con: Any, email_id: int) -> bool:
    """True if this address has ever verified valid.

    Such an address is never deleted on a later `invalid`. A 550 is not always a
    verdict about the mailbox -- a provider throttling us can reject rather than
    defer, and a policy rejection under directory-harvest suspicion is
    indistinguishable from "no such user" at the SMTP layer. The 2026-08-25
    backfill drew 921 throttling responses from one provider and tripped the
    host's outbound SMTP block; confirmed addresses fell 1,847 to 1,564 in that
    single run, and how much of that was genuine churn cannot now be known,
    because the rows were deleted.

    Fails CLOSED: if the column is missing or the query errors, the answer is
    "yes, it was valid", so the address survives. Keeping a dead address costs a
    stale row; deleting a live one costs the result outright.
    """
    try:
        row = con.execute(
            "SELECT first_valid_at FROM emails WHERE id = ?", (int(email_id),)
        ).fetchone()
    except Exception:
        log.debug("could not read first_valid_at; refusing deletion", exc_info=True)
        return True
    return bool(row and row[0])


def _mark_first_valid(con: Any, email_id: int | None) -> None:
    """Stamp the first confirmation. Never cleared, so the guard above holds."""
    if email_id is None:
        return
    try:
        con.execute(
            "UPDATE emails SET first_valid_at = ? WHERE id = ? AND first_valid_at IS NULL",
            (_utc_now_iso_z(), int(email_id)),
        )
        con.commit()
    except Exception:
        log.debug("could not stamp first_valid_at", exc_info=True, extra={"email_id": email_id})
        try:
            con.rollback()
        except Exception:
            pass


def _record_generation_outcome(con: Any, *, person_id: int, result: dict) -> None:
    """Persist WHY this person did or did not yield an address.

    The generator already knows: no_mx, no_candidates, all_invalid, exhausted,
    deferred, valid_found, risky_found. It used to discard that, and because
    CLEANUP_INVALID_GENERATED deletes the email row for every hard failure, a
    person who was probed and missed left no trace -- indistinguishable from one
    who was never probed at all.

    That is not a cosmetic gap. It is why the 2026-08-24 sweep was first read as
    an extraction failure when it was a pattern-evidence bug: the losing rows had
    already been deleted, so the funnel could not be attributed after the fact.

    Best-effort by design. A failure here must never lose a verified address, and
    must never leave an aborted transaction behind for the caller -- Postgres
    fails every subsequent statement on the same connection until rollback.
    """
    try:
        con.execute(
            "UPDATE people SET generation_status = ?, generation_at = ?, "
            "generation_probes = ? WHERE id = ?",
            (
                str(result.get("status") or "unknown"),
                _utc_now_iso_z(),
                int(result.get("verified") or 0),
                int(person_id),
            ),
        )
        con.commit()
    except Exception:
        log.debug(
            "could not record generation outcome",
            exc_info=True,
            extra={"person_id": person_id, "status": result.get("status")},
        )
        try:
            con.rollback()
        except Exception:
            pass


def _generate_emails_sequential(  # noqa: C901
    *,
    con: Any,
    person_id: int,
    domain: str,
    ranked_candidates: list[tuple[str, str]],
    effective_pattern: str | None,
    company_pattern: str | None,
    evidence: list[dict[str, Any]],
    max_probes: int,
    nf: str,
    nl: str,
    company_id: int | None = None,
) -> dict:
    """
    Walk the evidence-ranked candidates until one verifies.

    1. Probe candidates in order; STOP at the first `valid`. A single miss no
       longer loses the person -- that was the old one-blind-guess behaviour.
    2. Record every DECIDED outcome (valid/invalid) into the per-company pattern
       evidence. Catch-all and timeout results are recorded nowhere: they are
       not evidence either way.
    3. On a domain already known to be catch-all, probe NOTHING. Every
       local-part gets a 250 there, so the probes buy no information and only
       spend the domain's tolerance for us. Store the single best candidate and
       grade it from the pattern evidence instead.
    """
    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"

    _cleanup_env2 = os.getenv("CLEANUP_INVALID_GENERATED", "1").strip().lower()
    cleanup_enabled = _cleanup_env2 in ("1", "true", "yes")

    def _result(
        *,
        status: str,
        valid_email: str | None = None,
        valid_pattern: str | None = None,
        attempts: list[dict] | None = None,
        probes: int = 0,
        confidence: float | None = None,
        grade: str | None = None,
    ) -> dict:
        return {
            "count": 1 if valid_email else 0,
            "enqueued": 0,  # No async enqueue in sequential mode
            "verified": probes,
            "valid_email": valid_email,
            "valid_pattern": valid_pattern,
            "status": status,
            "attempts": attempts or [],
            "max_probes_per_person": max_probes,
            "only_pattern": effective_pattern,
            "company_pattern": company_pattern,
            "pattern_confidence": confidence,
            "pattern_confidence_grade": grade,
            "domain": domain,
            "person_id": person_id,
        }

    # Get MX info once for the domain
    mx_host, behavior_hint = _mx_info(domain, force=False, db_path=db_path)
    if not mx_host:
        log.warning(
            "R12 sequential: no MX host found",
            extra={"person_id": person_id, "domain": domain},
        )
        return _result(status="no_mx")

    # Get catch-all status for the domain.
    # Prefer the pre-resolved status passed via job meta from
    # task_generate_company_emails — this ensures all people on the
    # same domain see the same catch-all status (no race condition).
    tenant_id = _infer_tenant_id(con=con, company_id=company_id)
    catch_all_status: str | None = None

    try:
        _job = get_current_job()
        _meta_ca = (_job.meta or {}).get("domain_catch_all_status") if _job else None
        if isinstance(_meta_ca, str) and _meta_ca in {"catch_all", "not_catch_all"}:
            catch_all_status = _meta_ca
    except Exception:
        pass

    if catch_all_status is None:
        catch_all_status = _load_catchall_status_for_domain(db_path, domain, tenant_id=tenant_id)

    # If still unknown/stale, best-effort probe once.
    if catch_all_status not in {"catch_all", "not_catch_all"}:
        try:
            pre = _smtp_tcp25_preflight_mx(mx_host, timeout_s=3.0, redis=None)
            if bool(pre.get("ok")):
                _ensure_domain_resolution_row_for_domain(domain, tenant_id=tenant_id)
                ca_result = check_catchall_for_domain(domain)
                ca_status = (ca_result.status or "").strip().lower()
                if ca_status in {"catch_all", "not_catch_all"}:
                    catch_all_status = ca_status
        except Exception as exc:
            log.info(
                "R17: catch-all probe unavailable in sequential generator",
                extra={"domain": domain, "exc": str(exc)},
            )
            catch_all_status = None

    def _accept(
        email_addr: str,
        pattern_key: str,
        email_id: int | None,
        status: str,
        rank: int,
    ) -> None:
        """Mark a candidate as this person's address (valid or risky_catch_all)."""
        _set_email_source_note(con, email_id, f"verified:{status}:pattern={pattern_key}")
        if status == "valid":
            # Permanent: from here on a later 550 can never delete this row.
            _mark_first_valid(con, email_id)
        log.info(
            "R12 sequential: accepted %s address",
            status,
            extra={
                "person_id": person_id,
                "domain": domain,
                "email": email_addr,
                "email_id": email_id,
                "pattern": pattern_key,
                "rank": rank,
            },
        )

    # ----- CATCH-ALL: grade, do not probe -----
    if catch_all_status == "catch_all":
        if not ranked_candidates:
            return _result(status="no_candidates")

        email_addr, pattern_key = ranked_candidates[0]
        confidence, grade = catchall_confidence(evidence, pattern_key)

        email_id: int | None = None
        try:
            email_id = upsert_generated_email(
                conn=con,
                person_id=person_id,
                email=email_addr,
                domain=domain,
                source_note=f"sequential_candidate:rank=1:pattern={pattern_key}",
            )
            con.commit()
        except Exception:
            log.debug(
                "R12 sequential: failed to insert catch-all candidate",
                exc_info=True,
                extra={"person_id": person_id, "email": email_addr},
            )

        if email_id is not None:
            _persist_sequential_verification_result(
                con=con,
                email_id=email_id,
                email=email_addr,
                domain=domain,
                mx_host=mx_host,
                status="risky_catch_all",
                reason="catchall_pattern_graded",
                code=None,
                catch_all_status=catch_all_status,
                company_id=company_id,
                person_id=person_id,
                confidence=confidence,
                confidence_grade=grade,
            )
            _accept(email_addr, pattern_key, email_id, "risky_catch_all", 1)

        log.info(
            "R12 sequential: catch-all domain, graded without probing",
            extra={
                "person_id": person_id,
                "domain": domain,
                "email": email_addr,
                "pattern": pattern_key,
                "pattern_confidence": confidence,
                "pattern_confidence_grade": grade,
            },
        )
        return _result(
            status="risky_found",
            valid_email=email_addr,
            valid_pattern=pattern_key,
            attempts=[
                {
                    "email": email_addr,
                    "pattern": pattern_key,
                    "rank": 1,
                    "status": "risky_catch_all",
                    "reason": "catchall_pattern_graded",
                    "retries": 0,
                    "email_id": email_id,
                }
            ],
            confidence=confidence,
            grade=grade,
        )

    attempts: list[dict] = []
    total_probes = 0

    # ONE SMTP session for the whole plan, but only while we are GUESSING.
    # A company with a confirmed pattern lands on candidate #1, so opening a
    # session for three addresses there would spend probes it does not need.
    # With no evidence we expect to walk the list anyway, so lane A's
    # multi-RCPT session is strictly cheaper -- and it decides catch-all for
    # free, which is what makes the follow-up sanity connection unnecessary.
    batch_plan: list[str] = []
    if catch_all_status != "catch_all" and not has_trusted_pattern(evidence):
        batch_plan = [e for e, _p in ranked_candidates if not is_role_or_placeholder_email(e)]

    probed: dict[str, dict] = {}
    taught: set[str] = set()
    deferred_scheduled = False

    def _probe(addr: str) -> dict:
        if addr not in probed:
            plan = [a for a in batch_plan if a not in probed]
            if addr not in plan:
                plan = [addr]
            for res in _probe_candidates(plan, mx_host, catch_all_status):
                probed[res["email"]] = res
        return probed[addr]

    # Failures are HELD until this person proves they have a mailbox here.
    #
    # A 550 "no such user" for kellyleveque@magicspoon.com proves exactly one
    # thing: Kelly LeVeque has no mailbox at that domain. It says nothing about
    # whether the company uses `firstlast`. Recording it as pattern evidence
    # conflates a fact about a PERSON with a claim about a FORMAT.
    #
    # That conflation was measured doing real damage. Extraction legitimately
    # picks up non-employees -- advisors, brand ambassadors, testimonial names,
    # people who have left. Each one bounces on every format, and at
    # REFUTED_MIN_FAILURES=2 it took only two of them to refute a company's
    # ENTIRE vocabulary. Every remaining employee then ranked to an empty plan
    # and was never probed. Production state when this was found: 73 companies
    # fully refuted, 460 people at them never probed -- and spot-probing those
    # companies still returned valid addresses on `flast` and `first.last`,
    # the two formats their own evidence claimed were dead.
    #
    # The discriminating signal is an ACCEPT alongside the rejections. If some
    # candidate for this person is accepted and others are refused, the person
    # demonstrably exists, the domain demonstrably distinguishes mailboxes, and
    # the refusals are then genuine evidence about format. If NOTHING is
    # accepted, the whole probe set is the signature of an absent person and
    # teaches nothing.
    pending_failures: list[tuple[str, str]] = []

    def _teach(addr: str, pattern_key: str, ok: bool) -> None:
        if addr in taught:
            return
        taught.add(addr)
        if not ok:
            # Held, not discarded -- promoted by _flush_failures() on an accept.
            pending_failures.append((addr, pattern_key))
            return
        _record_pattern_outcome_safe(
            con,
            company_id=company_id,
            domain=domain,
            email=addr,
            first=nf,
            last=nl,
            ok=True,
            pattern=pattern_key,
        )

    def _flush_failures() -> None:
        """Commit the held failures. Called ONLY once a candidate has accepted.

        At that point the rejections on the same session are informative: this
        MX answered 250 for one local-part and 550 for another, so it is both
        distinguishing mailboxes and telling us which formats this company does
        not use.
        """
        for addr, pat in pending_failures:
            _record_pattern_outcome_safe(
                con,
                company_id=company_id,
                domain=domain,
                email=addr,
                first=nf,
                last=nl,
                ok=False,
                pattern=pat,
            )
        pending_failures.clear()

    def _teach_unprocessed() -> None:
        """Bank the hard failures this session already paid for.

        The loop returns at the first accept, so in production the RCPTs after
        it were discarded -- 36,320 hard failures stored exactly one row. They
        were issued on the same connection; recording them costs nothing and is
        what stops the next 5.9 people at this domain re-probing a dead format.

        These are still routed through _teach(), so they are HELD and only
        committed if this person also produced an accept.
        """
        for addr, pat in ranked_candidates:
            res = probed.get(addr)
            if res and res.get("status") == "invalid":
                _teach(addr, pat, False)

    for rank, (email_addr, pattern_key) in enumerate(ranked_candidates, 1):
        # Skip role/placeholder emails
        if is_role_or_placeholder_email(email_addr):
            attempts.append(
                {
                    "email": email_addr,
                    "pattern": pattern_key,
                    "rank": rank,
                    "status": "skipped",
                    "reason": "role_address",
                    "retries": 0,
                }
            )
            continue

        # Step 1: Probe FIRST, before inserting into DB.
        # This avoids creating email rows for permutations that are never
        # verified (which show up as "Unknown" in the dashboard).
        attempt_result = dict(_probe(email_addr))

        # An accept alongside a hard_fail on the same session proves this MX
        # distinguishes real mailboxes; that IS the catch-all measurement.
        if attempt_result.get("catchall_ruled_out") and catch_all_status != "catch_all":
            catch_all_status = "not_catch_all"

        attempt_result["pattern"] = pattern_key
        attempt_result["rank"] = rank
        attempts.append(attempt_result)
        total_probes += attempt_result.get("retries", 1)

        status = attempt_result.get("status")
        reason = attempt_result.get("reason")
        code = attempt_result.get("code")

        # Step 2: Only insert the email into DB if we got a definitive result.
        email_id = None
        # Only the deferral we are actually going to retry earns a row. The
        # siblings a greylisting MX also 4xx'd would otherwise become permanent
        # "Unknown" rows for addresses nobody will ever re-probe.
        will_defer = status == "deferred" and not deferred_scheduled
        if status in ("valid", "risky_catch_all", "invalid") or will_defer:
            try:
                email_id = upsert_generated_email(
                    conn=con,
                    person_id=person_id,
                    email=email_addr,
                    domain=domain,
                    source_note=f"sequential_candidate:rank={rank}:pattern={pattern_key}",
                )
                con.commit()
            except Exception:
                log.debug(
                    "R12 sequential: failed to insert candidate email",
                    exc_info=True,
                    extra={"person_id": person_id, "email": email_addr},
                )

        attempt_result["email_id"] = email_id

        # Step 3: A "valid" verdict is only trustworthy on a domain PROVEN not
        # to be catch-all. Some servers reject obvious garbage but accept any
        # plausible-looking local-part, which fools catch-all detection.
        if (
            status == "valid"
            and catch_all_status == "not_catch_all"
            and not attempt_result.get("catchall_ruled_out")
        ):
            try:
                import secrets as _secrets

                sanity_check_addr = f"_invalid_sanity_{_secrets.token_hex(6)}@{domain}"
                sanity_result = _verify_permutation_with_retry(
                    email_addr=sanity_check_addr,
                    mx_host=mx_host,
                    catch_all_status=None,  # Don't use cached status for sanity check
                )
                sanity_code = sanity_result.get("code")

                # If the sanity address ALSO gets 2xx, the domain is catch-all.
                if isinstance(sanity_code, int) and 200 <= sanity_code < 300:
                    log.warning(
                        "R12 sequential: catch-all sanity check FAILED"
                        " - domain accepts garbage addresses",
                        extra={
                            "person_id": person_id,
                            "domain": domain,
                            "email": email_addr,
                            "sanity_check_addr": sanity_check_addr,
                            "sanity_code": sanity_code,
                        },
                    )
                    status = "risky_catch_all"
                    reason = "rcpt_2xx_catchall_sanity_failed"
                    catch_all_status = "catch_all"  # Update for subsequent iterations
                    attempt_result["status"] = status
                    attempt_result["reason"] = reason
            except Exception:
                log.debug(
                    "R12 sequential: catch-all sanity check exception (ignoring)",
                    exc_info=True,
                    extra={"domain": domain, "email": email_addr},
                )

        # Step 4: Persist the (possibly downgraded) result, with a confidence
        # grade whenever SMTP could not decide it.
        confidence: float | None = None
        grade: str | None = None
        if status == "risky_catch_all":
            confidence, grade = catchall_confidence(evidence, pattern_key)

        # Step 4b: a server-issued 4xx is "come back later", not a verdict.
        # Requeue past the greylisting window instead of blocking the worker,
        # and let that path own the interim row rather than writing it twice.
        # ONE retry per person: a 4xx is this MX deferring US, not this mailbox,
        # so a second scheduled probe of a sibling address buys nothing. If a
        # later candidate resolves first, the pending job sees the decided row
        # and skips (_existing_definitive_status).
        just_deferred = False
        if will_defer and email_id is not None:
            just_deferred = _maybe_defer_greylist_retry(
                email_id=email_id,
                email=email_addr,
                domain=domain,
                mx_host=mx_host,
                code=code,
                message=attempt_result.get("message"),
                defer_attempt=0,
            )
            deferred_scheduled = just_deferred
            if not just_deferred:
                status = "temp_fail"
                reason = f"{reason}:not_scheduled"
                attempt_result["status"] = status
                attempt_result["reason"] = reason

        if email_id is not None and not just_deferred:
            try:
                _persist_sequential_verification_result(
                    con=con,
                    email_id=email_id,
                    email=email_addr,
                    domain=domain,
                    mx_host=mx_host,
                    status=status,
                    reason=reason,
                    code=code,
                    catch_all_status=catch_all_status,
                    company_id=company_id,
                    person_id=person_id,
                    confidence=confidence,
                    confidence_grade=grade,
                )
                con.commit()
            except Exception:
                log.debug(
                    "R12 sequential: failed to persist verification result",
                    exc_info=True,
                    extra={"person_id": person_id, "email": email_addr, "status": status},
                )

        # Every candidate the batch already answered is still worth walking --
        # those RCPTs are paid for. But do not open FRESH sessions into an MX
        # that is deferring us; that is the 6,671-probes-for-69-accepts pattern.
        if just_deferred and not batch_plan:
            break

        # Step 5: Teach the evidence store. Only decided outcomes count.
        if status in ("valid", "invalid"):
            _teach(email_addr, pattern_key, status == "valid")

        # Step 6: Stop on anything the server accepted; otherwise keep walking.
        if status in ("valid", "risky_catch_all"):
            _teach_unprocessed()
            # The accept is what makes this person's rejections informative.
            _flush_failures()
            _accept(email_addr, pattern_key, email_id, status, rank)
            return _result(
                status="valid_found" if status == "valid" else "risky_found",
                valid_email=email_addr,
                valid_pattern=pattern_key,
                attempts=attempts,
                probes=total_probes,
                confidence=confidence,
                grade=grade,
            )

        if status == "invalid":
            # Mark, then delete: the address is gone but the pattern-level
            # failure was already recorded above, which is the whole point.
            _set_email_source_note(con, email_id, f"invalid:{reason}:pattern={pattern_key}")
            # ...unless we had already CONFIRMED this address. Then the row
            # stays and simply carries the new invalid status: one 550, which a
            # throttling provider will happily hand out, must not erase a
            # verified result. It stays visible as invalid rather than vanishing.
            if email_id is not None and _was_ever_valid(con, int(email_id)):
                log.info(
                    "R12 sequential: keeping previously-confirmed address despite 550",
                    extra={"email_id": email_id, "email": email_addr, "reason": reason},
                )
                continue
            if cleanup_enabled and email_id is not None:
                try:
                    _delete_email_and_verification_results(con, int(email_id))
                    con.commit()
                except Exception:
                    log.warning(
                        "R12 sequential: failed to delete invalid email",
                        exc_info=True,
                        extra={"email_id": email_id, "email": email_addr},
                    )
                    try:
                        con.rollback()
                    except Exception:
                        pass
            log.debug(
                "R12 sequential: permutation invalid, trying next",
                extra={
                    "person_id": person_id,
                    "email": email_addr,
                    "email_id": email_id,
                    "reason": reason,
                },
            )
            continue

        # temp_fail / unknown after retries: keep the row, do not delete, do not
        # record evidence — we learned nothing about the pattern.
        _set_email_source_note(con, email_id, f"unverified:{status}:{reason}:pattern={pattern_key}")
        log.debug(
            "R12 sequential: permutation %s after retries, trying next",
            status,
            extra={
                "person_id": person_id,
                "email": email_addr,
                "retries": attempt_result.get("retries"),
            },
        )

    # Reached only when NOTHING accepted. pending_failures is therefore
    # deliberately dropped: every candidate for this person bounced, which is
    # the signature of a person who is not at this company, not proof that the
    # company uses none of these formats. Teaching here is what refuted 73
    # companies outright and left 460 of their people unprobed.
    pending_failures.clear()

    all_invalid = bool(attempts) and all(
        a.get("status") in ("invalid", "skipped") for a in attempts
    )
    final_status = "all_invalid" if all_invalid else "exhausted"
    if deferred_scheduled:
        # Not a dead end: a probe is queued and will land in verification_results.
        final_status = "deferred"

    log.info(
        "R12 sequential verification complete",
        extra={
            "person_id": person_id,
            "domain": domain,
            "status": final_status,
            "valid_email": None,
            "total_probes": total_probes,
            "attempts": len(attempts),
            "only_pattern": effective_pattern,
        },
    )
    return _result(status=final_status, attempts=attempts, probes=total_probes)


def _probe_candidates(
    emails: list[str],
    mx_host: str,
    catch_all_status: str | None,
) -> list[dict]:
    """
    Probe one or more addresses at one MX on a SINGLE SMTP session and classify
    each result for the sequential verifier.

    No sleeping, ever. The old loop slept 1s then 2s on a temp_fail and threw
    the answer away; greylisting needs 300s+, so those 20,443 probes were spent
    on an outcome that could not happen. A 4xx the server actually SPOKE (code
    and message present) now becomes "deferred": the caller persists it and
    hands it to _maybe_defer_greylist_retry, which re-probes minutes later off
    the worker thread. A connection-level temp_fail (code is None) stays
    "temp_fail" and the caller moves on to the next candidate immediately.

    When a multi-address batch contains BOTH an accept and a hard_fail, this MX
    has rejected one plausible local-part and accepted another -- which is
    exactly what the separate catch-all probe measures. That promotes the
    accept to "valid" and sets catchall_ruled_out, so the caller can skip the
    extra sanity connection it would otherwise open.
    """
    # Daily volume ceiling. The per-second RPS limiter caps BURST and had nothing
    # to say about the 2026-08-25 backfill, which issued ~8,700 probes over hours
    # -- every second within budget -- and got outbound port 25 blocked at the
    # host. Refusal returns "unknown", never "invalid": an address we declined to
    # test is untested, and grading it invalid would delete a good row.
    try:
        allowed, why = daily_probe_budget(get_redis(), mx_host, count=len(emails))
    except Exception:
        allowed, why = True, "budget_unavailable"
    if not allowed:
        log.warning(
            "SMTP daily probe budget exhausted; skipping probe",
            extra={"mx_host": mx_host, "reason": why, "count": len(emails)},
        )
        return [
            {
                "email": e,
                "status": "unknown",
                "reason": f"probe_budget:{why}",
                "code": None,
                "message": None,
                "retries": 0,
                "catchall_ruled_out": False,
            }
            for e in emails
        ]

    try:
        results = probe_rcpt_batch(
            emails,
            mx_host,
            helo_domain=SMTP_HELO_DOMAIN,
            mail_from=SMTP_MAIL_FROM,
            connect_timeout=float(SMTP_CONNECT_TIMEOUT),
            command_timeout=float(SMTP_COMMAND_TIMEOUT),
            behavior_hint=None,
        )
    except Exception as exc:
        return [
            {
                "email": e,
                "status": "unknown",
                "reason": f"exception:{type(exc).__name__}",
                "code": None,
                "message": None,
                "retries": 1,
                "catchall_ruled_out": False,
            }
            for e in emails
        ]

    seen = [r.get("category") for r in results if r.get("attempted", True)]
    discriminates = "accept" in seen and "hard_fail" in seen
    ca = catch_all_status
    if discriminates and ca != "catch_all":
        ca = "not_catch_all"

    out: list[dict] = []
    for email_addr, r in zip(emails, results, strict=True):
        code = r.get("code")
        message = r.get("message")
        status, reason = _classify_probe_for_sequential(
            code, r.get("error"), r.get("category", "unknown"), ca
        )
        if status == "temp_fail" and code is not None and message:
            status = "deferred"
            reason = (
                "greylist_deferred"
                if is_greylist_deferral(code, message)
                else "policy_temp_fail_deferred"
            )
        out.append(
            {
                "email": email_addr,
                "status": status,
                "reason": reason,
                "code": code,
                "message": message,
                "retries": 1,
                "catchall_ruled_out": discriminates,
            }
        )
    return out


def _verify_permutation_with_retry(
    email_addr: str,
    mx_host: str,
    catch_all_status: str | None,
) -> dict:
    """One address, one probe, no sleep. See _probe_candidates."""
    return _probe_candidates([email_addr], mx_host, catch_all_status)[0]


def _classify_probe_for_sequential(
    code: int | None,
    error: str | None,
    category: str,
    catch_all_status: str | None,
) -> tuple[str, str]:
    """Classify an SMTP RCPT probe for the sequential verifier (R12).

    Critical invariant: only a domain *proven* not to be catch-all can turn a
    2xx into "valid".

      - "not_catch_all" + 2xx -> valid.
      - "catch_all" + 2xx -> risky_catch_all: the server accepts every
        local-part, so the 2xx says nothing about this mailbox.
      - unknown catch-all + 2xx -> risky_catch_all as well.  We never
        established that this domain distinguishes real mailboxes, so the 2xx
        is just as unconfirmable; "valid" would assert what we did not measure.

    Mirrors src.verify.status._classify_good_rcpt, which classifies the same
    signals on the async R16/R18 path.
    """
    ca = (catch_all_status or "").strip().lower() or None

    if category == "accept":
        if ca == "not_catch_all":
            return "valid", "rcpt_2xx_accepted"
        if ca == "catch_all":
            return "risky_catch_all", "rcpt_2xx_catchall"
        return "risky_catch_all", "rcpt_2xx_catchall_unconfirmed"

    # Handle both "reject" and "hard_fail" - smtp.py returns "hard_fail" for 5xx codes
    if category in ("reject", "hard_fail"):
        return "invalid", "rcpt_rejected"

    if category == "temp_fail":
        return "temp_fail", "temp_fail"

    if category == "block":
        return "unknown", "blocked"

    # Fallback for implementations that only provide codes
    if isinstance(code, int) and 200 <= code < 300:
        if ca == "not_catch_all":
            return "valid", "rcpt_2xx_accepted"
        if ca == "catch_all":
            return "risky_catch_all", "rcpt_2xx_catchall"
        return "risky_catch_all", "rcpt_2xx_catchall_unconfirmed"

    # Fallback for implementations that only provide codes (not category)
    if isinstance(code, int) and 500 <= code < 600:
        # 5xx is a permanent failure - email is invalid
        return "invalid", "rcpt_5xx_rejected"

    if isinstance(code, int) and 400 <= code < 500:
        # 4xx is a temporary failure - retry is appropriate
        return "temp_fail", "smtp_temp_error"

    return "unknown", "unknown"


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




    This is the core "company Ã¢â€ â€™ crawl" building block used by auto-discovery.
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


def autodiscover_company(company_id: int) -> dict:  # noqa: C901
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

    job = get_current_job()
    meta = job.meta if job is not None else {}

    def _truthy(v: Any, default: bool = False) -> bool:
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
        return bool(v)

    def _maybe_check_run_completion() -> None:
        try:
            if job is None:
                return
            run_id = meta.get("run_id")
            if not run_id:
                return

            # Heartbeat first: this fires on EVERY stage, including the
            # non-terminal autodiscovery pass that takes most of the wall clock.
            _touch_run_heartbeat(run_id=str(run_id))

            if meta.get("is_terminal_stage") is False:
                return

            tenant_id = str(meta.get("tenant_id") or "dev")
            total = _safe_int(
                meta.get("total_companies") or meta.get("company_total") or meta.get("total")
            )

            _check_run_completion(
                run_id=str(run_id),
                tenant_id=tenant_id,
                company_id=int(company_id),
                total=total,
            )
        except Exception:
            return

    try:
        company = _load_company_name_and_domain(con, company_id)
        if company is None:
            result_obj.add_error("company_not_found")
            _store_result_in_job_meta(result_obj)
            _maybe_check_run_completion()
            return result_obj.to_dict()

        company_name, dom_db, fallback_domain = company

        # IMPORTANT: prefer pipeline-provided domain when present.
        dom_meta = (meta.get("domain") or meta.get("company_domain") or "").strip().lower()
        dom = dom_meta or (dom_db or "").strip().lower() or (fallback_domain or "").strip().lower()

        if not dom:
            result_obj.add_error("no_domain_for_company")
            _store_result_in_job_meta(result_obj)
            _maybe_check_run_completion()
            return result_obj.to_dict()

        result_obj.domain = dom

        # Determine AI enable (meta overrides global).
        ai_enabled = bool(AI_PEOPLE_ENABLED)
        if "ai_enabled" in meta:
            ai_enabled = _truthy(meta.get("ai_enabled"), default=ai_enabled)

        force_discovery = _truthy(
            meta.get("force_discovery") or meta.get("force") or meta.get("force_crawl"),
            default=False,
        )

        result_obj.ai_enabled = bool(ai_enabled)

        # Crawl (best-effort force flag).
        try:
            pages = crawl_domain(dom, result=result_obj, force=force_discovery)
        except TypeError:
            pages = crawl_domain(dom, result=result_obj)

        result_obj.pages_fetched = len(pages)

        # Persist pages
        if pages:
            try:
                save_pages(con, pages, company_id=company_id)  # type: ignore[call-arg]
            except TypeError:
                save_pages(con, pages)  # type: ignore[call-arg]
            con.commit()

        # Extract pass #1 (possibly AI)
        extract_payload = extract_candidates_for_company(company_id, result=result_obj)

        def _payload_counts(p: dict[str, Any]) -> tuple[int, int, int]:
            fe = int(p.get("found_candidates_email") or 0)
            fn = int(p.get("found_candidates_no_email") or 0)
            ft = int(p.get("found_candidates") or (fe + fn) or 0)
            return fe, fn, ft

        found_email_1, found_no_email_1, found_total_1 = _payload_counts(extract_payload)

        # Update counters from extract payload (best-effort).
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
        _maybe_check_run_completion()
        return result_obj.to_dict()

    except Exception as exc:
        log.exception(
            "autodiscover_company failed",
            extra={"company_id": company_id, "exc": str(exc)},
        )
        result_obj.add_error(f"{type(exc).__name__}: {exc}")
        _store_result_in_job_meta(result_obj)
        _maybe_check_run_completion()
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
    # Global gate
    if not bool(AI_PEOPLE_ENABLED):
        return False

    # Per-run overrides via RQ meta (best-effort)
    try:
        job = get_current_job()
        meta = job.meta if job is not None else {}

        def _truthy(v: Any) -> bool:
            if isinstance(v, str):
                return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            return bool(v)

        # Explicit disable for this run
        if "ai_enabled" in meta and not _truthy(meta.get("ai_enabled")):
            return False

        # Force AI for this run (ignores companies.attrs flag)
        force_val = (
            meta.get("force_ai_people")
            or meta.get("force_ai")
            or meta.get("force_discovery")
            or meta.get("force")
        )
        if _truthy(force_val):
            return True
    except Exception:
        pass

    # Default behavior: run at most once per company (persistent)
    attrs = _get_company_attrs(con, company_id)
    flag = attrs.get("ai_people_extracted")
    return not bool(flag)


def _ai_enabled_for_run() -> bool:
    """
    Policy gate: True when AI people refinement is enabled for the current run.

    This is distinct from the per-company once-only gate (ai_people_extracted).
    """
    if not bool(AI_PEOPLE_ENABLED):
        return False

    try:
        job = get_current_job()
        meta = job.meta if job is not None else {}

        def _truthy(v: Any) -> bool:
            if isinstance(v, str):
                return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            return bool(v)

        # Explicit disable for this run
        if "ai_enabled" in meta and not _truthy(meta.get("ai_enabled")):
            return False
        if "ai_people_enabled" in meta and not _truthy(meta.get("ai_people_enabled")):
            return False
    except Exception:
        pass

    return True


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
                # Only skip pages that are clearly third-party (not company employees)
                # Careers pages often list hiring managers, press releases mention execs
                if page_type in {"job_board", "testimonial"}:
                    log.debug(
                        "P1/P2 skipping extraction for page_type=%s url=%s",
                        page_type,
                        src_url,
                    )
                    continue
            except Exception:
                pass

        try:
            cands = extract_html_candidates(html_str, source_url=src_url, company_domain=dom)
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
    ai_strict: bool,
    result: AutodiscoveryResult | None = None,
) -> tuple[list[ExtractCandidate], bool]:
    """
    Optionally run AI refinement once per company.

    When ai_strict is True (AI enabled for this run), we NEVER fall back to
    heuristic/raw candidates. This prevents non-human candidates from being
    persisted when AI is expected to be authoritative.

    Returns (refined_people, ai_attempted).
    """
    if not personish_candidates:
        return ([], False) if ai_strict else (personish_candidates, False)

    if not ai_allowed_for_company:
        return ([], False) if ai_strict else (personish_candidates, False)

    if not _HAS_AI_WRAPPER or refine_candidates_with_ai is None:
        if ai_strict:
            log.debug(
                "AI enabled but wrapper missing; returning empty (strict AI-only mode)",
                extra={"domain": dom},
            )
            return [], False
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
        if ai_strict:
            log.debug(
                "AI wrapper failed; returning empty (strict AI-only mode)",
                exc_info=True,
                extra={"domain": dom, "exc": str(exc)},
            )
            return [], False

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


# Title values that mean "we never actually captured one". "Auto-discovered"
# is the legacy placeholder this function used to write; "" and None are the
# other ways the field ends up empty.
_PLACEHOLDER_TITLES: frozenset[str] = frozenset({"", "Auto-discovered"})


def _upsert_person_from_candidate(
    con: Any,
    *,
    company_id: int,
    cand: ExtractCandidate,
    full_name: str,
) -> tuple[int, bool]:
    """
    Upsert a person row for (company_id, full_name).

    This genuinely upserts. It previously returned on the existing row without
    touching any field, which made `title` WRITE-ONCE: whoever was inserted
    first without a title kept the "Auto-discovered" placeholder forever, and
    every later extraction pass -- including successful AI extractions that had
    read the title correctly -- looked the person up, found them, and discarded
    the result. 2,209 of 5,569 people carried no real title because of this,
    565 of them at companies where the extractor demonstrably DID get titles
    for their colleagues.

    So on an existing row we now fill in what is missing. We only ever fill
    blanks -- a real stored title is never overwritten by a later pass, because
    a fresh crawl is not automatically more correct than an earlier one.

    Returns (person_id, inserted_new).
    """
    cand_title = getattr(cand, "title", None)
    cand_title = str(cand_title).strip() if cand_title else None
    if cand_title in _PLACEHOLDER_TITLES:
        cand_title = None
    cand_source = getattr(cand, "source_url", None)

    row_p = con.execute(
        "SELECT id, title, source_url FROM people WHERE company_id = ? AND full_name = ?",
        (company_id, full_name),
    ).fetchone()
    if row_p:
        person_id = int(row_p[0])
        stored_title = (row_p[1] or "").strip()
        stored_source = (row_p[2] or "").strip()

        sets: list[str] = []
        vals: list[Any] = []
        if cand_title and stored_title in _PLACEHOLDER_TITLES:
            sets.append("title = ?")
            vals.append(cand_title)
        if cand_source and not stored_source:
            sets.append("source_url = ?")
            vals.append(cand_source)

        if sets:
            vals.append(person_id)
            con.execute(f"UPDATE people SET {', '.join(sets)} WHERE id = ?", tuple(vals))
        return person_id, False

    # NULL, not a magic string. The old "Auto-discovered" placeholder made the
    # gap invisible to COUNT(title) and to any "has a title" filter, which is
    # exactly how it went unnoticed.
    title_val = cand_title

    cur_p = con.execute(
        """
        INSERT INTO people (
          tenant_id,
          company_id,
          first_name,
          last_name,
          full_name,
          title,
          source_url
        )
        VALUES ('dev', ?, ?, ?, ?, ?, ?)
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

    _record_inferred_patterns_safe(
        con, company_id=company_id, dom=dom, emails=list(candidates_by_email)
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
        ai_strict = _ai_enabled_for_run()
        ai_allowed_for_company = _decide_ai_allowed_for_company(con, company_id)

        if ai_strict and not ai_allowed_for_company:
            # AI is enabled for this run, but we are not permitted to re-run it for this company.
            # Do not fall back to heuristic candidates.
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        pages_rows = _load_company_sources(con, company_id, dom)
        if not pages_rows:
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        raw_candidates = _extract_raw_candidates_from_pages(pages_rows, dom)
        if not raw_candidates:
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        role_candidates, personish_candidates = _split_role_and_personish_candidates(raw_candidates)

        role_candidates, blocked_role = _filter_blocked_sources(role_candidates, label="role")
        personish_candidates, blocked_personish = _filter_blocked_sources(
            personish_candidates, label="personish"
        )

        if ai_strict:
            # In strict AI-only mode, do not permit role/placeholder candidates through.
            role_candidates = []

        if not role_candidates and not personish_candidates:
            return _empty_extract_result(company_id=company_id, company_name=company_name, dom=dom)

        refined_people, ai_attempted = _maybe_refine_people_with_ai(
            company_name=company_name,
            dom=dom,
            fallback_domain=fallback_domain,
            personish_candidates=personish_candidates,
            ai_allowed_for_company=ai_allowed_for_company,
            ai_strict=ai_strict,
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
    """
    Best-effort column discovery.

    Primary path: Postgres information_schema (prod).
    Dev fallback: SQLite PRAGMA (if compat/dev uses SQLite).
    """
    # Postgres
    try:
        cur = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
            """,
            (table,),
        )
        rows = cur.fetchall() or []
        cols = {str(r[0]) for r in rows if r and len(r) > 0 and r[0]}
        if cols:
            return cols
    except Exception:
        pass

    # SQLite fallback
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


def _ensure_company_for_domain(  # noqa: C901
    con: Any,
    *,
    tenant_id: str,
    domain: str,
) -> tuple[int, str]:
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
