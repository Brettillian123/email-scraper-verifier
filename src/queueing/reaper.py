# src/queueing/reaper.py
"""
Run durability: reap stale runs, with a bounded run-level retry.

Why this exists: one run has been stuck in 'running' since 2026-03-02, and a
59-domain run failed 2026-03-03 and was never retried. Nothing else in the
codebase times a run out or retries one.

Heartbeat: runs.updated_at, falling back to started_at then created_at.

The worker path keeps that heartbeat moving. Fan-out itself finishes in seconds
(pipeline_v2._update_run_row every 10 domains), but the crawl/extract/generate/
verify work then runs for hours in separate per-company RQ jobs, and each of
those beats on completion via tasks._touch_run_heartbeat():
  - tasks.autodiscover_company -> _maybe_check_run_completion()
  - pipeline_v2.{task_generate_company_emails,verify_company_emails}
    -> _maybe_mark_run_complete_from_job()
Both beat before their is_terminal_stage gate, so non-terminal stages -- notably
autodiscovery, which owns most of the wall clock -- count as liveness. One tiny
guarded UPDATE per company per stage, best-effort, never fails its job. The
run leaves 'running' the moment the last terminal-stage job reports, so the
per-person probe tail after that is outside this reaper's window by construction.

`requeue` still defaults to False, for a different reason than it used to.
A heartbeat cannot distinguish a dead run from one whose jobs are merely
queued behind another run's backlog: pipeline_start_v2 sets status='running'
at fan-out (pipeline_v2.py:1729), before any worker touches a company, so a
run stuck behind a long queue looks exactly like a dead one. Marking such a
run failed mislabels one row and is recoverable; re-enqueueing it duplicates
every crawl and SMTP probe in it. So requeue -- and
db/migrations/2026_08_20_run_reaper.sql, which activates it -- stays an
explicit operator opt-in for runs known to be dead.

Any queued/running run whose heartbeat is older than RUN_STALE_TIMEOUT_S is
marked failed, or re-enqueued if the caller explicitly opts in and
runs.retry_count < RUN_MAX_RETRIES.

Idempotent + concurrency-safe: every write carries `AND status IN
('queued','running')`, so a second reaper (or a repeat pass) arriving after the
row already moved updates 0 rows and does nothing.

Cron / scheduler entry point (no new deployable, no rq-scheduler):
    python -m src.queueing.reaper                 # reap stale runs, mark them failed
    python -m src.queueing.reaper --dry-run       # show what it would do
    python -m src.queueing.reaper --requeue       # ALSO re-enqueue (see caveat above)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from src.db import get_conn

# Reuse rather than re-derive: _parse_utc_dt already handles every timestamp
# format these TEXT columns hold (ISO-Z, ISO-offset, "YYYY-MM-DD HH:MM:SS").
from src.queueing.pipeline_v2 import (
    DEFAULT_JOB_TIMEOUT,
    _parse_utc_dt,
    _utc_now_iso,
    pipeline_start,
)

log = logging.getLogger(__name__)

# Default well above DEFAULT_JOB_TIMEOUT (1800s, RQ's own per-job kill timeout)
# plus the RETRY_SCHEDULE backoff ceiling (5+15+45+90+180 = 335s), so the reaper
# never races a job RQ itself has not given up on yet.
DEFAULT_STALE_TIMEOUT_S = 3600
DEFAULT_MAX_RETRIES = 2

_ACTIVE_GUARD = "status IN ('queued','running')"


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


def _has_retry_count(con: Any) -> bool:
    """True if the 2026_08_20_run_reaper migration has been applied."""
    try:
        con.execute("SELECT retry_count FROM runs LIMIT 1").fetchone()
        return True
    except Exception:
        # Postgres aborts the transaction on a failed statement; clear it.
        try:
            con.rollback()
        except Exception:
            pass
        return False


def _enqueue_run(run_id: str, tenant_id: str, job_timeout: int) -> None:
    """Re-enqueue the orchestrator job (same pattern as app._enqueue_pipeline_start).

    Unlike that caller this passes job_timeout: without it RQ applies its
    built-in 180s default to the orchestrator instead of the pipeline's
    configured DEFAULT_JOB_TIMEOUT.
    """
    from redis import Redis
    from rq import Queue

    url = os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
    queue_name = (os.getenv("RUNS_QUEUE_NAME") or "orchestrator").strip()
    q = Queue(queue_name, connection=Redis.from_url(url))
    q.enqueue(pipeline_start, run_id=run_id, tenant_id=tenant_id, job_timeout=job_timeout)


def _mark_failed(con: Any, run_id: str, error: str) -> int:
    now = _utc_now_iso()
    cur = con.execute(
        f"""
        UPDATE runs SET status = 'failed', error = ?, finished_at = ?, updated_at = ?
         WHERE id = ? AND {_ACTIVE_GUARD}
        """,
        (error, now, now, run_id),
    )
    con.commit()
    return int(cur.rowcount or 0)


def _mark_requeued(con: Any, run_id: str) -> int:
    cur = con.execute(
        f"""
        UPDATE runs
           SET status = 'queued',
               retry_count = retry_count + 1,
               error = NULL,
               started_at = NULL,
               finished_at = NULL,
               updated_at = ?
         WHERE id = ? AND {_ACTIVE_GUARD}
        """,
        (_utc_now_iso(), run_id),
    )
    con.commit()
    return int(cur.rowcount or 0)


def reap_stale_runs(
    *,
    tenant_id: str | None = None,
    timeout_s: int | None = None,
    max_retries: int | None = None,
    requeue: bool = False,  # opt-in only: see the module docstring
    dry_run: bool = False,
) -> dict[str, Any]:
    """Fail or re-enqueue queued/running runs with no heartbeat in timeout_s.

    Returns {"checked", "stale", "requeued": [ids], "failed": [ids], "no_heartbeat": [ids]}.
    """
    timeout_s = timeout_s or _env_int("RUN_STALE_TIMEOUT_S", DEFAULT_STALE_TIMEOUT_S)
    if max_retries is None:
        max_retries = _env_int("RUN_MAX_RETRIES", DEFAULT_MAX_RETRIES)
    job_timeout = _env_int("RUN_JOB_TIMEOUT_S", DEFAULT_JOB_TIMEOUT)
    cutoff = datetime.now(UTC) - timedelta(seconds=timeout_s)

    out: dict[str, Any] = {
        "checked": 0,
        "stale": 0,
        "requeued": [],
        "failed": [],
        "no_heartbeat": [],
        "timeout_s": timeout_s,
        "dry_run": dry_run,
    }

    con = get_conn()
    try:
        has_retry = _has_retry_count(con)
        cols = "id, tenant_id, updated_at, started_at, created_at"
        if has_retry:
            cols += ", retry_count"
        sql = f"SELECT {cols} FROM runs WHERE {_ACTIVE_GUARD}"
        params: list[Any] = []
        if tenant_id:
            sql += " AND tenant_id = ?"
            params.append(tenant_id)

        rows = con.execute(sql, tuple(params)).fetchall() or []
        out["checked"] = len(rows)

        for r in rows:
            run_id, run_tenant = str(r[0]), str(r[1])
            beat = next(
                (d for d in (_parse_utc_dt(r[2]), _parse_utc_dt(r[3]), _parse_utc_dt(r[4])) if d),
                None,
            )
            if beat is None:
                # No usable timestamp at all: report it, never guess it is dead.
                out["no_heartbeat"].append(run_id)
                continue
            if beat >= cutoff:
                continue

            out["stale"] += 1
            tries = int(r[5] or 0) if has_retry else 0
            can_retry = requeue and has_retry and tries < max_retries
            age_s = int((datetime.now(UTC) - beat).total_seconds())

            if dry_run:
                out["requeued" if can_retry else "failed"].append(run_id)
                continue

            if can_retry:
                if not _mark_requeued(con, run_id):
                    continue  # another reaper got there first
                try:
                    _enqueue_run(run_id, run_tenant, job_timeout)
                    out["requeued"].append(run_id)
                    log.warning(
                        "reaper: re-enqueued stale run %s (attempt %d/%d, idle %ss)",
                        run_id,
                        tries + 1,
                        max_retries,
                        age_s,
                    )
                except Exception as exc:
                    _mark_failed(
                        con, run_id, f"reaped: re-enqueue failed: {type(exc).__name__}: {exc}"
                    )
                    out["failed"].append(run_id)
                    log.exception("reaper: re-enqueue failed for run %s", run_id)
                continue

            reason = f"reaped: no heartbeat for {age_s}s (limit {timeout_s}s)"
            if requeue and has_retry:
                reason += f"; retry budget exhausted ({tries}/{max_retries})"
            if _mark_failed(con, run_id, reason):
                out["failed"].append(run_id)
                log.warning("reaper: failed stale run %s (%s)", run_id, reason)

        return out
    finally:
        try:
            con.close()
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Reap stale pipeline runs.")
    ap.add_argument("--tenant", default=None, help="limit to one tenant_id")
    ap.add_argument(
        "--timeout-s", type=int, default=None, help="staleness threshold (env RUN_STALE_TIMEOUT_S)"
    )
    ap.add_argument(
        "--max-retries", type=int, default=None, help="run-level retry budget (env RUN_MAX_RETRIES)"
    )
    ap.add_argument(
        "--requeue",
        action="store_true",
        help=(
            "ALSO re-enqueue stale runs (default: only mark them failed). "
            "Use only for runs known to be dead: a run whose jobs are merely "
            "backed up behind another run looks stale too, and re-enqueueing it "
            "duplicates every crawl and SMTP probe in it."
        ),
    )
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = reap_stale_runs(
        tenant_id=args.tenant,
        timeout_s=args.timeout_s,
        max_retries=args.max_retries,
        requeue=args.requeue,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
