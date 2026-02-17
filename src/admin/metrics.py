# src/admin/metrics.py
"""
Admin metrics module - Compatible with db.py's SQLite/PostgreSQL abstraction.

Provides comprehensive metrics for the admin dashboard including:
- Queue/worker health (Redis/RQ)
- Verification statistics
- Company health metrics (403s, candidates, valid emails)
- User activity and run tracking
- Domain resolution statistics
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from rq import Queue, Worker

from src.config import settings
from src.db import get_conn

log = logging.getLogger(__name__)

# Prefer the existing Redis helper if it exists
try:
    from src.queueing.redis_conn import redis_connection as _redis_connection
except Exception:
    _redis_connection = None

try:
    import redis
except Exception:
    redis = None

QUEUE_NAMES: list[str] = ["ingest", "crawl", "mx", "smtp", "catchall", "export", "orchestrator"]


# ---------------------------------------------------------------------------
# Dataclasses (backwards compatible exports)
# ---------------------------------------------------------------------------


@dataclass
class QueueStats:
    name: str
    queued: int
    started: int
    failed: int


@dataclass
class WorkerStats:
    name: str
    queues: list[str]
    state: str
    last_heartbeat: dt.datetime | None


@dataclass
class VerificationStats:
    total_emails: int
    by_status: dict[str, int]
    valid_rate: float  # 0-1


@dataclass
class CostCounters:
    smtp_probes: int
    catchall_checks: int
    domains_resolved: int
    pages_crawled: int


@dataclass
class DomainStats:
    """
    Optional dataclass representation for per-domain analytics.

    The current APIs return plain dicts for JSON serialization, but this type
    is exported for callers that prefer a structured view.
    """

    domain: str
    total: int
    valid: int
    invalid: int
    risky_catch_all: int
    valid_rate: float  # 0-1


@dataclass
class TimeSeriesPoint:
    """
    Optional dataclass representation for per-day verification analytics.

    The public API uses plain dicts; this is exported for structured callers.
    """

    date: str  # "YYYY-MM-DD"
    total: int
    valid: int
    invalid: int
    risky_catch_all: int
    valid_rate: float  # 0-1


@dataclass
class CompanyHealthStats:
    """Company-level health metrics."""

    total_companies: int = 0
    companies_with_pages: int = 0
    companies_with_candidates: int = 0
    companies_with_valid_email: int = 0
    companies_403_blocked: int = 0
    companies_robots_blocked: int = 0
    companies_no_mx: int = 0
    companies_catch_all: int = 0


@dataclass
class UserRunStats:
    """Per-user run statistics."""

    user_id: str
    user_email: str | None
    runs_total: int = 0
    runs_queued: int = 0
    runs_running: int = 0
    runs_succeeded: int = 0
    runs_failed: int = 0
    runs_cancelled: int = 0
    last_run_at: str | None = None


@dataclass
class RunStatusBreakdown:
    """Run status counts."""

    total: int = 0
    queued: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0


# ---------------------------------------------------------------------------
# Redis / RQ helpers
# ---------------------------------------------------------------------------


def _get_redis_connection() -> Any | None:
    """Resolve a Redis connection."""
    if _redis_connection is not None:
        try:
            return _redis_connection()
        except Exception:
            pass

    if redis is None:
        return None

    try:
        return redis.from_url(settings.RQ_REDIS_URL)
    except Exception:
        return None


def get_queue_stats() -> tuple[list[QueueStats], list[WorkerStats]]:
    """Inspect Redis/RQ for queue and worker health."""
    queues: list[QueueStats] = []
    workers: list[WorkerStats] = []

    redis_conn = _get_redis_connection()
    if redis_conn is None:
        for name in QUEUE_NAMES:
            queues.append(QueueStats(name=name, queued=0, started=0, failed=0))
        return queues, workers

    for name in QUEUE_NAMES:
        q = Queue(name, connection=redis_conn)
        try:
            queued = q.count
            started = q.started_job_registry.count
            failed = q.failed_job_registry.count
        except Exception:
            queued = 0
            started = 0
            failed = 0

        queues.append(QueueStats(name=name, queued=queued, started=started, failed=failed))

    try:
        for w in Worker.all(connection=redis_conn):
            try:
                worker_queues = [q.name for q in w.queues]
                state = w.get_state()
                last_heartbeat = w.last_heartbeat
            except Exception:
                worker_queues = []
                state = "unknown"
                last_heartbeat = None

            workers.append(
                WorkerStats(
                    name=w.name,
                    queues=worker_queues,
                    state=state,
                    last_heartbeat=last_heartbeat,
                )
            )
    except Exception:
        pass

    return queues, workers


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _is_postgres() -> bool:
    """Check if we're using PostgreSQL."""
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


def _safe_execute(conn, query: str, params: tuple = ()) -> Any:
    """Safely execute a query."""
    try:
        return conn.execute(query, params)
    except Exception as e:
        log.debug(f"Query failed: {e}")
        return None


def _safe_fetchone(conn, query: str, params: tuple = ()) -> dict[str, Any] | None:
    """Safely execute a query and return one row as dict."""
    try:
        cur = conn.execute(query, params)
        row = cur.fetchone()
        if row is None:
            return None
        if hasattr(row, "keys"):
            return dict(row)
        if hasattr(row, "_asdict"):
            return row._asdict()
        # Tuple fallback
        cols = [d[0] for d in cur.description] if cur.description else []
        return dict(zip(cols, row, strict=False)) if cols else None
    except Exception as e:
        log.debug(f"Query failed: {e}")
        return None


def _safe_fetchall(conn, query: str, params: tuple = ()) -> list[dict[str, Any]]:
    """Safely execute a query and return all rows as dicts."""
    try:
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        result = []
        for row in rows:
            if hasattr(row, "keys"):
                result.append(dict(row))
            elif hasattr(row, "_asdict"):
                result.append(row._asdict())
            else:
                cols = [d[0] for d in cur.description] if cur.description else []
                if cols:
                    result.append(dict(zip(cols, row, strict=False)))
        return result
    except Exception as e:
        log.debug(f"Query failed: {e}")
        return []


def _row_int(row: dict[str, Any] | None, key: str, default: int = 0) -> int:
    """Extract int from row dict."""
    if row is None:
        return default
    val = row.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _get_window_date(window_days: int) -> str:
    """Get ISO date string for N days ago."""
    d = dt.datetime.now(dt.UTC) - dt.timedelta(days=window_days)
    return d.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Core metrics functions
# ---------------------------------------------------------------------------


def get_verification_stats(conn: Any) -> VerificationStats:
    """Compute verification distribution from emails/verification_results."""
    # Try using v_emails_latest view first, fall back to direct query
    row_data = _safe_fetchall(
        conn,
        """
        SELECT verify_status, COUNT(*) AS n
        FROM v_emails_latest
        WHERE verify_status IS NOT NULL
        GROUP BY verify_status
        """,
    )

    if not row_data:
        # Fallback: query verification_results directly
        row_data = _safe_fetchall(
            conn,
            """
            SELECT verify_status, COUNT(*) AS n
            FROM verification_results
            WHERE verify_status IS NOT NULL
            GROUP BY verify_status
            """,
        )

    by_status: dict[str, int] = {}
    for row in row_data:
        status = row.get("verify_status")
        count = _row_int(row, "n", 0)
        if status:
            by_status[str(status)] = count

    total_emails = sum(by_status.values())
    denom = sum(by_status.get(x, 0) for x in ("valid", "invalid", "risky_catch_all"))
    valid = by_status.get("valid", 0)
    valid_rate = float(valid / denom) if denom else 0.0

    return VerificationStats(
        total_emails=total_emails,
        by_status=by_status,
        valid_rate=valid_rate,
    )


def get_cost_counters(conn: Any) -> CostCounters:
    """Compute cost proxy counters."""
    smtp_row = _safe_fetchone(conn, "SELECT COUNT(*) AS n FROM verification_results")

    # Catch-all checks: count rows where catch_all_status is set (not null)
    catchall_row = _safe_fetchone(
        conn, "SELECT COUNT(*) AS n FROM domain_resolutions WHERE catch_all_status IS NOT NULL"
    )

    # Domains resolved: count all domain_resolutions rows
    resolved_row = _safe_fetchone(conn, "SELECT COUNT(*) AS n FROM domain_resolutions")

    pages_row = _safe_fetchone(conn, "SELECT COUNT(*) AS n FROM sources")

    return CostCounters(
        smtp_probes=_row_int(smtp_row, "n"),
        catchall_checks=_row_int(catchall_row, "n"),
        domains_resolved=_row_int(resolved_row, "n"),
        pages_crawled=_row_int(pages_row, "n"),
    )


def get_company_health_stats(conn: Any) -> CompanyHealthStats:
    """Get company-level health metrics."""
    stats = CompanyHealthStats()

    # Total companies
    row = _safe_fetchone(conn, "SELECT COUNT(*) AS n FROM companies")
    stats.total_companies = _row_int(row, "n")

    # Companies with pages
    row = _safe_fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT company_id) AS n
        FROM sources
        WHERE company_id IS NOT NULL
        """,
    )
    stats.companies_with_pages = _row_int(row, "n")

    # Companies with candidates (people)
    row = _safe_fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT company_id) AS n
        FROM people
        WHERE company_id IS NOT NULL
        """,
    )
    stats.companies_with_candidates = _row_int(row, "n")

    # Companies with at least 1 valid email
    row = _safe_fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT e.company_id) AS n
        FROM emails e
        JOIN verification_results vr ON vr.email_id = e.id
        WHERE vr.verify_status = 'valid'
        """,
    )
    stats.companies_with_valid_email = _row_int(row, "n")

    # Try to get 403/robots stats from run_metrics if available
    row = _safe_fetchone(
        conn,
        """
        SELECT
            COALESCE(SUM(companies_403_blocked), 0) AS blocked_403,
            COALESCE(SUM(companies_robots_blocked), 0) AS blocked_robots
        FROM run_metrics
        """,
    )
    if row:
        stats.companies_403_blocked = _row_int(row, "blocked_403")
        stats.companies_robots_blocked = _row_int(row, "blocked_robots")

    # Domains with no MX (from domain_resolutions)
    row = _safe_fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT company_id) AS n
        FROM domain_resolutions
        WHERE catch_all_status = 'no_mx'
        """,
    )
    stats.companies_no_mx = _row_int(row, "n")

    # Companies with catch-all domains
    row = _safe_fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT company_id) AS n
        FROM domain_resolutions
        WHERE catch_all_status = 'catch_all'
        """,
    )
    stats.companies_catch_all = _row_int(row, "n")

    return stats


def get_zero_candidate_companies(conn: Any) -> list[dict[str, Any]]:
    """
    Return companies that have crawled pages in ``sources`` but zero rows
    in ``people``.  Each result includes the company's crawled page URLs
    so the admin can inspect what was fetched.

    Returns a list of dicts:
        {
            "company_id": int,
            "company_name": str,
            "domain": str,
            "page_count": int,
            "pages": [{"source_url": str, "fetched_at": str | None}, ...],
        }
    Ordered by page_count DESC (most-crawled first).
    """
    # Step 1: identify companies with pages but no people
    company_rows = _safe_fetchall(
        conn,
        """
        SELECT
            c.id           AS company_id,
            c.name         AS company_name,
            COALESCE(c.official_domain, c.domain, c.user_supplied_domain, '') AS domain,
            COUNT(s.id)    AS page_count
        FROM companies c
        JOIN sources s ON s.company_id = c.id
        WHERE c.id NOT IN (
            SELECT DISTINCT company_id
            FROM people
            WHERE company_id IS NOT NULL
        )
        GROUP BY c.id, c.name
        ORDER BY page_count DESC
        """,
    )

    if not company_rows:
        return []

    # Step 2: batch-fetch page URLs for these companies
    company_ids = [row["company_id"] for row in company_rows]

    placeholders = ", ".join("?" for _ in company_ids)
    page_rows = _safe_fetchall(
        conn,
        f"""
        SELECT company_id, source_url, fetched_at
        FROM sources
        WHERE company_id IN ({placeholders})
        ORDER BY company_id, fetched_at DESC
        """,
        tuple(company_ids),
    )

    # Group pages by company_id
    pages_by_company: dict[int, list[dict[str, Any]]] = {}
    for pr in page_rows:
        cid = pr.get("company_id")
        if cid is None:
            continue
        pages_by_company.setdefault(int(cid), []).append(
            {
                "source_url": pr.get("source_url", ""),
                "fetched_at": pr.get("fetched_at"),
            }
        )

    # Step 3: assemble results
    result: list[dict[str, Any]] = []
    for row in company_rows:
        cid = int(row["company_id"])
        result.append(
            {
                "company_id": cid,
                "company_name": row.get("company_name") or "",
                "domain": row.get("domain") or "",
                "page_count": _row_int(row, "page_count"),
                "pages": pages_by_company.get(cid, []),
            }
        )

    return result


def get_run_status_breakdown(conn: Any) -> RunStatusBreakdown:
    """Get breakdown of run statuses."""
    breakdown = RunStatusBreakdown()

    rows = _safe_fetchall(
        conn,
        """
        SELECT status, COUNT(*) AS n
        FROM runs
        GROUP BY status
        """,
    )

    for row in rows:
        status = row.get("status", "")
        count = _row_int(row, "n")
        breakdown.total += count

        if status == "queued":
            breakdown.queued = count
        elif status == "running":
            breakdown.running = count
        elif status == "succeeded":
            breakdown.succeeded = count
        elif status == "failed":
            breakdown.failed = count
        elif status == "cancelled":
            breakdown.cancelled = count

    return breakdown


def get_user_run_stats(conn: Any, limit: int = 20) -> list[UserRunStats]:
    """Get per-user run statistics."""
    rows = _safe_fetchall(
        conn,
        """
        SELECT
            r.user_id,
            u.email AS user_email,
            COUNT(*) AS runs_total,
            SUM(CASE WHEN r.status = 'queued' THEN 1 ELSE 0 END) AS runs_queued,
            SUM(CASE WHEN r.status = 'running' THEN 1 ELSE 0 END) AS runs_running,
            SUM(CASE WHEN r.status = 'succeeded' THEN 1 ELSE 0 END) AS runs_succeeded,
            SUM(CASE WHEN r.status = 'failed' THEN 1 ELSE 0 END) AS runs_failed,
            SUM(CASE WHEN r.status = 'cancelled' THEN 1 ELSE 0 END) AS runs_cancelled,
            MAX(r.created_at) AS last_run_at
        FROM runs r
        LEFT JOIN users u ON r.user_id = u.id
        WHERE r.user_id IS NOT NULL
        GROUP BY r.user_id, u.email
        ORDER BY runs_total DESC
        LIMIT ?
        """,
        (limit,),
    )

    result = []
    for row in rows:
        result.append(
            UserRunStats(
                user_id=row.get("user_id") or "unknown",
                user_email=row.get("user_email"),
                runs_total=_row_int(row, "runs_total"),
                runs_queued=_row_int(row, "runs_queued"),
                runs_running=_row_int(row, "runs_running"),
                runs_succeeded=_row_int(row, "runs_succeeded"),
                runs_failed=_row_int(row, "runs_failed"),
                runs_cancelled=_row_int(row, "runs_cancelled"),
                last_run_at=row.get("last_run_at"),
            )
        )

    return result


def get_recent_runs(conn: Any, limit: int = 10) -> list[dict[str, Any]]:
    """Get most recent runs with basic info."""
    rows = _safe_fetchall(
        conn,
        """
        SELECT
            r.id,
            r.status,
            r.label,
            r.created_at,
            r.started_at,
            r.finished_at,
            r.error,
            u.email AS user_email,
            r.domains_json
        FROM runs r
        LEFT JOIN users u ON r.user_id = u.id
        ORDER BY r.created_at DESC
        LIMIT ?
        """,
        (limit,),
    )

    result = []
    for row in rows:
        domains_json = row.get("domains_json", "[]")
        try:
            domains = json.loads(domains_json) if isinstance(domains_json, str) else domains_json
            domain_count = len(domains) if isinstance(domains, list) else 0
        except Exception:
            domain_count = 0

        result.append(
            {
                "id": row.get("id"),
                "status": row.get("status"),
                "label": row.get("label"),
                "user_email": row.get("user_email"),
                "domain_count": domain_count,
                "created_at": row.get("created_at"),
                "started_at": row.get("started_at"),
                "finished_at": row.get("finished_at"),
                "error": row.get("error"),
            }
        )

    return result


def get_verification_time_series(
    conn: Any,
    window_days: int = 30,
) -> list[dict[str, Any]]:
    """Return daily verification counts."""
    if window_days <= 0:
        window_days = 30

    # Use a date string for the comparison (works on both SQLite and Postgres)
    cutoff_date = _get_window_date(window_days)

    rows = _safe_fetchall(
        conn,
        """
        SELECT
            DATE(COALESCE(verified_at, checked_at)) AS day,
            verify_status,
            COUNT(*) AS n
        FROM verification_results
        WHERE DATE(COALESCE(verified_at, checked_at)) >= ?
          AND verify_status IS NOT NULL
        GROUP BY day, verify_status
        ORDER BY day ASC
        """,
        (cutoff_date,),
    )

    by_day: dict[str, dict[str, int]] = {}
    for row in rows:
        day = str(row.get("day", ""))
        status = row.get("verify_status")
        n = _row_int(row, "n")

        bucket = by_day.setdefault(
            day, {"total": 0, "valid": 0, "invalid": 0, "risky_catch_all": 0}
        )
        bucket["total"] += n
        if status in bucket:
            bucket[status] += n

    points: list[dict[str, Any]] = []
    for day in sorted(by_day.keys()):
        bucket = by_day[day]
        total = bucket["total"]
        valid = bucket["valid"]
        invalid = bucket["invalid"]
        risky = bucket["risky_catch_all"]
        denom = valid + invalid + risky
        valid_rate = float(valid / denom) if denom else 0.0
        points.append(
            {
                "date": day,
                "total": total,
                "valid": valid,
                "invalid": invalid,
                "risky_catch_all": risky,
                "valid_rate": valid_rate,
            }
        )

    return points


def get_domain_breakdown(
    conn: Any,
    window_days: int = 30,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """Return per-domain verification breakdown."""
    if window_days <= 0:
        window_days = 30
    if top_n <= 0:
        top_n = 20

    cutoff_date = _get_window_date(window_days)

    # Use LOWER and SUBSTRING for email domain extraction (works on both)
    rows = _safe_fetchall(
        conn,
        """
        SELECT
            LOWER(SUBSTRING(e.email FROM POSITION('@' IN e.email) + 1)) AS domain,
            vr.verify_status,
            COUNT(*) AS n
        FROM emails e
        JOIN verification_results vr ON vr.email_id = e.id
        WHERE DATE(COALESCE(vr.verified_at, vr.checked_at)) >= ?
        GROUP BY domain, vr.verify_status
        """,
        (cutoff_date,),
    )

    by_domain: dict[str, dict[str, int]] = {}
    for row in rows:
        domain = row.get("domain", "")
        status = row.get("verify_status")
        n = _row_int(row, "n")

        bucket = by_domain.setdefault(
            domain, {"total": 0, "valid": 0, "invalid": 0, "risky_catch_all": 0}
        )
        bucket["total"] += n
        if status and status in bucket:
            bucket[status] += n

    items = sorted(
        by_domain.items(),
        key=lambda kv: kv[1]["total"],
        reverse=True,
    )[:top_n]

    out: list[dict[str, Any]] = []
    for domain, bucket in items:
        total = bucket["total"]
        valid = bucket["valid"]
        invalid = bucket["invalid"]
        risky = bucket["risky_catch_all"]
        denom = valid + invalid + risky
        valid_rate = float(valid / denom) if denom else 0.0
        out.append(
            {
                "domain": domain,
                "total": total,
                "valid": valid,
                "invalid": invalid,
                "risky_catch_all": risky,
                "valid_rate": valid_rate,
            }
        )
    return out


def get_error_breakdown(conn: Any, top_n: int = 20) -> dict[str, int]:
    """Return breakdown of verification errors."""
    if top_n <= 0:
        top_n = 20

    rows = _safe_fetchall(
        conn,
        """
        SELECT
            COALESCE(verify_reason, reason, status, 'unknown') AS key,
            COUNT(*) AS n
        FROM verification_results
        GROUP BY key
        ORDER BY n DESC
        LIMIT ?
        """,
        (top_n,),
    )

    out: dict[str, int] = {}
    for row in rows:
        key = row.get("key", "unknown")
        n = _row_int(row, "n")
        out[str(key)] = n
    return out


# ---------------------------------------------------------------------------
# Main summary functions (API endpoints use these)
# ---------------------------------------------------------------------------


def get_admin_summary(conn: Any | None = None) -> dict[str, Any]:
    """High-level summary for admin API."""
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    try:
        queues, workers = get_queue_stats()
        verif = get_verification_stats(conn)
        cost = get_cost_counters(conn)
        company_health = get_company_health_stats(conn)
        run_status = get_run_status_breakdown(conn)
        user_stats = get_user_run_stats(conn, limit=15)
        recent_runs = get_recent_runs(conn, limit=10)

        return {
            "queues": [q.__dict__ for q in queues],
            "workers": [
                {
                    **w.__dict__,
                    "last_heartbeat": w.last_heartbeat.isoformat() if w.last_heartbeat else None,
                }
                for w in workers
            ],
            "verification": {
                "total_emails": verif.total_emails,
                "by_status": verif.by_status,
                "valid_rate": verif.valid_rate,
            },
            "costs": {
                "smtp_probes": cost.smtp_probes,
                "catchall_checks": cost.catchall_checks,
                "domains_resolved": cost.domains_resolved,
                "pages_crawled": cost.pages_crawled,
            },
            "company_health": {
                "total_companies": company_health.total_companies,
                "companies_with_pages": company_health.companies_with_pages,
                "companies_with_candidates": company_health.companies_with_candidates,
                "companies_with_valid_email": company_health.companies_with_valid_email,
                "companies_403_blocked": company_health.companies_403_blocked,
                "companies_robots_blocked": company_health.companies_robots_blocked,
                "companies_no_mx": company_health.companies_no_mx,
                "companies_catch_all": company_health.companies_catch_all,
            },
            "run_status": {
                "total": run_status.total,
                "queued": run_status.queued,
                "running": run_status.running,
                "succeeded": run_status.succeeded,
                "failed": run_status.failed,
                "cancelled": run_status.cancelled,
            },
            "user_stats": [
                {
                    "user_id": u.user_id,
                    "user_email": u.user_email,
                    "runs_total": u.runs_total,
                    "runs_queued": u.runs_queued,
                    "runs_running": u.runs_running,
                    "runs_succeeded": u.runs_succeeded,
                    "runs_failed": u.runs_failed,
                    "runs_cancelled": u.runs_cancelled,
                    "last_run_at": u.last_run_at,
                }
                for u in user_stats
            ],
            "recent_runs": recent_runs,
        }
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def get_analytics_summary(
    conn: Any | None = None,
    window_days: int = 30,
    top_domains: int = 20,
    top_errors: int = 20,
) -> dict[str, Any]:
    """Aggregate analytics for admin dashboard."""
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    try:
        ts = get_verification_time_series(conn, window_days=window_days)
        domains = get_domain_breakdown(conn, window_days=window_days, top_n=top_domains)
        errors = get_error_breakdown(conn, top_n=top_errors)

        return {
            "verification_time_series": ts,
            "domain_breakdown": domains,
            "error_breakdown": errors,
        }
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass
