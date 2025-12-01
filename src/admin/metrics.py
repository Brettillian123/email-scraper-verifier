# src/admin/metrics.py
from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Any

from rq import Queue, Worker

from src.db import get_connection

# Prefer the existing Redis helper if it exists, but don't crash if its
# name/signature changes; fall back to redis.from_url(settings.RQ_REDIS_URL).
try:  # pragma: no cover - defensive shim
    from src.queueing.redis_conn import (
        redis_connection as _redis_connection,  # type: ignore[attr-defined]
    )
except Exception:  # pragma: no cover
    _redis_connection = None  # type: ignore[assignment]

try:  # pragma: no cover - import guarded for environments without Redis
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from src.config import settings

QUEUE_NAMES: list[str] = ["ingest", "crawl", "mx", "smtp", "catchall", "export"]


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
    state: str  # e.g. "busy", "idle"
    last_heartbeat: dt.datetime | None


@dataclass
class VerificationStats:
    total_emails: int
    by_status: dict[str, int]
    valid_rate: float  # 0–1


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
    valid_rate: float  # 0–1


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
    valid_rate: float  # 0–1


def _get_redis_connection() -> Any | None:
    """
    Resolve a Redis connection without hard-coding the helper name.

    Preference order:
      1. src.queueing.redis_conn.redis_connection() if available
      2. redis.from_url(settings.RQ_REDIS_URL)

    Returns None if Redis is unavailable; callers must handle that.
    """
    # Try the project helper first, if present.
    if _redis_connection is not None:
        try:
            return _redis_connection()
        except Exception:
            # Fall through to direct redis.from_url()
            pass

    # Fallback: construct directly from URL.
    if redis is None:
        return None

    try:
        return redis.from_url(settings.RQ_REDIS_URL)
    except Exception:
        return None


def get_queue_stats() -> tuple[list[QueueStats], list[WorkerStats]]:
    """
    Inspect Redis / RQ to compute basic queue and worker health.

    This function is intentionally defensive: if Redis is unavailable or RQ
    raises, it will return zeroed stats instead of crashing the caller.
    """
    queues: list[QueueStats] = []
    workers: list[WorkerStats] = []

    redis_conn = _get_redis_connection()
    if redis_conn is None:
        # If we cannot even construct a Redis connection, return empty stats.
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

        queues.append(
            QueueStats(
                name=name,
                queued=queued,
                started=started,
                failed=failed,
            )
        )

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
        # If Worker.all() fails (e.g. Redis down), we just return the queues.
        pass

    return queues, workers


def get_verification_stats(conn: sqlite3.Connection) -> VerificationStats:
    """
    Compute verification distribution and valid rate from the lead surface.

    We use v_emails_latest as the canonical "lead" view so the stats line up
    with exportable leads rather than raw verification rows.
    """
    try:
        cur = conn.execute(
            """
            SELECT verify_status, COUNT(*) AS n
            FROM v_emails_latest
            GROUP BY verify_status
            """
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        # Missing view or other DB issue – treat as "no data yet".
        return VerificationStats(total_emails=0, by_status={}, valid_rate=0.0)

    by_status: dict[str, int] = {}
    for row in rows:
        try:
            status = row["verify_status"]
            count = row["n"]
        except (KeyError, IndexError, TypeError):
            continue
        by_status[str(status)] = int(count)

    total_emails = sum(by_status.values())

    denom = sum(by_status.get(x, 0) for x in ("valid", "invalid", "risky_catch_all"))
    valid = by_status.get("valid", 0)
    valid_rate = float(valid / denom) if denom else 0.0

    return VerificationStats(
        total_emails=total_emails,
        by_status=by_status,
        valid_rate=valid_rate,
    )


def get_cost_counters(conn: sqlite3.Connection) -> CostCounters:
    """
    Compute simple cost proxy counters from existing tables.

    These are not hard currency costs, just volume proxies:
    - smtp_probes: total verification_result rows
    - catchall_checks: domain_resolutions rows with a catch-all check
    - domains_resolved: domain_resolutions rows with a resolved_at timestamp
    - pages_crawled: sources rows (HTML pages fetched by the crawler)
    """
    try:
        smtp_probes_row = conn.execute("SELECT COUNT(*) AS n FROM verification_results").fetchone()
        catchall_row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM domain_resolutions
            WHERE catch_all_checked_at IS NOT NULL
            """
        ).fetchone()
        resolved_row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM domain_resolutions
            WHERE resolved_at IS NOT NULL
            """
        ).fetchone()
        pages_row = conn.execute("SELECT COUNT(*) AS n FROM sources").fetchone()
    except sqlite3.Error:
        # Any missing table / view => zeroed cost counters.
        return CostCounters(
            smtp_probes=0,
            catchall_checks=0,
            domains_resolved=0,
            pages_crawled=0,
        )

    def _row_count(row: Any) -> int:
        if row is None:
            return 0
        try:
            if isinstance(row, sqlite3.Row):
                # Prefer alias "n" if present; fall back to first column.
                if "n" in row.keys():
                    return int(row["n"] or 0)
                return int(row[0] or 0)
            return int(row[0] or 0)
        except Exception:
            return 0

    smtp_probes = _row_count(smtp_probes_row)
    catchall_checks = _row_count(catchall_row)
    domains_resolved = _row_count(resolved_row)
    pages_crawled = _row_count(pages_row)

    return CostCounters(
        smtp_probes=smtp_probes,
        catchall_checks=catchall_checks,
        domains_resolved=domains_resolved,
        pages_crawled=pages_crawled,
    )


# ---------------------------------------------------------------------------
# O17: analytics helpers
# ---------------------------------------------------------------------------


def get_verification_time_series(
    conn: sqlite3.Connection,
    window_days: int = 30,
) -> list[dict[str, Any]]:
    """
    Return a per-day verification time series over the given rolling window.

    Each entry:
      {
        "date": "YYYY-MM-DD",
        "total": int,
        "valid": int,
        "invalid": int,
        "risky_catch_all": int,
        "valid_rate": float (0–1),
      }

    Only rows with a non-NULL verify_status are included; unclassified rows
    are ignored here (but still contribute to other metrics).
    """
    if window_days <= 0:
        window_days = 30

    try:
        rows = conn.execute(
            """
            SELECT
              date(COALESCE(verified_at, checked_at)) AS day,
              verify_status,
              COUNT(*) AS n
            FROM verification_results
            WHERE COALESCE(verified_at, checked_at) >= datetime('now', ?)
              AND verify_status IS NOT NULL
            GROUP BY day, verify_status
            ORDER BY day ASC
            """,
            (f"-{int(window_days)} days",),
        ).fetchall()
    except sqlite3.Error:
        return []

    by_day: dict[str, dict[str, int]] = {}
    for row in rows:
        try:
            day = row["day"]
            status = row["verify_status"]
            n = int(row["n"])
        except (KeyError, IndexError, TypeError, ValueError):
            continue

        bucket = by_day.setdefault(
            day,
            {
                "total": 0,
                "valid": 0,
                "invalid": 0,
                "risky_catch_all": 0,
            },
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
    conn: sqlite3.Connection,
    window_days: int = 30,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """
    Return a per-domain breakdown of verification outcomes.

    Each entry:
      {
        "domain": "example.com",
        "total": int,              # includes unverified leads
        "valid": int,
        "invalid": int,
        "risky_catch_all": int,
        "valid_rate": float (0–1), # computed from valid/invalid/risky only
      }

    Unverified leads (verify_status IS NULL) are included in `total` but do
    not change the valid/invalid/risky buckets or the valid_rate denominator.
    Domains with only unverified leads will still show up.
    """
    if window_days <= 0:
        window_days = 30
    if top_n <= 0:
        top_n = 20

    try:
        rows = conn.execute(
            """
            SELECT
              LOWER(company_domain) AS domain,
              verify_status,
              COUNT(*) AS n
            FROM v_emails_latest
            WHERE company_domain IS NOT NULL
              AND company_domain <> ''
              AND (
                    -- Verified rows respect the rolling window
                    date(COALESCE(verified_at, checked_at)) >= date('now', ?)
                    -- Completely unverified leads are always included
                    OR (verified_at IS NULL AND checked_at IS NULL)
                  )
            GROUP BY domain, verify_status
            """,
            (f"-{int(window_days)} days",),
        ).fetchall()
    except sqlite3.Error:
        return []

    by_domain: dict[str, dict[str, int]] = {}
    for row in rows:
        try:
            domain = row["domain"]
            status = row["verify_status"]
            n = int(row["n"])
        except (KeyError, IndexError, TypeError, ValueError):
            continue

        bucket = by_domain.setdefault(
            domain,
            {
                "total": 0,
                "valid": 0,
                "invalid": 0,
                "risky_catch_all": 0,
            },
        )
        bucket["total"] += n

        # Only increment specific outcome buckets for known statuses; NULL or
        # other statuses just contribute to "total".
        if status in bucket:
            bucket[status] += n

    # Sort by total volume and truncate.
    items = sorted(
        by_domain.items(),
        key=lambda kv: kv[1]["total"],
        reverse=True,
    )[: int(top_n)]

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


def get_error_breakdown(
    conn: sqlite3.Connection,
    top_n: int = 20,
) -> dict[str, int]:
    """
    Return a breakdown of verification errors keyed by reason/status.

    We coalesce verify_reason, legacy reason, and status into a single label
    to keep analytics simple.
    """
    if top_n <= 0:
        top_n = 20

    try:
        rows = conn.execute(
            """
            SELECT
              COALESCE(verify_reason, reason, status, 'unknown') AS key,
              COUNT(*) AS n
            FROM verification_results
            GROUP BY key
            ORDER BY n DESC
            LIMIT ?
            """,
            (int(top_n),),
        ).fetchall()
    except sqlite3.Error:
        return {}

    out: dict[str, int] = {}
    for row in rows:
        try:
            key = row["key"]
            n = int(row["n"])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        out[str(key)] = n
    return out


def get_admin_summary(conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    """
    High-level summary used by both the admin API and CLI.

    Returns a JSON-serializable dict with queues, workers, verification, and
    cost counters.
    """
    if conn is None:
        conn = get_connection()

    queues, workers = get_queue_stats()
    verif = get_verification_stats(conn)
    cost = get_cost_counters(conn)

    return {
        "queues": [q.__dict__ for q in queues],
        "workers": [w.__dict__ for w in workers],
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
    }


def get_analytics_summary(
    conn: sqlite3.Connection | None = None,
    window_days: int = 30,
    top_domains: int = 20,
    top_errors: int = 20,
) -> dict[str, Any]:
    """
    Aggregate O17 analytics into a single JSON-serializable payload.

    Shape:
      {
        "verification_time_series": [...],
        "domain_breakdown": [...],
        "error_breakdown": {...},
      }
    """
    if conn is None:
        conn = get_connection()

    ts = get_verification_time_series(conn, window_days=window_days)
    domains = get_domain_breakdown(
        conn,
        window_days=window_days,
        top_n=top_domains,
    )
    errors = get_error_breakdown(conn, top_n=top_errors)

    return {
        "verification_time_series": ts,
        "domain_breakdown": domains,
        "error_breakdown": errors,
    }
