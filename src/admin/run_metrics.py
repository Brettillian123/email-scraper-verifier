# src/admin/run_metrics.py
"""
Run-level metrics aggregation service.

This module provides functions to:
  - Aggregate AutodiscoveryResult metrics into run_metrics rows
  - Query run metrics for the admin dashboard
  - Track verification outcomes per run

Used by:
  - pipeline_start completion callback
  - /runs/{id}/metrics API endpoint
  - Admin dashboard run overview
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.db import get_conn

log = logging.getLogger(__name__)


@dataclass
class RunMetricsSummary:
    """Summary of metrics for a single run."""
    
    run_id: str
    tenant_id: str
    
    # Company metrics
    total_companies: int = 0
    companies_with_candidates: int = 0
    companies_zero_candidates: int = 0
    companies_with_pages: int = 0
    companies_zero_pages: int = 0
    companies_403_blocked: int = 0
    companies_robots_blocked: int = 0
    companies_timeout: int = 0
    
    # Candidate metrics
    total_candidates_extracted: int = 0
    candidates_with_email: int = 0
    candidates_no_email: int = 0
    people_upserted: int = 0
    
    # Email metrics
    emails_generated: int = 0
    emails_verified: int = 0
    emails_valid: int = 0
    emails_invalid: int = 0
    emails_risky_catch_all: int = 0
    emails_unknown_timeout: int = 0
    
    # Domain metrics
    domains_catch_all: int = 0
    domains_no_mx: int = 0
    domains_smtp_blocked: int = 0
    
    # AI metrics
    ai_enabled: bool = False
    ai_candidates_approved: int = 0
    ai_candidates_rejected: int = 0
    ai_total_tokens: int = 0
    ai_total_time_s: float = 0.0
    
    # Performance
    crawl_time_s: float = 0.0
    extract_time_s: float = 0.0
    generate_time_s: float = 0.0
    verify_time_s: float = 0.0
    total_time_s: float = 0.0
    
    # Errors
    total_errors: int = 0
    error_summary: dict[str, int] | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            
            # Companies
            "total_companies": self.total_companies,
            "companies_with_candidates": self.companies_with_candidates,
            "companies_zero_candidates": self.companies_zero_candidates,
            "companies_with_pages": self.companies_with_pages,
            "companies_zero_pages": self.companies_zero_pages,
            "companies_403_blocked": self.companies_403_blocked,
            "companies_robots_blocked": self.companies_robots_blocked,
            "companies_timeout": self.companies_timeout,
            
            # Candidates
            "total_candidates_extracted": self.total_candidates_extracted,
            "candidates_with_email": self.candidates_with_email,
            "candidates_no_email": self.candidates_no_email,
            "people_upserted": self.people_upserted,
            
            # Emails
            "emails_generated": self.emails_generated,
            "emails_verified": self.emails_verified,
            "emails_valid": self.emails_valid,
            "emails_invalid": self.emails_invalid,
            "emails_risky_catch_all": self.emails_risky_catch_all,
            "emails_unknown_timeout": self.emails_unknown_timeout,
            
            # Domains
            "domains_catch_all": self.domains_catch_all,
            "domains_no_mx": self.domains_no_mx,
            "domains_smtp_blocked": self.domains_smtp_blocked,
            
            # AI
            "ai_enabled": self.ai_enabled,
            "ai_candidates_approved": self.ai_candidates_approved,
            "ai_candidates_rejected": self.ai_candidates_rejected,
            "ai_total_tokens": self.ai_total_tokens,
            "ai_total_time_s": round(self.ai_total_time_s, 2),
            
            # Performance
            "crawl_time_s": round(self.crawl_time_s, 2),
            "extract_time_s": round(self.extract_time_s, 2),
            "generate_time_s": round(self.generate_time_s, 2),
            "verify_time_s": round(self.verify_time_s, 2),
            "total_time_s": round(self.total_time_s, 2),
            
            # Errors
            "total_errors": self.total_errors,
            "error_summary": self.error_summary or {},
            
            # Derived
            "valid_rate": (
                round(self.emails_valid / self.emails_verified * 100, 1)
                if self.emails_verified > 0 else 0
            ),
            "has_issues": (
                self.companies_zero_candidates > 0 or
                self.companies_403_blocked > 0 or
                self.emails_unknown_timeout > 0 or
                self.total_errors > 0
            ),
        }


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _table_exists(conn, table: str) -> bool:
    """Check if a table exists."""
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return True
    except Exception:
        return False


def aggregate_autodiscovery_results(
    run_id: str,
    tenant_id: str,
    results: list[dict[str, Any]],
) -> RunMetricsSummary:
    """
    Aggregate a list of AutodiscoveryResult dicts into a RunMetricsSummary.
    
    Args:
        run_id: The run ID
        tenant_id: The tenant ID
        results: List of AutodiscoveryResult.to_dict() outputs
        
    Returns:
        RunMetricsSummary with aggregated metrics
    """
    summary = RunMetricsSummary(run_id=run_id, tenant_id=tenant_id)
    summary.total_companies = len(results)
    
    error_counts: dict[str, int] = {}
    
    for r in results:
        # Pages
        pages = r.get("pages_fetched", 0)
        if pages > 0:
            summary.companies_with_pages += 1
        else:
            summary.companies_zero_pages += 1
        
        # Candidates
        cand_email = r.get("candidates_with_email", 0)
        cand_no_email = r.get("candidates_no_email", 0)
        total_cand = cand_email + cand_no_email
        
        if total_cand > 0:
            summary.companies_with_candidates += 1
        else:
            summary.companies_zero_candidates += 1
        
        summary.total_candidates_extracted += total_cand
        summary.candidates_with_email += cand_email
        summary.candidates_no_email += cand_no_email
        
        # 403 / robots
        if r.get("pages_403", 0) > 0:
            summary.companies_403_blocked += 1
        if r.get("pages_blocked_robots", 0) > 0:
            summary.companies_robots_blocked += 1
        
        # People/emails
        summary.people_upserted += r.get("people_upserted", 0)
        summary.emails_generated += r.get("emails_upserted", 0)
        
        # AI
        if r.get("ai_enabled"):
            summary.ai_enabled = True
            summary.ai_candidates_approved += r.get("ai_approved", 0)
            summary.ai_candidates_rejected += r.get("ai_rejected", 0)
            summary.ai_total_tokens += r.get("ai_tokens_used", 0)
            summary.ai_total_time_s += r.get("ai_time_s", 0)
        
        # Timing
        summary.crawl_time_s += r.get("crawl_time_s", 0)
        
        # Errors
        for err in r.get("errors", []):
            # Extract error type from message
            err_type = err.split(":")[0] if ":" in err else err
            err_type = err_type[:50]  # Truncate long types
            error_counts[err_type] = error_counts.get(err_type, 0) + 1
            summary.total_errors += 1
    
    summary.error_summary = error_counts if error_counts else None
    
    return summary


def save_run_metrics(
    summary: RunMetricsSummary,
    *,
    conn: Any = None,
) -> bool:
    """
    Save or update run metrics in the database.
    
    Args:
        summary: The RunMetricsSummary to save
        conn: Optional database connection (creates one if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()
    
    try:
        if not _table_exists(conn, "run_metrics"):
            log.warning("run_metrics table does not exist; skipping save")
            return False
        
        now = _utc_now_iso()
        error_json = json.dumps(summary.error_summary or {})
        
        # Upsert using ON CONFLICT
        conn.execute(
            """
            INSERT INTO run_metrics (
                run_id, tenant_id,
                total_companies, companies_with_candidates, companies_zero_candidates,
                companies_with_pages, companies_zero_pages, companies_403_blocked,
                companies_robots_blocked, companies_timeout,
                total_candidates_extracted, candidates_with_email, candidates_no_email,
                people_upserted, emails_generated, emails_verified,
                emails_valid, emails_invalid, emails_risky_catch_all, emails_unknown_timeout,
                domains_catch_all, domains_no_mx, domains_smtp_blocked,
                ai_enabled, ai_candidates_approved, ai_candidates_rejected,
                ai_total_tokens, ai_total_time_s,
                crawl_time_s, extract_time_s, generate_time_s, verify_time_s, total_time_s,
                total_errors, error_summary,
                created_at, updated_at
            ) VALUES (
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?,
                ?, ?
            )
            ON CONFLICT (run_id) DO UPDATE SET
                total_companies = EXCLUDED.total_companies,
                companies_with_candidates = EXCLUDED.companies_with_candidates,
                companies_zero_candidates = EXCLUDED.companies_zero_candidates,
                companies_with_pages = EXCLUDED.companies_with_pages,
                companies_zero_pages = EXCLUDED.companies_zero_pages,
                companies_403_blocked = EXCLUDED.companies_403_blocked,
                companies_robots_blocked = EXCLUDED.companies_robots_blocked,
                companies_timeout = EXCLUDED.companies_timeout,
                total_candidates_extracted = EXCLUDED.total_candidates_extracted,
                candidates_with_email = EXCLUDED.candidates_with_email,
                candidates_no_email = EXCLUDED.candidates_no_email,
                people_upserted = EXCLUDED.people_upserted,
                emails_generated = EXCLUDED.emails_generated,
                emails_verified = EXCLUDED.emails_verified,
                emails_valid = EXCLUDED.emails_valid,
                emails_invalid = EXCLUDED.emails_invalid,
                emails_risky_catch_all = EXCLUDED.emails_risky_catch_all,
                emails_unknown_timeout = EXCLUDED.emails_unknown_timeout,
                domains_catch_all = EXCLUDED.domains_catch_all,
                domains_no_mx = EXCLUDED.domains_no_mx,
                domains_smtp_blocked = EXCLUDED.domains_smtp_blocked,
                ai_enabled = EXCLUDED.ai_enabled,
                ai_candidates_approved = EXCLUDED.ai_candidates_approved,
                ai_candidates_rejected = EXCLUDED.ai_candidates_rejected,
                ai_total_tokens = EXCLUDED.ai_total_tokens,
                ai_total_time_s = EXCLUDED.ai_total_time_s,
                crawl_time_s = EXCLUDED.crawl_time_s,
                extract_time_s = EXCLUDED.extract_time_s,
                generate_time_s = EXCLUDED.generate_time_s,
                verify_time_s = EXCLUDED.verify_time_s,
                total_time_s = EXCLUDED.total_time_s,
                total_errors = EXCLUDED.total_errors,
                error_summary = EXCLUDED.error_summary,
                updated_at = EXCLUDED.updated_at
            """,
            (
                summary.run_id, summary.tenant_id,
                summary.total_companies, summary.companies_with_candidates, 
                summary.companies_zero_candidates,
                summary.companies_with_pages, summary.companies_zero_pages,
                summary.companies_403_blocked,
                summary.companies_robots_blocked, summary.companies_timeout,
                summary.total_candidates_extracted, summary.candidates_with_email,
                summary.candidates_no_email,
                summary.people_upserted, summary.emails_generated, summary.emails_verified,
                summary.emails_valid, summary.emails_invalid, 
                summary.emails_risky_catch_all, summary.emails_unknown_timeout,
                summary.domains_catch_all, summary.domains_no_mx, summary.domains_smtp_blocked,
                summary.ai_enabled, summary.ai_candidates_approved, summary.ai_candidates_rejected,
                summary.ai_total_tokens, summary.ai_total_time_s,
                summary.crawl_time_s, summary.extract_time_s, 
                summary.generate_time_s, summary.verify_time_s, summary.total_time_s,
                summary.total_errors, error_json,
                now, now,
            ),
        )
        conn.commit()
        return True
        
    except Exception:
        log.exception("Failed to save run metrics", extra={"run_id": summary.run_id})
        return False
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def get_run_metrics(
    run_id: str,
    tenant_id: str,
    *,
    conn: Any = None,
) -> RunMetricsSummary | None:
    """
    Load run metrics from the database.
    
    Returns None if not found or table doesn't exist.
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()
    
    try:
        if not _table_exists(conn, "run_metrics"):
            return None
        
        cur = conn.execute(
            """
            SELECT * FROM run_metrics
            WHERE run_id = ? AND tenant_id = ?
            LIMIT 1
            """,
            (run_id, tenant_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        
        # Convert row to dict if needed
        if hasattr(row, "_asdict"):
            data = row._asdict()
        elif hasattr(row, "keys"):
            data = dict(row)
        else:
            # Tuple - need column names
            cols = [d[0] for d in cur.description]
            data = dict(zip(cols, row, strict=False))
        
        summary = RunMetricsSummary(
            run_id=data.get("run_id", run_id),
            tenant_id=data.get("tenant_id", tenant_id),
        )
        
        # Populate from row
        for field in [
            "total_companies", "companies_with_candidates", "companies_zero_candidates",
            "companies_with_pages", "companies_zero_pages", "companies_403_blocked",
            "companies_robots_blocked", "companies_timeout",
            "total_candidates_extracted", "candidates_with_email", "candidates_no_email",
            "people_upserted", "emails_generated", "emails_verified",
            "emails_valid", "emails_invalid", "emails_risky_catch_all", "emails_unknown_timeout",
            "domains_catch_all", "domains_no_mx", "domains_smtp_blocked",
            "ai_enabled", "ai_candidates_approved", "ai_candidates_rejected",
            "ai_total_tokens", "ai_total_time_s",
            "crawl_time_s", "extract_time_s", "generate_time_s", "verify_time_s", "total_time_s",
            "total_errors",
        ]:
            if field in data:
                setattr(summary, field, data[field])
        
        # Parse error_summary JSON
        err_json = data.get("error_summary")
        if err_json:
            try:
                summary.error_summary = (
                    json.loads(err_json) if isinstance(err_json, str)
                    else err_json
                )
            except Exception:
                pass
        
        return summary
        
    except Exception:
        log.exception("Failed to load run metrics", extra={"run_id": run_id})
        return None
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def update_verification_metrics(
    run_id: str,
    tenant_id: str,
    *,
    emails_verified: int = 0,
    emails_valid: int = 0,
    emails_invalid: int = 0,
    emails_risky_catch_all: int = 0,
    emails_unknown_timeout: int = 0,
    verify_time_s: float = 0.0,
    conn: Any = None,
) -> bool:
    """
    Update verification-specific metrics for a run.
    
    Called after verification stage completes.
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()
    
    try:
        if not _table_exists(conn, "run_metrics"):
            return False
        
        conn.execute(
            """
            UPDATE run_metrics SET
                emails_verified = emails_verified + ?,
                emails_valid = emails_valid + ?,
                emails_invalid = emails_invalid + ?,
                emails_risky_catch_all = emails_risky_catch_all + ?,
                emails_unknown_timeout = emails_unknown_timeout + ?,
                verify_time_s = verify_time_s + ?,
                updated_at = ?
            WHERE run_id = ? AND tenant_id = ?
            """,
            (
                emails_verified, emails_valid, emails_invalid,
                emails_risky_catch_all, emails_unknown_timeout,
                verify_time_s, _utc_now_iso(),
                run_id, tenant_id,
            ),
        )
        conn.commit()
        return True
        
    except Exception:
        log.exception("Failed to update verification metrics", extra={"run_id": run_id})
        return False
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


__all__ = [
    "RunMetricsSummary",
    "aggregate_autodiscovery_results",
    "save_run_metrics",
    "get_run_metrics",
    "update_verification_metrics",
]
