# src/api/run_finalize.py
"""
Run finalization utilities.

Provides functions to:
1. Check if all jobs for a run have completed
2. Recalculate metrics from actual database state
3. Finalize run status

This fixes the issue where run_completion_callback runs before
autodiscovery jobs actually finish.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["runs-v2"])


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _db_connect():
    from src.db import get_conn

    return get_conn()


# Auth (same as runs_v2)
class AuthContextV2(BaseModel):
    tenant_id: str
    user_id: str
    email: str | None = None


DEV_TENANT_ID = os.getenv("DEV_TENANT_ID", "dev").strip()
DEV_USER_ID = os.getenv("DEV_USER_ID", "user_dev").strip()
AUTH_MODE = os.getenv("AUTH_MODE", "dev").strip().lower()


def get_auth_context_v2(
    x_tenant_id: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
    x_user_email: str | None = Header(default=None),
) -> AuthContextV2:
    if AUTH_MODE == "none":
        return AuthContextV2(tenant_id=DEV_TENANT_ID, user_id=DEV_USER_ID, email=x_user_email)
    tenant_id = (x_tenant_id or "").strip() or DEV_TENANT_ID
    user_id = (x_user_id or "").strip() or DEV_USER_ID
    return AuthContextV2(tenant_id=tenant_id, user_id=user_id, email=x_user_email)


def recalculate_run_metrics(run_id: str, tenant_id: str) -> dict[str, Any]:
    """
    Recalculate run metrics from actual database state.

    This queries the database directly to get accurate counts,
    regardless of whether the pipeline callback ran correctly.
    """
    con = _db_connect()

    try:
        # Get domains from run
        row = con.execute(
            "SELECT domains_json FROM runs WHERE tenant_id = ? AND id = ?",
            (tenant_id, run_id),
        ).fetchone()

        if not row:
            raise ValueError(f"Run not found: {run_id}")

        domains_json = row[0]
        domains = json.loads(domains_json) if isinstance(domains_json, str) else domains_json

        # Get company IDs for these domains
        placeholders = ",".join(["?"] * len(domains))
        company_rows = con.execute(
            f"SELECT id, domain FROM companies WHERE domain IN ({placeholders})",
            tuple(domains),
        ).fetchall()

        company_ids = [r[0] for r in company_rows]
        domain_map = {r[0]: r[1] for r in company_rows}

        if not company_ids:
            return {
                "run_id": run_id,
                "total_companies": len(domains),
                "companies_found": 0,
                "note": "No companies found for domains",
            }

        cid_placeholders = ",".join(["?"] * len(company_ids))

        # Count people per company
        people_stats = con.execute(
            f"""
            SELECT company_id, COUNT(*) as cnt
            FROM people
            WHERE company_id IN ({cid_placeholders})
            GROUP BY company_id
            """,
            tuple(company_ids),
        ).fetchall()
        people_by_company = {r[0]: r[1] for r in people_stats}

        # Count emails per company
        email_stats = con.execute(
            f"""
            SELECT company_id, COUNT(*) as cnt
            FROM emails
            WHERE company_id IN ({cid_placeholders})
            GROUP BY company_id
            """,
            tuple(company_ids),
        ).fetchall()
        emails_by_company = {r[0]: r[1] for r in email_stats}

        # Count verification results
        verify_stats = con.execute(
            f"""
            SELECT 
                e.company_id,
                vr.verify_status,
                COUNT(*) as cnt
            FROM emails e
            JOIN verification_results vr ON vr.email_id = e.id
            WHERE e.company_id IN ({cid_placeholders})
            GROUP BY e.company_id, vr.verify_status
            """,
            tuple(company_ids),
        ).fetchall()

        # Aggregate metrics
        metrics = {
            "run_id": run_id,
            "total_companies": len(domains),
            "companies_found": len(company_ids),
            "companies_with_people": sum(
                1 for cid in company_ids if people_by_company.get(cid, 0) > 0
            ),
            "companies_zero_people": sum(
                1 for cid in company_ids if people_by_company.get(cid, 0) == 0
            ),
            "companies_with_emails": sum(
                1 for cid in company_ids if emails_by_company.get(cid, 0) > 0
            ),
            "total_people": sum(people_by_company.values()),
            "total_emails": sum(emails_by_company.values()),
            "emails_verified": 0,
            "emails_valid": 0,
            "emails_invalid": 0,
            "emails_risky_catch_all": 0,
            "emails_unknown_timeout": 0,
        }

        for row in verify_stats:
            company_id, status, count = row
            metrics["emails_verified"] += count
            if status == "valid":
                metrics["emails_valid"] += count
            elif status == "invalid":
                metrics["emails_invalid"] += count
            elif status == "risky_catch_all":
                metrics["emails_risky_catch_all"] += count
            elif status == "unknown_timeout":
                metrics["emails_unknown_timeout"] += count

        # Per-company breakdown
        metrics["companies"] = []
        for cid in company_ids:
            metrics["companies"].append(
                {
                    "company_id": cid,
                    "domain": domain_map.get(cid),
                    "people": people_by_company.get(cid, 0),
                    "emails": emails_by_company.get(cid, 0),
                }
            )

        return metrics

    finally:
        try:
            con.close()
        except Exception:
            pass


def finalize_run(run_id: str, tenant_id: str, force: bool = False) -> dict[str, Any]:
    """
    Finalize a run by recalculating metrics and updating status.

    Args:
        run_id: The run ID
        tenant_id: The tenant ID
        force: If True, finalize even if run already succeeded
    """
    con = _db_connect()

    try:
        # Check current status
        row = con.execute(
            "SELECT status, progress_json FROM runs WHERE tenant_id = ? AND id = ?",
            (tenant_id, run_id),
        ).fetchone()

        if not row:
            raise ValueError(f"Run not found: {run_id}")

        current_status = row[0]
        progress = json.loads(row[1]) if row[1] else {}

        if current_status == "succeeded" and not force:
            return {
                "run_id": run_id,
                "status": current_status,
                "note": "Run already succeeded. Use force=true to recalculate.",
            }

        # Recalculate metrics
        metrics = recalculate_run_metrics(run_id, tenant_id)

        # Update progress
        now = _utc_now_iso()
        progress["metrics"] = metrics
        progress["phase"] = "finalized"
        progress["finalized_at"] = now

        # Determine status
        new_status = (
            "completed_with_warnings"
            if metrics.get("companies_zero_people", 0) > 0
            else "succeeded"
        )

        # Update run
        con.execute(
            """
            UPDATE runs
            SET status = ?, progress_json = ?, updated_at = ?,
                finished_at = COALESCE(finished_at, ?)
            WHERE tenant_id = ? AND id = ?
            """,
            (new_status, json.dumps(progress), now, now, tenant_id, run_id),
        )
        con.commit()

        return {
            "run_id": run_id,
            "status": new_status,
            "metrics": metrics,
            "finalized_at": now,
        }

    finally:
        try:
            con.close()
        except Exception:
            pass


def check_run_jobs_status(run_id: str, tenant_id: str) -> dict[str, Any]:
    """
    Check the status of all jobs for a run in Redis/RQ.
    """
    try:
        import os

        from redis import Redis
        from rq.job import Job

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        redis = Redis.from_url(redis_url)

        con = _db_connect()
        row = con.execute(
            "SELECT progress_json FROM runs WHERE tenant_id = ? AND id = ?",
            (tenant_id, run_id),
        ).fetchone()
        con.close()

        if not row:
            return {"error": "Run not found"}

        progress = json.loads(row[0]) if row[0] else {}
        domains = progress.get("domains", [])

        job_statuses = []
        for d in domains:
            for job_info in d.get("jobs", []):
                job_id = job_info.get("job_id")
                if job_id:
                    try:
                        job = Job.fetch(job_id, connection=redis)
                        job_statuses.append(
                            {
                                "domain": d.get("domain"),
                                "job_id": job_id,
                                "status": job.get_status(),
                                "result": job.result if job.is_finished else None,
                            }
                        )
                    except Exception as e:
                        job_statuses.append(
                            {
                                "domain": d.get("domain"),
                                "job_id": job_id,
                                "status": "not_found",
                                "error": str(e),
                            }
                        )

        total = len(job_statuses)
        finished = sum(1 for j in job_statuses if j["status"] == "finished")
        failed = sum(1 for j in job_statuses if j["status"] == "failed")

        return {
            "run_id": run_id,
            "total_jobs": total,
            "finished": finished,
            "failed": failed,
            "pending": total - finished - failed,
            "all_done": finished + failed == total,
            "jobs": job_statuses,
        }

    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/runs/{run_id}/finalize")
async def finalize_run_endpoint(
    run_id: str,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
    force: bool = False,
):
    """
    Recalculate metrics and finalize a run.

    Use this after autodiscovery jobs have completed to get accurate metrics.
    """
    try:
        result = finalize_run(run_id, auth_ctx.tenant_id, force=force)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/runs/{run_id}/jobs")
async def get_run_jobs_status(
    run_id: str,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
):
    """
    Check the status of all RQ jobs for a run.

    Useful for debugging and monitoring job completion.
    """
    result = check_run_jobs_status(run_id, auth_ctx.tenant_id)
    return result


@router.get("/runs/{run_id}/recalculate")
async def recalculate_metrics_endpoint(
    run_id: str,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
):
    """
    Recalculate metrics for a run without changing status.

    Read-only operation to check actual database state.
    """
    try:
        metrics = recalculate_run_metrics(run_id, auth_ctx.tenant_id)
        return metrics
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


__all__ = [
    "router",
    "recalculate_run_metrics",
    "finalize_run",
    "check_run_jobs_status",
]
