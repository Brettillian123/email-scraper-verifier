# src/api/runs_v2.py
"""
Enhanced Runs API endpoints for web-app operation.

New features:
  - Mode selection: autodiscovery, verify, generate (separately or combined)
  - Company limit enforcement (default 1000)
  - Run metrics endpoint
  - User activity tracking integration
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_COMPANY_LIMIT = 1000
MAX_COMPANY_LIMIT = 5000

RQ_REDIS_URL = (
    os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
).strip()
RUNS_QUEUE_NAME = os.getenv("RUNS_QUEUE_NAME", "orchestrator").strip()

# Dev auth defaults
DEV_TENANT_ID = os.getenv("DEV_TENANT_ID", "dev").strip()
DEV_USER_ID = os.getenv("DEV_USER_ID", "user_dev").strip()
AUTH_MODE = os.getenv("AUTH_MODE", "dev").strip().lower()


class PipelineMode(str, Enum):
    """Available pipeline execution modes."""
    AUTODISCOVERY = "autodiscovery"
    GENERATE = "generate"
    VERIFY = "verify"
    FULL = "full"


# ---------------------------------------------------------------------------
# Auth Context (self-contained to avoid circular import)
# ---------------------------------------------------------------------------

class AuthContextV2(BaseModel):
    """Auth context for v2 endpoints."""
    tenant_id: str
    user_id: str
    email: str | None = None


def get_auth_context_v2(
    x_tenant_id: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
    x_user_email: str | None = Header(default=None),
) -> AuthContextV2:
    """
    Resolve auth context from headers.
    
    This is a self-contained version to avoid circular imports.
    """
    if AUTH_MODE == "none":
        return AuthContextV2(
            tenant_id=DEV_TENANT_ID,
            user_id=DEV_USER_ID,
            email=x_user_email,
        )
    
    # Dev mode: use headers with fallback to defaults
    tenant_id = (x_tenant_id or "").strip() or DEV_TENANT_ID
    user_id = (x_user_id or "").strip() or DEV_USER_ID
    
    return AuthContextV2(
        tenant_id=tenant_id,
        user_id=user_id,
        email=x_user_email,
    )


# ---------------------------------------------------------------------------
# Request/Response Models
# ---------------------------------------------------------------------------

class RunOptionsV2(BaseModel):
    """Enhanced run options with mode selection and limits."""
    modes: list[PipelineMode] = Field(
        default=[PipelineMode.FULL],
        description="Pipeline stages to execute"
    )
    company_limit: int = Field(
        default=DEFAULT_COMPANY_LIMIT,
        ge=1,
        le=MAX_COMPANY_LIMIT,
        description=f"Maximum companies to process (1-{MAX_COMPANY_LIMIT})"
    )
    skip_verified: bool = Field(default=True)
    skip_catch_all: bool = Field(default=False)
    timeout_per_company_s: int = Field(default=300, ge=30, le=1800)
    ai_enabled: bool = Field(default=True)
    discovery_queue: str | None = None
    verify_queue: str | None = None
    
    @field_validator("modes", mode="before")
    @classmethod
    def parse_modes(cls, v):
        if isinstance(v, str):
            v = [v]
        result = []
        for mode in v:
            if isinstance(mode, str):
                if mode.lower() in ("full", "all"):
                    result.append(PipelineMode.FULL)
                else:
                    result.append(PipelineMode(mode.lower()))
            else:
                result.append(mode)
        return result
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "modes": [m.value for m in self.modes],
            "company_limit": self.company_limit,
            "skip_verified": self.skip_verified,
            "skip_catch_all": self.skip_catch_all,
            "timeout_per_company_s": self.timeout_per_company_s,
            "ai_enabled": self.ai_enabled,
            "discovery_queue": self.discovery_queue,
            "verify_queue": self.verify_queue,
        }


class RunCreateRequestV2(BaseModel):
    """Enhanced run creation request."""
    domains: list[str] = Field(..., min_length=1, max_length=MAX_COMPANY_LIMIT)
    options: RunOptionsV2 = Field(default_factory=RunOptionsV2)
    label: str | None = Field(default=None, max_length=255)
    
    @field_validator("domains", mode="before")
    @classmethod
    def normalize_domains(cls, v):
        if isinstance(v, str):
            v = [v]
        seen = set()
        result = []
        for d in v:
            d_norm = (d or "").strip().lower()
            if d_norm and d_norm not in seen:
                seen.add(d_norm)
                result.append(d_norm)
        return result


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/v2", tags=["runs-v2"])


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _db_connect():
    """Get database connection."""
    from src.db import get_conn
    return get_conn()


def _log_activity(
    tenant_id: str,
    user_id: str,
    action: str,
    resource_type: str | None = None,
    resource_id: str | None = None,
    metadata: dict | None = None,
    request: Request | None = None,
) -> None:
    """Log user activity (best-effort, non-blocking)."""
    try:
        from src.admin.user_activity import log_user_activity
        
        ip = None
        ua = None
        if request:
            ip = request.client.host if request.client else None
            ua = request.headers.get("user-agent")
        
        log_user_activity(
            tenant_id=tenant_id,
            user_id=user_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            ip_address=ip,
            user_agent=ua,
            metadata=metadata,
        )
    except Exception:
        pass


def _bootstrap_tenant_user(con, tenant_id: str, user_id: str, email: str | None) -> None:
    """Ensure tenant and user rows exist."""
    now = _utc_now_iso()
    try:
        con.execute(
            "INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            (tenant_id, tenant_id, now),
        )
    except Exception:
        pass
    try:
        con.execute(
            "INSERT INTO users (id, tenant_id, email, created_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (user_id, tenant_id, email, now),
        )
    except Exception:
        pass


def _enqueue_pipeline(run_id: str, tenant_id: str) -> None:
    """Enqueue the pipeline_start job."""
    try:
        from redis import Redis
        from rq import Queue
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"Redis/RQ not available: {exc}") from exc
    
    # Try v2 first, fall back to v1
    try:
        from src.queueing.pipeline_v2 import pipeline_start_v2 as pipeline_func
    except ImportError:
        try:
            from src.queueing.tasks import pipeline_start as pipeline_func
        except ImportError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"No pipeline_start available: {exc}",
            ) from exc
    
    redis = Redis.from_url(RQ_REDIS_URL)
    q = Queue(RUNS_QUEUE_NAME, connection=redis)
    q.enqueue(pipeline_func, run_id=run_id, tenant_id=tenant_id)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/runs")
async def create_run_v2(
    payload: RunCreateRequestV2,
    request: Request,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
):
    """
    Create a new pipeline run with enhanced options.
    
    Supports mode selection and company limit enforcement.
    """
    domains = payload.domains
    effective_limit = min(len(domains), payload.options.company_limit)
    
    if len(domains) > payload.options.company_limit:
        domains = domains[:payload.options.company_limit]
    
    options_dict = payload.options.to_dict()
    options_dict["_original_domain_count"] = len(payload.domains)
    options_dict["_effective_domain_count"] = len(domains)
    
    run_id = str(uuid.uuid4())
    now = _utc_now_iso()
    
    domains_json = json.dumps(domains, separators=(",", ":"))
    options_json = json.dumps(options_dict, separators=(",", ":"))
    
    con = _db_connect()
    try:
        _bootstrap_tenant_user(con, auth_ctx.tenant_id, auth_ctx.user_id, auth_ctx.email)
        
        con.execute(
            """
            INSERT INTO runs (
              id, tenant_id, user_id, label,
              status, domains_json, options_json,
              progress_json, error,
              created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                auth_ctx.tenant_id,
                auth_ctx.user_id,
                payload.label,
                "queued",
                domains_json,
                options_json,
                json.dumps({"phase": "queued"}, separators=(",", ":")),
                None,
                now,
                now,
            ),
        )
        con.commit()
    except Exception as exc:
        try:
            con.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create run: {type(exc).__name__}: {exc}",
        ) from exc
    finally:
        try:
            con.close()
        except Exception:
            pass
    
    _enqueue_pipeline(run_id, auth_ctx.tenant_id)
    
    _log_activity(
        tenant_id=auth_ctx.tenant_id,
        user_id=auth_ctx.user_id,
        action="run_created",
        resource_type="run",
        resource_id=run_id,
        metadata={
            "domains_count": len(domains),
            "modes": [m.value for m in payload.options.modes],
            "company_limit": payload.options.company_limit,
        },
        request=request,
    )
    
    return {
        "run_id": run_id,
        "status": "queued",
        "tenant_id": auth_ctx.tenant_id,
        "domains_count": len(domains),
        "effective_limit": effective_limit,
        "modes": [m.value for m in payload.options.modes],
        "created_at": now,
    }


@router.get("/runs/{run_id}/metrics")
async def get_run_metrics(
    run_id: str,
    request: Request,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
):
    """Get detailed metrics for a run."""
    try:
        from src.admin.run_metrics import get_run_metrics as load_metrics
        summary = load_metrics(run_id, auth_ctx.tenant_id)
        if summary:
            return summary.to_dict()
    except ImportError:
        pass
    
    con = _db_connect()
    try:
        cur = con.execute(
            "SELECT progress_json, status, started_at, finished_at "
            "FROM runs WHERE tenant_id = ? AND id = ?",
            (auth_ctx.tenant_id, run_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")
        
        progress_raw = row[0] if isinstance(row, tuple) else row["progress_json"]
        status = row[1] if isinstance(row, tuple) else row["status"]
        started = row[2] if isinstance(row, tuple) else row["started_at"]
        finished = row[3] if isinstance(row, tuple) else row["finished_at"]
        
        progress = {}
        if progress_raw:
            try:
                progress = (
                    json.loads(progress_raw)
                    if isinstance(progress_raw, str)
                    else progress_raw
                )
            except Exception:
                pass
        
        metrics = progress.get("metrics", {})
        
        return {
            "run_id": run_id,
            "tenant_id": auth_ctx.tenant_id,
            "status": status,
            "started_at": started,
            "finished_at": finished,
            **metrics,
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/runs/{run_id}")
async def get_run_v2(
    run_id: str,
    request: Request,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
):
    """Get run details including progress."""
    con = _db_connect()
    try:
        cur = con.execute(
            """
            SELECT id, tenant_id, user_id, label, status,
                   domains_json, options_json, progress_json, error,
                   created_at, updated_at, started_at, finished_at
            FROM runs
            WHERE tenant_id = ? AND id = ?
            """,
            (auth_ctx.tenant_id, run_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")
        
        return {
            "id": row[0],
            "tenant_id": row[1],
            "user_id": row[2],
            "label": row[3],
            "status": row[4],
            "domains": json.loads(row[5]) if row[5] else [],
            "options": json.loads(row[6]) if row[6] else {},
            "progress": json.loads(row[7]) if row[7] else {},
            "error": row[8],
            "created_at": row[9],
            "updated_at": row[10],
            "started_at": row[11],
            "finished_at": row[12],
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/users/me/activity")
async def get_my_activity(
    request: Request,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    action: str | None = Query(default=None),
):
    """Get activity history for the current user."""
    try:
        from src.admin.user_activity import get_user_activity
        
        entries = get_user_activity(
            tenant_id=auth_ctx.tenant_id,
            user_id=auth_ctx.user_id,
            limit=limit,
            offset=offset,
            action_filter=action,
        )
        
        return {
            "user_id": auth_ctx.user_id,
            "results": [e.to_dict() for e in entries],
            "limit": limit,
            "offset": offset,
        }
    except ImportError:
        return {
            "user_id": auth_ctx.user_id,
            "results": [],
            "limit": limit,
            "offset": offset,
            "note": "Activity tracking module not available",
        }


@router.get("/users/me/usage")
async def get_my_usage(
    request: Request,
    auth_ctx: Annotated[AuthContextV2, Depends(get_auth_context_v2)],
    days: int | None = Query(default=None, ge=1, le=365),
):
    """Get usage summary for the current user."""
    try:
        from src.admin.user_activity import get_user_usage_summary
        
        summary = get_user_usage_summary(
            tenant_id=auth_ctx.tenant_id,
            user_id=auth_ctx.user_id,
            since_days=days,
        )
        
        return summary.to_dict()
    except ImportError:
        return {
            "user_id": auth_ctx.user_id,
            "note": "Usage tracking module not available",
        }


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "version": "v2"}


__all__ = [
    "router",
    "PipelineMode",
    "RunOptionsV2",
    "RunCreateRequestV2",
    "AuthContextV2",
    "get_auth_context_v2",
    "DEFAULT_COMPANY_LIMIT",
    "MAX_COMPANY_LIMIT",
]
