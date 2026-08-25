# src/api/app.py

from __future__ import annotations

import csv
import io
import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from src.api import admin as admin_routes
from src.api import browser as browser_routes
from src.api import pipeline as pipeline_routes

# AUTH_MODE is re-exported so existing callers/tests can still read
# src.api.app.AUTH_MODE; src.api.deps owns the value and all auth decisions.
from src.api.deps import AUTH_MODE, AuthContext, get_auth_context  # noqa: F401
from src.api.middleware.body_limit import BodySizeLimitMiddleware
from src.auth import routes as auth_routes
from src.auth.middleware import RequireAuthMiddleware

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------

# Configurable via env; default 5 MiB
BODY_LIMIT_BYTES = int(os.getenv("BODY_LIMIT_BYTES", str(5 * 1024 * 1024)))
# NOTE: Database access uses src.db.get_conn() which reads DATABASE_URL.
# The legacy DB_PATH env var is no longer used by the API layer.
# If you need the DB_PATH for backwards compatibility in other scripts,
# use: os.getenv("DB_PATH", "data/dev.db")


def _is_postgres_configured() -> bool:
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


# Auth modes are documented in src/api/deps.py, which owns AuthContext resolution.

# RQ / Redis
RUNS_QUEUE_NAME = os.getenv("RUNS_QUEUE_NAME", "orchestrator").strip()
RQ_REDIS_URL = (
    os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
).strip()

app = FastAPI(title="Email Scraper API")

# Register early so limits apply to all routes
app.add_middleware(BodySizeLimitMiddleware, max_bytes=BODY_LIMIT_BYTES)

# Require authentication for all routes except auth, API (uses its own auth), static, health
app.add_middleware(
    RequireAuthMiddleware,
    exclude_paths=[
        "/auth/",  # Login, register, logout, etc.
        "/api/",  # Enforced per-route by the get_auth_context dependency (src/api/deps.py)
        "/leads/",  # Same: get_auth_context dependency
        "/admin/",  # Admin routes use require_admin dependency
        "/static/",  # Static files
        "/health",  # Health checks
        "/favicon.ico",
        "/demo",  # Public demo page (no auth required)
    ],
    login_url="/auth/login",
    pending_url="/auth/pending",
)


# R24: admin UI + metrics JSON
app.include_router(admin_routes.router)

# Auth routes for session-based authentication
app.include_router(auth_routes.router)

# Browser/dashboard API routes
app.include_router(browser_routes.router)

# Sales pipeline tracker
app.include_router(pipeline_routes.router)

# Email queue (Gmail auto-sender) is deliberately NOT mounted: it sends on a
# timer outside the approval queue, suppression and CAN-SPAM checks, and its
# routes carried no auth at all. Business rule is that no send is automated.

# ---------------------------------------------------------------------------
# Dashboard Route (Main UI at root)
# ---------------------------------------------------------------------------

# Templates for dashboard
dashboard_templates = Jinja2Templates(directory="src/api/templates")


def _get_current_user_from_session(request: Request):
    """Get the current user from session cookie if available."""
    try:
        from src.auth.core import SESSION_COOKIE_NAME, get_session

        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            return None

        session, user = get_session(session_id)
        return user
    except Exception:
        return None


@app.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    """
    Main dashboard UI accessible at root URL.
    """
    user = _get_current_user_from_session(request)
    return dashboard_templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "user": user,
        },
    )


@app.get("/demo", response_class=HTMLResponse)
def demo_page(request: Request) -> HTMLResponse:
    """
    Public demo dashboard with fake data.

    Identical layout to the real dashboard but uses hardcoded sample
    companies, people, and emails.  Run creation is disabled.
    No authentication required.
    """
    return dashboard_templates.TemplateResponse(
        request,
        "demo.html",
        {
            "request": request,
        },
    )


# --------------------------------------------------------------------------------------
# Error helpers
# --------------------------------------------------------------------------------------


def _error_response(status_code: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": error, "detail": detail},
    )


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# Dependency object (avoids B008: function calls in defaults)
AUTH_CTX_DEP = Depends(get_auth_context)

# --------------------------------------------------------------------------------------
# DB helpers (uses existing src.db.get_conn compatibility wrapper)
# --------------------------------------------------------------------------------------


def _db_connect():
    from src.db import get_conn  # type: ignore

    return get_conn()


def _db_table_columns(conn, table: str) -> set[str]:
    """Return column names for a table using PRAGMA table_info(...).

    Works for SQLite and for Postgres via src.db.CompatCursor PRAGMA emulation.
    """
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall() or []
    except Exception:
        return set()

    cols: set[str] = set()
    for row in rows:
        try:
            name = row[1]
        except Exception:
            continue
        if name:
            cols.add(str(name))
    return cols


def _db_bootstrap_tenant_user(
    conn,
    *,
    tenant_id: str,
    user_id: str,
    user_email: str | None = None,
) -> None:
    """Ensure tenants/users rows exist for the current auth context.

    This prevents FK failures when creating runs on a fresh Postgres database.

    The logic is schema-drift tolerant:
      - if tenants/users tables are missing, it no-ops
      - inserts only columns that exist
    """
    tenant_id = (tenant_id or "").strip()
    user_id = (user_id or "").strip()
    if not tenant_id or not user_id:
        return

    now = _utc_now_iso()

    tcols = _db_table_columns(conn, "tenants")
    if tcols:
        cols: list[str] = []
        vals: list[Any] = []
        if "id" in tcols:
            cols.append("id")
            vals.append(tenant_id)
        if "name" in tcols:
            # Keep a friendly name for common dev tenants; otherwise just reuse id
            name = "Development" if tenant_id in {"dev", "tenant_dev"} else tenant_id
            cols.append("name")
            vals.append(name)
        if "created_at" in tcols:
            cols.append("created_at")
            vals.append(now)
        if "updated_at" in tcols:
            cols.append("updated_at")
            vals.append(now)

        if cols:
            ph = ", ".join(["?"] * len(cols))
            conn.execute(
                f"INSERT INTO tenants ({', '.join(cols)}) VALUES ({ph}) ON CONFLICT DO NOTHING",
                tuple(vals),
            )

    ucols = _db_table_columns(conn, "users")
    if ucols:
        cols = []
        vals = []
        if "id" in ucols:
            cols.append("id")
            vals.append(user_id)
        if "tenant_id" in ucols:
            cols.append("tenant_id")
            vals.append(tenant_id)
        if user_email and "email" in ucols:
            cols.append("email")
            vals.append(user_email)
        if "created_at" in ucols:
            cols.append("created_at")
            vals.append(now)
        if "updated_at" in ucols:
            cols.append("updated_at")
            vals.append(now)

        if cols:
            ph = ", ".join(["?"] * len(cols))
            conn.execute(
                f"INSERT INTO users ({', '.join(cols)}) VALUES ({ph}) ON CONFLICT DO NOTHING",
                tuple(vals),
            )


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    rows = cur.fetchall() or []
    desc = getattr(cur, "_cursor", None)
    columns: list[str] = []
    if desc is not None and getattr(desc, "description", None):
        columns = [d[0] for d in desc.description]
    if not columns and rows:
        columns = [f"col{i}" for i in range(len(rows[0]))]
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _fetchone_dict(cur) -> dict[str, Any] | None:
    row = cur.fetchone()
    if row is None:
        return None
    desc = getattr(cur, "_cursor", None)
    columns: list[str] = []
    if desc is not None and getattr(desc, "description", None):
        columns = [d[0] for d in desc.description]
    if not columns:
        columns = [f"col{i}" for i in range(len(row))]
    return dict(zip(columns, row, strict=False))


# --------------------------------------------------------------------------------------
# New: Runs API (Control Plane)
# --------------------------------------------------------------------------------------


class RunCreateRequest(BaseModel):
    domains: list[str] = Field(..., min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)
    label: str | None = None


def _enqueue_pipeline_start(run_id: str, tenant_id: str) -> None:
    """
    Enqueue the orchestrator job that kicks off the fan-out pipeline.

    The orchestrator is src.queueing.pipeline_v2.pipeline_start (an alias for
    pipeline_start_v2, pipeline_v2.py:2290). This used to import it from
    src.queueing.tasks, where it has never existed -- so the import always
    failed and POST /runs answered 501 on every call. Nothing could start a run
    through the API at all, which is the likeliest reason no run has been
    created since 2026-04-08. The 501 remains as a genuine guard, but it now
    only fires if the orchestrator is actually missing.
    """
    try:
        from redis import Redis  # type: ignore
        from rq import Queue  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"RQ/Redis not available: {exc}") from exc

    try:
        from src.queueing.pipeline_v2 import pipeline_start  # type: ignore
    except Exception as exc:
        raise HTTPException(
            status_code=501,
            detail=(
                "pipeline_start is not available (expected at "
                "src.queueing.pipeline_v2.pipeline_start)"
            ),
        ) from exc

    redis = Redis.from_url(RQ_REDIS_URL)
    q = Queue(RUNS_QUEUE_NAME, connection=redis)
    q.enqueue(pipeline_start, run_id=run_id, tenant_id=tenant_id)


def _db_insert_run(
    *,
    tenant_id: str,
    user_id: str,
    user_email: str | None,
    domains: list[str],
    options: dict[str, Any],
    label: str | None,
) -> str:
    run_id = str(uuid.uuid4())
    now = _utc_now_iso()

    domains_json = json.dumps(domains, separators=(",", ":"))
    options_json = json.dumps(options or {}, separators=(",", ":"))

    con = _db_connect()
    try:
        _db_bootstrap_tenant_user(con, tenant_id=tenant_id, user_id=user_id, user_email=user_email)

        cur = con.cursor()
        # Expected table shape (to be added in the Postgres completion work):
        # runs(
        #   id TEXT/UUID PK,
        #   tenant_id TEXT/UUID,
        #   user_id TEXT,
        #   label TEXT,
        #   status TEXT,
        #   domains_json TEXT/JSON,
        #   options_json TEXT/JSON,
        #   progress_json TEXT/JSON,
        #   error TEXT,
        #   created_at TEXT/TIMESTAMPTZ,
        #   updated_at TEXT/TIMESTAMPTZ,
        #   started_at TEXT/TIMESTAMPTZ,
        #   finished_at TEXT/TIMESTAMPTZ
        # )
        cur.execute(
            """
            INSERT INTO runs (
              id, tenant_id, user_id, label,
              status, domains_json, options_json,
              progress_json, error,
              created_at, updated_at, started_at, finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                tenant_id,
                user_id,
                label,
                "queued",
                domains_json,
                options_json,
                json.dumps({}, separators=(",", ":")),
                None,
                now,
                now,
                None,
                None,
            ),
        )
        con.commit()
        return run_id
    except Exception as exc:
        try:
            con.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to create run (runs table missing or schema mismatch): "
                f"{type(exc).__name__}: {exc}"
            ),
        ) from exc
    finally:
        try:
            con.close()
        except Exception:
            pass


def _db_get_run(*, tenant_id: str, run_id: str) -> dict[str, Any] | None:
    con = _db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              id, tenant_id, user_id, label,
              status, domains_json, options_json,
              progress_json, error,
              created_at, updated_at, started_at, finished_at
            FROM runs
            WHERE tenant_id = ? AND id = ?
            LIMIT 1
            """,
            (tenant_id, run_id),
        )
        row = _fetchone_dict(cur)
        return row
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to load run (runs table missing or schema mismatch): "
                f"{type(exc).__name__}: {exc}"
            ),
        ) from exc
    finally:
        try:
            con.close()
        except Exception:
            pass


def _db_list_runs(*, tenant_id: str, limit: int = 50) -> list[dict[str, Any]]:
    cap = max(1, min(int(limit), 200))
    con = _db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              id, tenant_id, user_id, label,
              status, created_at, updated_at, started_at, finished_at, error
            FROM runs
            WHERE tenant_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (tenant_id, cap),
        )
        return _fetchall_dict(cur)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to list runs (runs table missing or schema mismatch): "
                f"{type(exc).__name__}: {exc}"
            ),
        ) from exc
    finally:
        try:
            con.close()
        except Exception:
            pass


def _db_run_results(
    *,
    tenant_id: str,
    run_id: str,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    """
    Requires tenant_id/run_id plumbing in the data tables.

    Expected future shape:
      - companies.tenant_id, companies.run_id
      - v_emails_latest exposes company_id
    """
    lim = max(1, min(int(limit), 500))
    off = max(0, int(offset))

    con = _db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              vel.email,
              vel.first_name,
              vel.last_name,
              vel.full_name,
              vel.title,
              vel.company,
              vel.company_id,
              vel.company_domain,
              vel.icp_score,
              vel.verify_status,
              vel.verify_reason,
              vel.verified_at,
              vel.verify_label,
              vel.is_primary_for_person,
              vel.source_url
            FROM v_emails_latest AS vel
            JOIN companies AS c
              ON c.id = vel.company_id
            WHERE c.tenant_id = ?
              AND c.run_id = ?
            ORDER BY
              CASE WHEN vel.icp_score IS NULL THEN 1 ELSE 0 END,
              vel.icp_score DESC,
              vel.verified_at DESC
            LIMIT ?
            OFFSET ?
            """,
            (tenant_id, run_id, lim, off),
        )
        rows = _fetchall_dict(cur)
        return rows
    except Exception as exc:
        raise HTTPException(
            status_code=501,
            detail=(
                "Run results are not available until tenant_id/run_id columns are added "
                f"and populated (schema mismatch): {type(exc).__name__}: {exc}"
            ),
        ) from exc
    finally:
        try:
            con.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


@app.get("/leads/search")
async def leads_search(
    q: str = "",
    limit: str | None = None,
    sort: str | None = None,
    _auth: AuthContext = AUTH_CTX_DEP,
):
    """Compatibility stub for the retired lead-search subsystem.

    The subsystem behind this route (src/search/*, ~2,700 lines) was deleted:
    it read from `lead_search_docs`, which has 0 rows in production and has
    never been populated, so every query it ever served returned nothing.

    The route itself is kept because two live admin UI components still call
    it -- CrestwellIQ.tsx and PipelineTracker.tsx in the marketing site. They
    handle an empty result set; they would surface a 404 as an error. Removing
    the route without touching them would turn a silently-empty search box into
    a visibly broken one.

    So this returns exactly the shape the old endpoint returned on its success
    path, with no results. Behaviour in the UI is unchanged from today.

    To actually retire it: point those two components at
    /api/browser/search (which works and reads live data), then delete this.
    """
    try:
        limit_val = max(1, min(int(limit), 200)) if limit else 50
    except (TypeError, ValueError):
        limit_val = 50
    return {
        "results": [],
        "limit": limit_val,
        "sort": sort or "relevance",
        "next_cursor": None,
        "facets": {},
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True}


# Example endpoint that reads the raw body (works for CSV/JSONL uploads)
@app.post("/ingest")
async def ingest(request: Request, _auth: AuthContext = AUTH_CTX_DEP):
    data = await request.body()  # middleware will cap size before this
    # TODO: tenant-scope ingest into Postgres and enqueue pipeline jobs
    return {"ok": True, "received_bytes": len(data)}


# -----------------------
# Runs (Control Plane API)
# -----------------------


@app.post("/runs")
async def create_run(payload: RunCreateRequest, auth: AuthContext = AUTH_CTX_DEP):
    # Normalize domains
    domains: list[str] = []
    for d in payload.domains:
        d2 = (d or "").strip().lower()
        if d2:
            domains.append(d2)

    if not domains:
        return _error_response(
            400,
            "invalid_domains",
            "domains must contain at least one non-empty domain",
        )

    run_id = _db_insert_run(
        tenant_id=auth.tenant_id,
        user_id=auth.user_id,
        user_email=auth.email,
        domains=domains,
        options=payload.options or {},
        label=(payload.label or None),
    )

    # Enqueue orchestrator job
    _enqueue_pipeline_start(run_id=run_id, tenant_id=auth.tenant_id)

    return {
        "run_id": run_id,
        "status": "queued",
        "tenant_id": auth.tenant_id,
        "created_at": _utc_now_iso(),
    }


@app.get("/runs")
async def list_runs(limit: int = 50, auth: AuthContext = AUTH_CTX_DEP):
    rows = _db_list_runs(tenant_id=auth.tenant_id, limit=limit)
    return {"results": rows, "limit": min(max(1, int(limit)), 200)}


@app.get("/runs/{run_id}")
async def get_run(run_id: str, auth: AuthContext = AUTH_CTX_DEP):
    row = _db_get_run(tenant_id=auth.tenant_id, run_id=run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found")

    # Parse JSON fields if present
    def _j(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return v
        try:
            return json.loads(str(v))
        except Exception:
            return v

    return {
        "run_id": row.get("id"),
        "tenant_id": row.get("tenant_id"),
        "user_id": row.get("user_id"),
        "label": row.get("label"),
        "status": row.get("status"),
        "domains": _j(row.get("domains_json")),
        "options": _j(row.get("options_json")),
        "progress": _j(row.get("progress_json")),
        "error": row.get("error"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
    }


@app.get("/runs/{run_id}/results")
async def get_run_results(
    run_id: str,
    limit: int = 100,
    offset: int = 0,
    auth: AuthContext = AUTH_CTX_DEP,
):
    rows = _db_run_results(
        tenant_id=auth.tenant_id,
        run_id=run_id,
        limit=limit,
        offset=offset,
    )
    return {
        "results": rows,
        "limit": max(1, min(int(limit), 500)),
        "offset": max(0, int(offset)),
    }


@app.get("/runs/{run_id}/export")
async def export_run(
    run_id: str,
    format: str = "csv",
    limit: int = 10000,
    auth: AuthContext = AUTH_CTX_DEP,
):
    fmt = (format or "csv").strip().lower()
    lim = max(1, min(int(limit), 100000))

    rows = _db_run_results(tenant_id=auth.tenant_id, run_id=run_id, limit=lim, offset=0)

    if fmt == "json":
        return {"run_id": run_id, "count": len(rows), "results": rows}

    if fmt != "csv":
        return _error_response(400, "invalid_format", "format must be one of: csv, json")

    # Stream CSV
    output = io.StringIO()
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = [
            "email",
            "first_name",
            "last_name",
            "full_name",
            "title",
            "company",
            "company_id",
            "company_domain",
            "icp_score",
            "verify_status",
            "verify_reason",
            "verified_at",
            "verify_label",
            "is_primary_for_person",
            "source_url",
        ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames})

    data = output.getvalue().encode("utf-8")
    headers = {
        "Content-Disposition": f'attachment; filename="run_{run_id}.csv"',
        "Content-Type": "text/csv; charset=utf-8",
    }
    return StreamingResponse(io.BytesIO(data), headers=headers)
