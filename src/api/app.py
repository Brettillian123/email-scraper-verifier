# src/api/app.py

from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import io
import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from src.api import admin as admin_routes
from src.api.middleware.body_limit import BodySizeLimitMiddleware
from src.search.backend import SearchBackend, SearchResult, SqliteFtsBackend
from src.search.indexing import LeadSearchParams

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------

# Configurable via env; default 5 MiB
BODY_LIMIT_BYTES = int(os.getenv("BODY_LIMIT_BYTES", str(5 * 1024 * 1024)))
DB_PATH = os.getenv("DB_PATH", "data/dev.db")


def _is_postgres_configured() -> bool:
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


# Auth modes:
#   - "none": no auth enforced (NOT for production)
#   - "dev": header-based dev auth (recommended for local testing)
#   - "hs256": minimal HS256 JWT verification using stdlib only
AUTH_MODE = os.getenv("AUTH_MODE", "dev").strip().lower()

# Dev auth defaults (used when AUTH_MODE is none/dev)
DEV_TENANT_ID = os.getenv("DEV_TENANT_ID", "dev").strip()
DEV_USER_ID = os.getenv("DEV_USER_ID", "user_dev").strip()

# HS256 JWT settings (AUTH_MODE=hs256)
AUTH_HS256_SECRET = os.getenv("AUTH_HS256_SECRET", "").strip()
AUTH_JWT_ISSUER = os.getenv("AUTH_JWT_ISSUER", "").strip() or None
AUTH_JWT_AUDIENCE = os.getenv("AUTH_JWT_AUDIENCE", "").strip() or None

# RQ / Redis
RUNS_QUEUE_NAME = os.getenv("RUNS_QUEUE_NAME", "orchestrator").strip()
RQ_REDIS_URL = (
    os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
).strip()

app = FastAPI(title="Email Scraper API")

# Register early so limits apply to all routes
app.add_middleware(BodySizeLimitMiddleware, max_bytes=BODY_LIMIT_BYTES)

# R24: admin UI + metrics JSON
app.include_router(admin_routes.router)

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


# --------------------------------------------------------------------------------------
# Auth (tenant scoping primitive)
# --------------------------------------------------------------------------------------


class AuthContext(BaseModel):
    tenant_id: str
    user_id: str
    email: str | None = None
    roles: list[str] = Field(default_factory=list)


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("utf-8"))


def _jwt_hs256_verify(token: str) -> dict[str, Any]:
    """
    Minimal HS256 JWT verifier using the Python stdlib.

    This is intentionally minimal for an MVP without adding new deps.
    In production, prefer a hosted auth provider + RS256/JWKS validation.
    """
    if not AUTH_HS256_SECRET:
        raise HTTPException(
            status_code=500,
            detail="AUTH_MODE=hs256 requires AUTH_HS256_SECRET to be set",
        )

    parts = token.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid bearer token format")

    header_b64, payload_b64, sig_b64 = parts
    try:
        header = json.loads(_b64url_decode(header_b64).decode("utf-8"))
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid bearer token encoding") from exc

    if (header.get("alg") or "").upper() != "HS256":
        raise HTTPException(status_code=401, detail="Unsupported JWT alg (expected HS256)")

    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected_sig = hmac.new(
        AUTH_HS256_SECRET.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    actual_sig = _b64url_decode(sig_b64)

    if not hmac.compare_digest(expected_sig, actual_sig):
        raise HTTPException(status_code=401, detail="Invalid token signature")

    # Optional issuer/audience checks
    if AUTH_JWT_ISSUER and payload.get("iss") != AUTH_JWT_ISSUER:
        raise HTTPException(status_code=401, detail="Invalid token issuer")
    if AUTH_JWT_AUDIENCE:
        aud = payload.get("aud")
        if isinstance(aud, str):
            ok = aud == AUTH_JWT_AUDIENCE
        elif isinstance(aud, list):
            ok = AUTH_JWT_AUDIENCE in aud
        else:
            ok = False
        if not ok:
            raise HTTPException(status_code=401, detail="Invalid token audience")

    return payload


def get_auth_context(
    authorization: str | None = Header(default=None),
    x_tenant_id: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
    x_user_email: str | None = Header(default=None),
) -> AuthContext:
    """
    Resolve (tenant_id, user_id) for tenant scoping.

    - AUTH_MODE=none: returns DEV_* defaults
    - AUTH_MODE=dev: uses X-Tenant-Id / X-User-Id headers (falls back to DEV_*)
    - AUTH_MODE=hs256: reads Authorization: Bearer <jwt> and maps claims to tenant/user
    """
    if AUTH_MODE == "none":
        return AuthContext(tenant_id=DEV_TENANT_ID, user_id=DEV_USER_ID, email=x_user_email)

    if AUTH_MODE == "dev":
        tenant = (x_tenant_id or "").strip() or DEV_TENANT_ID
        user = (x_user_id or "").strip() or DEV_USER_ID
        email = (x_user_email or "").strip() or None
        return AuthContext(tenant_id=tenant, user_id=user, email=email)

    if AUTH_MODE == "hs256":
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token")
        token = authorization.split(" ", 1)[1].strip()
        claims = _jwt_hs256_verify(token)

        # Map common claim names to your tenant model.
        # Adjust these once you choose Clerk/Auth0/Supabase Auth, etc.
        tenant = (
            claims.get("tenant_id") or claims.get("org_id") or claims.get("organization_id") or ""
        ).strip()
        user = (claims.get("sub") or claims.get("user_id") or "").strip()
        email = (claims.get("email") or "").strip() or None

        if not tenant or not user:
            raise HTTPException(status_code=401, detail="Token missing tenant_id and/or sub")

        roles: list[str] = []
        raw_roles = claims.get("roles") or claims.get("role") or []
        if isinstance(raw_roles, str):
            roles = [raw_roles]
        elif isinstance(raw_roles, list):
            roles = [str(r) for r in raw_roles if r]

        return AuthContext(tenant_id=tenant, user_id=user, email=email, roles=roles)

    raise HTTPException(status_code=500, detail=f"Unsupported AUTH_MODE={AUTH_MODE!r}")


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
# Existing search API helpers
# --------------------------------------------------------------------------------------


def _parse_csv_param(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    parts = [item.strip() for item in raw.split(",")]
    values = [item for item in parts if item]
    return values or None


def _decode_cursor(cursor: str) -> dict[str, Any] | None:
    if not cursor:
        return None
    padding = "=" * (-len(cursor) % 4)
    token = cursor + padding
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    token = base64.urlsafe_b64encode(raw).decode("ascii")
    return token


def _extract_tech_keywords(company_attrs: Any) -> list[str]:
    if not company_attrs:
        return []
    data: Any
    if isinstance(company_attrs, str):
        try:
            data = json.loads(company_attrs)
        except json.JSONDecodeError:
            return []
    elif isinstance(company_attrs, dict):
        data = company_attrs
    else:
        return []
    tech = data.get("tech_keywords")
    if isinstance(tech, list):
        return [str(t) for t in tech if t]
    if isinstance(tech, str):
        return [tech]
    return []


def _row_to_lead(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "email": row.get("email"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "full_name": row.get("full_name"),
        "title": row.get("title"),
        "role_family": row.get("role_family"),
        "seniority": row.get("seniority"),
        "company": row.get("company"),
        "company_id": row.get("company_id"),
        "company_domain": row.get("company_domain"),
        "industry": row.get("industry"),
        "company_size": row.get("company_size"),
        "tech": _extract_tech_keywords(row.get("company_attrs")),
        "icp_score": row.get("icp_score"),
        "verify_status": row.get("verify_status"),
        "verified_at": row.get("verified_at"),
        "source": row.get("source"),
        "source_url": row.get("source_url"),
        "verify_label": row.get("verify_label"),
        "is_primary_for_person": row.get("is_primary_for_person"),
    }


def _get_search_backend(request: Request) -> SearchBackend:
    backend: SearchBackend | None = getattr(request.app.state, "search_backend", None)
    if backend is not None:
        return backend

    # Prevent accidental backend mixing: /leads/search is still SQLite-only.
    if _is_postgres_configured():
        raise HTTPException(
            status_code=501,
            detail=(
                "/leads/search is SQLite-FTS-only (SqliteFtsBackend). "
                "You are configured for Postgres via DATABASE_URL/DB_URL. "
                "Implement a Postgres-backed search backend (Phase 3) "
                "or run with SQLite for now."
            ),
        )

    # Import here to avoid circular import issues at module import time.
    from src.db import get_connection  # type: ignore[import]

    conn = get_connection(DB_PATH)
    backend = SqliteFtsBackend(conn)
    request.app.state.search_backend = backend
    return backend


def _normalize_sort(sort: str | None) -> tuple[str, JSONResponse | None]:
    normalized = sort or "icp_desc"
    if normalized not in {"icp_desc", "verified_desc"}:
        return normalized, _error_response(
            400,
            "invalid_sort",
            "sort must be one of: icp_desc, verified_desc",
        )
    return normalized, None


def _parse_icp_min(raw: str | None) -> tuple[int | None, JSONResponse | None]:
    if raw is None:
        return None, None
    try:
        return int(raw), None
    except ValueError:
        return None, _error_response(400, "invalid_icp_min", "icp_min must be an integer")


def _parse_recency_days(raw: str | None) -> tuple[int | None, JSONResponse | None]:
    if raw is None:
        return None, None
    try:
        value = int(raw)
    except ValueError:
        return None, _error_response(
            400,
            "invalid_recency_days",
            "recency_days must be an integer",
        )
    if value <= 0:
        return None, _error_response(
            400,
            "invalid_recency_days",
            "recency_days must be a positive integer",
        )
    return value, None


def _parse_limit(raw: str | None) -> tuple[int, JSONResponse | None]:
    if raw is None or not raw.strip():
        value = 50
    else:
        try:
            value = int(raw)
        except ValueError:
            return 0, _error_response(400, "invalid_limit", "limit must be an integer")
    if value < 1 or value > 100:
        return 0, _error_response(400, "invalid_limit", "limit must be between 1 and 100")
    return value, None


def _parse_cursor(
    cursor: str | None,
    sort: str,
) -> tuple[int | None, str | None, int | None, JSONResponse | None]:
    if cursor is None:
        return None, None, None, None

    cursor_data = _decode_cursor(cursor)
    if cursor_data is None:
        return (
            None,
            None,
            None,
            _error_response(
                400,
                "invalid_cursor",
                "cursor is malformed or cannot be decoded",
            ),
        )

    cursor_sort = cursor_data.get("sort")
    if cursor_sort != sort:
        return (
            None,
            None,
            None,
            _error_response(
                400,
                "invalid_cursor",
                "cursor sort does not match requested sort",
            ),
        )

    try:
        if sort == "icp_desc":
            cursor_icp = int(cursor_data["icp_score"])
            cursor_person_id = int(cursor_data["person_id"])
            return cursor_icp, None, cursor_person_id, None

        cursor_verified_at = str(cursor_data["verified_at"])
        cursor_person_id = int(cursor_data["person_id"])
        return None, cursor_verified_at, cursor_person_id, None
    except (KeyError, TypeError, ValueError):
        return (
            None,
            None,
            None,
            _error_response(
                400,
                "invalid_cursor",
                "cursor payload is missing required fields",
            ),
        )


def _search_leads_with_cache(
    backend: SearchBackend,
    params: LeadSearchParams,
    cursor: str | None,
) -> SearchResult:
    if cursor is not None:
        return backend.search(params)
    try:
        from src.search.cache import search_with_cache
    except ImportError:
        return backend.search(params)
    return search_with_cache(backend, params)


def _build_next_cursor(rows: list[dict[str, Any]], sort: str, limit: int) -> str | None:
    if len(rows) != limit or not rows:
        return None

    last = rows[-1]
    if sort == "icp_desc":
        icp_score = last.get("icp_score")
        person_id = last.get("person_id")
        if icp_score is None or person_id is None:
            return None
        payload = {"sort": "icp_desc", "icp_score": int(icp_score), "person_id": int(person_id)}
        return _encode_cursor(payload)

    verified_at = last.get("verified_at")
    person_id = last.get("person_id")
    if verified_at is None or person_id is None:
        return None
    payload = {
        "sort": "verified_desc",
        "verified_at": str(verified_at),
        "person_id": int(person_id),
    }
    return _encode_cursor(payload)


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

    Expected future function:
        src.queueing.tasks.pipeline_start(run_id=..., tenant_id=...)

    If not yet implemented, we raise a clear 501 so the API remains explicit.
    """
    try:
        from redis import Redis  # type: ignore
        from rq import Queue  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"RQ/Redis not available: {exc}") from exc

    try:
        from src.queueing.tasks import pipeline_start  # type: ignore
    except Exception as exc:
        raise HTTPException(
            status_code=501,
            detail=(
                "pipeline_start is not implemented yet (expected at "
                "src.queueing.tasks.pipeline_start)"
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


# -----------------------
# Existing Leads Search API
# -----------------------


@app.get("/leads/search")
async def leads_search(
    request: Request,
    q: str = "",
    verify_status: str | None = None,
    icp_min: str | None = None,
    roles: str | None = None,
    seniority: str | None = None,
    industries: str | None = None,
    sizes: str | None = None,
    tech: str | None = None,
    source: str | None = None,
    recency_days: str | None = None,
    sort: str = "icp_desc",
    limit: str | None = None,
    cursor: str | None = None,
    facets: str | None = None,
    _auth: AuthContext = AUTH_CTX_DEP,
):
    """
    R22/R23: /leads/search API.

    Note:
      - This endpoint currently uses the SqliteFtsBackend and is not yet tenant-scoped.
      - When you migrate to Postgres as the system of record, replace this with a
        Postgres-backed search (or keep SQLite as a derived read model).
    """
    if not q or not q.strip():
        return _error_response(400, "invalid_query", "q must be a non-empty search query")

    normalized_sort, sort_error = _normalize_sort(sort)
    if sort_error is not None:
        return sort_error

    icp_min_val, icp_error = _parse_icp_min(icp_min)
    if icp_error is not None:
        return icp_error

    recency_days_val, recency_error = _parse_recency_days(recency_days)
    if recency_error is not None:
        return recency_error

    limit_val, limit_error = _parse_limit(limit)
    if limit_error is not None:
        return limit_error

    verify_status_list = _parse_csv_param(verify_status)
    roles_list = _parse_csv_param(roles)
    seniority_list = _parse_csv_param(seniority)
    industries_list = _parse_csv_param(industries)
    sizes_list = _parse_csv_param(sizes)
    tech_list = _parse_csv_param(tech)
    source_list = _parse_csv_param(source)
    facets_list = _parse_csv_param(facets)

    cursor_icp, cursor_verified_at, cursor_person_id, cursor_error = _parse_cursor(
        cursor,
        normalized_sort,
    )
    if cursor_error is not None:
        return cursor_error

    params = LeadSearchParams(
        query=q,
        verify_status=verify_status_list,
        icp_min=icp_min_val,
        roles=roles_list,
        seniority=seniority_list,
        industries=industries_list,
        sizes=sizes_list,
        tech=tech_list,
        source=source_list,
        recency_days=recency_days_val,
        sort=normalized_sort,
        limit=limit_val,
        cursor_icp=cursor_icp,
        cursor_verified_at=cursor_verified_at,
        cursor_person_id=cursor_person_id,
        facets=facets_list,
    )

    backend = _get_search_backend(request)
    result = _search_leads_with_cache(backend, params, cursor)
    results = [_row_to_lead(row) for row in result.leads]
    next_cursor = _build_next_cursor(result.leads, normalized_sort, limit_val)

    return {
        "results": results,
        "limit": limit_val,
        "sort": normalized_sort,
        "next_cursor": next_cursor,
        "facets": result.facets or {},
    }
