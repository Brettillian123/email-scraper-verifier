# src/api/browser.py - PostgreSQL version
from __future__ import annotations

import csv
import io
import json
import os
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

RQ_REDIS_URL = (
    os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
).strip()
router = APIRouter(prefix="/api/browser", tags=["browser"])


# --------------------------------------------------------------------------------------
# Auth / tenant scoping
# --------------------------------------------------------------------------------------

try:
    # Preferred dependency provider (avoids importing FastAPI app module).
    from src.api.deps import AuthContext, get_auth_context  # type: ignore
except Exception:  # pragma: no cover
    from fastapi import Header

    class AuthContext(BaseModel):  # type: ignore[no-redef]
        tenant_id: str = Field(default=os.getenv("DEV_TENANT_ID", "dev").strip() or "dev")
        user_id: str = Field(default=os.getenv("DEV_USER_ID", "user_dev").strip() or "user_dev")
        email: str | None = None
        roles: list[str] = Field(default_factory=list)

    def get_auth_context(  # type: ignore[no-redef]
        request: Request,
        x_tenant_id: str | None = Header(default=None),
        x_user_id: str | None = Header(default=None),
        x_user_email: str | None = Header(default=None),
    ) -> AuthContext:
        # 1. Try session cookie first (dashboard browser calls)
        try:
            from src.auth.core import SESSION_COOKIE_NAME, get_session

            session_id = request.cookies.get(SESSION_COOKIE_NAME)
            if session_id:
                session, user = get_session(session_id)
                if session and user:
                    return AuthContext(
                        tenant_id=user.tenant_id,
                        user_id=user.id,
                        email=user.email,
                        roles=["admin"] if user.is_superuser else [],
                    )
        except Exception:
            pass

        # 2. Fall back to header-based auth (API / dev callers)
        tenant = (x_tenant_id or "").strip() or os.getenv("DEV_TENANT_ID", "dev").strip() or "dev"
        user = (
            (x_user_id or "").strip() or os.getenv("DEV_USER_ID", "user_dev").strip() or "user_dev"
        )
        email = (x_user_email or "").strip() or None
        return AuthContext(tenant_id=tenant, user_id=user, email=email, roles=[])


def _get_conn():
    from src.db import get_conn

    return get_conn()


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _has_table(con: Any, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = %s LIMIT 1",
            (table,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _has_column(con: Any, table: str, column: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = current_schema() "
            "AND table_name = %s AND column_name = %s LIMIT 1",
            (table, column),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


# --------------------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------------------


class RunCreateRequest(BaseModel):
    domains: list[str] = Field(..., min_length=1, max_length=5000)
    ai_enabled: bool = Field(default=True)
    force_discovery: bool = Field(default=False)
    modes: list[str] = Field(default=["full"])
    company_limit: int = Field(default=1000, ge=1, le=5000)
    label: str | None = Field(default=None, max_length=255)


class PaginatedResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    page: int
    page_size: int
    total_pages: int


class ManualCandidateInput(BaseModel):
    """Single candidate in a manual submission batch."""

    first_name: str | None = None
    last_name: str | None = None
    full_name: str | None = None
    title: str | None = None
    email: str | None = None

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().lower()
        if not v:
            return None
        if "@" not in v:
            raise ValueError("Invalid email format: missing '@'")
        return v

    @field_validator("first_name", "last_name", "full_name", "title", mode="before")
    @classmethod
    def strip_strings(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = str(v).strip()
        return v if v else None


class ManualCandidateRequest(BaseModel):
    """Batch submission of manual candidates to a company."""

    candidates: list[ManualCandidateInput] = Field(
        ...,
        min_length=1,
        max_length=50,
        description="List of candidates to add (max 50 per batch)",
    )


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

_ALLOWED_MODES = {"full", "autodiscovery", "generate", "verify"}
_ALLOWED_EXPORT_STATUS_FILTERS = {"valid", "invalid", "risky_catch_all", "unknown_timeout"}


def _normalize_modes(modes: Iterable[str] | None) -> list[str]:
    """
    Normalize UI-provided modes into the canonical list expected by the pipeline:

      - ["full"]
      - ["autodiscovery"]
      - ["generate", "verify"]
      - ["verify"]

    Also accepts a few historical/synonym forms (e.g., "generate_verify", "generate+verify").
    """
    if not modes:
        return ["full"]

    out: list[str] = []
    for raw in modes:
        m = str(raw or "").strip().lower()
        if not m:
            continue

        if m in {"full"}:
            return ["full"]

        if m in {"autodiscovery", "discovery", "auto"}:
            if "autodiscovery" not in out:
                out.append("autodiscovery")
            continue

        if m in {"generate_verify", "generate+verify", "gen_verify", "generateandverify"}:
            if "generate" not in out:
                out.append("generate")
            if "verify" not in out:
                out.append("verify")
            continue

        if m in {"generate", "gen"}:
            if "generate" not in out:
                out.append("generate")
            continue

        if m in {"verify", "verification"}:
            if "verify" not in out:
                out.append("verify")
            continue

        # Ignore unknown mode strings rather than failing hard (keeps UI forward-compatible).
        continue

    return out or ["full"]


def _domain_risk_levels_for_company_ids(
    con: Any,
    *,
    company_ids: list[int],
) -> dict[int, str]:
    """
    Best-effort domain risk/unknown signal per company_id.

    Interpretation heuristic (domain-level UX):
      - risky: any latest verify_status starts with 'risky'
      - unknown: any latest verify_status starts with 'unknown' or 'temp'
      - unknown: company has emails but none have verification_results (untested permutations)
      - else: no flag
    """
    if not company_ids:
        return {}

    where_parts: list[str] = [f"e.company_id IN ({','.join(['%s'] * len(company_ids))})"]
    params: list[Any] = list(company_ids)

    # NOTE: use %% inside SQL literals to avoid psycopg2 percent interpolation.
    sql = f"""
        SELECT
            e.company_id,
            MAX(CASE WHEN vr.verify_status ILIKE 'risky%%' THEN 1 ELSE 0 END) AS has_risky,
            MAX(
                CASE WHEN vr.verify_status ILIKE 'unknown%%'
                    OR vr.verify_status ILIKE 'temp%%'
                THEN 1 ELSE 0 END
            ) AS has_unknown,
            SUM(CASE WHEN vr.verify_status IS NOT NULL THEN 1 ELSE 0 END) AS status_rows,
            COUNT(e.id) AS email_rows
        FROM emails e
        LEFT JOIN LATERAL (
            SELECT verify_status
            FROM verification_results vr
            WHERE vr.email_id = e.id
            ORDER BY vr.verified_at DESC NULLS LAST, vr.id DESC
            LIMIT 1
        ) vr ON true
        WHERE {" AND ".join(where_parts)}
        GROUP BY e.company_id
    """

    out: dict[int, str] = {}
    try:
        rows = con.execute(sql, tuple(params)).fetchall()
        for company_id, has_risky, has_unknown, status_rows, email_rows in rows:
            if int(has_risky or 0) == 1:
                out[int(company_id)] = "risky"
            elif int(has_unknown or 0) == 1:
                out[int(company_id)] = "unknown"
            else:
                if int(email_rows or 0) > 0 and int(status_rows or 0) == 0:
                    out[int(company_id)] = "unknown"
    except Exception:
        return {}

    return out


def _enforce_24h_hard_limit(con: Any, *, tenant_id: str, requested: int) -> None:
    """
    Best-effort upfront enforcement of the 24h hard company cap.

    The pipeline enforces/trims as well; this is a nicer UX for the dashboard.
    """
    try:
        from src.queueing.pipeline_v2 import HARD_COMPANY_LIMIT_24H, _count_companies_last_24h
    except Exception:
        return

    try:
        used, method = _count_companies_last_24h(con, tenant_id=tenant_id)
    except Exception:
        return

    if used is None:
        return

    remaining = max(0, int(HARD_COMPANY_LIMIT_24H) - int(used))
    if remaining <= 0:
        raise HTTPException(
            status_code=429,
            detail=(
                f"24h company limit exceeded for tenant '{tenant_id}'. "
                f"Limit={HARD_COMPANY_LIMIT_24H} per 24h; used={used} (method={method})."
            ),
        )
    if requested > remaining:
        raise HTTPException(
            status_code=429,
            detail=(
                f"24h company limit would be exceeded for tenant '{tenant_id}'. "
                f"Remaining={remaining} of {HARD_COMPANY_LIMIT_24H}; "
                f"requested={requested} (method={method})."
            ),
        )


def _parse_company_ids(ids: str) -> list[int]:
    parts = [p.strip() for p in (ids or "").split(",")]
    out: list[int] = []
    seen: set[int] = set()
    for p in parts:
        if not p:
            continue
        try:
            cid = int(p)
        except ValueError:
            continue
        if cid <= 0:
            continue
        if cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
    return out


def _iter_fetchmany(cur: Any, batch_size: int = 1000):
    """
    Robust row iterator. Prefers fetchmany() for streaming, falls back to fetchall().

    Important: some drivers/cursors can become closed after an
    exception; in that case, stop cleanly.
    """
    while True:
        try:
            batch = cur.fetchmany(batch_size)
        except Exception:
            try:
                rows = cur.fetchall() or []
            except Exception:
                return
            for row in rows:
                yield row
            return

        if not batch:
            return
        for row in batch:
            yield row


def _export_name_expr() -> str:
    return (
        "COALESCE("
        "NULLIF(p.full_name, ''), "
        "NULLIF(BTRIM(COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'')), '')"
        ")"
    )


def _export_vr_expr(vr_has_fallback: bool) -> tuple[str, str]:
    if vr_has_fallback:
        return "COALESCE(vr.verify_status, vr.status, vr.fallback_status, '')", (
            "verify_status, status, fallback_status, verified_at, id"
        )
    return "COALESCE(vr.verify_status, vr.status, '')", "verify_status, status, verified_at, id"


def _export_status_filter_clause(
    *,
    status_filter: str,
    vr_status_expr: str,
    where_parts: list[str],
) -> tuple[str, list[Any]]:
    if status_filter not in _ALLOWED_EXPORT_STATUS_FILTERS:
        return "", []

    if status_filter == "valid":
        status_where = f"AND LOWER({vr_status_expr}) LIKE %s"
        status_params: list[Any] = ["valid%"]
    else:
        status_where = f"AND LOWER({vr_status_expr}) = %s"
        status_params = [status_filter]

    where_parts.append("e.id IS NOT NULL")
    where_parts.append("vr.id IS NOT NULL")
    return status_where, status_params


def _export_join_tenant_clauses() -> tuple[str, list[Any], str, list[Any]]:
    """Return empty tenant clauses — all authenticated users see all data."""
    return "", [], "", []


def _export_selected_companies_sql(
    con: Any,
    *,
    company_ids: list[int],
    status_filter: str,
) -> tuple[str, tuple[Any, ...]]:
    vr_has_fallback = _has_column(con, "verification_results", "fallback_status")

    name_expr = _export_name_expr()
    vr_status_expr, vr_select_cols = _export_vr_expr(vr_has_fallback)

    placeholders = ",".join(["%s"] * len(company_ids))
    where_parts: list[str] = [f"c.id IN ({placeholders})"]
    where_params: list[Any] = list(company_ids)

    where_parts.append(f"{name_expr} IS NOT NULL AND {name_expr} <> ''")

    status_where, status_params = _export_status_filter_clause(
        status_filter=status_filter,
        vr_status_expr=vr_status_expr,
        where_parts=where_parts,
    )

    where_clause = " AND ".join(where_parts)

    (
        e_tenant_clause,
        e_tenant_params,
        vr_tenant_clause,
        vr_tenant_params,
    ) = _export_join_tenant_clauses()

    sql = f"""
        SELECT DISTINCT ON (c.id, p.id)
            COALESCE(NULLIF(c.domain,''), NULLIF(c.official_domain,'')) AS company_domain,
            {name_expr} AS full_name,
            p.title AS title,
            e.email AS email,
            CASE WHEN e.id IS NOT NULL AND vr.id IS NOT NULL
                 THEN LOWER({vr_status_expr})
                 ELSE ''
            END AS verify_status
        FROM companies c
        JOIN people p
          ON p.company_id = c.id
        LEFT JOIN emails e
          ON e.company_id = c.id
         AND e.person_id = p.id
         AND e.email IS NOT NULL
         AND e.email <> ''
         {e_tenant_clause}
        LEFT JOIN LATERAL (
            SELECT {vr_select_cols}
            FROM verification_results vr
            WHERE vr.email_id = e.id
              {vr_tenant_clause}
            ORDER BY vr.verified_at DESC NULLS LAST, vr.id DESC
            LIMIT 1
        ) vr ON e.id IS NOT NULL
        WHERE {where_clause}
        {status_where}
        ORDER BY c.id, p.id, vr.verified_at DESC NULLS LAST, vr.id DESC, e.id DESC
    """

    all_params = tuple(e_tenant_params + vr_tenant_params + where_params + status_params)
    return sql, all_params


def _export_selected_companies_csv_generator(con: Any, cur: Any):
    try:
        buf = io.StringIO()
        writer = csv.writer(buf)

        writer.writerow(["company_domain", "full_name", "title", "email", "verify_status"])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        for row in _iter_fetchmany(cur, batch_size=1000):
            writer.writerow(
                [
                    row[0] or "",  # company_domain
                    row[1] or "",  # full_name
                    row[2] or "",  # title
                    row[3] or "",  # email
                    row[4] or "",  # verify_status
                ]
            )
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)
    finally:
        try:
            con.close()
        except Exception:
            pass


def _export_selected_companies_filename(status_filter: str) -> str:
    filename = "companies_export"
    if status_filter:
        filename += f"_{status_filter}"
    return f"{filename}.csv"


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


@router.get("/stats")
def get_stats(auth: Annotated[AuthContext, Depends(get_auth_context)]) -> dict[str, Any]:
    con = _get_conn()
    try:
        stats: dict[str, Any] = {}

        for table, key in [
            ("companies", "companies"),
            ("people", "people"),
            ("emails", "emails"),
            ("sources", "sources"),
            ("runs", "runs"),
            ("verification_results", "verifications"),
        ]:
            try:
                stats[key] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception:
                stats[key] = 0

        try:
            sql = (
                "SELECT verify_status, COUNT(*) "
                "FROM verification_results "
                "WHERE verify_status IS NOT NULL "
                "GROUP BY verify_status"
            )
            rows = con.execute(sql).fetchall()
            stats["verification_breakdown"] = {r[0]: r[1] for r in rows}
        except Exception:
            stats["verification_breakdown"] = {}

        try:
            rows = con.execute(
                "SELECT status, COUNT(*) FROM runs GROUP BY status",
            ).fetchall()
            stats["runs_by_status"] = {r[0]: r[1] for r in rows}
        except Exception:
            stats["runs_by_status"] = {}

        return stats
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/companies")
def list_companies(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    search: str = Query(None),
    min_people: int = Query(None, ge=0),
    exported: str = Query(None),
) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size

        where_parts: list[str] = []
        params: list[Any] = []

        if search:
            st = f"%{search.lower()}%"
            where_parts.append(
                "(LOWER(c.domain) LIKE %s"
                " OR LOWER(c.name) LIKE %s"
                " OR LOWER(c.official_domain) LIKE %s)"
            )
            params.extend([st, st, st])

        # Filter: min_people — hide companies with fewer than N people
        if min_people is not None and min_people > 0:
            where_parts.append("(SELECT COUNT(*) FROM people p WHERE p.company_id = c.id) >= %s")
            params.append(min_people)

        # Filter: exported — "yes" = only exported, "no" = only not exported
        if exported == "yes":
            where_parts.append("c.exported_at IS NOT NULL")
        elif exported == "no":
            where_parts.append("c.exported_at IS NULL")

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        total = con.execute(
            f"SELECT COUNT(*) FROM companies c {where_clause}",
            tuple(params),
        ).fetchone()[0]

        sql = f"""
            SELECT
                c.id, c.name, c.domain, c.official_domain,
                c.attrs, c.created_at,
                (SELECT COUNT(*) FROM people p
                 WHERE p.company_id = c.id),
                (SELECT COUNT(*) FROM emails e
                 WHERE e.company_id = c.id),
                (SELECT COUNT(*) FROM sources s
                 WHERE s.company_id = c.id),
                c.exported_at
            FROM companies c
            {where_clause}
            ORDER BY c.id DESC
            LIMIT %s OFFSET %s
        """

        rows = con.execute(sql, tuple(params + [page_size, offset])).fetchall()

        items: list[dict[str, Any]] = []
        company_ids: list[int] = []
        for r in rows:
            company_ids.append(int(r[0]))
            attrs: dict[str, Any] = {}
            if r[4]:
                try:
                    attrs = json.loads(r[4]) if isinstance(r[4], str) else r[4]
                except Exception:
                    attrs = {}
            items.append(
                {
                    "id": r[0],
                    "name": r[1],
                    "domain": r[2],
                    "official_domain": r[3],
                    "ai_extracted": bool(attrs.get("ai_people_extracted", False)),
                    "created_at": r[5],
                    "people_count": r[6],
                    "emails_count": r[7],
                    "pages_count": r[8],
                    "exported_at": r[9],
                }
            )

        risk_map = _domain_risk_levels_for_company_ids(
            con,
            company_ids=company_ids,
        )
        for it in items:
            rid = risk_map.get(int(it["id"]))
            if rid:
                it["domain_risk_level"] = rid

        return PaginatedResponse(
            items=items,
            total=int(total or 0),
            page=page,
            page_size=page_size,
            total_pages=max(1, (int(total or 0) + page_size - 1) // page_size),
        )
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/companies/export.csv")
def export_selected_companies_csv(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    ids: str = Query(..., description="Comma-separated company IDs"),
    status: str = Query(
        "",
        description="Filter: valid, invalid, risky_catch_all, or empty for all",
    ),
) -> StreamingResponse:
    """
    Export selected companies' people as CSV.

    Default: ALL people (even those without emails yet).
    Pass ?status=valid to restrict to verified-valid emails only.

    CSV columns: company_domain, full_name, title, email, verify_status
    """
    company_ids = _parse_company_ids(ids)
    if not company_ids:
        raise HTTPException(
            status_code=400,
            detail="ids must contain at least one valid integer company id",
        )

    status_filter = (status or "").strip().lower()

    con = _get_conn()
    try:
        sql, params = _export_selected_companies_sql(
            con,
            company_ids=company_ids,
            status_filter=status_filter,
        )
        try:
            cur = con.execute(sql, params)
        except Exception as exc:
            try:
                con.close()
            except Exception:
                pass
            raise HTTPException(
                status_code=500,
                detail=f"CSV export query failed: {exc}",
            ) from exc

        # Mark exported companies
        try:
            placeholders = ",".join(["%s"] * len(company_ids))
            con.execute(
                f"UPDATE companies SET exported_at = NOW() "
                f"WHERE id IN ({placeholders}) AND exported_at IS NULL",
                tuple(company_ids),
            )
            con.commit()
        except Exception:
            pass  # best-effort: don't block export if marking fails

        filename = _export_selected_companies_filename(status_filter)
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}

        return StreamingResponse(
            _export_selected_companies_csv_generator(con, cur),
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )
    except HTTPException:
        raise
    except Exception as exc:
        try:
            con.close()
        except Exception:
            pass
        raise HTTPException(
            status_code=500,
            detail=f"CSV export failed: {exc}",
        ) from exc


@router.get("/companies/{company_id}")
def get_company(
    company_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    con = _get_conn()
    try:
        row = con.execute(
            "SELECT id, name, domain, official_domain,"
            " website_url, attrs, created_at, updated_at"
            " FROM companies WHERE id = %s",
            (company_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")

        attrs: dict[str, Any] = {}
        if row[5]:
            try:
                attrs = json.loads(row[5]) if isinstance(row[5], str) else row[5]
            except Exception:
                attrs = {}

        people = con.execute(
            "SELECT id, first_name, last_name, full_name,"
            " title, source_url"
            " FROM people WHERE company_id = %s ORDER BY id",
            (company_id,),
        ).fetchall()

        emails = con.execute(
            """
            SELECT
                e.id,
                e.email,
                e.source_url,
                e.person_id,
                vr.verify_status,
                vr.verify_reason
            FROM emails e
            LEFT JOIN LATERAL (
                SELECT verify_status, verify_reason
                FROM verification_results vr
                WHERE vr.email_id = e.id
                ORDER BY vr.verified_at DESC NULLS LAST, vr.id DESC
                LIMIT 1
            ) vr ON true
            WHERE e.company_id = %s
            ORDER BY e.id
            """,
            (company_id,),
        ).fetchall()

        pages = con.execute(
            "SELECT id, source_url, LENGTH(html), fetched_at "
            "FROM sources WHERE company_id = %s "
            "ORDER BY fetched_at DESC",
            (company_id,),
        ).fetchall()

        risk_map = _domain_risk_levels_for_company_ids(
            con,
            company_ids=[int(company_id)],
        )
        risk_level = risk_map.get(int(company_id))

        out: dict[str, Any] = {
            "id": row[0],
            "name": row[1],
            "domain": row[2],
            "official_domain": row[3],
            "website_url": row[4],
            "attrs": attrs,
            "created_at": row[6],
            "updated_at": row[7],
            "people": [
                {
                    "id": p[0],
                    "first_name": p[1],
                    "last_name": p[2],
                    "full_name": p[3],
                    "title": p[4],
                    "source_url": p[5],
                }
                for p in people
            ],
            "emails": [
                {
                    "id": e[0],
                    "email": e[1],
                    "source_url": e[2],
                    "person_id": e[3],
                    "verify_status": e[4],
                    "verify_reason": e[5],
                }
                for e in emails
            ],
            "pages": [
                {
                    "id": pg[0],
                    "source_url": pg[1],
                    "html_size": pg[2],
                    "fetched_at": pg[3],
                }
                for pg in pages
            ],
        }
        if risk_level:
            out["domain_risk_level"] = risk_level
        return out
    finally:
        try:
            con.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Manual candidate submission
# --------------------------------------------------------------------------------------

MANUAL_SOURCE_URL = "manual:user_added"


@router.post("/companies/{company_id}/candidates")
def submit_manual_candidates(
    company_id: int,
    request: ManualCandidateRequest,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    """
    Submit manual candidates for a company.

    Each candidate is inserted as a person (and optionally email), then an
    RQ job is enqueued to verify all candidates in the batch.  Invalid
    candidates are cleaned up after verification; valid ones are kept.

    Returns a batch_id + job_id for status polling.
    """
    from redis import Redis
    from rq import Queue

    con = _get_conn()
    try:
        tenant_id = auth.tenant_id
        user_id = auth.user_id
        now = _utc_now_iso()
        batch_id = str(uuid.uuid4())

        # Validate company exists
        company_row = con.execute(
            "SELECT id, domain, official_domain FROM companies WHERE id = %s",
            (company_id,),
        ).fetchone()
        if not company_row:
            raise HTTPException(status_code=404, detail="Company not found")

        # Validate each candidate has at least a name or email
        validated: list[dict[str, Any]] = []
        for i, cand in enumerate(request.candidates):
            has_name = bool(
                cand.full_name
                or (cand.first_name and cand.last_name)
                or cand.first_name
                or cand.last_name
            )
            if not has_name and not cand.email:
                raise HTTPException(
                    status_code=400,
                    detail=f"Candidate #{i + 1}: must provide at least a name or email",
                )

            # Derive full_name if not provided
            full_name = cand.full_name
            first_name = cand.first_name
            last_name = cand.last_name

            if not full_name and (first_name or last_name):
                full_name = f"{first_name or ''} {last_name or ''}".strip()
            elif full_name and not first_name and not last_name:
                parts = full_name.strip().split(None, 1)
                first_name = parts[0] if parts else None
                last_name = parts[1] if len(parts) > 1 else None

            validated.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "title": cand.title,
                    "email": cand.email,
                }
            )

        # Insert people + audit rows
        person_ids: list[int] = []

        for v in validated:
            person_row = con.execute(
                "INSERT INTO people"
                " (tenant_id, company_id, first_name, last_name, full_name,"
                "  title, source_url, created_at, updated_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                " RETURNING id",
                (
                    tenant_id,
                    company_id,
                    v["first_name"],
                    v["last_name"],
                    v["full_name"],
                    v["title"],
                    MANUAL_SOURCE_URL,
                    now,
                    now,
                ),
            ).fetchone()
            person_id = int(person_row[0])
            person_ids.append(person_id)

            con.execute(
                "INSERT INTO manual_candidate_attempts"
                " (tenant_id, company_id, batch_id, first_name, last_name,"
                "  full_name, title, submitted_email, outcome, person_id,"
                "  submitted_by, submitted_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s, %s)",
                (
                    tenant_id,
                    company_id,
                    batch_id,
                    v["first_name"],
                    v["last_name"],
                    v["full_name"],
                    v["title"],
                    v["email"],
                    person_id,
                    user_id,
                    now,
                ),
            )

        con.commit()

        # Enqueue the verification job
        try:
            redis = Redis.from_url(RQ_REDIS_URL)
            q = Queue(name="generate", connection=redis)

            from src.queueing.manual_candidates import task_verify_manual_candidates

            job = q.enqueue(
                task_verify_manual_candidates,
                tenant_id=tenant_id,
                company_id=company_id,
                batch_id=batch_id,
                job_timeout=600,
                meta={
                    "stage": "manual_candidate_verification",
                    "tenant_id": tenant_id,
                    "company_id": company_id,
                    "batch_id": batch_id,
                    "candidate_count": len(validated),
                },
            )

            return {
                "ok": True,
                "batch_id": batch_id,
                "job_id": job.id,
                "candidates_submitted": len(validated),
                "person_ids": person_ids,
                "status": "processing",
            }

        except Exception as exc:
            # Enqueue failed â€” roll back the inserts
            try:
                for pid in person_ids:
                    con.execute("DELETE FROM people WHERE id = %s", (pid,))
                con.execute(
                    "DELETE FROM manual_candidate_attempts WHERE batch_id = %s",
                    (batch_id,),
                )
                con.commit()
            except Exception:
                try:
                    con.rollback()
                except Exception:
                    pass

            raise HTTPException(
                status_code=500,
                detail=f"Failed to start verification: {exc}",
            ) from exc

    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/companies/{company_id}/candidates/batches/{batch_id}")
def get_manual_candidate_batch(
    company_id: int,
    batch_id: str,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    """Poll the status of a manual candidate submission batch."""
    con = _get_conn()
    try:
        rows = con.execute(
            "SELECT id, first_name, last_name, full_name, title,"
            "  submitted_email, outcome, verified_email,"
            "  verify_status, verify_reason, error_detail,"
            "  person_id, email_id, submitted_at, processed_at"
            " FROM manual_candidate_attempts"
            " WHERE batch_id = %s AND company_id = %s"
            " ORDER BY id",
            (batch_id, company_id),
        ).fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Batch not found")

        candidates = []
        pending = valid = invalid = errors = 0

        for r in rows:
            outcome = r[6]
            if outcome == "pending":
                pending += 1
            elif outcome == "valid":
                valid += 1
            elif outcome in ("invalid", "no_mx"):
                invalid += 1
            elif outcome == "error":
                errors += 1

            candidates.append(
                {
                    "id": r[0],
                    "first_name": r[1],
                    "last_name": r[2],
                    "full_name": r[3],
                    "title": r[4],
                    "submitted_email": r[5],
                    "outcome": outcome,
                    "verified_email": r[7],
                    "verify_status": r[8],
                    "verify_reason": r[9],
                    "error_detail": r[10],
                    "person_id": r[11],
                    "email_id": r[12],
                    "submitted_at": r[13],
                    "processed_at": r[14],
                }
            )

        total = len(rows)
        batch_status = "processing" if pending > 0 else "complete"

        return {
            "batch_id": batch_id,
            "company_id": company_id,
            "status": batch_status,
            "total": total,
            "pending": pending,
            "valid": valid,
            "invalid": invalid,
            "errors": errors,
            "candidates": candidates,
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/companies/{company_id}/candidates/history")
def get_manual_candidate_history(
    company_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    """Get the history of all manual candidate submissions for a company."""
    con = _get_conn()
    try:
        # Check if the table exists before querying
        if not _has_table(con, "manual_candidate_attempts"):
            return {"company_id": company_id, "batches": []}

        rows = con.execute(
            "SELECT batch_id,"
            "  COUNT(*) AS total,"
            "  COUNT(*) FILTER (WHERE outcome = 'valid') AS valid,"
            "  COUNT(*) FILTER (WHERE outcome IN ('invalid', 'no_mx')) AS invalid,"
            "  COUNT(*) FILTER (WHERE outcome = 'pending') AS pending,"
            "  COUNT(*) FILTER (WHERE outcome = 'error') AS errors,"
            "  MIN(submitted_at) AS submitted_at,"
            "  MAX(processed_at) AS last_processed_at,"
            "  MAX(submitted_by) AS submitted_by"
            " FROM manual_candidate_attempts"
            " WHERE company_id = %s"
            " GROUP BY batch_id"
            " ORDER BY MIN(submitted_at) DESC"
            " LIMIT %s",
            (company_id, limit),
        ).fetchall()

        batches = []
        for r in rows:
            batch_status = "processing" if r[4] > 0 else "complete"
            batches.append(
                {
                    "batch_id": r[0],
                    "total": r[1],
                    "valid": r[2],
                    "invalid": r[3],
                    "pending": r[4],
                    "errors": r[5],
                    "status": batch_status,
                    "submitted_at": r[6],
                    "last_processed_at": r[7],
                    "submitted_by": r[8],
                }
            )

        return {"company_id": company_id, "batches": batches}
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/people")
def list_people(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    company_id: int = Query(None),
    search: str = Query(None),
) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size

        where_parts: list[str] = []
        params: list[Any] = []

        if company_id:
            where_parts.append("p.company_id = %s")
            params.append(company_id)

        if search:
            st = f"%{search.lower()}%"
            where_parts.append(
                "(LOWER(p.first_name) LIKE %s"
                " OR LOWER(p.last_name) LIKE %s"
                " OR LOWER(p.full_name) LIKE %s"
                " OR LOWER(p.title) LIKE %s)"
            )
            params.extend([st, st, st, st])

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        total = con.execute(
            f"SELECT COUNT(*) FROM people p {where_clause}",
            tuple(params),
        ).fetchone()[0]

        sql = f"""
            SELECT
                p.id, p.first_name, p.last_name, p.full_name,
                p.title, p.source_url, p.company_id, c.domain
            FROM people p
            LEFT JOIN companies c ON c.id = p.company_id
            {where_clause}
            ORDER BY p.id DESC
            LIMIT %s OFFSET %s
        """
        rows = con.execute(sql, tuple(params + [page_size, offset])).fetchall()

        items = [
            {
                "id": r[0],
                "first_name": r[1],
                "last_name": r[2],
                "full_name": r[3],
                "title": r[4],
                "source_url": r[5],
                "company_id": r[6],
                "company_domain": r[7],
            }
            for r in rows
        ]

        return PaginatedResponse(
            items=items,
            total=int(total or 0),
            page=page,
            page_size=page_size,
            total_pages=max(1, (int(total or 0) + page_size - 1) // page_size),
        )
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/emails")
def list_emails(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    company_id: int = Query(None),
    status: str = Query(None),
    search: str = Query(None),
) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size

        where_parts: list[str] = []
        params: list[Any] = []

        if company_id:
            where_parts.append("e.company_id = %s")
            params.append(company_id)

        if search:
            where_parts.append("LOWER(e.email) LIKE %s")
            params.append(f"%{search.lower()}%")

        status_clause = ""
        status_param: list[Any] = []
        if status:
            status_clause = "AND vr.verify_status = %s"
            status_param.append(status)

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else "WHERE 1=1"

        count_sql = f"""
            SELECT COUNT(*)
            FROM emails e
            LEFT JOIN LATERAL (
                SELECT verify_status
                FROM verification_results vr
                WHERE vr.email_id = e.id
                ORDER BY vr.verified_at DESC NULLS LAST, vr.id DESC
                LIMIT 1
            ) vr ON true
            {where_clause}
            {status_clause}
        """
        total = con.execute(count_sql, tuple(params + status_param)).fetchone()[0]

        sql = f"""
            SELECT
                e.id, e.email, e.source_url, e.company_id, e.person_id,
                c.domain, p.first_name, p.last_name,
                vr.verify_status, vr.verify_reason, vr.verified_at
            FROM emails e
            LEFT JOIN companies c ON c.id = e.company_id
            LEFT JOIN people p ON p.id = e.person_id
            LEFT JOIN LATERAL (
                SELECT verify_status, verify_reason, verified_at
                FROM verification_results vr
                WHERE vr.email_id = e.id
                ORDER BY vr.verified_at DESC NULLS LAST, vr.id DESC
                LIMIT 1
            ) vr ON true
            {where_clause}
            {status_clause}
            ORDER BY e.id DESC
            LIMIT %s OFFSET %s
        """
        rows = con.execute(sql, tuple(params + status_param + [page_size, offset])).fetchall()

        items = [
            {
                "id": r[0],
                "email": r[1],
                "source_url": r[2],
                "company_id": r[3],
                "person_id": r[4],
                "company_domain": r[5],
                "first_name": r[6],
                "last_name": r[7],
                "verify_status": r[8],
                "verify_reason": r[9],
                "verified_at": r[10],
            }
            for r in rows
        ]

        return PaginatedResponse(
            items=items,
            total=int(total or 0),
            page=page,
            page_size=page_size,
            total_pages=max(1, (int(total or 0) + page_size - 1) // page_size),
        )
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/runs")
def list_runs(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size

        total = con.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        rows = con.execute(
            "SELECT id, status, label, domains_json,"
            " options_json, progress_json, error,"
            " created_at, started_at, finished_at "
            "FROM runs ORDER BY created_at DESC "
            "LIMIT %s OFFSET %s",
            (page_size, offset),
        ).fetchall()

        items: list[dict[str, Any]] = []
        for r in rows:
            domains: list[Any]
            options: dict[str, Any]
            progress: dict[str, Any]
            domains, options, progress = [], {}, {}
            try:
                domains = json.loads(r[3]) if r[3] else []
            except Exception:
                domains = []
            try:
                options = json.loads(r[4]) if r[4] else {}
            except Exception:
                options = {}
            try:
                progress = json.loads(r[5]) if r[5] else {}
            except Exception:
                progress = {}
            items.append(
                {
                    "id": r[0],
                    "status": r[1],
                    "label": r[2],
                    "domain_count": len(domains) if isinstance(domains, list) else 0,
                    "domains": domains[:5] if isinstance(domains, list) else [],
                    "options": options,
                    "progress": progress,
                    "error": r[6],
                    "created_at": r[7],
                    "started_at": r[8],
                    "finished_at": r[9],
                }
            )

        return PaginatedResponse(
            items=items,
            total=int(total or 0),
            page=page,
            page_size=page_size,
            total_pages=max(1, (int(total or 0) + page_size - 1) // page_size),
        )
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/runs/{run_id}")
def get_run(run_id: str, auth: Annotated[AuthContext, Depends(get_auth_context)]) -> dict[str, Any]:
    con = _get_conn()
    try:
        row = con.execute(
            "SELECT id, status, label, domains_json,"
            " options_json, progress_json, error,"
            " created_at, started_at, finished_at"
            " FROM runs WHERE id = %s",
            (run_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")

        domains = json.loads(row[3]) if row[3] else []
        options = json.loads(row[4]) if row[4] else {}
        progress = json.loads(row[5]) if row[5] else {}
        return {
            "id": row[0],
            "status": row[1],
            "label": row[2],
            "domains": domains,
            "options": options,
            "progress": progress,
            "error": row[6],
            "created_at": row[7],
            "started_at": row[8],
            "finished_at": row[9],
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.post("/runs")
def create_run(
    request: RunCreateRequest,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    from redis import Redis
    from rq import Queue

    con = _get_conn()
    try:
        tenant_id = auth.tenant_id
        user_id = auth.user_id
        run_id, now = str(uuid.uuid4()), _utc_now_iso()

        domains = [d.strip().lower() for d in request.domains if str(d or "").strip()]
        if not domains:
            raise HTTPException(status_code=400, detail="No valid domains provided")

        modes = _normalize_modes(request.modes)

        _enforce_24h_hard_limit(con, tenant_id=tenant_id, requested=len(domains))

        options = {
            "modes": modes,
            "ai_enabled": bool(request.ai_enabled),
            "force_discovery": bool(request.force_discovery),
            "company_limit": int(request.company_limit),
        }

        label = (request.label or "").strip() or None

        # Build dynamic column list based on what exists
        has_tenant = _has_column(con, "runs", "tenant_id")
        has_user = _has_column(con, "runs", "user_id")
        has_label = _has_column(con, "runs", "label")

        cols = ["id", "status", "domains_json", "options_json", "created_at", "updated_at"]
        vals: list[Any] = [run_id, "queued", json.dumps(domains), json.dumps(options), now, now]

        if has_tenant:
            cols.append("tenant_id")
            vals.append(tenant_id)

        if has_user and user_id:
            cols.append("user_id")
            vals.append(user_id)

        if has_label and label:
            cols.append("label")
            vals.append(label)

        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)

        con.execute(
            f"INSERT INTO runs ({col_names}) VALUES ({placeholders})",
            tuple(vals),
        )

        con.commit()

        try:
            redis = Redis.from_url(RQ_REDIS_URL)
            q = Queue(name="orchestrator", connection=redis)
            from src.queueing.pipeline_v2 import pipeline_start_v2

            job = q.enqueue(pipeline_start_v2, run_id=run_id, tenant_id=tenant_id, job_timeout=3600)
            return {
                "ok": True,
                "run_id": run_id,
                "job_id": job.id,
                "status": "queued",
                "domain_count": len(domains),
                "modes": modes,
            }
        except Exception as exc:
            try:
                con.execute(
                    "UPDATE runs SET status = 'failed', error = %s WHERE id = %s",
                    (f"Failed to enqueue: {exc}", run_id),
                )
                con.commit()
            except Exception:
                pass
            raise HTTPException(
                status_code=500,
                detail=f"Failed to enqueue run: {exc}",
            ) from exc
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.get("/search")
def search_all(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    con = _get_conn()
    try:
        st = f"%{q.lower()}%"

        companies = con.execute(
            "SELECT id, name, domain, official_domain FROM companies"
            " WHERE (LOWER(domain) LIKE %s OR LOWER(name) LIKE %s"
            " OR LOWER(official_domain) LIKE %s)"
            " LIMIT %s",
            (st, st, st, limit),
        ).fetchall()

        people = con.execute(
            "SELECT p.id, p.first_name, p.last_name, p.full_name, p.title, c.domain "
            "FROM people p LEFT JOIN companies c ON c.id = p.company_id "
            "WHERE (LOWER(p.first_name) LIKE %s"
            " OR LOWER(p.last_name) LIKE %s"
            " OR LOWER(p.full_name) LIKE %s)"
            " LIMIT %s",
            (st, st, st, limit),
        ).fetchall()

        emails = con.execute(
            "SELECT e.id, e.email, c.domain, vr.verify_status "
            "FROM emails e "
            "LEFT JOIN companies c ON c.id = e.company_id "
            "LEFT JOIN LATERAL ("
            "  SELECT verify_status FROM verification_results vr "
            "  WHERE vr.email_id = e.id "
            "  ORDER BY vr.verified_at DESC NULLS LAST, vr.id DESC "
            "  LIMIT 1"
            ") vr ON true "
            "WHERE LOWER(e.email) LIKE %s LIMIT %s",
            (st, limit),
        ).fetchall()

        return {
            "query": q,
            "companies": [
                {
                    "id": c[0],
                    "name": c[1],
                    "domain": c[2],
                    "official_domain": c[3],
                }
                for c in companies
            ],
            "people": [
                {
                    "id": p[0],
                    "first_name": p[1],
                    "last_name": p[2],
                    "full_name": p[3],
                    "title": p[4],
                    "company_domain": p[5],
                }
                for p in people
            ],
            "emails": [
                {
                    "id": e[0],
                    "email": e[1],
                    "company_domain": e[2],
                    "verify_status": e[3],
                }
                for e in emails
            ],
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Google Discovery endpoints
# --------------------------------------------------------------------------------------


@router.get("/discovery/config")
def get_discovery_config(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    """Get current Google Discovery configuration."""
    from src.search.google_discovery import is_api_configured

    tenant_id = auth.tenant_id
    con = _get_conn()
    try:
        row = con.execute(
            "SELECT enabled, companies_per_day, min_people_threshold, "
            "target_roles, daily_query_budget, updated_at "
            "FROM google_discovery_config WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()

        if not row:
            return {
                "enabled": False,
                "companies_per_day": 20,
                "min_people_threshold": 2,
                "target_roles": ["CEO", "CFO", "COO", "CTO", "CIO", "CHRO", "CMO"],
                "daily_query_budget": 140,
                "updated_at": None,
                "api_configured": is_api_configured(),
            }

        return {
            "enabled": bool(row[0]),
            "companies_per_day": row[1],
            "min_people_threshold": row[2],
            "target_roles": [r.strip() for r in (row[3] or "").split(",") if r.strip()],
            "daily_query_budget": row[4],
            "updated_at": row[5],
            "api_configured": is_api_configured(),
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


class DiscoveryConfigUpdate(BaseModel):
    enabled: bool = False
    companies_per_day: int = 20
    min_people_threshold: int = 2
    target_roles: list[str] = ["CEO", "CFO", "COO", "CTO", "CIO", "CHRO", "CMO"]
    daily_query_budget: int = 140


@router.put("/discovery/config")
def update_discovery_config(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    body: DiscoveryConfigUpdate,
) -> dict[str, Any]:
    """Update Google Discovery configuration (upsert)."""
    tenant_id = auth.tenant_id
    now = _utc_now_iso()
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO google_discovery_config
              (tenant_id, enabled, companies_per_day, min_people_threshold,
               target_roles, daily_query_budget, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tenant_id) DO UPDATE SET
              enabled = EXCLUDED.enabled,
              companies_per_day = EXCLUDED.companies_per_day,
              min_people_threshold = EXCLUDED.min_people_threshold,
              target_roles = EXCLUDED.target_roles,
              daily_query_budget = EXCLUDED.daily_query_budget,
              updated_at = EXCLUDED.updated_at
            """,
            (
                tenant_id,
                body.enabled,
                body.companies_per_day,
                body.min_people_threshold,
                ",".join(body.target_roles),
                body.daily_query_budget,
                now,
                now,
            ),
        )
        con.commit()
        return {"ok": True}
    finally:
        try:
            con.close()
        except Exception:
            pass


@router.post("/discovery/run")
def trigger_discovery_run(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    """Trigger a manual Google Discovery run via RQ."""
    from src.search.google_discovery import is_api_configured

    tenant_id = auth.tenant_id

    if not is_api_configured():
        raise HTTPException(
            status_code=400,
            detail="Serper API key not configured on VPS. Set SERPER_API_KEY environment variable.",
        )

    from redis import Redis
    from rq import Queue

    from src.queueing.google_discovery_task import task_google_discovery

    redis = Redis.from_url(RQ_REDIS_URL)
    q = Queue(name="generate", connection=redis)

    job = q.enqueue(
        task_google_discovery,
        tenant_id=tenant_id,
        trigger_type="manual",
        job_timeout=3600,
    )

    return {"ok": True, "job_id": job.id}


@router.get("/discovery/history")
def get_discovery_history(
    auth: Annotated[AuthContext, Depends(get_auth_context)],
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get past Google Discovery run history."""
    tenant_id = auth.tenant_id
    con = _get_conn()
    try:
        offset = (page - 1) * page_size

        total_row = con.execute(
            "SELECT COUNT(*) FROM google_discovery_runs WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        total = int(total_row[0]) if total_row else 0

        rows = con.execute(
            """
            SELECT id, status, trigger_type, companies_queried, queries_used,
                   people_found, people_inserted, emails_generated,
                   errors, started_at, finished_at
            FROM google_discovery_runs
            WHERE tenant_id = ?
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
            """,
            (tenant_id, page_size, offset),
        ).fetchall()

        items = []
        for r in rows:
            errors_list: list[str] = []
            if r[8]:
                try:
                    errors_list = json.loads(r[8])
                except Exception:
                    errors_list = [str(r[8])]
            items.append(
                {
                    "id": r[0],
                    "status": r[1],
                    "trigger_type": r[2],
                    "companies_queried": r[3],
                    "queries_used": r[4],
                    "people_found": r[5],
                    "people_inserted": r[6],
                    "emails_generated": r[7],
                    "errors": errors_list,
                    "started_at": r[9],
                    "finished_at": r[10],
                }
            )

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        try:
            con.close()
        except Exception:
            pass
