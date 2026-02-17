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
from pydantic import BaseModel, Field

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
    tenant_id: str,
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

    emails_has_tenant = _has_column(con, "emails", "tenant_id")
    where_parts: list[str] = [f"e.company_id IN ({','.join(['%s'] * len(company_ids))})"]
    params: list[Any] = list(company_ids)

    if emails_has_tenant:
        where_parts.append("e.tenant_id = %s")
        params.append(tenant_id)

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


def _export_join_tenant_clauses(
    *,
    tenant_id: str,
    emails_has_tenant: bool,
    vr_has_tenant: bool,
) -> tuple[str, list[Any], str, list[Any]]:
    e_tenant_clause = ""
    e_tenant_params: list[Any] = []
    if emails_has_tenant:
        e_tenant_clause = "AND e.tenant_id = %s"
        e_tenant_params = [tenant_id]

    vr_tenant_clause = ""
    vr_tenant_params: list[Any] = []
    if vr_has_tenant and emails_has_tenant:
        vr_tenant_clause = "AND vr.tenant_id = e.tenant_id"
    elif vr_has_tenant:
        vr_tenant_clause = "AND vr.tenant_id = %s"
        vr_tenant_params = [tenant_id]

    return e_tenant_clause, e_tenant_params, vr_tenant_clause, vr_tenant_params


def _export_selected_companies_sql(
    con: Any,
    *,
    tenant_id: str,
    company_ids: list[int],
    status_filter: str,
) -> tuple[str, tuple[Any, ...]]:
    companies_has_tenant = _has_column(con, "companies", "tenant_id")
    people_has_tenant = _has_column(con, "people", "tenant_id")
    emails_has_tenant = _has_column(con, "emails", "tenant_id")
    vr_has_tenant = _has_column(con, "verification_results", "tenant_id")
    vr_has_fallback = _has_column(con, "verification_results", "fallback_status")

    name_expr = _export_name_expr()
    vr_status_expr, vr_select_cols = _export_vr_expr(vr_has_fallback)

    placeholders = ",".join(["%s"] * len(company_ids))
    where_parts: list[str] = [f"c.id IN ({placeholders})"]
    where_params: list[Any] = list(company_ids)

    if companies_has_tenant:
        where_parts.append("c.tenant_id = %s")
        where_params.append(tenant_id)
    if people_has_tenant:
        where_parts.append("p.tenant_id = %s")
        where_params.append(tenant_id)

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
    ) = _export_join_tenant_clauses(
        tenant_id=tenant_id,
        emails_has_tenant=emails_has_tenant,
        vr_has_tenant=vr_has_tenant,
    )

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
        tenant_id = auth.tenant_id
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
                if _has_column(con, table, "tenant_id"):
                    stats[key] = con.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE tenant_id = %s",
                        (tenant_id,),
                    ).fetchone()[0]
                else:
                    stats[key] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception:
                stats[key] = 0

        try:
            where = "WHERE verify_status IS NOT NULL"
            params: list[Any] = []
            if _has_column(con, "verification_results", "tenant_id"):
                where += " AND tenant_id = %s"
                params.append(tenant_id)

            sql = (
                "SELECT verify_status, COUNT(*) "
                f"FROM verification_results {where} "
                "GROUP BY verify_status"
            )
            rows = con.execute(sql, tuple(params)).fetchall()
            stats["verification_breakdown"] = {r[0]: r[1] for r in rows}
        except Exception:
            stats["verification_breakdown"] = {}

        try:
            where = ""
            params2: list[Any] = []
            if _has_column(con, "runs", "tenant_id"):
                where = "WHERE tenant_id = %s"
                params2.append(tenant_id)
            rows = con.execute(
                f"SELECT status, COUNT(*) FROM runs {where} GROUP BY status",
                tuple(params2),
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
) -> PaginatedResponse:
    con = _get_conn()
    try:
        tenant_id = auth.tenant_id
        offset = (page - 1) * page_size

        companies_has_tenant = _has_column(con, "companies", "tenant_id")

        where_parts: list[str] = []
        params: list[Any] = []

        if companies_has_tenant:
            where_parts.append("c.tenant_id = %s")
            params.append(tenant_id)

        if search:
            st = f"%{search.lower()}%"
            where_parts.append(
                "(LOWER(c.domain) LIKE %s"
                " OR LOWER(c.name) LIKE %s"
                " OR LOWER(c.official_domain) LIKE %s)"
            )
            params.extend([st, st, st])

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        total = con.execute(
            f"SELECT COUNT(*) FROM companies c {where_clause}",
            tuple(params),
        ).fetchone()[0]

        p_tenant = "AND p.tenant_id = %s" if _has_column(con, "people", "tenant_id") else ""
        e_tenant = "AND e.tenant_id = %s" if _has_column(con, "emails", "tenant_id") else ""
        s_tenant = "AND s.tenant_id = %s" if _has_column(con, "sources", "tenant_id") else ""

        sql = f"""
            SELECT
                c.id, c.name, c.domain, c.official_domain,
                c.attrs, c.created_at,
                (SELECT COUNT(*) FROM people p
                 WHERE p.company_id = c.id {p_tenant}),
                (SELECT COUNT(*) FROM emails e
                 WHERE e.company_id = c.id {e_tenant}),
                (SELECT COUNT(*) FROM sources s
                 WHERE s.company_id = c.id {s_tenant})
            FROM companies c
            {where_clause}
            ORDER BY c.id DESC
            LIMIT %s OFFSET %s
        """

        sub_params: list[Any] = []
        if _has_column(con, "people", "tenant_id"):
            sub_params.append(tenant_id)
        if _has_column(con, "emails", "tenant_id"):
            sub_params.append(tenant_id)
        if _has_column(con, "sources", "tenant_id"):
            sub_params.append(tenant_id)

        rows = con.execute(sql, tuple(sub_params + params + [page_size, offset])).fetchall()

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
                }
            )

        risk_map = _domain_risk_levels_for_company_ids(
            con,
            tenant_id=tenant_id,
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

    tenant_id = auth.tenant_id
    status_filter = (status or "").strip().lower()

    con = _get_conn()
    try:
        sql, params = _export_selected_companies_sql(
            con,
            tenant_id=tenant_id,
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
        tenant_id = auth.tenant_id
        companies_has_tenant = _has_column(con, "companies", "tenant_id")

        where = "WHERE id = %s"
        params: list[Any] = [company_id]
        if companies_has_tenant:
            where += " AND tenant_id = %s"
            params.append(tenant_id)

        row = con.execute(
            "SELECT id, name, domain, official_domain,"
            " website_url, attrs, created_at, updated_at"
            f" FROM companies {where}",
            tuple(params),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")

        attrs: dict[str, Any] = {}
        if row[5]:
            try:
                attrs = json.loads(row[5]) if isinstance(row[5], str) else row[5]
            except Exception:
                attrs = {}

        people_has_tenant = _has_column(con, "people", "tenant_id")
        emails_has_tenant = _has_column(con, "emails", "tenant_id")
        sources_has_tenant = _has_column(con, "sources", "tenant_id")

        people_where = "WHERE company_id = %s"
        people_params: list[Any] = [company_id]
        if people_has_tenant:
            people_where += " AND tenant_id = %s"
            people_params.append(tenant_id)

        people = con.execute(
            "SELECT id, first_name, last_name, full_name,"
            " title, source_url"
            f" FROM people {people_where} ORDER BY id",
            tuple(people_params),
        ).fetchall()

        emails_where = "WHERE e.company_id = %s"
        emails_params: list[Any] = [company_id]
        if emails_has_tenant:
            emails_where += " AND e.tenant_id = %s"
            emails_params.append(tenant_id)

        emails = con.execute(
            f"""
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
            {emails_where}
            ORDER BY e.id
            """,
            tuple(emails_params),
        ).fetchall()

        pages_where = "WHERE company_id = %s"
        pages_params: list[Any] = [company_id]
        if sources_has_tenant:
            pages_where += " AND tenant_id = %s"
            pages_params.append(tenant_id)

        pages_sql = (
            "SELECT id, source_url, LENGTH(html), fetched_at "
            f"FROM sources {pages_where} "
            "ORDER BY fetched_at DESC"
        )
        pages = con.execute(pages_sql, tuple(pages_params)).fetchall()

        risk_map = _domain_risk_levels_for_company_ids(
            con,
            tenant_id=tenant_id,
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
        tenant_id = auth.tenant_id
        offset = (page - 1) * page_size

        people_has_tenant = _has_column(con, "people", "tenant_id")

        where_parts: list[str] = []
        params: list[Any] = []

        if people_has_tenant:
            where_parts.append("p.tenant_id = %s")
            params.append(tenant_id)

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
        tenant_id = auth.tenant_id
        offset = (page - 1) * page_size

        emails_has_tenant = _has_column(con, "emails", "tenant_id")

        where_parts: list[str] = []
        params: list[Any] = []

        if emails_has_tenant:
            where_parts.append("e.tenant_id = %s")
            params.append(tenant_id)

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
        tenant_id = auth.tenant_id
        offset = (page - 1) * page_size

        runs_has_tenant = _has_column(con, "runs", "tenant_id")
        where = ""
        params: list[Any] = []
        if runs_has_tenant:
            where = "WHERE tenant_id = %s"
            params.append(tenant_id)

        total = con.execute(f"SELECT COUNT(*) FROM runs {where}", tuple(params)).fetchone()[0]
        rows = con.execute(
            "SELECT id, status, label, domains_json,"
            " options_json, progress_json, error,"
            " created_at, started_at, finished_at "
            f"FROM runs {where} ORDER BY created_at DESC "
            "LIMIT %s OFFSET %s",
            tuple(params + [page_size, offset]),
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
        tenant_id = auth.tenant_id
        runs_has_tenant = _has_column(con, "runs", "tenant_id")

        where = "WHERE id = %s"
        params: list[Any] = [run_id]
        if runs_has_tenant:
            where += " AND tenant_id = %s"
            params.append(tenant_id)

        row = con.execute(
            "SELECT id, status, label, domains_json,"
            " options_json, progress_json, error,"
            " created_at, started_at, finished_at"
            f" FROM runs {where}",
            tuple(params),
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
        tenant_id = auth.tenant_id
        st = f"%{q.lower()}%"

        companies_has_tenant = _has_column(con, "companies", "tenant_id")
        people_has_tenant = _has_column(con, "people", "tenant_id")
        emails_has_tenant = _has_column(con, "emails", "tenant_id")

        companies_where = (
            "WHERE (LOWER(domain) LIKE %s OR LOWER(name) LIKE %s OR LOWER(official_domain) LIKE %s)"
        )
        companies_params: list[Any] = [st, st, st]
        if companies_has_tenant:
            companies_where += " AND tenant_id = %s"
            companies_params.append(tenant_id)
        companies = con.execute(
            f"SELECT id, name, domain, official_domain FROM companies {companies_where} LIMIT %s",
            tuple(companies_params + [limit]),
        ).fetchall()

        people_where = (
            "WHERE (LOWER(p.first_name) LIKE %s"
            " OR LOWER(p.last_name) LIKE %s"
            " OR LOWER(p.full_name) LIKE %s)"
        )
        people_params: list[Any] = [st, st, st]
        if people_has_tenant:
            people_where += " AND p.tenant_id = %s"
            people_params.append(tenant_id)
        people = con.execute(
            "SELECT p.id, p.first_name, p.last_name, p.full_name, p.title, c.domain "
            f"FROM people p LEFT JOIN companies c ON c.id = p.company_id {people_where} LIMIT %s",
            tuple(people_params + [limit]),
        ).fetchall()

        emails_where = "WHERE LOWER(e.email) LIKE %s"
        emails_params: list[Any] = [st]
        if emails_has_tenant:
            emails_where += " AND e.tenant_id = %s"
            emails_params.append(tenant_id)
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
            f"{emails_where} LIMIT %s",
            tuple(emails_params + [limit]),
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
