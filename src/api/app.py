from __future__ import annotations

import base64
import json
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api import admin as admin_routes
from src.api.middleware.body_limit import BodySizeLimitMiddleware
from src.search.backend import SearchBackend, SearchResult, SqliteFtsBackend
from src.search.indexing import LeadSearchParams

# Configurable via env; default 5 MiB
BODY_LIMIT_BYTES = int(os.getenv("BODY_LIMIT_BYTES", str(5 * 1024 * 1024)))
DB_PATH = os.getenv("DB_PATH", "data/dev.db")

app = FastAPI(title="Email Scraper API")

# Register early so limits apply to all routes
app.add_middleware(BodySizeLimitMiddleware, max_bytes=BODY_LIMIT_BYTES)

# R24: admin UI + metrics JSON
app.include_router(admin_routes.router)


def _error_response(status_code: int, error: str, detail: str) -> JSONResponse:
    """
    Helper to return a JSON error payload with a consistent shape.

    Example:
        { "error": "invalid_sort", "detail": "sort must be one of ..." }
    """
    return JSONResponse(
        status_code=status_code,
        content={"error": error, "detail": detail},
    )


def _parse_csv_param(raw: str | None) -> list[str] | None:
    """
    Parse a comma-separated query parameter into a list of non-empty strings.

    Returns None if the input is None or effectively empty.
    """
    if raw is None:
        return None
    parts = [item.strip() for item in raw.split(",")]
    values = [item for item in parts if item]
    return values or None


def _decode_cursor(cursor: str) -> dict[str, Any] | None:
    """
    Decode an opaque keyset cursor from URL-safe base64 JSON.

    Returns a dict on success, or None if decoding/parsing fails.
    """
    if not cursor:
        return None

    # Add padding if needed (base64 length must be multiple of 4).
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
    """
    Encode a cursor payload as URL-safe base64 JSON.
    """
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    token = base64.urlsafe_b64encode(raw).decode("ascii")
    return token


def _extract_tech_keywords(company_attrs: Any) -> list[str]:
    """
    Extract tech keywords from the company attrs JSON blob.

    Expects attrs to be either:
      - a JSON string, or
      - a dict with a "tech_keywords" key.

    Returns a list of string keywords.
    """
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
    """
    Map a raw DB/search row to the public /leads/search JSON schema.

    O26 note:
      - verify_label is passed through from the search backend when present.
      - is_primary_for_person is a boolean for valid emails when the backend
        has chosen a canonical primary for the person; it may be absent/None
        when there is no valid primary.
    """
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
        # O26 fields:
        "verify_label": row.get("verify_label"),
        "is_primary_for_person": row.get("is_primary_for_person"),
    }


def _get_search_backend(request: Request) -> SearchBackend:
    """
    Lazily construct and cache a SqliteFtsBackend instance on app.state.

    This keeps the HTTP layer decoupled from the specific backend
    implementation and makes it easy to inject a different backend in tests.
    """
    backend: SearchBackend | None = getattr(request.app.state, "search_backend", None)
    if backend is not None:
        return backend

    # Import here to avoid circular import issues at module import time.
    from src.db import get_connection  # type: ignore[import]

    conn = get_connection(DB_PATH)
    backend = SqliteFtsBackend(conn)
    request.app.state.search_backend = backend
    return backend


def _normalize_sort(sort: str | None) -> tuple[str, JSONResponse | None]:
    """
    Normalize and validate the sort parameter.
    """
    normalized = sort or "icp_desc"
    if normalized not in {"icp_desc", "verified_desc"}:
        return normalized, _error_response(
            400,
            "invalid_sort",
            "sort must be one of: icp_desc, verified_desc",
        )
    return normalized, None


def _parse_icp_min(raw: str | None) -> tuple[int | None, JSONResponse | None]:
    """
    Parse icp_min into an optional integer.
    """
    if raw is None:
        return None, None
    try:
        return int(raw), None
    except ValueError:
        return None, _error_response(
            400,
            "invalid_icp_min",
            "icp_min must be an integer",
        )


def _parse_recency_days(raw: str | None) -> tuple[int | None, JSONResponse | None]:
    """
    Parse recency_days into an optional positive integer.
    """
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
    """
    Parse limit with sane bounds (1–100), defaulting to 50.
    """
    if raw is None or not raw.strip():
        value = 50
    else:
        try:
            value = int(raw)
        except ValueError:
            return 0, _error_response(
                400,
                "invalid_limit",
                "limit must be an integer",
            )

    if value < 1 or value > 100:
        return 0, _error_response(
            400,
            "invalid_limit",
            "limit must be between 1 and 100",
        )

    return value, None


def _parse_cursor(
    cursor: str | None,
    sort: str,
) -> tuple[int | None, str | None, int | None, JSONResponse | None]:
    """
    Decode and validate the cursor payload for the given sort.
    """
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
    """
    Optional O15 cache layer: only cache first pages (no cursor).

    Returns a SearchResult so higher layers can access leads, facets, and
    next_cursor (if/when the backend starts emitting it directly).
    """
    if cursor is not None:
        return backend.search(params)

    try:
        from src.search.cache import search_with_cache
    except ImportError:  # O15 not present yet
        return backend.search(params)
    return search_with_cache(backend, params)


def _build_next_cursor(
    rows: list[dict[str, Any]],
    sort: str,
    limit: int,
) -> str | None:
    """
    Build an opaque next_cursor token from the final row in a page.
    """
    if len(rows) != limit or not rows:
        return None

    last = rows[-1]
    if sort == "icp_desc":
        icp_score = last.get("icp_score")
        person_id = last.get("person_id")
        if icp_score is None or person_id is None:
            return None
        payload = {
            "sort": "icp_desc",
            "icp_score": int(icp_score),
            "person_id": int(person_id),
        }
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


@app.get("/health")
async def health():
    return {"ok": True}


# Example endpoint that reads the raw body (works for CSV/JSONL uploads)
@app.post("/ingest")
async def ingest(request: Request):
    data = await request.body()  # middleware will cap size before this
    # TODO: pass `data` to your existing ingest pipeline if/when you wire it up
    return {"ok": True, "received_bytes": len(data)}


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
):
    """
    R22/R23: /leads/search API.

    Query parameters are parsed into a LeadSearchParams instance, passed to the
    SearchBackend, and returned as a stable JSON shape suitable for clients.

    Supports:
      - FTS q
      - verify_status / icp_min
      - roles / seniority
      - industries / sizes / tech
      - source
      - recency_days
      - sort: icp_desc (default), verified_desc
      - keyset pagination via opaque cursor
      - facets: comma-separated list of facet names, e.g. "verify_status,icp_bucket"

    O26:
      - Each result row now includes:
          * verify_label: second-dimension label on top of verify_status
          * is_primary_for_person: boolean for canonical primary valid emails
    """
    if not q or not q.strip():
        return _error_response(
            400,
            "invalid_query",
            "q must be a non-empty search query",
        )

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
