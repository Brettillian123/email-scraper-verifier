# src/search/cache.py
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from src.search.backend import SearchBackend, SearchResult
from src.search.indexing import LeadSearchParams

# Default TTL: 15 minutes. Can be overridden via env.
DEFAULT_TTL_SECONDS = int(os.getenv("LEAD_SEARCH_CACHE_TTL_SECONDS", "900"))

# Optional hard switch to disable caching entirely (useful in tests / local runs).
LEAD_SEARCH_CACHE_ENABLED = os.getenv("LEAD_SEARCH_CACHE_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

# Cache key version. Bump when changing key construction to avoid stale collisions.
CACHE_KEY_VERSION = "v2"


def _normalize_sequence(value: Any) -> list[str] | None:
    """
    Normalize a sequence-like value into a sorted list of strings, or None.

    This ensures that ["sales", "marketing"] and ("sales", "marketing") produce
    the same cache key, and that ordering of filters does not affect caching.
    """
    if value is None:
        return None
    if isinstance(value, str):
        # Single string: treat as a single-element list.
        return [value]
    try:
        items = list(value)
    except TypeError:
        return [str(value)]
    if not items:
        return None
    return sorted(str(item) for item in items if item is not None)


def _hash_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _get_cache_namespace(backend: SearchBackend) -> str:
    """
    Return a namespace string that scopes cache entries to the active backend / DB.

    This prevents cross-test and cross-database collisions for identical query params.

    Resolution order:
      1) Explicit env override: LEAD_SEARCH_CACHE_NAMESPACE
      2) backend.cache_namespace() if present
      3) Fallback: class identity + instance id

    NOTE:
      We intentionally do NOT attempt to synthesize an in-memory SQLite namespace
      using id(conn) here, because Python may reuse ids within a long-running
      process, causing rare but real cross-test cache collisions. For SQLite
      backends, implement backend.cache_namespace() (see SqliteFtsBackend).
    """
    env_ns = os.getenv("LEAD_SEARCH_CACHE_NAMESPACE")
    if env_ns:
        return env_ns.strip()

    cache_ns_fn = getattr(backend, "cache_namespace", None)
    if callable(cache_ns_fn):
        try:
            ns = cache_ns_fn()
        except Exception:
            ns = None
        if isinstance(ns, str) and ns.strip():
            return ns.strip()

    return f"{backend.__class__.__module__}.{backend.__class__.__name__}:{id(backend)}"


def _build_cache_key(backend: SearchBackend, params: LeadSearchParams) -> str:
    """
    Build a deterministic cache key from the LeadSearchParams and backend namespace.

    Only inputs that affect the first page of results are included. Cursor
    fields are *not* part of the cache key; pages with any cursor_* set are
    never cached.

    R23 note:
      - The requested facets set is included in the key so that different
        facet combinations do not collide.

    IMPORTANT:
      - The key is namespaced by backend/database identity to prevent
        cross-database collisions (e.g., pytest temp DB vs in-memory DB).
      - The key is versioned to invalidate any pre-fix Redis entries that
        used older formats (e.g., leads_search:<digest>).
    """
    key_payload = {
        "q": params.query,
        "verify_status": _normalize_sequence(params.verify_status),
        "icp_min": params.icp_min,
        "roles": _normalize_sequence(params.roles),
        "seniority": _normalize_sequence(params.seniority),
        "industries": _normalize_sequence(params.industries),
        "sizes": _normalize_sequence(params.sizes),
        "tech": _normalize_sequence(params.tech),
        "source": _normalize_sequence(params.source),
        "recency_days": params.recency_days,
        "sort": params.sort,
        "limit": params.limit,
        "facets": _normalize_sequence(params.facets),
    }

    namespace = _get_cache_namespace(backend)
    namespace_digest = _hash_hex(namespace)[:16]

    raw = json.dumps(key_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()

    return f"leads_search:{CACHE_KEY_VERSION}:{namespace_digest}:{digest}"


def _get_redis_client() -> Any | None:
    """
    Best-effort helper to obtain a Redis client.

    Prefers the project-level helper in src.queueing.redis_conn, but falls
    back gracefully if Redis is not configured or not available.

    The returned object is expected to support:
      - get(key: str) -> bytes | str | None
      - setex(key: str, ttl: int, value: str | bytes) -> Any
    """
    try:
        from src.queueing import redis_conn as redis_conn_mod  # type: ignore[import]
    except Exception:
        return None

    # Try a few likely factory function names; this makes us resilient to
    # small naming differences (get_redis, get_redis_client, get_connection, etc.).
    for name in ("get_redis", "get_redis_client", "get_connection", "get_conn"):
        factory = getattr(redis_conn_mod, name, None)
        if callable(factory):
            try:
                client = factory()
            except Exception:
                continue
            if client is not None:
                return client

    return None


def search_with_cache(backend: SearchBackend, params: LeadSearchParams) -> SearchResult:
    """
    Execute a lead search with an optional Redis-backed cache.

    Cache policy:

      - Only cache the *first* page of results:
          * cursor_icp, cursor_verified_at, cursor_person_id must all be None.
      - Cache key is derived from the query + filters + sort + limit + facets,
        AND is namespaced by backend/database identity.
      - TTL is DEFAULT_TTL_SECONDS (15 minutes) by default.
      - If Redis is unavailable or any error occurs, falls back to direct
        backend.search() without failing the request.

    The cached payload is a JSON object with the shape:

        {
          "leads": [...],
          "next_cursor": "... or null ...",
          "facets": { ... }  // may be null or omitted
        }

    For backwards-compatibility with older cache entries that stored just
    list[dict], we also accept a raw list and wrap it in a SearchResult
    without facets.
    """
    # Do not cache keyset pages.
    if (
        params.cursor_icp is not None
        or params.cursor_verified_at is not None
        or params.cursor_person_id is not None
    ):
        return backend.search(params)

    if not LEAD_SEARCH_CACHE_ENABLED or DEFAULT_TTL_SECONDS <= 0:
        return backend.search(params)

    redis_client = _get_redis_client()
    if redis_client is None:
        # No cache configured; just hit the backend.
        return backend.search(params)

    key = _build_cache_key(backend, params)

    # Attempt cache read
    try:
        cached = redis_client.get(key)
    except Exception:
        cached = None

    if cached is not None:
        try:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8")
            data = json.loads(cached)

            # New-style payload: dict with leads/next_cursor/facets.
            if isinstance(data, dict) and "leads" in data:
                leads = data.get("leads") or []
                next_cursor = data.get("next_cursor")
                facets = data.get("facets")
                if isinstance(leads, list):
                    return SearchResult(
                        leads=leads,  # type: ignore[arg-type]
                        next_cursor=(
                            next_cursor if isinstance(next_cursor, (str, type(None))) else None
                        ),
                        facets=facets if isinstance(facets, dict) or facets is None else None,
                    )

            # Old-style payload: list[dict] only.
            if isinstance(data, list):
                return SearchResult(
                    leads=data,  # type: ignore[arg-type]
                    next_cursor=None,
                    facets=None,
                )
        except Exception:
            # On any decode error, ignore and treat as cache miss.
            pass

    # Cache miss: run the search
    result = backend.search(params)

    # Try to write back to cache, but never let caching errors bubble up.
    try:
        payload_obj = {
            "leads": result.leads,
            "next_cursor": result.next_cursor,
            "facets": result.facets,
        }
        payload = json.dumps(payload_obj, separators=(",", ":"))
        redis_client.setex(key, DEFAULT_TTL_SECONDS, payload)
    except Exception:
        pass

    return result
