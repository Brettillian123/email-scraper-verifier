# src/search/cache.py
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from src.search.backend import SearchBackend
from src.search.indexing import LeadSearchParams

# Default TTL: 15 minutes. Can be overridden via env.
DEFAULT_TTL_SECONDS = int(os.getenv("LEAD_SEARCH_CACHE_TTL_SECONDS", "900"))


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


def _build_cache_key(params: LeadSearchParams) -> str:
    """
    Build a deterministic cache key from the LeadSearchParams.

    Only inputs that affect the first page of results are included. Cursor
    fields are *not* part of the cache key; pages with any cursor_* set are
    never cached.
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
    }

    # Stable JSON encoding -> SHA-256 -> hex digest.
    raw = json.dumps(key_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return f"leads_search:{digest}"


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


def search_with_cache(backend: SearchBackend, params: LeadSearchParams) -> list[dict[str, Any]]:
    """
    Execute a lead search with an optional Redis-backed cache.

    Cache policy:

      - Only cache the *first* page of results:
          * cursor_icp, cursor_verified_at, cursor_person_id must all be None.
      - Cache key is derived from the query + filters + sort + limit.
      - TTL is DEFAULT_TTL_SECONDS (15 minutes) by default.
      - If Redis is unavailable or any error occurs, falls back to direct
        backend.search_leads() without failing the request.

    The cached payload is just `list[dict]` as returned by backend.search_leads().
    """
    # Do not cache keyset pages.
    if (
        params.cursor_icp is not None
        or params.cursor_verified_at is not None
        or params.cursor_person_id is not None
    ):
        return backend.search_leads(params)

    redis_client = _get_redis_client()
    if redis_client is None or DEFAULT_TTL_SECONDS <= 0:
        # No cache configured; just hit the backend.
        return backend.search_leads(params)

    key = _build_cache_key(params)

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
            if isinstance(data, list):
                # Assume list[dict]; let callers handle any schema mismatches.
                return data  # type: ignore[return-value]
        except Exception:
            # On any decode error, ignore and treat as cache miss.
            pass

    # Cache miss: run the search
    rows = backend.search_leads(params)

    # Try to write back to cache, but never let caching errors bubble up.
    try:
        payload = json.dumps(rows, separators=(",", ":"))
        redis_client.setex(key, DEFAULT_TTL_SECONDS, payload)
    except Exception:
        pass

    return rows
