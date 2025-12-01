# tests/test_o15_search_cache.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from src.search.backend import SearchResult
from src.search.cache import search_with_cache
from src.search.indexing import LeadSearchParams


@dataclass
class FakeBackend:
    """
    Simple in-memory SearchBackend stub for cache tests.

    Tracks how many times search() was called and returns a fixed SearchResult
    for a given invocation.
    """

    rows_to_return: list[dict[str, Any]]
    next_cursor: str | None = None
    facets_to_return: dict[str, list[dict[str, Any]]] | None = None
    call_count: int = 0

    def search(self, params: LeadSearchParams) -> SearchResult:  # type: ignore[override]
        self.call_count += 1
        # For test purposes, echo back the configured rows/facets unmodified.
        return SearchResult(
            leads=list(self.rows_to_return),
            next_cursor=self.next_cursor,
            facets=self.facets_to_return,
        )


class FakeRedis:
    """
    Minimal Redis-like stub implementing get/setex against an in-memory dict.
    """

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.setex_calls: list[tuple[str, int, str]] = []

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value
        self.setex_calls.append((key, ttl, value))


# ---------------------------------------------------------------------------
# Cache behavior tests
# ---------------------------------------------------------------------------


def test_cache_hits_on_second_call_including_facets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    search_with_cache should call the backend only once for identical params
    when Redis is available and no cursor is set, and it should cache both
    leads and facets.
    """
    fake_backend = FakeBackend(
        rows_to_return=[{"email": "a@example.com", "icp_score": 90}],
        next_cursor="next123",
        facets_to_return={
            "verify_status": [{"value": "valid", "count": 1}],
            "icp_bucket": [{"value": "80-100", "count": 1}],
        },
    )
    fake_redis = FakeRedis()

    # Force search_with_cache to use our fake Redis client.
    monkeypatch.setattr(
        "src.search.cache._get_redis_client",
        lambda: fake_redis,
    )

    params = LeadSearchParams(
        query="sales",
        icp_min=80,
        sort="icp_desc",
        limit=10,
    )

    # First call: should hit backend and populate cache.
    result1 = search_with_cache(fake_backend, params)
    assert result1.leads == fake_backend.rows_to_return
    assert result1.next_cursor == "next123"
    assert result1.facets == fake_backend.facets_to_return
    assert fake_backend.call_count == 1
    assert fake_redis.setex_calls, "expected cache to be written on first call"

    # Mutate backend to prove second call is served from cache, not backend.
    fake_backend.rows_to_return = [{"email": "changed@example.com", "icp_score": 50}]
    fake_backend.facets_to_return = {
        "verify_status": [{"value": "invalid", "count": 1}],
    }
    fake_backend.next_cursor = "changed_cursor"

    # Second call: should hit cache and NOT call backend again.
    result2 = search_with_cache(fake_backend, params)
    assert fake_backend.call_count == 1, "backend.search should not be called again"

    # Cached result should still match the original payload, not the mutated one.
    assert result2.leads == result1.leads
    assert result2.next_cursor == result1.next_cursor
    assert result2.facets == result1.facets


def test_cache_key_changes_when_params_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Changing any relevant parameter (e.g. icp_min) should result in a cache
    miss and a new backend call.
    """
    fake_backend = FakeBackend(
        rows_to_return=[{"email": "a@example.com", "icp_score": 90}],
    )
    fake_redis = FakeRedis()
    monkeypatch.setattr(
        "src.search.cache._get_redis_client",
        lambda: fake_redis,
    )

    params1 = LeadSearchParams(
        query="sales",
        icp_min=80,
        sort="icp_desc",
        limit=10,
    )
    params2 = LeadSearchParams(
        query="sales",
        icp_min=70,  # different threshold -> different cache key
        sort="icp_desc",
        limit=10,
    )

    result1 = search_with_cache(fake_backend, params1)
    result2 = search_with_cache(fake_backend, params2)

    assert result1.leads == result2.leads == fake_backend.rows_to_return
    # Each distinct param set should trigger a backend call at least once.
    assert fake_backend.call_count == 2


def test_facets_affect_cache_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Different requested facet sets should produce different cache keys,
    even if all other filters are identical.
    """
    fake_backend = FakeBackend(
        rows_to_return=[{"email": "a@example.com", "icp_score": 90}],
        facets_to_return={"verify_status": [{"value": "valid", "count": 1}]},
    )
    fake_redis = FakeRedis()
    monkeypatch.setattr(
        "src.search.cache._get_redis_client",
        lambda: fake_redis,
    )

    params1 = LeadSearchParams(
        query="sales",
        icp_min=80,
        sort="icp_desc",
        limit=10,
        facets=["verify_status"],
    )
    params2 = LeadSearchParams(
        query="sales",
        icp_min=80,
        sort="icp_desc",
        limit=10,
        facets=["verify_status", "icp_bucket"],  # different facet set
    )

    result1 = search_with_cache(fake_backend, params1)
    result2 = search_with_cache(fake_backend, params2)

    assert result1.leads == result2.leads == fake_backend.rows_to_return
    # Backend should have been called twice, once per distinct facet set.
    assert fake_backend.call_count == 2


def test_cursor_pages_bypass_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Pages with any cursor_* set should bypass the cache entirely.
    """
    fake_backend = FakeBackend(
        rows_to_return=[
            {"email": "a@example.com", "icp_score": 90},
            {"email": "b@example.com", "icp_score": 85},
        ],
    )
    fake_redis = FakeRedis()
    monkeypatch.setattr(
        "src.search.cache._get_redis_client",
        lambda: fake_redis,
    )

    params_with_cursor = LeadSearchParams(
        query="sales",
        sort="icp_desc",
        limit=2,
        cursor_icp=90,
        cursor_person_id=1,
    )

    result = search_with_cache(fake_backend, params_with_cursor)
    assert result.leads == fake_backend.rows_to_return

    # Backend should have been called, but cache should not have been used.
    assert fake_backend.call_count == 1
    assert not fake_redis.setex_calls, "cursor pages should not be cached"


def test_no_redis_falls_back_to_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If Redis is unavailable (or _get_redis_client returns None), the function
    should fall back to hitting the backend directly on every call.
    """
    fake_backend = FakeBackend(
        rows_to_return=[{"email": "a@example.com", "icp_score": 90}],
    )

    # Simulate "no Redis configured".
    monkeypatch.setattr(
        "src.search.cache._get_redis_client",
        lambda: None,
    )

    params = LeadSearchParams(
        query="sales",
        sort="icp_desc",
        limit=10,
    )

    result1 = search_with_cache(fake_backend, params)
    result2 = search_with_cache(fake_backend, params)

    assert result1.leads == result2.leads == fake_backend.rows_to_return
    # With no Redis, both calls should go to the backend.
    assert fake_backend.call_count == 2
