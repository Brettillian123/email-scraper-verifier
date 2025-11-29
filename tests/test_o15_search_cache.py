# tests/test_o15_search_cache.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from src.search.cache import search_with_cache
from src.search.indexing import LeadSearchParams


@dataclass
class FakeBackend:
    """
    Simple in-memory SearchBackend stub for cache tests.

    Tracks how many times search_leads() was called and returns a fixed list
    of rows for a given invocation.
    """

    rows_to_return: list[dict[str, Any]]
    call_count: int = 0

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:  # type: ignore[override]
        self.call_count += 1
        # For test purposes, echo back the rows unmodified.
        return list(self.rows_to_return)


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


def test_cache_hits_on_second_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    search_with_cache should call the backend only once for identical params
    when Redis is available and no cursor is set.
    """
    fake_backend = FakeBackend(
        rows_to_return=[{"email": "a@example.com", "icp_score": 90}],
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
    rows1 = search_with_cache(fake_backend, params)
    assert rows1 == fake_backend.rows_to_return
    assert fake_backend.call_count == 1
    assert fake_redis.setex_calls, "expected cache to be written on first call"

    # Second call: should hit cache and NOT call backend again.
    rows2 = search_with_cache(fake_backend, params)
    assert rows2 == fake_backend.rows_to_return
    assert fake_backend.call_count == 1, "backend.search_leads should not be called again"


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

    rows1 = search_with_cache(fake_backend, params1)
    rows2 = search_with_cache(fake_backend, params2)

    assert rows1 == rows2 == fake_backend.rows_to_return
    # Each distinct param set should trigger a backend call at least once.
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

    rows = search_with_cache(fake_backend, params_with_cursor)
    assert rows == fake_backend.rows_to_return

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

    rows1 = search_with_cache(fake_backend, params)
    rows2 = search_with_cache(fake_backend, params)

    assert rows1 == rows2 == fake_backend.rows_to_return
    # With no Redis, both calls should go to the backend.
    assert fake_backend.call_count == 2
