# tests/test_fetch_cache.py
from __future__ import annotations

import contextlib
from collections.abc import Iterator

import pytest

cache_mod = pytest.importorskip("src.fetch.cache")


# ------------------------------ test utilities ----------------------------------------


@contextlib.contextmanager
def fake_time(monkeypatch, start_epoch: float = 1_700_000_000.0) -> Iterator[dict]:
    """
    Freeze time.time() and expose .advance(dt) to move wall time forward.
    The cache uses time.time() for TTL and freshness checks.
    """
    state = {"now": float(start_epoch)}

    def _time():
        return state["now"]

    monkeypatch.setattr("time.time", _time)
    yield {
        "now": lambda: state["now"],
        "advance": lambda dt: state.__setitem__("now", state["now"] + float(dt)),
    }


@pytest.fixture
def cache():
    c = cache_mod.Cache(":memory:")
    try:
        yield c
    finally:
        c.close()


# ------------------------------ ETag / Last-Modified ----------------------------------


def test_etag_last_modified_and_conditionals(monkeypatch, cache):
    url = "https://example.test/page"
    headers = {
        "ETag": '"abc123"',
        "Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "Cache-Control": "max-age=120",
        "Content-Type": "text/html; charset=utf-8",
    }
    cache.store_200(url, 200, headers["Content-Type"], b"<html>ok</html>", headers)
    cond = cache.conditionals(url)
    assert cond.get("If-None-Match") == '"abc123"'
    assert cond.get("If-Modified-Since") == "Wed, 21 Oct 2015 07:28:00 GMT"


# ------------------------------ 200 → 304 refresh flow --------------------------------


def test_200_to_304_refreshes_ttl_and_keeps_body(monkeypatch, cache):
    url = "https://cacheflow.test/item"
    # Initial 200 with small TTL
    hdr_200 = {
        "Cache-Control": "max-age=2",
        "ETag": 'W/"v1"',
        "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        "Content-Type": "text/html",
    }

    with fake_time(monkeypatch) as t:
        cache.store_200(url, 200, hdr_200["Content-Type"], b"v1-body", hdr_200)

        # Fresh within TTL
        entry, fresh = cache.get(url)
        assert fresh is True
        assert entry is not None and entry.body == b"v1-body"

        # Advance past TTL -> stale
        t["advance"](3.0)
        _, fresh2 = cache.get(url)
        assert fresh2 is False

        # Server returns 304 with a longer TTL
        hdr_304 = {
            "Cache-Control": "max-age=5",
            # ETag/Last-Modified may or may not be echoed; we also accept missing ones
        }
        updated = cache.store_304(url, hdr_304)
        assert updated is not None

        # Immediately fresh again
        _, fresh3 = cache.get(url)
        assert fresh3 is True

        # And body is preserved from original 200
        assert updated.body == b"v1-body"

        # After 5s more, it should go stale again
        t["advance"](5.1)
        _, fresh4 = cache.get(url)
        assert fresh4 is False


# ------------------------------ default TTL & expiry -----------------------------------


def test_default_ttl_applies_when_no_headers(monkeypatch, cache):
    url = "https://defaultttl.test/no-cache-headers"
    # Make default TTL small/deterministic for the test
    monkeypatch.setattr(cache_mod, "FETCH_CACHE_TTL_SEC", 4.0, raising=False)

    with fake_time(monkeypatch) as t:
        cache.store_200(url, 200, "text/html", b"body", headers={})

        # Within default TTL → fresh
        _, fresh = cache.get(url)
        assert fresh is True

        # After TTL → stale
        t["advance"](4.1)
        _, fresh2 = cache.get(url)
        assert fresh2 is False


# ------------------------------ no-store & body policy --------------------------------


def test_no_store_prevents_persisting_body(monkeypatch, cache):
    url = "https://nostore.test/page"
    headers = {
        "Cache-Control": "no-store",
        "Content-Type": "text/html",
    }
    cache.store_200(url, 200, headers["Content-Type"], b"hello", headers)
    entry, fresh = cache.get(url)
    # Entry exists for metadata, but fresh=False and body should be None
    assert entry is not None
    assert entry.body is None
    assert fresh is False


def test_body_not_saved_for_unapproved_types_and_oversized(monkeypatch, cache):
    url1 = "https://files.test/file.pdf"
    headers1 = {"Cache-Control": "max-age=60", "Content-Type": "application/pdf"}
    cache.store_200(url1, 200, headers1["Content-Type"], b"%PDF...", headers1)
    entry1, _ = cache.get(url1)
    assert entry1 is not None
    assert entry1.body is None  # not an allowed content type for body storage

    # Oversized body is also dropped even if type is text/html
    monkeypatch.setattr(cache_mod, "FETCH_MAX_BODY_BYTES", 10, raising=False)
    url2 = "https://big.test/page"
    headers2 = {"Cache-Control": "max-age=60", "Content-Type": "text/html"}
    big_body = b"x" * 100
    cache.store_200(url2, 200, headers2["Content-Type"], big_body, headers2)
    entry2, _ = cache.get(url2)
    assert entry2 is not None
    assert entry2.body is None  # dropped due to size cap
