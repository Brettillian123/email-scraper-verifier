# tests/test_fetch_client.py
from __future__ import annotations

import contextlib
import types
from collections.abc import Iterator

import pytest
import respx
from httpx import Response

client_mod = pytest.importorskip("src.fetch.client")
robots_mod = pytest.importorskip("src.fetch.robots")
throttle_mod = pytest.importorskip("src.fetch.throttle")


# -------------------------------- test utilities --------------------------------------


@contextlib.contextmanager
def fake_clock(monkeypatch) -> Iterator[types.SimpleNamespace]:
    """
    Freeze time and capture sleeps.

    - Overrides time.monotonic()/perf_counter()/time.time so throttle/caching see our clock.
    - Overrides time.sleep(dt) to *advance* the frozen clock by dt and accumulate total slept time.

    Exposes:
      now() -> float            current monotonic time
      advance(dt)               manually advance without calling sleep()
      slept() -> float          total seconds 'slept'
      reset_slept()             zero the sleep accumulator
    """
    t = {"now": 1_000_000.0, "slept": 0.0}
    epoch0 = 1_700_000_000.0

    def monotonic():
        return t["now"]

    def perf_counter():
        return t["now"]

    def time_time():
        # derive a wall-clock from our monotonic base
        return epoch0 + (t["now"] - 1_000_000.0)

    def sleep(dt):
        dt = float(dt)
        if dt <= 0:
            return
        t["slept"] += dt
        # advance our monotonic clock as real sleep would
        t["now"] += dt

    monkeypatch.setattr("time.monotonic", monotonic)
    monkeypatch.setattr("time.perf_counter", perf_counter)
    monkeypatch.setattr("time.time", time_time)
    monkeypatch.setattr("time.sleep", sleep)

    ns = types.SimpleNamespace(
        now=lambda: t["now"],
        advance=lambda dt: t.__setitem__("now", t["now"] + float(dt)),
        slept=lambda: t["slept"],
        reset_slept=lambda: t.__setitem__("slept", 0.0),
    )
    yield ns


@pytest.fixture(autouse=True)
def _reset_state():
    # make tests independent
    robots_mod.clear_cache()
    throttle_mod.clear()
    yield
    robots_mod.clear_cache()
    throttle_mod.clear()


def _host_path(url: str) -> tuple[str, str]:
    from urllib.parse import urlsplit

    p = urlsplit(url)
    host = p.netloc.lower()
    path = p.path or "/"
    if p.query:
        path = f"{path}?{p.query}"
    return host, path


# -------------------------------- robots blocking -------------------------------------


@respx.mock
def test_robots_block_prevents_network_fetch():
    url = "https://blocked.test/secret"
    host, path = _host_path(url)

    # Robots disallow for our UA
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(
            200,
            text="User-agent: Email-Scraper\nDisallow: /\n",
        )
    )

    # Content route should NEVER be called
    content_route = respx.get(url).mock(return_value=Response(200, text="should-not-hit"))

    with client_mod.FetcherClient() as fc:
        res = fc.fetch(url)

    assert res.status == 451
    assert res.reason == "blocked-by-robots"
    assert res.body is None
    assert content_route.call_count == 0  # no network fetch to the page


# -------------------------------- crawl-delay at client layer --------------------------


@respx.mock
def test_crawl_delay_observed_between_client_fetches(monkeypatch):
    url = "https://delay.test/page"
    host, path = _host_path(url)

    # Robots allow + crawl-delay 1.5s
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text="User-agent: *\nCrawl-delay: 1.5\n")
    )
    # Content returns no-store to avoid cache interaction
    headers = {"Cache-Control": "no-store", "Content-Type": "text/html"}
    content_route = respx.get(url).mock(return_value=Response(200, headers=headers, text="ok"))

    with fake_clock(monkeypatch) as clk, client_mod.FetcherClient() as fc:
        r1 = fc.fetch(url)
        assert r1.status == 200
        assert content_route.call_count == 1

        clk.reset_slept()
        r2 = fc.fetch(url)
        # The second call should have slept ≈ crawl-delay before issuing request
        assert clk.slept() == pytest.approx(1.5, rel=0, abs=1e-6)
        assert r2.status == 200
        assert content_route.call_count == 2


# -------------------------------- caching fresh hit -----------------------------------


@respx.mock
def test_caching_serves_fresh_without_network(monkeypatch):
    url = "https://cache.test/page"
    host, path = _host_path(url)

    # Robots allow with 0 delay to avoid sleeps
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text="User-agent: *\nCrawl-delay: 0\n")
    )
    headers = {
        "Cache-Control": "max-age=60",
        "ETag": '"v1"',
        "Content-Type": "text/html",
    }
    content_route = respx.get(url)
    content_route.mock(return_value=Response(200, headers=headers, text="v1"))

    with client_mod.FetcherClient() as fc:
        r1 = fc.fetch(url)
        assert r1.status == 200
        assert r1.from_cache is False
        assert content_route.call_count == 1

        # Second call should be served from cache (fresh) and not hit the network
        r2 = fc.fetch(url)
        assert r2.from_cache is True
        assert r2.status == 200
        assert (r2.body or b"").startswith(b"v1")
        assert content_route.call_count == 1  # unchanged


# ------------------------------- 200 → 304 validation ---------------------------------


@respx.mock
def test_200_then_304_validates_cache(monkeypatch):
    url = "https://validate.test/item"
    host, path = _host_path(url)

    # Robots allow with 0 delay
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text="User-agent: *\nCrawl-delay: 0\n")
    )

    # First 200 with max-age=0 (immediately stale) and an ETag
    first_headers = {
        "Cache-Control": "max-age=0",
        "ETag": '"etag-v1"',
        "Content-Type": "text/html",
    }
    # Second response: 304 Not Modified
    route = respx.get(url)
    route.mock(
        side_effect=[
            Response(200, headers=first_headers, text="BODY_V1"),
            Response(304, headers={"Cache-Control": "max-age=60"}),
        ]
    )

    with client_mod.FetcherClient() as fc:
        r1 = fc.fetch(url)
        assert r1.status == 200
        assert r1.from_cache is False
        assert (r1.body or b"").startswith(b"BODY_V1")
        assert route.call_count == 1

        r2 = fc.fetch(url)
        assert r2.status == 200  # from cached entry
        assert r2.from_cache is True
        assert (r2.body or b"") == b"BODY_V1"
        assert r2.reason == "validated-cache"
        assert route.call_count == 2

        # Verify conditional header was sent on the second network call
        assert len(route.calls) >= 2
        req2 = route.calls[1].request
        assert req2.headers.get("If-None-Match") == '"etag-v1"'


# -------------------------------- WAF handling (429/403) -------------------------------


@pytest.mark.parametrize("status, expected_first", [(429, 6.0), (403, 6.0)])
@respx.mock
def test_waf_penalize_and_future_wait(monkeypatch, status, expected_first):
    url = "https://waf.test/login"
    host, path = _host_path(url)

    # Robots allow with 0 delay
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text="User-agent: *\nCrawl-delay: 0\n")
    )
    # First hit → WAF response
    respx.get(url).mock(return_value=Response(status))

    # Deterministic WAF backoff
    monkeypatch.setattr(throttle_mod, "BASE_BACKOFF_S", 3.0, raising=False)
    monkeypatch.setattr(throttle_mod, "MAX_BACKOFF_S", 60.0, raising=False)

    with fake_clock(monkeypatch) as clk, client_mod.FetcherClient() as fc:
        r = fc.fetch(url)
        assert r.status == status
        assert r.reason == "waf-throttle"

        # Next attempt should sleep the penalized duration
        clk.reset_slept()
        slept = throttle_mod.wait_for_turn(host)
        assert slept == pytest.approx(expected_first)


# -------------------------------- 5xx retries then success -----------------------------


@respx.mock
def test_5xx_retries_then_success(monkeypatch):
    url = "https://retry.test/flaky"
    host, path = _host_path(url)

    # Robots allow with 0 delay
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text="User-agent: *\nCrawl-delay: 0\n")
    )

    # Two 502s then a 200
    route = respx.get(url)
    route.mock(
        side_effect=[
            Response(502),
            Response(502),
            Response(
                200, headers={"Cache-Control": "no-store", "Content-Type": "text/plain"}, text="ok"
            ),
        ]
    )

    # Control client retry parameters
    monkeypatch.setattr(client_mod, "FETCH_MAX_RETRIES", 2, raising=False)
    monkeypatch.setattr(client_mod, "RETRY_BASE_S", 0.5, raising=False)

    with fake_clock(monkeypatch) as clk, client_mod.FetcherClient() as fc:
        r = fc.fetch(url)
        assert r.status == 200
        assert route.call_count == 3
        # Backoff sleeps: 0.5 then 1.0 seconds
        assert clk.slept() == pytest.approx(1.5, rel=0, abs=1e-6)
