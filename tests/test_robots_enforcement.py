# tests/test_robots_enforcement.py
from __future__ import annotations

import contextlib
import hashlib
import os
import types
from collections.abc import Iterator

import pytest
import respx
from httpx import Response

# Import the module under test
robots = pytest.importorskip("src.fetch.robots")


# ------------------------------- test utilities ---------------------------------------


def _current_ua_token() -> str:
    """
    Return the product token used for robots UA matching, e.g.:
      "EmailVerifierBot/0.9 (...)" -> "EmailVerifierBot"

    This keeps tests aligned with the project's configured UA and avoids
    brittle hardcoding (e.g., "Email-Scraper").
    """
    candidates = [
        getattr(robots, "FETCH_USER_AGENT", None),
        getattr(robots, "DEFAULT_USER_AGENT", None),
        os.environ.get("EMAIL_SCRAPER_USER_AGENT"),
        os.environ.get("USER_AGENT"),
    ]
    for ua in candidates:
        if isinstance(ua, str) and ua.strip():
            first = ua.strip().split()[0]  # "Product/1.0"
            return first.split("/")[0]  # "Product"
    return "Email-Scraper"


def _stable_host(prefix: str, text: str, path: str) -> str:
    """
    Produce a stable, deterministic host per (text,path) to avoid any possible
    cross-test/cross-param interference, even if cache clearing is imperfect.
    """
    digest = hashlib.md5(f"{text}|{path}".encode()).hexdigest()[:10]  # noqa: S324
    return f"{prefix}-{digest}.test"


@contextlib.contextmanager
def fake_monotonic(monkeypatch, start: float = 1_000_000.0) -> Iterator[types.SimpleNamespace]:
    """
    Freeze time.monotonic() and expose .advance(dt) to move time forward deterministically.
    robots.py uses time.monotonic() via _now(), so this controls cache TTLs.
    """
    state = {"t": float(start)}

    def _mono() -> float:
        return state["t"]

    ns = types.SimpleNamespace(
        now=lambda: state["t"],
        advance=lambda dt: state.__setitem__("t", state["t"] + float(dt)),
    )
    monkeypatch.setattr("time.monotonic", _mono)
    yield ns


@pytest.fixture(autouse=True)
def _reset_between_tests():
    # Clear in-process memoization across tests
    robots.clear_cache()
    yield
    robots.clear_cache()


# --------------------------------- table-driven ---------------------------------------


@pytest.mark.parametrize(
    "text,path,expect_allowed,expect_delay",
    [
        # UA-specific group should win over '*'
        (
            """User-agent: {UA_TOKEN}
Disallow: /private
Allow: /
Crawl-delay: 3

User-agent: *
Disallow: /
Crawl-delay: 10
""",
            "/private",
            False,
            3.0,
        ),
        (
            """User-agent: {UA_TOKEN}
Disallow: /private
Allow: /
Crawl-delay: 3

User-agent: *
Disallow: /
Crawl-delay: 10
""",
            "/public",
            True,
            3.0,
        ),
        # Longest-prefix wins; tie -> Allow beats Disallow
        (
            """User-agent: *
Disallow: /a
Allow: /ab
""",
            "/ab",
            True,
            None,  # no crawl-delay specified → use default (asserted separately)
        ),
    ],
)
@respx.mock
def test_allow_deny_and_crawl_delay(monkeypatch, text, path, expect_allowed, expect_delay):
    # Make default delay deterministic for the "None" case in the table
    monkeypatch.setattr(robots, "ROBOTS_DEFAULT_DELAY_SECONDS", 1.25, raising=False)

    # Make the robots.txt fixture match the project's configured UA token.
    text = text.replace("{UA_TOKEN}", _current_ua_token())

    host = _stable_host("example", text, path)
    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(200, text=text))

    allowed = robots.is_allowed(host, path)
    assert allowed is expect_allowed

    cd = robots.get_crawl_delay(host)
    if expect_delay is None:
        assert cd == pytest.approx(1.25, rel=0, abs=1e-6)
    else:
        assert cd == pytest.approx(expect_delay, rel=0, abs=1e-6)


# ------------------------------ status handling ---------------------------------------


@respx.mock
def test_404_treated_as_no_robots(monkeypatch):
    monkeypatch.setattr(robots, "ROBOTS_DEFAULT_DELAY_SECONDS", 2.5, raising=False)

    host = "norobots.test"
    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(404))

    assert robots.is_allowed(host, "/anything") is True
    assert robots.get_crawl_delay(host) == pytest.approx(2.5)


@respx.mock
def test_server_error_denies_for_deny_ttl(monkeypatch):
    # Shrink the deny window to make this test quick to advance through
    monkeypatch.setattr(robots, "ROBOTS_DENY_TTL_SECONDS", 7.0, raising=False)

    host = "flaky.test"
    route = respx.get(f"https://{host}/robots.txt")
    route.mock(return_value=Response(503))

    # First fetch → deny
    assert robots.is_allowed(host, "/x") is False

    # During deny window, it should stay denied without refetch (cache hit)
    assert robots.is_allowed(host, "/y") is False
    assert route.call_count == 1  # cached deny policy used


# ------------------------------ caching behavior --------------------------------------


@respx.mock
def test_robots_caching_fresh_vs_stale(monkeypatch):
    """
    First call caches a policy; within TTL no refetch; after TTL we refetch and apply new policy.
    """
    # Make TTL small for the test
    monkeypatch.setattr(robots, "ROBOTS_TTL_SECONDS", 5.0, raising=False)
    # Default delay for both policies when not specified
    monkeypatch.setattr(robots, "ROBOTS_DEFAULT_DELAY_SECONDS", 1.0, raising=False)

    host = "cache.test"
    url = f"https://{host}/robots.txt"

    first = """User-agent: *
Disallow: /old
"""
    second = """User-agent: *
Allow: /
"""

    # Queue two responses: first policy then a changed one
    route = respx.get(url)
    route.mock(side_effect=[Response(200, text=first), Response(200, text=second)])

    with fake_monotonic(monkeypatch) as clk:
        # Initial fetch → disallow /old
        assert robots.is_allowed(host, "/old") is False
        assert route.call_count == 1

        # Within TTL → should NOT refetch; still disallowed
        clk.advance(3.0)
        assert robots.is_allowed(host, "/old") is False
        assert route.call_count == 1  # still cached

        # After TTL expiry → refetch applies the new policy (allow all)
        clk.advance(3.5)  # total 6.5s > 5s TTL
        assert robots.is_allowed(host, "/old") is True
        assert route.call_count == 2


@respx.mock
def test_crawl_delay_is_cached_with_policy(monkeypatch):
    """
    Crawl-delay should come from the cached policy and update after re-fetch.
    """
    monkeypatch.setattr(robots, "ROBOTS_TTL_SECONDS", 4.0, raising=False)

    host = "delay.test"
    url = f"https://{host}/robots.txt"

    v1 = """User-agent: *
Crawl-delay: 2
"""
    v2 = """User-agent: *
Crawl-delay: 7
"""

    route = respx.get(url)
    route.mock(side_effect=[Response(200, text=v1), Response(200, text=v2)])

    with fake_monotonic(monkeypatch) as clk:
        assert robots.get_crawl_delay(host) == pytest.approx(2.0)
        assert route.call_count == 1

        # Within TTL, should remain the same without refetch
        clk.advance(2.5)
        assert robots.get_crawl_delay(host) == pytest.approx(2.0)
        assert route.call_count == 1

        # After TTL, refetch picks up new delay
        clk.advance(2.0)  # total 4.5s > 4s
        assert robots.get_crawl_delay(host) == pytest.approx(7.0)
        assert route.call_count == 2
