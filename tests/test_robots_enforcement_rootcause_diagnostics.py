# tests/test_robots_enforcement_rootcause_diagnostics.py
"""
Robots Enforcement Diagnostics Tests

Validates that the configured User-Agent correctly selects the right
robots.txt group, and that disallowed paths are blocked.

PREVIOUSLY: All tests were SKIPPED due to UA mismatch between
"EmailVerifierBot" (config) and "Email-Scraper" (test fixtures).

FIX: Tests now dynamically extract the bot name from the configured UA
and build robots.txt rules targeting that name, so they pass regardless
of whether the UA is "EmailVerifierBot", "Email-Scraper", or anything else.
"""

from __future__ import annotations

import os

import pytest
import respx
from httpx import Response

# Try to import robots module
try:
    import src.fetch.robots as robots

    HAS_ROBOTS = True
except ImportError:
    HAS_ROBOTS = False
    robots = None  # type: ignore

# Check configured User-Agent
try:
    import src.config as config

    CONFIGURED_UA = getattr(config, "FETCH_USER_AGENT", None) or getattr(
        config, "DEFAULT_USER_AGENT", "Unknown"
    )
except ImportError:
    CONFIGURED_UA = "Unknown"


def _extract_bot_name(ua: str) -> str:
    """
    Extract the bot product name from a User-Agent string.

    Examples:
        "EmailVerifierBot/0.9 (+https://...)" -> "EmailVerifierBot"
        "Email-Scraper/1.0" -> "Email-Scraper"
        "" -> "*"
    """
    if not ua or ua == "Unknown":
        return "*"
    token = ua.split("/")[0].split(" ")[0].strip()
    return token or "*"


BOT_NAME = _extract_bot_name(CONFIGURED_UA)


def _build_robots_txt_with_ua(
    *,
    bot_name: str,
    bot_disallow: str = "/private",
    bot_crawl_delay: int = 3,
    wildcard_disallow: str = "/",
    wildcard_crawl_delay: int = 10,
) -> str:
    """Build a robots.txt with a UA-specific group and a wildcard group."""
    return (
        f"User-agent: {bot_name}\n"
        f"Disallow: {bot_disallow}\n"
        f"Allow: /\n"
        f"Crawl-delay: {bot_crawl_delay}\n"
        f"\n"
        f"User-agent: *\n"
        f"Disallow: {wildcard_disallow}\n"
        f"Crawl-delay: {wildcard_crawl_delay}\n"
    )


def _maybe_clear_cache() -> None:
    """Clear robots.txt cache if possible."""
    if not HAS_ROBOTS:
        return
    clear = getattr(robots, "clear_cache", None)
    if callable(clear):
        clear()


def _infer_group_used(delay_value: float | None) -> str:
    """Infer which robots.txt group was used based on crawl-delay."""
    if delay_value is None:
        return "UNKNOWN(None)"
    if abs(delay_value - 3.0) < 1e-9:
        return "Bot-specific group"
    if abs(delay_value - 10.0) < 1e-9:
        return "Wildcard(*) group"
    return f"UNKNOWN({delay_value})"


def _collect_env_ua() -> dict[str, str | None]:
    """Collect User-Agent related environment variables."""
    keys = [
        "USER_AGENT",
        "EMAIL_SCRAPER_USER_AGENT",
        "SCRAPER_USER_AGENT",
        "ROBOTS_USER_AGENT",
        "HTTP_USER_AGENT",
        "FETCH_USER_AGENT",
    ]
    return {k: os.environ.get(k) for k in keys}


@pytest.fixture(autouse=True)
def _clean_cache_each_test():
    """Clear robots cache before and after each test."""
    _maybe_clear_cache()
    yield
    _maybe_clear_cache()


@pytest.mark.skipif(not HAS_ROBOTS, reason="Robots module not available")
def test_robots_module_exists_and_has_expected_interface():
    """Basic sanity check that the robots module has expected functions."""
    assert hasattr(robots, "is_allowed"), "robots module should have is_allowed()"
    assert hasattr(robots, "get_crawl_delay"), "robots module should have get_crawl_delay()"

    # Document the current UA configuration
    print(f"\nConfigured User-Agent: {CONFIGURED_UA}")
    print(f"Extracted bot name: {BOT_NAME}")
    print(f"Environment UA vars: {_collect_env_ua()}")


@pytest.mark.skipif(not HAS_ROBOTS, reason="Robots module not available")
@respx.mock
def test_rootcause_default_user_agent_used_for_group_selection_and_fetch():
    """
    Verify that the configured UA selects its bot-specific robots.txt group
    (crawl-delay=3) rather than the wildcard group (crawl-delay=10).
    """
    host = "rootcause-ua.test"

    robots_txt = _build_robots_txt_with_ua(
        bot_name=BOT_NAME,
        bot_disallow="/private",
        bot_crawl_delay=3,
        wildcard_disallow="/",
        wildcard_crawl_delay=10,
    )

    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(200, text=robots_txt))

    # /public should be ALLOWED by the bot-specific group (Allow: /)
    allowed = robots.is_allowed(host, "/public")
    delay = robots.get_crawl_delay(host)

    group_used = _infer_group_used(delay)
    print(f"\nUA: {CONFIGURED_UA}")
    print(f"Bot name: {BOT_NAME}")
    print(f"/public allowed: {allowed}")
    print(f"Crawl-delay: {delay}")
    print(f"Group used: {group_used}")

    assert allowed is True, (
        f"Expected /public to be allowed for bot '{BOT_NAME}', "
        f"but got allowed={allowed}. Group used: {group_used}"
    )
    assert delay == pytest.approx(3.0, abs=1e-6), (
        f"Expected crawl-delay=3 (bot-specific group), but got {delay}. Group used: {group_used}"
    )


@pytest.mark.skipif(not HAS_ROBOTS, reason="Robots module not available")
@respx.mock
def test_rootcause_disallowed_path_is_blocked():
    """
    Verify that /private is DISALLOWED by the bot-specific group.
    """
    host = "rootcause-block.test"

    robots_txt = _build_robots_txt_with_ua(
        bot_name=BOT_NAME,
        bot_disallow="/private",
        bot_crawl_delay=3,
        wildcard_disallow="/",
        wildcard_crawl_delay=10,
    )

    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(200, text=robots_txt))

    blocked = not robots.is_allowed(host, "/private")
    print(f"\n/private blocked: {blocked}")

    assert blocked is True, (
        f"Expected /private to be disallowed for bot '{BOT_NAME}', but it was allowed"
    )


@pytest.mark.skipif(not HAS_ROBOTS, reason="Robots module not available")
@respx.mock
def test_rootcause_patched_user_agent_propagates_to_fetch_and_selection(monkeypatch):
    """
    Verify that patching the UA in the robots module changes group selection.

    Patches UA to "OtherBot" which should fall through to the wildcard group.
    """
    monkeypatch.setattr(robots, "FETCH_USER_AGENT", "OtherBot/1.0", raising=False)

    host = "rootcause-patch.test"

    # Build robots.txt with the REAL bot name — OtherBot won't match it
    robots_txt = _build_robots_txt_with_ua(
        bot_name=BOT_NAME,
        bot_disallow="/private",
        bot_crawl_delay=3,
        wildcard_disallow="/",
        wildcard_crawl_delay=10,
    )

    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(200, text=robots_txt))

    # OtherBot should match wildcard group → Disallow: / → everything blocked
    allowed = robots.is_allowed(host, "/public")
    delay = robots.get_crawl_delay(host)

    group_used = _infer_group_used(delay)
    print("\nPatched UA: OtherBot/1.0")
    print(f"/public allowed: {allowed}")
    print(f"Crawl-delay: {delay}")
    print(f"Group used: {group_used}")

    assert allowed is False, (
        f"Expected /public to be blocked for 'OtherBot' (wildcard group), "
        f"but got allowed={allowed}. Group used: {group_used}"
    )
    assert delay == pytest.approx(10.0, abs=1e-6), (
        f"Expected crawl-delay=10 (wildcard group), but got {delay}. Group used: {group_used}"
    )
