# tests/test_fetch_client.py
"""
Fetch Client Tests

Tests the HTTP fetch client with robots.txt enforcement.

FIXED: Previously SKIPPED due to User-Agent mismatch:
- Configured UA: "EmailVerifierBot/0.9 (+https://verifier.crestwellpartners.com; ...)"
- Tests previously used: "Email-Scraper"

Now aligned: robots.txt rules in tests use the configured UA's bot name
(extracted dynamically) so tests pass regardless of config changes.
"""

from __future__ import annotations

import pytest
import respx
from httpx import Response

try:
    import src.fetch.client as client_mod

    HAS_CLIENT = True
except ImportError:
    HAS_CLIENT = False
    client_mod = None  # type: ignore

# Extract the bot name from the configured User-Agent for use in test robots.txt.
# This avoids hardcoding "Email-Scraper" or "EmailVerifierBot" and ensures tests
# always match the actual configured UA.
try:
    import src.config as config

    CONFIGURED_UA = getattr(config, "FETCH_USER_AGENT", "") or getattr(
        config, "DEFAULT_USER_AGENT", ""
    )
except ImportError:
    CONFIGURED_UA = ""


def _extract_bot_name(ua: str) -> str:
    """
    Extract the bot product name from a User-Agent string.

    Examples:
        "EmailVerifierBot/0.9 (+https://...)" -> "EmailVerifierBot"
        "Email-Scraper/1.0" -> "Email-Scraper"
        "" -> "*"
    """
    if not ua:
        return "*"
    # Take the first token before '/' or space
    token = ua.split("/")[0].split(" ")[0].strip()
    return token or "*"


BOT_NAME = _extract_bot_name(CONFIGURED_UA)


def _host_path(url: str) -> tuple[str, str]:
    """Extract host and path from URL."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return parsed.netloc, parsed.path


@pytest.mark.skipif(not HAS_CLIENT, reason="Fetch client not available")
@respx.mock
def test_basic_fetch_returns_content():
    """Test basic fetch returns content."""
    # Mock robots.txt to allow all
    respx.get("https://example.com/robots.txt").mock(
        return_value=Response(200, text="User-agent: *\nAllow: /")
    )
    respx.get("https://example.com/page").mock(return_value=Response(200, text="Hello World"))

    with client_mod.FetcherClient() as fc:
        res = fc.fetch("https://example.com/page")

    assert res.status == 200
    assert b"Hello World" in res.body


@pytest.mark.skipif(not HAS_CLIENT, reason="Fetch client not available")
@respx.mock
def test_robots_block_prevents_network_fetch():
    """
    Test that robots.txt blocking returns 451 status.

    Uses the configured bot name dynamically so this test works
    regardless of whether UA is "EmailVerifierBot" or "Email-Scraper".
    """
    url = "https://blocked.test/secret"
    host, _path = _host_path(url)

    # Use the actual configured bot name in the Disallow rule
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(
            200,
            text=f"User-agent: {BOT_NAME}\nDisallow: /\n",
        )
    )

    respx.get(url).mock(return_value=Response(200, text="should-not-hit"))

    with client_mod.FetcherClient() as fc:
        res = fc.fetch(url)

    assert res.status == 451


@pytest.mark.skipif(not HAS_CLIENT, reason="Fetch client not available")
@respx.mock
def test_robots_allow_permits_fetch():
    """Test that robots.txt allow permits fetch."""
    url = "https://allowed.test/public"
    host, _path = _host_path(url)

    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(
            200,
            text="User-agent: *\nAllow: /\n",
        )
    )

    respx.get(url).mock(return_value=Response(200, text="public content"))

    with client_mod.FetcherClient() as fc:
        res = fc.fetch(url)

    assert res.status == 200
    assert b"public content" in res.body


@pytest.mark.skipif(not HAS_CLIENT, reason="Fetch client not available")
@respx.mock
def test_robots_404_allows_fetch():
    """Test that missing robots.txt (404) allows fetch."""
    url = "https://norobots.test/page"
    host, _path = _host_path(url)

    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(404))
    respx.get(url).mock(return_value=Response(200, text="page content"))

    with client_mod.FetcherClient() as fc:
        res = fc.fetch(url)

    assert res.status == 200
    assert b"page content" in res.body


@pytest.mark.skipif(not HAS_CLIENT, reason="Fetch client not available")
@respx.mock
def test_robots_partial_block_allows_other_paths():
    """
    Test that a Disallow on /secret still allows /public.

    Uses the configured bot name for the robots.txt rules.
    """
    host = "partial.test"

    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(
            200,
            text=f"User-agent: {BOT_NAME}\nDisallow: /secret\nAllow: /\n",
        )
    )

    respx.get(f"https://{host}/public").mock(return_value=Response(200, text="public ok"))
    respx.get(f"https://{host}/secret").mock(return_value=Response(200, text="should-not-hit"))

    with client_mod.FetcherClient() as fc:
        public_res = fc.fetch(f"https://{host}/public")
        secret_res = fc.fetch(f"https://{host}/secret")

    assert public_res.status == 200
    assert b"public ok" in public_res.body
    assert secret_res.status == 451


@pytest.mark.skipif(not HAS_CLIENT, reason="Fetch client not available")
@respx.mock
def test_robots_server_error_denies_all():
    """Test that 5xx on robots.txt results in deny-all (conservative)."""
    host = "error.test"

    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(500, text="Internal Server Error")
    )

    respx.get(f"https://{host}/page").mock(return_value=Response(200, text="should-not-hit"))

    with client_mod.FetcherClient() as fc:
        res = fc.fetch(f"https://{host}/page")

    # 5xx on robots.txt → deny_all → 451
    assert res.status == 451
