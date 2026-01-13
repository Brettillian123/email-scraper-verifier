# tests/test_robots_enforcement.py
"""
Tests for robots.txt enforcement (R10 compliance).

Validates that:
  - UA-specific groups override '*' groups
  - Allow/Disallow longest-prefix logic works (tie -> Allow wins)
  - Crawl-delay is taken from the selected group, or defaults when absent

Important:
  - This test explicitly pins robots.FETCH_USER_AGENT to avoid flakiness from
    external config/env/conftest overrides.
"""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from src.fetch import robots


@pytest.fixture(autouse=True)
def clear_robots_cache():
    robots.clear_cache()
    yield
    robots.clear_cache()


@pytest.fixture
def pin_user_agent(monkeypatch):
    monkeypatch.setattr(robots, "FETCH_USER_AGENT", "Email-Scraper/pytest", raising=False)
    return robots.FETCH_USER_AGENT


@pytest.mark.parametrize(
    "text,path,expect_allowed,expect_delay",
    [
        # UA-specific group should win over '*'
        (
            """User-agent: Email-Scraper
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
            """User-agent: Email-Scraper
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
def test_allow_deny_and_crawl_delay(
    monkeypatch, pin_user_agent, text, path, expect_allowed, expect_delay
):
    # Make default delay deterministic for the "None" case in the table
    monkeypatch.setattr(robots, "ROBOTS_DEFAULT_DELAY_SECONDS", 1.25, raising=False)

    host = "example.test"
    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(200, text=text))

    allowed = robots.is_allowed(host, path)
    assert allowed is expect_allowed

    cd = robots.get_crawl_delay(host)
    if expect_delay is None:
        assert cd == pytest.approx(1.25, rel=0, abs=1e-6)
    else:
        assert cd == pytest.approx(expect_delay, rel=0, abs=1e-6)


@respx.mock
def test_falls_back_to_star_group_when_no_ua_match(monkeypatch):
    monkeypatch.setattr(robots, "FETCH_USER_AGENT", "OtherBot/1.0", raising=False)
    monkeypatch.setattr(robots, "ROBOTS_DEFAULT_DELAY_SECONDS", 1.25, raising=False)

    host = "fallback.test"
    robots_txt = """User-agent: Email-Scraper
Allow: /
Crawl-delay: 3

User-agent: *
Disallow: /
Crawl-delay: 10
"""
    respx.get(f"https://{host}/robots.txt").mock(return_value=Response(200, text=robots_txt))

    assert robots.is_allowed(host, "/public") is False
    assert robots.get_crawl_delay(host) == pytest.approx(10.0, rel=0, abs=1e-6)
