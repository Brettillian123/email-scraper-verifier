# src/extract/url_filters.py
"""
URL Filtering for People Page Detection.

This module provides intelligent URL filtering to determine if a URL
is likely a people/team/leadership page vs. a product page, blog post,
or other non-team content.

The key insight is that naive substring matching fails:
  - "/teams-phone-system" contains "team" but is a PRODUCT page
  - "/thought-leadership" contains "leadership" but is a CONTENT page
  - "/blog/author/leadership" is a BLOG page, not a team page

This module uses:
  1. A blocklist of patterns that should NEVER match (highest priority)
  2. An allowlist of patterns that indicate team pages
  3. Word-boundary matching where appropriate
  4. Path segment analysis for more precise matching
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Blocklist: URLs that should NEVER be processed for people extraction
# ---------------------------------------------------------------------------

# These patterns take HIGHEST PRIORITY - if matched, URL is blocked
# Ordered from most specific to least specific
_BLOCKED_URL_PATTERNS: tuple[str, ...] = (
    # =========================================================================
    # PRODUCT PAGES with misleading keywords
    # =========================================================================
    # Microsoft Teams / team collaboration products
    "/teams-phone",
    "/teams-chat",
    "/teams-meeting",
    "/teams-integration",
    "/teams-calling",
    "/teams-voice",
    "/teams-video",
    "/teams-collaboration",
    "/microsoft-teams",
    "/ms-teams",
    # Team management / productivity products (not actual team pages)
    "/team-management",
    "/team-productivity",
    "/team-collaboration",
    "/team-software",
    "/team-tools",
    "/team-platform",
    # Thought leadership / leadership content (not leadership team)
    "/thought-leadership",
    "/thought_leadership",
    "/leadership-insights",
    "/leadership-blog",
    "/leadership-articles",
    "/leadership-resources",
    "/leadership-content",
    "/leadership-series",
    "/leadership-podcast",
    "/leadership-webinar",
    # =========================================================================
    # BLOG / CONTENT pages
    # =========================================================================
    "/blog/",
    "/blogs/",
    "/article/",
    "/articles/",
    "/post/",
    "/posts/",
    "/news/2",  # News articles with dates
    "/news/1",
    "/press-release/",
    "/press-releases/",
    "/press/2",
    "/press/1",
    "/podcast/",
    "/podcasts/",
    "/episode/",
    "/episodes/",
    "/webinar/",
    "/webinars/",
    "/video/",
    "/videos/",
    "/event/",
    "/events/",
    "/conference/",
    "/workshop/",
    "/whitepaper/",
    "/whitepapers/",
    "/ebook/",
    "/ebooks/",
    "/guide/",
    "/guides/",
    "/report/",
    "/reports/",
    "/resource/",
    "/resources/",
    # =========================================================================
    # CUSTOMER / CASE STUDY pages (contain third-party names)
    # =========================================================================
    "/customer-stor",
    "/customer_stor",
    "/case-stud",
    "/case_stud",
    "/success-stor",
    "/success_stor",
    "/testimonial",
    "/client-stor",
    "/client_stor",
    "/review/",
    "/reviews/",
    "/reference/",
    "/references/",
    # =========================================================================
    # JOB / CAREER pages (job listings, not team members)
    # =========================================================================
    "/career",
    "/careers",
    "/job/",
    "/jobs/",
    "/opening",  # matches /opening and /openings (also see explicit /openings below)
    "/openings",  # matches /openings (no trailing slash)
    "/opening/",
    "/openings/",
    "/position/",
    "/positions/",
    "/work-with-us",
    "/work_with_us",
    "/join-us",
    "/join_us",
    "/hiring",
    "/recruitment",
    "/apply/",
    "/application/",
    "/greenhouse",
    "/lever.co",
    "/workable",
    "/bamboohr",
    # =========================================================================
    # PRODUCT / FEATURE pages
    # =========================================================================
    "/pricing",
    "/product/",
    "/products/",
    "/feature/",
    "/features/",
    "/solution/",
    "/solutions/",
    "/platform/",
    "/service/",
    "/services/",
    "/capability/",
    "/capabilities/",
    "/offering/",
    "/offerings/",
    "/module/",
    "/modules/",
    # =========================================================================
    # LEGAL / SUPPORT / DOCS pages
    # =========================================================================
    "/legal/",
    "/terms",
    "/privacy",
    "/cookie",
    "/gdpr",
    "/compliance/",
    "/security/",
    "/trust/",
    "/support/",
    "/help/",
    "/faq/",
    "/docs/",
    "/documentation/",
    "/api/",
    "/developer/",
    "/developers/",
    "/sdk/",
    # =========================================================================
    # AUTH / ACCOUNT pages
    # =========================================================================
    "/login",
    "/signin",
    "/signup",
    "/register",
    "/demo/",
    "/trial/",
    "/get-started",
    "/start-free",
    "/request-demo",
    "/schedule",
    "/book-",
    "/contact-sales",
    # =========================================================================
    # MARKETING / CAMPAIGN pages
    # =========================================================================
    "/campaign/",
    "/campaigns/",
    "/lp/",  # Landing page
    "/landing/",
    "/promo/",
    "/offer/",
    "/utm_",
    # =========================================================================
    # INTEGRATION / PARTNER pages (contain third-party names)
    # =========================================================================
    "/integration/",
    "/integrations/",
    "/connector/",
    "/connectors/",
    "/marketplace/",
    "/app-store/",
    "/partner-",  # /partner-program, /partner-portal, etc.
    # =========================================================================
    # GEOGRAPHIC / LOCALIZED pages (duplicates of main pages)
    # =========================================================================
    "/en-us/",
    "/en-gb/",
    "/en-au/",
    "/en-ca/",
    "/de/",
    "/fr/",
    "/es/",
    "/it/",
    "/pt/",
    "/ja/",
    "/zh/",
    "/ko/",
)

# Regex patterns for more complex blocking
_BLOCKED_REGEX_PATTERNS: tuple[re.Pattern, ...] = (
    # Blog posts with dates: /blog/2024/..., /news/2024-01-15/...
    re.compile(r"/blog/\d{4}", re.I),
    re.compile(r"/news/\d{4}", re.I),
    re.compile(r"/press/\d{4}", re.I),
    # Blog author/tag/category pages (not team pages)
    re.compile(r"/blog/author/", re.I),
    re.compile(r"/blog/tag/", re.I),
    re.compile(r"/blog/category/", re.I),
    re.compile(r"/blog/topic/", re.I),
    # Individual blog posts (slug patterns)
    re.compile(r"/blog/[a-z0-9-]+/[a-z0-9-]+", re.I),  # /blog/category/post-slug
    # Press releases with dates
    re.compile(r"/press-release/\d{4}", re.I),
    re.compile(r"/news/[a-z]+-\d{4}", re.I),  # /news/january-2024
    # Localized about pages (duplicates)
    re.compile(r"/[a-z]{2}/about", re.I),  # /au/about, /uk/about
    re.compile(r"/[a-z]{2}-[a-z]{2}/about", re.I),  # /en-us/about
)


# ---------------------------------------------------------------------------
# Allowlist: URLs that ARE good sources for people extraction
# ---------------------------------------------------------------------------

# These patterns indicate legitimate team/leadership/people pages
# Must match one of these after passing the blocklist check
_ALLOWED_URL_PATTERNS: tuple[str, ...] = (
    # =========================================================================
    # ABOUT pages (most common location for leadership info)
    # =========================================================================
    "/about",
    "/about/",
    "/about-us",
    "/about-us/",
    "/about_us",
    "/about_us/",
    "/aboutus",
    "/aboutus/",
    "/our-story",
    "/our-story/",
    "/our_story",
    "/our_story/",
    "/who-we-are",
    "/who-we-are/",
    "/who_we_are",
    "/who_we_are/",
    "/our-company",
    "/our-company/",
    "/our_company",
    "/our_company/",
    # =========================================================================
    # TEAM pages (dedicated team listings)
    # =========================================================================
    "/team",
    "/team/",
    "/teams",  # Only when it's the full segment (not teams-phone)
    "/teams/",
    "/our-team",
    "/our-team/",
    "/our_team",
    "/our_team/",
    "/the-team",
    "/the-team/",
    "/meet-the-team",
    "/meet-the-team/",
    "/meet-our-team",
    "/meet-our-team/",
    "/meet-us",
    "/meet-us/",
    "/care-team",  # e.g., /care-team (healthcare orgs)
    "/care-team/",
    # =========================================================================
    # LEADERSHIP pages (dedicated leadership listings)
    # =========================================================================
    "/leadership",
    "/leadership/",
    "/leaders",
    "/leaders/",
    "/our-leadership",
    "/our-leadership/",
    "/our_leadership",
    "/our_leadership/",
    "/leadership-team",
    "/leadership-team/",
    "/leadership_team",
    "/leadership_team/",
    "/executive-team",
    "/executive-team/",
    "/executive_team",
    "/executive_team/",
    "/executives",
    "/executives/",
    "/executive-leadership",
    "/executive-leadership/",
    "/senior-leadership",
    "/senior-leadership/",
    "/senior-team",
    "/senior-team/",
    "/c-suite",
    "/c-suite/",
    # =========================================================================
    # MANAGEMENT / BOARD pages
    # =========================================================================
    "/management",
    "/management/",
    "/management-team",
    "/management-team/",
    "/board",
    "/board/",
    "/board-of-directors",
    "/board-of-directors/",
    "/board_of_directors",
    "/board_of_directors/",
    "/directors",
    "/directors/",
    "/advisors",
    "/advisors/",
    "/advisory-board",
    "/advisory-board/",
    "/advisory_board",
    "/advisory_board/",
    # =========================================================================
    # PEOPLE / STAFF pages
    # =========================================================================
    "/people",
    "/people/",
    "/our-people",
    "/our-people/",
    "/our_people",
    "/our_people/",
    "/staff",
    "/staff/",
    "/employees",
    "/employees/",
    "/founders",
    "/founders/",
    "/our-founders",
    "/our-founders/",
    "/partners",  # In professional services context
    "/partners/",
    # =========================================================================
    # COMPANY pages (often contain leadership sections)
    # =========================================================================
    "/company",
    "/company/",
    "/company/about",
    "/company/about/",
    "/company/team",
    "/company/team/",
    "/company/leadership",
    "/company/leadership/",
    "/company/people",
    "/company/people/",
    # =========================================================================
    # PRESS / NEWSROOM (often have leadership bios)
    # =========================================================================
    "/press-room",
    "/press-room/",
    "/pressroom",
    "/pressroom/",
    "/press",  # Only the main press page, not /press/article
    "/press/",
    "/newsroom",
    "/newsroom/",
    "/news-room",
    "/news-room/",
    "/media-room",
    "/media-room/",
    "/media",
    "/media/",
)

# Regex patterns for more precise allowlist matching
_ALLOWED_REGEX_PATTERNS: tuple[re.Pattern, ...] = (
    # Team page as exact segment: /team or /team/
    re.compile(r"^/teams?/?$", re.I),
    # Leadership page as exact segment
    re.compile(r"^/leadership/?$", re.I),
    # About page with subpath like /about/leadership
    re.compile(r"^/about/(?:leadership|team|people)", re.I),
    # Company subpages
    re.compile(r"^/company/(?:about|team|leadership|people)", re.I),
    # Care team pages (healthcare contexts): /care-team, /care_team, /careteam
    re.compile(r"^/care(?:[-_]?team)?/?$", re.I),
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_blocked_url(url: str) -> tuple[bool, str | None]:
    """
    Check if URL should be blocked from people extraction.

    Args:
        url: The URL to check

    Returns:
        Tuple of (is_blocked, reason)
        - is_blocked: True if URL should be blocked
        - reason: Description of why blocked (for logging), or None
    """
    if not url:
        return False, None

    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False, None

    # Check substring blocklist first
    for pattern in _BLOCKED_URL_PATTERNS:
        if pattern in path:
            return True, f"blocked_substring:{pattern}"

    # Check regex blocklist
    for regex in _BLOCKED_REGEX_PATTERNS:
        if regex.search(path):
            return True, f"blocked_regex:{regex.pattern}"

    return False, None


def is_allowed_url(url: str) -> bool:
    """
    Check if URL is in the allowlist for people extraction.

    Args:
        url: The URL to check

    Returns:
        True if URL matches an allowed pattern
    """
    if not url:
        return False

    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False

    # Check substring allowlist
    for pattern in _ALLOWED_URL_PATTERNS:
        if pattern in path:
            return True

    # Check regex allowlist
    for regex in _ALLOWED_REGEX_PATTERNS:
        if regex.search(path):
            return True

    return False


def is_people_page_url(url: str) -> bool:
    """
    Determine if URL should be processed for people extraction.

    This is the main entry point for URL filtering. Logic:
    1. If URL matches a blocked pattern -> False (highest priority)
    2. If URL matches an allowed pattern -> True
    3. Otherwise -> False (conservative default)

    Args:
        url: The URL to check

    Returns:
        True if URL should be processed for people extraction
    """
    # Step 1: Check blocklist (highest priority)
    is_blocked, reason = is_blocked_url(url)
    if is_blocked:
        log.debug("Blocking URL from extraction: %s (reason=%s)", url, reason)
        return False

    # Step 2: Check allowlist
    if is_allowed_url(url):
        log.debug("URL matches allowlist: %s", url)
        return True

    # Step 3: Default to blocking unknown URLs
    log.debug("URL not in allowlist, blocking: %s", url)
    return False


def classify_url(url: str) -> dict:
    """
    Classify a URL and return detailed information.

    Useful for debugging and understanding why a URL was filtered.

    Args:
        url: The URL to classify

    Returns:
        Dict with classification details:
        - is_people_page: Final determination
        - is_blocked: Whether blocked by blocklist
        - block_reason: Why blocked (if applicable)
        - is_allowed: Whether in allowlist
        - path: Extracted path
    """
    result = {
        "url": url,
        "path": None,
        "is_blocked": False,
        "block_reason": None,
        "is_allowed": False,
        "is_people_page": False,
    }

    try:
        result["path"] = urlparse(url).path.lower()
    except Exception:
        return result

    # Check blocklist
    is_blocked, reason = is_blocked_url(url)
    result["is_blocked"] = is_blocked
    result["block_reason"] = reason

    if not is_blocked:
        result["is_allowed"] = is_allowed_url(url)

    result["is_people_page"] = not is_blocked and result["is_allowed"]

    return result


# ---------------------------------------------------------------------------
# Utility functions for debugging
# ---------------------------------------------------------------------------


def explain_url_filtering(url: str) -> str:
    """
    Return a human-readable explanation of why a URL was filtered.

    Useful for logging and debugging.
    """
    info = classify_url(url)

    if info["is_blocked"]:
        return f"BLOCKED: {url}\n  Reason: {info['block_reason']}"
    elif info["is_allowed"]:
        return f"ALLOWED: {url}\n  Path: {info['path']}"
    else:
        return f"NOT IN ALLOWLIST: {url}\n  Path: {info['path']}"


__all__ = [
    "is_blocked_url",
    "is_allowed_url",
    "is_people_page_url",
    "classify_url",
    "explain_url_filtering",
]
