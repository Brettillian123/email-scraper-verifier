# src/crawl/runner.py
"""
Web crawler for company sites.

Key features:
  - BFS crawl from seed paths
  - Respects robots.txt via is_allowed()
  - Canonical-host locking: resolves the site's final host once (after redirects)
    and builds all future URLs off that origin (stops www/non-www duplication)
  - Tracks final URLs after redirects to avoid duplicates
  - Soft-404 detection to skip error pages returned as HTTP 200
  - Optional robots explainability logging (non-fatal if unavailable)
  - Optional AutodiscoveryResult integration for metrics tracking

Tiered seeding (R10 refinement):
  - Seeds are attempted by tiers (T1 -> T2 -> T3)
  - Lower tiers are only attempted if higher tiers fail to yield “people pages”
  - Once enough people pages are found, additional seed attempts stop
  - Seed URLs are processed with higher priority than discovered crawl URLs

Discover-first seeds (tightener):
  - When CRAWL_SEEDS_LINKED_ONLY is enabled, seed paths are only enqueued if the
    path is discovered by parsing internal links from CRAWL_DISCOVERY_PATHS
    (default "/,/about"). This stops probing unlinked endpoints.

High-ROI tighteners (robustness):
  - Discovery pages are filtered for soft-404s (prevents /error/404/ nav pollution)
  - People-page stop heuristic uses path segments + strong title phrases
    (prevents "Subscription Management" from counting as a people page)
  - Follow-keyword matching is segment-aware with special-case handling for
    "management" to avoid matching "subscription-management" product pages
  - Skips common SaaS taxonomy pattern /solutions/teams/* (usually not people pages)

Efficiency controls (batch-polish additions):
  - WAF/403 early-abort when the first N meaningful fetches are all 403 and no pages
    have been persisted (prevents 30–50s burn on blocked sites)
  - Per-domain 403 ceiling (abort once 403 count crosses a cap)
  - Per-domain time budgets:
      * budget_no_pages_s: abort if exceeded before persisting any page
      * budget_total_s: hard cap for crawl wall-clock time
  - Pagination guardrails for content hubs (/blog, /news, /press, /newsroom, /resources):
      * never follow /page/2, /p2, /p/3, etc. on content hubs
  - Stronger URL de-duplication keys (normalize path casing, trailing slashes,
    index.* default docs, repeated slashes; query/fragment dropped) to reduce duplicates
  - Dynamic nav-link expansion (last-resort):
      * if all seed tiers are exhausted and 0 pages were persisted, fetch discovery pages
        and enqueue the top N high-signal internal nav links (team/leadership/about/etc.)
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

from src.config import (
    CRAWL_CONNECT_TIMEOUT_S,
    CRAWL_DISCOVERY_PATHS,
    CRAWL_FOLLOW_KEYWORDS,
    CRAWL_MAX_DEPTH,
    CRAWL_MAX_PAGES_PER_DOMAIN,
    CRAWL_READ_TIMEOUT_S,
    CRAWL_SEED_STOP_MIN_PEOPLE_PAGES,
    CRAWL_SEED_TIERS,
    CRAWL_SEEDS_LINKED_ONLY,
    FETCH_USER_AGENT,
)

# Override HTML size limit to 2MB to handle large marketing pages (like Paddle's /about).
# The config default is 1.5MB which may drop some valid pages silently.
CRAWL_HTML_MAX_BYTES = 2_000_000  # 2 MB

# Import robots helpers - is_allowed is required, explain_block is optional
from src.fetch.robots import is_allowed  # noqa: E402

# Optional: robots explainability (non-fatal if not available)
try:
    from src.fetch.robots import explain_block  # noqa: E402

    _HAS_EXPLAIN_BLOCK = True
except ImportError:  # pragma: no cover
    _HAS_EXPLAIN_BLOCK = False
    explain_block = None  # type: ignore[assignment]


log = logging.getLogger(__name__)


@dataclass
class Page:
    """A crawled page."""

    url: str
    html: bytes
    fetched_at: float
    company_id: int | None = None


@dataclass
class _CrawlState:
    seed_q: deque[tuple[str, int]] = field(default_factory=deque)
    crawl_q: deque[tuple[str, int]] = field(default_factory=deque)

    seen_request_keys: set[str] = field(default_factory=set)
    seen_final_keys: set[str] = field(default_factory=set)

    pages: list[Page] = field(default_factory=list)

    seed_people_found: int = 0
    seed_people_seen: set[str] = field(default_factory=set)
    seed_attempted_total: int = 0
    seed_tiers_enqueued: int = 0

    aborted: bool = False
    abort_reason: str = ""
    abort_stage: str = ""

    meaningful_fetches: int = 0
    meaningful_403: int = 0

    nav_expanded: bool = False
    nav_enqueued: int = 0


# ---------------------------------------------------------------------------
# Local config (env-backed) for crawl efficiency controls
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


# WAF / 403 early-abort: if the first N meaningful fetches are all 403 and we have 0 pages.
_CRAWL_WAF_ABORT_FIRST_N = max(1, _env_int("CRAWL_WAF_ABORT_FIRST_N", 10))
# 403 ceiling: if 403 count reaches this, abort (regardless of first-N heuristic).
_CRAWL_403_CEILING = max(1, _env_int("CRAWL_403_CEILING", 18))

# Time budgets (wall-clock)
_CRAWL_TIME_BUDGET_NO_PAGES_S = max(1.0, _env_float("CRAWL_TIME_BUDGET_NO_PAGES_S", 25.0))
_CRAWL_TIME_BUDGET_TOTAL_S = max(1.0, _env_float("CRAWL_TIME_BUDGET_TOTAL_S", 60.0))

# Dynamic nav expansion (last-resort when seeds fail to yield any persisted pages)
_CRAWL_NAV_EXPANSION_ENABLED = _env_bool("CRAWL_NAV_EXPANSION_ENABLED", True)
_CRAWL_NAV_EXPANSION_MAX_LINKS = max(1, _env_int("CRAWL_NAV_EXPANSION_MAX_LINKS", 20))

# ---------------------------------------------------------------------------
# Sparse discovery fallback (bot-specific HTML detection)
# ---------------------------------------------------------------------------

_CRAWL_SPARSE_DISCOVERY_THRESHOLD = max(1, _env_int("CRAWL_SPARSE_DISCOVERY_THRESHOLD", 10))
_CRAWL_SPARSE_FALLBACK_MIN_PAGES = max(1, _env_int("CRAWL_SPARSE_FALLBACK_MIN_PAGES", 5))
_CRAWL_SPARSE_FALLBACK_ENABLED = _env_bool("CRAWL_SPARSE_FALLBACK_ENABLED", True)

_EXPECTED_NAV_PATHS = frozenset(
    {
        "/about",
        "/about-us",
        "/company",
        "/team",
        "/leadership",
        "/contact",
        "/people",
        "/careers",
        "/news",
        "/blog",
    }
)

_HIGH_VALUE_PATH_TERMS = frozenset(
    {
        "about",
        "team",
        "leadership",
        "people",
        "staff",
        "executive",
        "executives",
        "management",
        "founder",
        "founders",
        "board",
        "director",
        "directors",
        "who-we-are",
        "our-team",
        "our-people",
        "our-leadership",
        "meet-the-team",
        "providers",
        "provider",
        "physicians",
        "doctors",
        "specialists",
        "dietitians",
        "dietitian",
        "therapists",
        "counselors",
        "clinicians",
        "company",
        "our-story",
        "our-company",
        "meet-us",
    }
)

# ---------------------------------------------------------------------------
# Host + URL normalization
# ---------------------------------------------------------------------------


def _normalize_host(host: str) -> str:
    """Strip www. prefix for canonical host comparison."""
    h = (host or "").lower().strip()
    if h.startswith("www."):
        return h[4:]
    return h


def _hosts_match(base_host: str, url_host: str) -> bool:
    """Return True if hosts are equivalent (ignoring www. prefix)."""
    return _normalize_host(base_host) == _normalize_host(url_host)


def _normalize_path(path: str) -> str:
    """
    Normalize a URL path for comparison against seed paths and crawl de-dup keys.

    - ensures leading slash
    - collapses empty to "/"
    - strips trailing slash (except "/")
    - lowercases (pragmatic; most marketing sites are case-insensitive)
    - collapses repeated slashes
    - strips default document suffixes (index.html, index.htm, index.php)
    """
    p = (path or "").strip()
    if not p:
        p = "/"
    if not p.startswith("/"):
        p = "/" + p

    p = re.sub(r"/{2,}", "/", p)

    p_low = p.lower()
    for suffix in ("/index.html", "/index.htm", "/index.php"):
        if p_low.endswith(suffix):
            p = p[: -len(suffix)] or "/"
            p_low = p.lower()
            break

    if p != "/" and p.endswith("/"):
        p = p[:-1]
    if not p:
        p = "/"
    return p.lower()


def _path_segments(path: str) -> list[str]:
    """Split a path into lowercase slash-delimited segments, excluding empties."""
    p = (path or "").strip()
    if not p:
        return []
    if p.startswith("http://") or p.startswith("https://"):
        try:
            p = urlparse(p).path or "/"
        except Exception:
            p = "/"
    p = re.sub(r"/{2,}", "/", p)
    return [seg for seg in p.lower().strip("/").split("/") if seg]


def _is_high_value_path(path: str) -> bool:
    """
    Check if a path contains high-value terms indicating a people/about page.

    Catches paths like /about-foo, /team-members, /our-leadership, etc.
    """
    segments = _path_segments(path)
    for seg in segments:
        for term in _HIGH_VALUE_PATH_TERMS:
            if seg == term:
                return True
            if seg.startswith(term + "-") or seg.startswith(term + "_"):
                return True
            parts = re.split(r"[-_]+", seg)
            if term in parts:
                return True
    return False


def _canonicalize_to_origin(abs_url: str, *, origin_base: str, base_host: str) -> str | None:
    """
    If abs_url is same-site (considering www equivalence), rewrite it to the
    canonical origin host/scheme and return the canonical absolute URL.

    Returns None if abs_url is off-site or malformed.

    Note: Query/fragment are dropped intentionally.
    """
    try:
        u = urlparse(abs_url)
    except Exception:
        return None

    host = (u.netloc or "").lower()
    if not host:
        return None
    if not _hosts_match(base_host, host):
        return None

    path = u.path or "/"
    path_norm = _normalize_path(path)
    return urljoin(origin_base, path_norm)


def _url_key(url: str, *, origin_base: str, base_host: str) -> str | None:
    """
    Convert a URL into a stable de-duplication key:
      - must be same-site
      - canonical origin
      - normalized path
      - query/fragment dropped
    """
    canon = _canonicalize_to_origin(url, origin_base=origin_base, base_host=base_host)
    if not canon:
        return None
    try:
        p = urlparse(canon).path or "/"
    except Exception:
        p = "/"
    return urljoin(origin_base, _normalize_path(p))


# ---------------------------------------------------------------------------
# Pagination guardrails (content hubs)
# ---------------------------------------------------------------------------

_CONTENT_HUB_SEGMENTS = {
    "blog",
    "news",
    "press",
    "newsroom",
    "media",
    "resources",
    "insights",
    "articles",
    "events",
    "stories",
    "updates",
}

_PAGINATION_SEGMENTS = {"page", "p"}
_PAGINATION_SUFFIX_RE = re.compile(r"^(?:p|page)[-_]?\d+$", re.IGNORECASE)
_PAGINATION_TRAILING_NUM_RE = re.compile(r"^\d{1,4}$")


def _is_content_hub_path(path: str) -> bool:
    segs = _path_segments(path)
    return any(seg in _CONTENT_HUB_SEGMENTS for seg in segs)


def _looks_like_pagination_path(path: str) -> bool:
    """
    Detect common pagination patterns:
      - /page/2, /p/3
      - /p2, /page2, /page-2, /p-2
      - /blog/2 (when clearly a hub path)
    """
    segs = _path_segments(path)
    if not segs:
        return False

    for i, seg in enumerate(segs[:-1]):
        if seg in _PAGINATION_SEGMENTS and _PAGINATION_TRAILING_NUM_RE.match(segs[i + 1] or ""):
            return True

    if _PAGINATION_SUFFIX_RE.match(segs[-1] or ""):
        return True

    if _PAGINATION_TRAILING_NUM_RE.match(segs[-1] or "") and _is_content_hub_path(path):
        return True

    return False


def _should_skip_pagination(path: str) -> bool:
    """Apply pagination suppression only on content hubs."""
    p = _normalize_path(path)
    if not _is_content_hub_path(p):
        return False
    return _looks_like_pagination_path(p)


# ---------------------------------------------------------------------------
# Soft-404 detection
# ---------------------------------------------------------------------------

_SOFT_404_PATH_PATTERNS = (
    "/404",
    "/error/404",
    "/error",
    "/not-found",
    "/page-not-found",
    "/notfound",
    "/_error",
)

_SOFT_404_TITLE_RE = re.compile(
    r"<title[^>]*>.*?(404|not\s*found|page\s*not\s*found|error\s*page).*?</title>",
    re.IGNORECASE | re.DOTALL,
)

_SOFT_404_BODY_PATTERNS = (
    b"page not found",
    b"page you requested could not be found",
    b"this page doesn't exist",
    b"this page does not exist",
    b"we couldn't find",
    b"we could not find",
    b"404 error",
)


def _is_soft_404(final_url: str, body: bytes) -> bool:
    """Detect soft-404 pages that return HTTP 200 but are actually error pages."""
    path = urlparse(final_url).path.lower()
    if any(pattern in path for pattern in _SOFT_404_PATH_PATTERNS):
        return True

    head = body[:4096].decode("utf-8", "ignore")
    if _SOFT_404_TITLE_RE.search(head):
        return True

    body_lower = body[:8192].lower()
    if any(pattern in body_lower for pattern in _SOFT_404_BODY_PATTERNS):
        return True

    return False


def _looks_like_soft_404_path(path: str) -> bool:
    p = _normalize_path(path)
    return any(pattern in p for pattern in _SOFT_404_PATH_PATTERNS)


# ---------------------------------------------------------------------------
# People-page detection (for tier stop condition)
# ---------------------------------------------------------------------------

_PEOPLE_PATH_SEGMENTS = {
    "team",
    "teams",
    "people",
    "staff",
    "directory",
    "leadership",
    "executive",
    "executives",
    "management",
    "founder",
    "founders",
    "board",
    "director",
    "directors",
    "advisors",
    "advisor",
    "advisory",
    "governance",
    "bio",
    "bios",
}

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)

_PEOPLE_TITLE_PHRASES_RE = re.compile(
    r"\b("
    r"leadership|leadership\s+team|executive\s+team|executive\s+leadership|"
    r"management\s+team|board\s+of\s+directors|directors|advisory\s+board|"
    r"our\s+team|meet\s+(the\s+)?team|team\s+members|company\s+leadership"
    r")\b",
    re.IGNORECASE,
)


def _looks_like_people_page(url: str, body: bytes) -> bool:
    """Cheap people-page heuristic used only to stop further seed-tier attempts."""
    path = urlparse(url).path or "/"
    segs = _path_segments(path)
    if any(seg in _PEOPLE_PATH_SEGMENTS for seg in segs):
        return True

    head = body[:8192].decode("utf-8", "ignore")
    m = _TITLE_RE.search(head)
    if not m:
        return False
    title = re.sub(r"\s+", " ", m.group(1)).strip()
    if not title:
        return False
    return bool(_PEOPLE_TITLE_PHRASES_RE.search(title))


# ---------------------------------------------------------------------------
# Robots explainability helper
# ---------------------------------------------------------------------------


def _log_robots_block(host: str, path: str, url: str, result: Any = None) -> None:
    """Log a robots block with explainability if available (and update result if provided)."""
    if _HAS_EXPLAIN_BLOCK and explain_block is not None:
        try:
            block_info = explain_block(host, path)
            log.info(
                "ROBOTS BLOCK: url=%s robots=%s ua=%s reason=%s rule=%r notes=%r",
                block_info.blocked_url,
                block_info.robots_url,
                block_info.user_agent,
                block_info.reason,
                block_info.matched_rule,
                block_info.notes,
            )
            if result is not None and hasattr(result, "add_robots_block"):
                result.add_robots_block(block_info.to_dict())
            return
        except Exception as exc:  # pragma: no cover
            log.info(
                "ROBOTS BLOCK: url=%s (explainability unavailable: %s)",
                url,
                exc,
            )
            if result is not None and hasattr(result, "add_robots_block"):
                result.add_robots_block(
                    {
                        "blocked_url": url,
                        "robots_url": f"https://{host}/robots.txt",
                        "user_agent": "email-scraper",
                        "allowed": False,
                        "reason": "blocked",
                        "matched_rule": None,
                        "notes": f"explainability_error: {exc}",
                    }
                )
            return

    log.info("ROBOTS BLOCK: url=%s host=%s path=%s", url, host, path)
    if result is not None and hasattr(result, "add_robots_block"):
        result.add_robots_block(
            {
                "blocked_url": url,
                "robots_url": f"https://{host}/robots.txt",
                "user_agent": "email-scraper",
                "allowed": False,
                "reason": "blocked",
                "matched_rule": None,
                "notes": "rule_not_available_from_parser",
            }
        )


def _robots_deny_all(host: str) -> bool:
    """
    Best-effort detection for "Disallow: /" (deny-all) scenarios.

    Prefer explain_block() when available; otherwise use a conservative
    multi-path probe against is_allowed().
    """
    if _HAS_EXPLAIN_BLOCK and explain_block is not None:
        try:
            info = explain_block(host, "/")
            reason = str(getattr(info, "reason", "") or "").strip().lower()
            allowed = bool(getattr(info, "allowed", False))
            matched = getattr(info, "matched_rule", None)

            if not allowed and any(s in reason for s in ("deny_all", "deny-all", "disallow_all")):
                return True

            if not allowed and matched is not None:
                ms = str(matched).strip().lower()
                if "disallow" in ms and (ms.endswith(("'/", "/'", "/")) or " /" in ms):
                    if "allow" not in ms:
                        return True
        except Exception:  # pragma: no cover
            pass

    probes = ["/", "/about", "/team", "/contact"]
    return all(not is_allowed(host, p) for p in probes)


def _is_sparse_discovery(discovered_paths: set[str]) -> tuple[bool, str]:
    """
    Detect if discovery yielded suspiciously few paths (indicating bot-specific HTML).

    Returns (is_sparse, reason).
    """
    if not _CRAWL_SPARSE_FALLBACK_ENABLED:
        return False, ""

    real_paths = {p for p in discovered_paths if p != "/"}
    path_count = len(real_paths)

    if path_count < _CRAWL_SPARSE_DISCOVERY_THRESHOLD:
        return (
            True,
            f"only {path_count} paths discovered (threshold={_CRAWL_SPARSE_DISCOVERY_THRESHOLD})",
        )

    normalized_real = {p.rstrip("/") or "/" for p in real_paths}
    found_nav = normalized_real & _EXPECTED_NAV_PATHS
    if not found_nav and path_count < _CRAWL_SPARSE_DISCOVERY_THRESHOLD * 2:
        return True, f"no expected nav paths found in {path_count} discovered paths"

    return False, ""


# ---------------------------------------------------------------------------
# Discover-first seed support
# ---------------------------------------------------------------------------


_HREF_RE = re.compile(r"""href\s*=\s*['"]([^'"]+)['"]""", re.IGNORECASE)

_SKIP_EXT_RE = re.compile(
    r"\.(?:pdf|png|jpg|jpeg|gif|webp|svg|ico|css|js|map|json|xml|zip|gz|tgz|rar|7z|mp4|mov|avi|mp3|wav)$",
    re.IGNORECASE,
)


def _extract_internal_paths(
    html_text: str,
    *,
    base_url: str,
    base_host: str,
    origin_base: str,
) -> set[str]:
    """Extract same-site paths from HTML by scanning href="..."."""
    out: set[str] = set()
    for m in _HREF_RE.finditer(html_text):
        href = (m.group(1) or "").strip()
        if not href:
            continue
        if href.startswith(("mailto:", "tel:", "javascript:", "data:")):
            continue

        abs_url = urljoin(base_url, href)
        canon = _canonicalize_to_origin(abs_url, origin_base=origin_base, base_host=base_host)
        if not canon:
            continue

        parsed = urlparse(canon)
        path = parsed.path or "/"
        if _looks_like_soft_404_path(path):
            continue
        if _SKIP_EXT_RE.search(path):
            continue
        if _should_skip_pagination(path):
            continue

        norm = _normalize_path(path)
        out.add(norm)
        if norm != "/":
            out.add(norm + "/")
    return out


def _build_discovered_paths(
    client: Any,
    *,
    origin_base: str,
    base_host: str,
    timeout: Any,
) -> set[str]:
    """
    Build discovered internal paths by fetching and parsing discovery pages.

    Notes:
      - Applies robots checks to discovery requests (conservative)
      - Filters out discovery responses that are soft-404s
      - Applies pagination guardrails to discovered link sets
    """
    discovered: set[str] = set()

    for p in CRAWL_DISCOVERY_PATHS:
        dp = _normalize_path(p)
        if _looks_like_soft_404_path(dp):
            continue

        if not is_allowed(base_host, dp):
            continue

        url = urljoin(origin_base, dp)
        try:
            resp = client.get(url, timeout=timeout)
        except Exception:
            continue

        if resp.status_code != 200:
            continue

        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type.lower():
            continue

        body = resp.content
        final_path = urlparse(str(resp.url)).path or dp
        final_path_norm = _normalize_path(final_path)

        final_url_canon = urljoin(origin_base, final_path_norm)
        if _is_soft_404(final_url_canon, body):
            continue

        discovered.add(dp)
        if dp != "/":
            discovered.add(dp + "/")

        discovered.add(final_path_norm)
        if final_path_norm != "/":
            discovered.add(final_path_norm + "/")

        try:
            text = resp.text
        except Exception:
            try:
                text = body.decode("utf-8", "ignore")
            except Exception:
                continue

        discovered |= _extract_internal_paths(
            text,
            base_url=str(resp.url),
            base_host=base_host,
            origin_base=origin_base,
        )

    return discovered


# ---------------------------------------------------------------------------
# Follow logic (segment-aware)
# ---------------------------------------------------------------------------


def _matches_follow_hints(path: str, hints: list[str]) -> bool:
    """Return True if the path matches follow hints, using segment-aware matching."""
    segs = _path_segments(path)
    if not segs:
        return False

    for seg in segs:
        if not seg:
            continue

        for hint in hints:
            if not hint:
                continue
            h = hint.lower()

            if h == "management":
                if seg.startswith("management"):
                    return True
                continue

            if seg == h or seg.startswith(h):
                return True

            parts = re.split(r"[-_]+", seg)
            for part in parts:
                if not part:
                    continue
                if part == h or part.startswith(h):
                    return True

    return False


def _should_skip_taxonomy(path: str) -> bool:
    """Hard skip patterns that commonly look like people pages but aren't."""
    segs = _path_segments(path)
    if len(segs) >= 2 and segs[0] == "solutions" and segs[1] in {"team", "teams"}:
        strong = {
            "leadership",
            "executive",
            "executives",
            "board",
            "people",
            "staff",
            "directory",
            "bios",
            "bio",
        }
        if any(s in strong for s in segs[2:]):
            return False
        return True
    return False


# ---------------------------------------------------------------------------
# Dynamic nav-link expansion (last-resort)
# ---------------------------------------------------------------------------

_NAV_POS_STRONG = {
    "leadership",
    "executive",
    "executives",
    "team",
    "teams",
    "people",
    "staff",
    "directory",
    "management",
    "board",
    "directors",
    "governance",
    "advisors",
    "advisor",
    "advisory",
    "bios",
    "bio",
}
_NAV_POS_WEAK = {
    "about",
    "company",
    "who-we-are",
    "who_we_are",
    "our-team",
    "our_team",
    "meet-the-team",
    "meet_the_team",
    "leadership-team",
    "leadership_team",
    "careers",
}
_NAV_NEG = {
    "privacy",
    "terms",
    "legal",
    "cookie",
    "cookies",
    "support",
    "help",
    "docs",
    "documentation",
    "pricing",
    "contact",
    "press",
    "news",
    "newsroom",
    "blog",
    "resources",
    "events",
    "partners",
    "partner",
    "customers",
    "customer",
    "case-studies",
    "case_studies",
}


def _score_nav_path(path: str) -> int:
    """Score a candidate path for last-resort nav expansion."""
    p = _normalize_path(path)
    if p == "/":
        return 0
    segs = _path_segments(p)
    if not segs:
        return 0

    score = 0
    if any(seg in _NAV_NEG for seg in segs):
        score -= 5
    if any(seg in _NAV_POS_STRONG for seg in segs):
        score += 10
    if any(seg in _NAV_POS_WEAK for seg in segs):
        score += 4

    if len(segs) <= 2:
        score += 2
    if len(segs) >= 5:
        score -= 2

    if _should_skip_pagination(p):
        score -= 10

    if any(seg in _PEOPLE_PATH_SEGMENTS for seg in segs):
        score += 3

    return score


def _elapsed_s(start_monotonic: float) -> float:
    return time.monotonic() - start_monotonic


def _check_time_budget(
    *,
    state: _CrawlState,
    start_monotonic: float,
    stage: str,
) -> bool:
    elapsed = _elapsed_s(start_monotonic)

    if not state.pages and elapsed >= _CRAWL_TIME_BUDGET_NO_PAGES_S:
        state.aborted = True
        state.abort_reason = "time_budget_no_pages"
        state.abort_stage = stage
        return True

    if elapsed >= _CRAWL_TIME_BUDGET_TOTAL_S:
        state.aborted = True
        state.abort_reason = "time_budget_total"
        state.abort_stage = stage
        return True

    return False


def _maybe_abort_for_waf(
    *,
    state: _CrawlState,
    stage: str,
) -> bool:
    if (
        not state.pages
        and state.meaningful_fetches >= _CRAWL_WAF_ABORT_FIRST_N
        and state.meaningful_403 >= _CRAWL_WAF_ABORT_FIRST_N
    ):
        state.aborted = True
        state.abort_reason = "waf_403_first_n"
        state.abort_stage = stage
        return True

    if state.meaningful_403 >= _CRAWL_403_CEILING:
        state.aborted = True
        state.abort_reason = "http_403_ceiling"
        state.abort_stage = stage
        return True

    return False


def _resolve_origin(client: Any, *, dom: str, timeout: Any) -> tuple[str, str]:
    """
    Resolve canonical origin by requesting the site root with redirects.

    Returns (origin_base, base_host).
    """
    fallback = f"https://{dom}/"
    try:
        resp = client.get(fallback, timeout=timeout)
        scheme = resp.url.scheme
        host = resp.url.host
        if scheme and host:
            origin = f"{scheme}://{host}".rstrip("/") + "/"
            return origin, host.lower()
    except Exception:
        pass

    parsed = urlparse(fallback)
    return fallback, (parsed.netloc or dom).lower()


def _should_enqueue_seed_path(
    seed_path: str,
    *,
    seeds_linked_only: bool,
    discovered_paths: set[str],
    sparse_fallback_active: bool,
) -> bool:
    if sparse_fallback_active:
        return True
    if not seeds_linked_only:
        return True
    sp = _normalize_path(seed_path)
    return sp in discovered_paths or (sp != "/" and (sp + "/") in discovered_paths)


def _enqueue_next_nonempty_tier(
    *,
    seed_q: deque[tuple[str, int]],
    origin_base: str,
    start_idx: int,
    discovered_paths: set[str],
    sparse_fallback_active: bool,
) -> int:
    """
    Enqueue the next non-empty tier at/after start_idx into seed_q.

    Returns the index of the tier enqueued, or len(CRAWL_SEED_TIERS) if none.
    """
    idx = start_idx
    while idx < len(CRAWL_SEED_TIERS):
        tier_paths = CRAWL_SEED_TIERS[idx] or []
        enqueued_any = False
        seen: set[str] = set()

        for p in tier_paths:
            sp = (p or "").strip()
            if not sp:
                continue
            if not sp.startswith("/"):
                sp = "/" + sp

            if not _should_enqueue_seed_path(
                sp,
                seeds_linked_only=bool(CRAWL_SEEDS_LINKED_ONLY),
                discovered_paths=discovered_paths,
                sparse_fallback_active=sparse_fallback_active,
            ):
                continue

            if _should_skip_pagination(sp):
                continue

            u = urljoin(origin_base, _normalize_path(sp))
            if u in seen:
                continue

            seen.add(u)
            seed_q.append((u, 0))
            enqueued_any = True

        if enqueued_any:
            return idx
        idx += 1

    return idx


def _nav_candidates(
    *,
    cand_paths: set[str],
    origin_base: str,
    base_host: str,
    seen_request_keys: set[str],
    seen_final_keys: set[str],
) -> list[tuple[int, str]]:
    scored: list[tuple[int, str]] = []

    for raw in cand_paths:
        pth = _normalize_path(raw)
        if pth == "/":
            continue
        if _looks_like_soft_404_path(pth):
            continue
        if _SKIP_EXT_RE.search(pth):
            continue
        if _should_skip_taxonomy(pth):
            continue
        if _should_skip_pagination(pth):
            continue

        score = _score_nav_path(pth)
        if score <= 0:
            continue

        if not is_allowed(base_host, pth):
            continue

        u = urljoin(origin_base, pth)
        k = _url_key(u, origin_base=origin_base, base_host=base_host)
        if not k:
            continue
        if k in seen_request_keys or k in seen_final_keys:
            continue

        scored.append((score, k))

    return scored


def _maybe_nav_expand(
    *,
    client: Any,
    state: _CrawlState,
    stage: str,
    origin_base: str,
    base_host: str,
    timeout: Any,
    discovered_paths: set[str],
    next_tier_idx: int,
) -> bool:
    """
    Last-resort: when tiered seeding is exhausted and 0 pages were persisted,
    enqueue a small set of high-signal internal nav links.
    """
    if state.nav_expanded or not _CRAWL_NAV_EXPANSION_ENABLED:
        return False
    if state.pages:
        return False
    if next_tier_idx < len(CRAWL_SEED_TIERS):
        return False
    if state.meaningful_fetches >= 3 and state.meaningful_403 >= state.meaningful_fetches:
        return False

    if CRAWL_SEEDS_LINKED_ONLY and discovered_paths:
        cand_paths = set(discovered_paths)
    else:
        cand_paths = _build_discovered_paths(
            client,
            origin_base=origin_base,
            base_host=base_host,
            timeout=timeout,
        )
        cand_paths.add("/")

    scored = _nav_candidates(
        cand_paths=cand_paths,
        origin_base=origin_base,
        base_host=base_host,
        seen_request_keys=state.seen_request_keys,
        seen_final_keys=state.seen_final_keys,
    )
    if not scored:
        state.nav_expanded = True
        return False

    scored.sort(key=lambda t: (-t[0], t[1]))
    for _s, k in scored[:_CRAWL_NAV_EXPANSION_MAX_LINKS]:
        state.seed_q.append((k, 0))
        state.nav_enqueued += 1

    state.nav_expanded = True
    log.debug(
        "Nav expansion triggered at stage=%s: enqueued=%d origin=%s",
        stage,
        state.nav_enqueued,
        origin_base.rstrip("/"),
    )
    return state.nav_enqueued > 0


def _fetch_html_page(
    *,
    client: Any,
    key: str,
    origin_base: str,
    base_host: str,
    state: _CrawlState,
) -> tuple[str, bytes] | None:
    resp = client.get(key)

    state.meaningful_fetches += 1
    if resp.status_code == 403:
        state.meaningful_403 += 1

    if _maybe_abort_for_waf(state=state, stage="post_fetch"):
        return None

    final_path_raw = urlparse(str(resp.url)).path or "/"
    final_path = _normalize_path(final_path_raw)
    if _should_skip_pagination(final_path):
        return None

    final_url = urljoin(origin_base, final_path)
    final_key = _url_key(final_url, origin_base=origin_base, base_host=base_host) or final_url

    if final_key in state.seen_final_keys:
        log.debug("Skipping duplicate final URL: %s -> %s", key, final_url)
        return None

    if resp.status_code != 200:
        log.debug("Skipping non-200 response: %s (status=%s)", final_url, resp.status_code)
        return None

    content_type = resp.headers.get("content-type", "")
    if "text/html" not in content_type.lower():
        log.debug("Skipping non-HTML: %s (content-type=%s)", final_url, content_type)
        return None

    body = resp.content
    if len(body) > CRAWL_HTML_MAX_BYTES:
        log.info(
            "Skipping oversized page: url=%s size=%d limit=%d "
            "(increase CRAWL_HTML_MAX_BYTES if needed)",
            final_url,
            len(body),
            CRAWL_HTML_MAX_BYTES,
        )
        return None

    if _is_soft_404(final_url, body):
        log.debug("Skipping soft-404 page: %s", final_url)
        return None

    state.seen_final_keys.add(final_key)
    state.seen_final_keys.add(key)

    return final_url, body


def _enqueue_dynamic_high_value_paths(
    *,
    text: str,
    final_url: str,
    base_host: str,
    origin_base: str,
    base_host_lock: str,
    state: _CrawlState,
) -> None:
    for m in _HREF_RE.finditer(text):
        href = (m.group(1) or "").strip()
        if not href or href.startswith(("mailto:", "tel:", "javascript:", "data:")):
            continue

        abs_url = urljoin(final_url, href)
        canon = _canonicalize_to_origin(
            abs_url,
            origin_base=origin_base,
            base_host=base_host_lock,
        )
        if not canon:
            continue

        pth = urlparse(canon).path or "/"
        if _looks_like_soft_404_path(pth) or _SKIP_EXT_RE.search(pth):
            continue

        if not _is_high_value_path(pth):
            continue

        k_hv = _url_key(canon, origin_base=origin_base, base_host=base_host_lock)
        if not k_hv:
            continue
        if k_hv in state.seen_request_keys or k_hv in state.seen_final_keys:
            continue

        hv_path = urlparse(k_hv).path or "/"
        if not is_allowed(base_host, hv_path):
            continue

        state.seed_q.append((k_hv, 0))
        log.debug("Dynamic high-value path found: %s (from %s)", hv_path, final_url)


def _extract_and_enqueue_links(
    *,
    body: bytes,
    final_url: str,
    depth: int,
    max_depth: int,
    origin_base: str,
    base_host: str,
    hints: list[str],
    state: _CrawlState,
    enable_dynamic_hv: bool,
) -> None:
    if depth >= max_depth:
        return

    text = body.decode("utf-8", "ignore")

    if enable_dynamic_hv:
        _enqueue_dynamic_high_value_paths(
            text=text,
            final_url=final_url,
            base_host=base_host,
            origin_base=origin_base,
            base_host_lock=base_host,
            state=state,
        )

    for m in re.finditer(r'href\s*=\s*[\'"]([^\'"#?]+)', text, re.I):
        href = m.group(1).strip()
        if href.startswith(("mailto:", "tel:", "javascript:", "data:")):
            continue

        abs_url = urljoin(final_url, href)
        canon = _canonicalize_to_origin(
            abs_url,
            origin_base=origin_base,
            base_host=base_host,
        )
        if not canon:
            continue

        pth = urlparse(canon).path or "/"
        if _looks_like_soft_404_path(pth):
            continue
        if _SKIP_EXT_RE.search(pth):
            continue
        if _should_skip_pagination(pth):
            continue
        if _should_skip_taxonomy(pth):
            continue

        if not _matches_follow_hints(pth, hints):
            continue

        k2 = _url_key(canon, origin_base=origin_base, base_host=base_host)
        if not k2:
            continue
        if k2 in state.seen_request_keys or k2 in state.seen_final_keys:
            continue

        state.crawl_q.append((k2, depth + 1))


def _should_continue_seeding(
    *,
    sparse_fallback_active: bool,
    pages_count: int,
    seed_people_found: int,
    stop_min_people: int,
) -> bool:
    if sparse_fallback_active:
        return pages_count < _CRAWL_SPARSE_FALLBACK_MIN_PAGES
    return seed_people_found < stop_min_people


def _select_next_url(state: _CrawlState) -> tuple[str, int, bool]:
    from_seed = bool(state.seed_q)
    if state.seed_q:
        url, depth = state.seed_q.popleft()
        return url, depth, True
    url, depth = state.crawl_q.popleft()
    return url, depth, from_seed


def _attach_robots_deny_all_metrics(
    *,
    dom: str,
    origin_base: str,
    base_host: str,
    stop_min_people: int,
    start_monotonic: float,
    result: Any,
) -> None:
    if result is None:
        return

    payload = {
        "pages_crawled": 0,
        "urls_attempted": 0,
        "seeds_attempted": 0,
        "seed_people_pages": 0,
        "seed_stop_min_people_pages": stop_min_people,
        "seed_tiers_enqueued": 0,
        "origin": origin_base.rstrip("/"),
        "canonical_host": base_host,
        "robots_deny_all": True,
        "aborted": False,
        "abort_reason": "",
        "abort_stage": "",
        "waf_detected": False,
        "waf_first_n": _CRAWL_WAF_ABORT_FIRST_N,
        "http_403_ceiling": _CRAWL_403_CEILING,
        "time_budget_no_pages_s": _CRAWL_TIME_BUDGET_NO_PAGES_S,
        "time_budget_total_s": _CRAWL_TIME_BUDGET_TOTAL_S,
        "nav_expansion_enabled": bool(_CRAWL_NAV_EXPANSION_ENABLED),
        "nav_expanded": False,
        "nav_enqueued": 0,
        "elapsed_seconds": round(_elapsed_s(start_monotonic), 3),
    }

    if hasattr(result, "add_crawl_stats"):
        try:
            result.add_crawl_stats(payload)
        except Exception:  # pragma: no cover
            pass
        return

    if hasattr(result, "metrics") and isinstance(result.metrics, dict):
        try:
            result.metrics.update({"crawl": payload})
        except Exception:  # pragma: no cover
            pass


def _final_log(
    *,
    dom: str,
    origin_base: str,
    state: _CrawlState,
    seed_people_found: int,
    seed_attempted_total: int,
    seed_tiers_enqueued: int,
    high_value_enqueued: int,
    sparse_fallback_active: bool,
    abort_reason: str,
    abort_stage: str,
    start_monotonic: float,
) -> None:
    log.info(
        "Crawl complete for %s: origin=%s pages=%d urls_attempted=%d seeds_attempted=%d "
        "seed_people_pages=%d tiers_enqueued=%d high_value_discovered=%d sparse_fallback=%s "
        "aborted=%s abort_reason=%s abort_stage=%s waf_first_n=%d waf_fetches=%d waf_403=%d "
        "nav_expanded=%s nav_enqueued=%d time_budget_no_pages_s=%.1f time_budget_total_s=%.1f "
        "elapsed_s=%.3f",
        dom,
        origin_base.rstrip("/"),
        len(state.pages),
        len(state.seen_request_keys),
        seed_attempted_total,
        seed_people_found,
        seed_tiers_enqueued,
        high_value_enqueued,
        str(bool(sparse_fallback_active)).lower(),
        str(bool(state.aborted)).lower(),
        abort_reason or "",
        abort_stage or "",
        _CRAWL_WAF_ABORT_FIRST_N,
        state.meaningful_fetches,
        state.meaningful_403,
        str(bool(state.nav_expanded)).lower(),
        state.nav_enqueued,
        _CRAWL_TIME_BUDGET_NO_PAGES_S,
        _CRAWL_TIME_BUDGET_TOTAL_S,
        _elapsed_s(start_monotonic),
    )


# ---------------------------------------------------------------------------
# Crawl orchestration helpers (to keep crawl_domain complexity low)
# ---------------------------------------------------------------------------


@dataclass
class _CrawlRun:
    dom: str
    origin_base: str
    base_host: str
    max_pages: int
    max_depth: int
    stop_min_people: int
    hints: list[str]
    timeout: Any
    start_monotonic: float

    discovered_paths: set[str] = field(default_factory=set)
    sparse_fallback_active: bool = False
    sparse_fallback_reason: str = ""

    seed_people_found: int = 0
    seed_people_seen: set[str] = field(default_factory=set)
    seed_attempted_total: int = 0
    seed_tiers_enqueued: int = 0
    high_value_enqueued: int = 0
    next_tier_idx: int = 0


def _sanitize_domain(domain: str) -> str:
    dom = (domain or "").strip()
    if dom.startswith(("http://", "https://")):
        return (urlparse(dom).netloc or dom).strip()
    return dom


def _build_hints() -> list[str]:
    return [h.strip().lower() for h in CRAWL_FOLLOW_KEYWORDS.split(",") if h.strip()]


def _maybe_do_discovery(
    client: Any,
    *,
    dom: str,
    origin_base: str,
    base_host: str,
    timeout: Any,
) -> tuple[set[str], bool, str]:
    if not CRAWL_SEEDS_LINKED_ONLY:
        return set(), False, ""

    discovered_paths = _build_discovered_paths(
        client,
        origin_base=origin_base,
        base_host=base_host,
        timeout=timeout,
    )
    discovered_paths.add("/")

    sparse_active, sparse_reason = _is_sparse_discovery(discovered_paths)
    if sparse_active:
        log.info(
            "Sparse discovery detected for %s: %s. Falling back to exhaustive seed probing "
            "(target=%d pages).",
            dom,
            sparse_reason,
            _CRAWL_SPARSE_FALLBACK_MIN_PAGES,
        )
    else:
        log.debug(
            "Discovery found %d paths for %s (threshold=%d, fallback not needed)",
            len(discovered_paths),
            dom,
            _CRAWL_SPARSE_DISCOVERY_THRESHOLD,
        )

    return discovered_paths, sparse_active, sparse_reason


def _enqueue_high_value_discovered(
    state: _CrawlState,
    *,
    dom: str,
    discovered_paths: set[str],
    origin_base: str,
    base_host: str,
) -> int:
    if not CRAWL_SEEDS_LINKED_ONLY or not discovered_paths:
        return 0

    high_value_discovered: set[str] = set()
    for dp in discovered_paths:
        if dp != "/" and _is_high_value_path(dp):
            high_value_discovered.add(dp)

    if high_value_discovered:
        log.debug(
            "Found %d high-value discovered paths for %s: %s",
            len(high_value_discovered),
            dom,
            sorted(high_value_discovered)[:10],
        )

    seen_hv: set[str] = set()
    enqueued = 0
    for hvp in sorted(high_value_discovered):
        if not is_allowed(base_host, hvp):
            continue
        u = urljoin(origin_base, _normalize_path(hvp))
        if u in seen_hv:
            continue
        seen_hv.add(u)
        state.seed_q.append((u, 0))
        enqueued += 1

    if enqueued:
        log.info(
            "Enqueued %d high-value discovered paths for %s (e.g., %s)",
            enqueued,
            dom,
            sorted(high_value_discovered)[0],
        )

    return enqueued


def _enqueue_first_seed_tier(
    state: _CrawlState,
    *,
    origin_base: str,
    discovered_paths: set[str],
    sparse_fallback_active: bool,
) -> tuple[int, int]:
    next_tier_idx = 0
    enq_idx = _enqueue_next_nonempty_tier(
        seed_q=state.seed_q,
        origin_base=origin_base,
        start_idx=next_tier_idx,
        discovered_paths=discovered_paths,
        sparse_fallback_active=sparse_fallback_active,
    )
    if enq_idx < len(CRAWL_SEED_TIERS):
        return 1, enq_idx + 1
    return 0, len(CRAWL_SEED_TIERS)


def _maybe_enqueue_more_seed_tiers(run: _CrawlRun, state: _CrawlState) -> None:
    if state.seed_q:
        return
    if run.next_tier_idx >= len(CRAWL_SEED_TIERS):
        return
    if not _should_continue_seeding(
        sparse_fallback_active=run.sparse_fallback_active,
        pages_count=len(state.pages),
        seed_people_found=run.seed_people_found,
        stop_min_people=run.stop_min_people,
    ):
        return

    enq_idx = _enqueue_next_nonempty_tier(
        seed_q=state.seed_q,
        origin_base=run.origin_base,
        start_idx=run.next_tier_idx,
        discovered_paths=run.discovered_paths,
        sparse_fallback_active=run.sparse_fallback_active,
    )
    if enq_idx < len(CRAWL_SEED_TIERS):
        run.seed_tiers_enqueued += 1
        run.next_tier_idx = enq_idx + 1
    else:
        run.next_tier_idx = len(CRAWL_SEED_TIERS)


def _prepare_next_request(run: _CrawlRun, state: _CrawlState) -> tuple[str, int, bool] | None:
    url, depth, from_seed = _select_next_url(state)

    key = _url_key(url, origin_base=run.origin_base, base_host=run.base_host)
    if not key:
        return None

    if key in state.seen_request_keys:
        return None
    state.seen_request_keys.add(key)

    if from_seed:
        run.seed_attempted_total += 1

    return key, depth, from_seed


def _pre_fetch_checks(
    run: _CrawlRun,
    state: _CrawlState,
    *,
    key: str,
    result: Any,
) -> bool:
    parsed = urlparse(key)
    host = (parsed.netloc or "").lower()
    path = parsed.path or "/"

    if not _hosts_match(run.base_host, host):
        return False
    if _should_skip_pagination(path):
        return False
    if not is_allowed(run.base_host, path):
        _log_robots_block(run.base_host, path, key, result=result)
        return False

    return True


def _update_seed_people_metrics(
    run: _CrawlRun,
    state: _CrawlState,
    *,
    from_seed: bool,
    final_url: str,
    body: bytes,
) -> None:
    if not from_seed:
        return
    if final_url in run.seed_people_seen:
        return
    if not _looks_like_people_page(final_url, body):
        return

    run.seed_people_seen.add(final_url)
    run.seed_people_found += 1

    if run.sparse_fallback_active:
        return
    if run.seed_people_found < run.stop_min_people:
        return

    state.seed_q.clear()
    run.next_tier_idx = len(CRAWL_SEED_TIERS)
    log.debug(
        "Seed stop threshold reached: people_pages=%d (min=%d).Halting further seed-tier attempts.",
        run.seed_people_found,
        run.stop_min_people,
    )


def _fetch_persist_extract(
    client: Any,
    run: _CrawlRun,
    state: _CrawlState,
    *,
    key: str,
    depth: int,
    from_seed: bool,
) -> bool:
    fetched = _fetch_html_page(
        client=client,
        key=key,
        origin_base=run.origin_base,
        base_host=run.base_host,
        state=state,
    )
    if fetched is None:
        return not state.aborted

    final_url, body = fetched
    state.pages.append(Page(url=final_url, html=body, fetched_at=time.time()))
    log.debug("Crawled page: %s (from %s)", final_url, key)

    _update_seed_people_metrics(
        run,
        state,
        from_seed=from_seed,
        final_url=final_url,
        body=body,
    )

    if _check_time_budget(state=state, start_monotonic=run.start_monotonic, stage="post_persist"):
        return False

    _extract_and_enqueue_links(
        body=body,
        final_url=final_url,
        depth=depth,
        max_depth=run.max_depth,
        origin_base=run.origin_base,
        base_host=run.base_host,
        hints=run.hints,
        state=state,
        enable_dynamic_hv=bool(CRAWL_SEEDS_LINKED_ONLY),
    )

    if _check_time_budget(state=state, start_monotonic=run.start_monotonic, stage="post_extract"):
        return False

    return True


def _maybe_expand_when_empty(
    client: Any,
    run: _CrawlRun,
    state: _CrawlState,
) -> bool:
    if state.seed_q or state.crawl_q or state.pages or state.aborted:
        return False

    return _maybe_nav_expand(
        client=client,
        state=state,
        stage="queues_empty",
        origin_base=run.origin_base,
        base_host=run.base_host,
        timeout=run.timeout,
        discovered_paths=run.discovered_paths,
        next_tier_idx=run.next_tier_idx,
    )


def _crawl_loop(client: Any, run: _CrawlRun, state: _CrawlState, *, result: Any) -> None:
    while (state.seed_q or state.crawl_q) and len(state.pages) < run.max_pages:
        if _check_time_budget(state=state, start_monotonic=run.start_monotonic, stage="loop_start"):
            break

        _maybe_enqueue_more_seed_tiers(run, state)

        step = _prepare_next_request(run, state)
        if step is None:
            continue
        key, depth, from_seed = step

        if not _pre_fetch_checks(run, state, key=key, result=result):
            continue

        if _check_time_budget(state=state, start_monotonic=run.start_monotonic, stage="pre_fetch"):
            break

        try:
            ok = _fetch_persist_extract(
                client,
                run,
                state,
                key=key,
                depth=depth,
                from_seed=from_seed,
            )
        except Exception as exc:
            log.debug("Error fetching %s: %s", key, exc)
            ok = True

        if not ok:
            break

        if _maybe_expand_when_empty(client, run, state):
            continue


def _init_run(
    client: Any,
    state: _CrawlState,
    *,
    dom: str,
    timeout: Any,
    stop_min_people: int,
    max_pages: int,
    max_depth: int,
    hints: list[str],
    start_monotonic: float,
    result: Any,
) -> _CrawlRun | None:
    origin_base, base_host = _resolve_origin(client, dom=dom, timeout=timeout)

    if _robots_deny_all(base_host):
        root_url = urljoin(origin_base, "/")
        if not is_allowed(base_host, "/"):
            _log_robots_block(base_host, "/", root_url, result=result)

        log.info("Crawl blocked by robots for %s: deny-all detected (no seeds attempted).", dom)
        _attach_robots_deny_all_metrics(
            dom=dom,
            origin_base=origin_base,
            base_host=base_host,
            stop_min_people=stop_min_people,
            start_monotonic=start_monotonic,
            result=result,
        )
        return None

    discovered_paths, sparse_active, sparse_reason = _maybe_do_discovery(
        client,
        dom=dom,
        origin_base=origin_base,
        base_host=base_host,
        timeout=timeout,
    )

    high_value_enqueued = _enqueue_high_value_discovered(
        state,
        dom=dom,
        discovered_paths=discovered_paths,
        origin_base=origin_base,
        base_host=base_host,
    )

    seed_tiers_enqueued, next_tier_idx = _enqueue_first_seed_tier(
        state,
        origin_base=origin_base,
        discovered_paths=discovered_paths,
        sparse_fallback_active=sparse_active,
    )

    return _CrawlRun(
        dom=dom,
        origin_base=origin_base,
        base_host=base_host,
        max_pages=max_pages,
        max_depth=max_depth,
        stop_min_people=stop_min_people,
        hints=hints,
        timeout=timeout,
        start_monotonic=start_monotonic,
        discovered_paths=discovered_paths,
        sparse_fallback_active=sparse_active,
        sparse_fallback_reason=sparse_reason,
        seed_tiers_enqueued=seed_tiers_enqueued,
        high_value_enqueued=high_value_enqueued,
        next_tier_idx=next_tier_idx,
    )


def _attach_result_metrics_from_run(run: _CrawlRun, state: _CrawlState, *, result: Any) -> None:
    if result is None:
        return

    waf_detected = bool(
        state.aborted
        and (state.abort_reason.startswith("waf_") or state.abort_reason == "http_403_ceiling")
    )
    payload: dict[str, Any] = {
        "pages_crawled": len(state.pages),
        "urls_attempted": len(state.seen_request_keys),
        "seeds_attempted": run.seed_attempted_total,
        "seed_people_pages": run.seed_people_found,
        "seed_stop_min_people_pages": run.stop_min_people,
        "seed_tiers_enqueued": run.seed_tiers_enqueued,
        "origin": run.origin_base.rstrip("/"),
        "canonical_host": run.base_host,
        "seeds_linked_only": bool(CRAWL_SEEDS_LINKED_ONLY),
        "discovery_paths": list(CRAWL_DISCOVERY_PATHS),
        "robots_deny_all": False,
        "sparse_fallback_triggered": bool(run.sparse_fallback_active),
        "sparse_fallback_reason": run.sparse_fallback_reason,
        "sparse_discovery_threshold": _CRAWL_SPARSE_DISCOVERY_THRESHOLD,
        "sparse_fallback_min_pages": _CRAWL_SPARSE_FALLBACK_MIN_PAGES,
        "high_value_paths_enqueued": run.high_value_enqueued,
        "aborted": bool(state.aborted),
        "abort_reason": state.abort_reason,
        "abort_stage": state.abort_stage,
        "waf_detected": waf_detected,
        "waf_first_n": _CRAWL_WAF_ABORT_FIRST_N,
        "http_403_ceiling": _CRAWL_403_CEILING,
        "waf_fetches": state.meaningful_fetches,
        "waf_403": state.meaningful_403,
        "time_budget_no_pages_s": _CRAWL_TIME_BUDGET_NO_PAGES_S,
        "time_budget_total_s": _CRAWL_TIME_BUDGET_TOTAL_S,
        "pagination_guardrails": True,
        "nav_expansion_enabled": bool(_CRAWL_NAV_EXPANSION_ENABLED),
        "nav_expanded": bool(state.nav_expanded),
        "nav_enqueued": int(state.nav_enqueued),
        "elapsed_seconds": round(_elapsed_s(run.start_monotonic), 3),
    }
    if CRAWL_SEEDS_LINKED_ONLY:
        payload["discovered_paths_count"] = len(run.discovered_paths)

    if hasattr(result, "add_crawl_stats"):
        try:
            result.add_crawl_stats(payload)
        except Exception:  # pragma: no cover
            pass
    elif hasattr(result, "metrics") and isinstance(result.metrics, dict):
        try:
            result.metrics.update({"crawl": payload})
        except Exception:  # pragma: no cover
            pass


# ---------------------------------------------------------------------------
# Main crawl function
# ---------------------------------------------------------------------------


def crawl_domain(domain: str, *, result: Any = None) -> list[Page]:
    """
    BFS crawl from seed paths, respecting robots.txt.

    See module docstring for behavior details.
    """
    import httpx

    dom = _sanitize_domain(domain)
    max_pages = CRAWL_MAX_PAGES_PER_DOMAIN
    max_depth = CRAWL_MAX_DEPTH
    stop_min_people = max(1, int(CRAWL_SEED_STOP_MIN_PEOPLE_PAGES))
    hints = _build_hints()

    state = _CrawlState()

    headers = {"User-Agent": FETCH_USER_AGENT}
    timeout = httpx.Timeout(CRAWL_READ_TIMEOUT_S, connect=CRAWL_CONNECT_TIMEOUT_S)
    start_monotonic = time.monotonic()

    with httpx.Client(follow_redirects=True, headers=headers, timeout=timeout) as client:
        run = _init_run(
            client,
            state,
            dom=dom,
            timeout=timeout,
            stop_min_people=stop_min_people,
            max_pages=max_pages,
            max_depth=max_depth,
            hints=hints,
            start_monotonic=start_monotonic,
            result=result,
        )
        if run is None:
            return []

        _crawl_loop(client, run, state, result=result)

    _final_log(
        dom=run.dom,
        origin_base=run.origin_base,
        state=state,
        seed_people_found=run.seed_people_found,
        seed_attempted_total=run.seed_attempted_total,
        seed_tiers_enqueued=run.seed_tiers_enqueued,
        high_value_enqueued=run.high_value_enqueued,
        sparse_fallback_active=run.sparse_fallback_active,
        abort_reason=state.abort_reason,
        abort_stage=state.abort_stage,
        start_monotonic=run.start_monotonic,
    )

    _attach_result_metrics_from_run(run, state, result=result)
    return state.pages


def crawl_domain_for_company(domain: str, company_id: int, *, result: Any = None) -> list[Page]:
    """
    Convenience helper: crawl a domain and tag each Page with a company_id.

    This does not touch the database; it simply runs the standard crawl
    and annotates the resulting Page objects.
    """
    pages = crawl_domain(domain, result=result)
    for p in pages:
        p.company_id = company_id
    return pages


def crawl_domain_for_company(domain: str, company_id: int) -> list[Page]:
    """
    Convenience helper: crawl a domain and tag each Page with a company_id.

    This does not touch the database; it simply runs the standard R10 crawl
    and annotates the resulting Page objects. Callers can then pass the pages
    to src.db_pages.save_pages(..., default_company_id=company_id) or rely
    on save_pages() to read the Page.company_id attribute directly.
    """
    pages = crawl_domain(domain)
    for p in pages:
        p.company_id = company_id
    return pages
