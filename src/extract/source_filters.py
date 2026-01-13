# src/extract/source_filters.py
"""
Source URL Filtering + Page Classification

Primary purpose:
  - Block extraction from non-employee pages (customer stories, case studies, etc.)
  - Provide positive signals for employee/leadership pages
  - Provide a lightweight page classifier (URL + optional HTML heuristics) that
    downstream extractors (e.g., people_cards) can use to decide whether to run.

Why this exists:
  - Many sites mention real people on non-employee pages (customers, partners,
    webinars, podcasts, etc.). Those names are not employees and pollute output.
  - URL-only filters are useful but insufficient; a cheap HTML-aware classifier
    materially reduces false positives (e.g., newsroom pagination, blog posts).

Design:
  - "Blocked" rules are high-confidence, but allow strong overrides for clearly
    employee-centric paths (e.g., /about/leadership even if under /blog/).
  - is_employee_page_url() is URL-only and conservative.
  - classify_page_for_people_extraction() is the intended API for gating heavy
    extractors: it combines URL and optional HTML signals into a score.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from urllib.parse import urlparse

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_path(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).path or "").lower()
    except Exception:
        return ""


def _candidate_source_url(candidate: object) -> str:
    """
    Normalize how we fetch a candidate's origin URL across refactors.
    Common attribute names observed:
      - source_url
      - page_url
      - url
    """
    for attr in ("source_url", "page_url", "url"):
        v = getattr(candidate, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _candidate_name(candidate: object) -> str:
    for attr in ("raw_name", "full_name", "name"):
        v = getattr(candidate, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return "unknown"


def _candidate_title(candidate: object) -> str:
    for attr in ("title", "raw_title"):
        v = getattr(candidate, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


# ---------------------------------------------------------------------------
# URL patterns that should NEVER yield employee candidates (high confidence)
# ---------------------------------------------------------------------------

# NOTE: Prefer segment-ish patterns to avoid accidental substring matches.
_BLOCKED_URL_PATTERNS: list[tuple[str, str]] = [
    # Customer content - HIGH CONFIDENCE
    ("customer_story", r"/customer[-_]stor(?:y|ies)(?:/|$)"),
    ("case_study", r"/case[-_]stud(?:y|ies)(?:/|$)"),
    ("success_story", r"/success[-_]stor(?:y|ies)(?:/|$)"),
    ("testimonial", r"/testimonial(?:s)?(?:/|$)"),
    ("reviews_detail", r"/review(?:s)?/[^/]+"),  # allow /reviews index, block detail
    ("client_story", r"/client[-_]stor(?:y|ies)(?:/|$)"),
    ("customer_spotlight", r"/customer[-_](?:spotlight|success)(?:/|$)"),
    # Generic "customers" and "partners" detail pages
    ("customers_detail", r"/customers/[^/]+"),
    ("clients_detail", r"/clients/[^/]+"),
    ("partners_detail", r"/partner(?:s)?/[^/]+"),
    ("integrations_detail", r"/integration(?:s)?/[^/]+"),
    ("marketplace_detail", r"/marketplace/[^/]+"),
    ("app_directory", r"/app[-_]directory(?:/|$)"),
    # Careers (job listings, not leadership)
    ("careers_detail", r"/career(?:s)?/[^/]+"),
    ("jobs_detail", r"/job(?:s)?/[^/]+"),
    ("openings", r"/opening(?:s)?(?:/|$)"),
    ("positions", r"/position(?:s)?(?:/|$)"),
    ("work_with_us", r"/work[-_]with[-_]us(?:/|$)"),
    ("join_us", r"/join[-_]us(?:/|$)"),
    ("hiring", r"/hiring(?:/|$)"),
    # Legal/support
    ("legal", r"/legal(?:/|$)"),
    ("terms", r"/terms(?:/|$)"),
    ("privacy", r"/privacy(?:/|$)"),
    ("support", r"/support(?:/|$)"),
    ("help", r"/help(?:/|$)"),
    ("faq", r"/faq(?:/|$)"),
    ("documentation", r"/documentation(?:/|$)"),
    ("docs", r"/docs(?:/|$)"),
    # Events (speakers may not be employees)
    ("events_detail", r"/event(?:s)?/[^/]+"),
    ("webinars_detail", r"/webinar(?:s)?/[^/]+"),
    ("conferences", r"/conference(?:s)?(?:/|$)"),
    ("workshops", r"/workshop(?:s)?(?:/|$)"),
    # Podcast/media (guests are often not employees)
    ("podcasts_detail", r"/podcast(?:s)?/[^/]+"),
    ("episodes_detail", r"/episode(?:s)?/[^/]+"),
]

_BLOCKED_RULES: list[tuple[str, re.Pattern[str]]] = [
    (name, re.compile(pat, re.IGNORECASE)) for name, pat in _BLOCKED_URL_PATTERNS
]

# Blog post blocking: block *likely individual posts* (but allow some leadership/press exceptions)
_BLOG_POST_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("blog_date_post", re.compile(r"/blog/\d{4}(?:/|$)", re.IGNORECASE)),  # /blog/2024/...
    ("blog_deep_post", re.compile(r"/blog/[^/]+/[^/]+/[^/]+", re.IGNORECASE)),  # deep paths
]

# If a blog URL looks like a leadership/press announcement, allow it.
_BLOG_ALLOW_KEYWORDS_RE = re.compile(
    r"\b("
    r"appoint|appointed|appointment|joins?|welcomes?|announc(?:e|es|ed|ing)|"
    r"leadership|executive|exec|board|director|ceo|cto|cfo|coo|cmo|cpo|cro|"
    r"founder|co[- ]?founder|president"
    r")\b",
    re.IGNORECASE,
)

# Strong allow overrides (even if otherwise “blocked-ish” like under /blog/)
_STRONG_EMPLOYEE_PATH_RE = re.compile(
    r"/("
    r"team|teams|leadership|executive(?:s)?|management|people|staff|"
    r"board|directors?|governance|advisors?|bios?|who[-_]?we[-_]?are|our[-_]?team"
    r")(?:/|$)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# URL patterns that ARE good sources for employees (positive signals)
# ---------------------------------------------------------------------------

_ALLOWED_URL_PATTERNS: list[tuple[str, str]] = [
    # Standard about/people pages
    ("about", r"/about(?:/|$)"),
    ("about_us", r"/about[-_]us(?:/|$)"),
    ("company_about", r"/company/(?:about|team|leadership)(?:/|$)"),
    ("team", r"/team(?:/|$)"),
    ("teams", r"/teams(?:/|$)"),
    ("leadership", r"/leadership(?:/|$)"),
    ("people", r"/people(?:/|$)"),
    ("staff", r"/staff(?:/|$)"),
    ("executive", r"/executive(?:s)?(?:/|$)"),
    ("management", r"/management(?:/|$)"),
    ("founders", r"/founder(?:s)?(?:/|$)"),
    ("board", r"/board(?:/|$)"),
    ("directors", r"/director(?:s)?(?:/|$)"),
    ("who_we_are", r"/who[-_]?we[-_]?are(?:/|$)"),
    ("our_team", r"/our[-_]?team(?:/|$)"),
    ("our_people", r"/our[-_]?people(?:/|$)"),
    ("our_story", r"/our[-_]?story(?:/|$)"),
    # Press/news (often includes leadership announcements) — keep conservative
    ("press_room", r"/press[-_]?room(?:/|$)"),
    ("newsroom", r"/newsroom(?:/|$)"),
    ("press_index", r"/press(?:/|$)"),
    ("news_index", r"/news(?:/|$)"),
    ("press_release", r"/press[-_]?release(?:/|$)"),
    # Security/compliance pages (may list leadership)
    ("security", r"/security(?:/|$)"),
    ("trust", r"/trust(?:/|$)"),
    ("compliance", r"/compliance(?:/|$)"),
    # Investor relations (lists leadership)
    ("investor", r"/investor(?:s)?(?:/|$)"),
    ("ir", r"/ir(?:/|$)"),
    # Blog “meta” pages that sometimes list leadership
    ("blog_author", r"/blog/author/[^/]+(?:/|$)"),
    ("blog_category_about", r"/blog/category/(?:about|company)(?:/|$)"),
    ("blog_tag_about", r"/blog/tag/(?:about|team|leadership|company)(?:/|$)"),
]

_ALLOWED_RULES: list[tuple[str, re.Pattern[str]]] = [
    (name, re.compile(pat, re.IGNORECASE)) for name, pat in _ALLOWED_URL_PATTERNS
]


# ---------------------------------------------------------------------------
# Lightweight HTML signals for a "people page" classifier
# ---------------------------------------------------------------------------

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
_H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>", re.IGNORECASE | re.DOTALL)

_STRONG_PEOPLE_PHRASES_RE = re.compile(
    r"\b("
    r"leadership|leadership\s+team|executive\s+team|executive\s+leadership|"
    r"management\s+team|board\s+of\s+directors|advisory\s+board|"
    r"our\s+team|meet\s+(the\s+)?team|team\s+members|company\s+leadership|"
    r"meet\s+our\s+leaders|leadership\s+profiles|executive\s+profiles"
    r")\b",
    re.IGNORECASE,
)

_NEGATIVE_PAGE_PHRASES_RE = re.compile(
    r"\b("
    r"case\s+study|customer\s+story|customer\s+stories|testimonial|reviews?|"
    r"webinar|event|podcast|episode|partner|integration|marketplace"
    r")\b",
    re.IGNORECASE,
)

# Very cheap JSON-LD person indicators (not a full parser)
_JSONLD_PERSON_RE = re.compile(r'"@type"\s*:\s*"Person"', re.IGNORECASE)
_JSONLD_EMPLOYEE_RE = re.compile(r'"@type"\s*:\s*"(?:Employee|Staff)"', re.IGNORECASE)

# Common DOM markers seen on team pages
_TEAM_MARKER_RE = re.compile(
    r"\b("
    r"team[-_ ]member|leadership[-_ ]team|executive[-_ ]team|board[-_ ]member|"
    r"bio[-_ ]card|person[-_ ]card|profile[-_ ]card|staff[-_ ]directory"
    r")\b",
    re.IGNORECASE,
)


def _html_text_snippet(html: str | bytes | None, *, max_bytes: int = 200_000) -> str:
    if html is None:
        return ""
    if isinstance(html, bytes):
        b = html[:max_bytes]
        try:
            return b.decode("utf-8", "ignore")
        except Exception:
            return ""
    # If it's already a string, truncate to avoid pathological cost.
    return html[:max_bytes]


def _strip_tags(s: str) -> str:
    # Minimal and safe enough for headings/titles.
    s2 = re.sub(r"<[^>]+>", " ", s or "")
    s2 = re.sub(r"\s+", " ", s2)
    return s2.strip()


def _is_blog_post_path(path: str) -> bool:
    if "/blog" not in path:
        return False
    return any(r.search(path) for _, r in _BLOG_POST_RULES)


def _blog_post_has_allow_keywords(path: str) -> bool:
    return _BLOG_ALLOW_KEYWORDS_RE.search(path) is not None


def _apply_blog_penalty(*, path: str, reasons: list[str]) -> int:
    if "/blog" not in path:
        return 0
    if _STRONG_EMPLOYEE_PATH_RE.search(path):
        return 0
    if not _is_blog_post_path(path):
        return 0
    if _blog_post_has_allow_keywords(path):
        return 0
    reasons.append("blog_post_penalty")
    return -6


def _extract_heading_blob(text: str) -> str:
    head_title = ""
    m = _TITLE_RE.search(text[:100_000])
    if m:
        head_title = _strip_tags(m.group(1))

    h1 = ""
    m = _H1_RE.search(text[:150_000])
    if m:
        h1 = _strip_tags(m.group(1))

    h2_hits: list[str] = []
    for m in _H2_RE.finditer(text[:200_000]):
        h2_hits.append(_strip_tags(m.group(1)))
        if len(h2_hits) >= 3:
            break
    h2_joined = " | ".join([h for h in h2_hits if h])

    return " ".join([head_title, h1, h2_joined]).strip()


def _score_html_signals(text: str, reasons: list[str]) -> int:
    score = 0

    heading_blob = _extract_heading_blob(text)
    if heading_blob and _STRONG_PEOPLE_PHRASES_RE.search(heading_blob):
        score += 8
        reasons.append("html_people_phrases")

    if heading_blob and _NEGATIVE_PAGE_PHRASES_RE.search(heading_blob):
        score -= 8
        reasons.append("html_negative_phrases")

    if _JSONLD_PERSON_RE.search(text) or _JSONLD_EMPLOYEE_RE.search(text):
        score += 6
        reasons.append("jsonld_person")

    if _TEAM_MARKER_RE.search(text):
        score += 4
        reasons.append("team_dom_markers")

    if _NEGATIVE_PAGE_PHRASES_RE.search(text) and "html_negative_phrases" not in reasons:
        score -= 4
        reasons.append("body_negative_phrases")

    return score


def _score_url_signals(url: str, path: str, reasons: list[str]) -> tuple[int, bool]:
    score = 0
    blocked = False

    is_blocked, block_reason = is_blocked_source_url(url)
    if is_blocked:
        blocked = True
        score -= 12
        reasons.append(block_reason or "blocked_url")

    if _STRONG_EMPLOYEE_PATH_RE.search(path):
        score += 4
        reasons.append("strong_employee_path")

    if is_employee_page_url(url):
        score += 8
        reasons.append("allowed_employee_url")

    score += _apply_blog_penalty(path=path, reasons=reasons)
    return score, blocked


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_blocked_source_url(url: str) -> tuple[bool, str | None]:
    """
    Check if a URL should be blocked from candidate extraction.

    Returns:
        (is_blocked, reason)
    """
    if not url:
        return False, None

    path = _safe_path(url)
    if not path:
        return False, None

    # Strong employee paths override blog-post blocking (and some “generic blocks”).
    if _STRONG_EMPLOYEE_PATH_RE.search(path):
        return False, None

    # Blog post logic: block likely post pages unless keyword exceptions apply.
    if "/blog" in path:
        if any(r.search(path) for _, r in _BLOG_POST_RULES):
            if _BLOG_ALLOW_KEYWORDS_RE.search(path):
                return False, None
            return True, "blocked_blog_post"

    # Hard blocked rules.
    for name, rule in _BLOCKED_RULES:
        m = rule.search(path)
        if m:
            return True, f"blocked_pattern:{name}"

    return False, None


def is_employee_page_url(url: str) -> bool:
    """
    URL-only check for likely employee/leadership pages.

    Conservative:
      - Returns False if the URL is blocked
      - Returns True if it matches allowed patterns
      - Otherwise False
    """
    if not url:
        return False

    path = _safe_path(url)
    if not path:
        return False

    blocked, _ = is_blocked_source_url(url)
    if blocked:
        return False

    for _, rule in _ALLOWED_RULES:
        if rule.search(path):
            return True

    return False


@dataclass(frozen=True)
class PageClassification:
    """
    Score-based decision for whether a page is a good candidate for people extraction.

    Intended usage:
      - Gate heavy extractors (people_cards) to avoid newsroom/blog/marketing noise.
    """

    ok: bool
    score: int
    reasons: tuple[str, ...]


def classify_page_for_people_extraction(
    url: str,
    html: str | bytes | None = None,
    *,
    min_score: int = 8,
) -> PageClassification:
    """
    Classify whether a page should run "people_cards" style extraction.

    Inputs:
      - url: required (used for block/allow signals)
      - html: optional; if present, adds strong disambiguation
      - min_score: threshold for ok=True

    Scoring model (simple, explainable):
      URL signals:
        +8  allowed employee URL pattern match
        +4  strong employee segment match (team/leadership/etc.)
        -12 blocked source URL (hard)
        -6  blog post (non-exception) or other “likely non-employee” sources
      HTML signals (optional):
        +8  strong people phrases in title/h1/h2
        +6  JSON-LD Person/Employee
        +4  common team/people DOM markers
        -8  negative phrases (case study, customer story, webinar, partner...)

    Returns:
      PageClassification(ok, score, reasons)
    """
    if not url:
        return PageClassification(ok=False, score=0, reasons=("missing_url",))

    reasons: list[str] = []
    score = 0

    path = _safe_path(url)

    url_score, blocked = _score_url_signals(url, path, reasons)
    score += url_score

    text = _html_text_snippet(html)
    if text:
        score += _score_html_signals(text, reasons)

    ok = score >= min_score and not blocked
    reasons.append("ok" if ok else "not_ok")

    return PageClassification(ok=ok, score=score, reasons=tuple(reasons))


def filter_candidates_by_source(
    candidates: list,
    strict: bool = True,
) -> list:
    """
    Filter a list of candidates, removing those from blocked sources.

    Args:
        candidates: List of Candidate-like objects with source_url/page_url/url
        strict: If True, only keep candidates from explicitly allowed URLs
                If False, only block explicitly blocked URLs

    Returns:
        Filtered list of candidates
    """
    filtered = []
    blocked_count = 0

    for candidate in candidates:
        source_url = _candidate_source_url(candidate)

        # Check if blocked
        is_blocked, reason = is_blocked_source_url(source_url)
        if is_blocked:
            log.info(
                "Filtering candidate '%s' from blocked source: %s (%s)",
                _candidate_name(candidate),
                source_url,
                reason,
            )
            blocked_count += 1
            continue

        # In strict mode, also require it to be an allowed URL
        if strict and source_url and not is_employee_page_url(source_url):
            log.debug(
                "Filtering candidate '%s' - source not in allowed patterns: %s",
                _candidate_name(candidate),
                source_url,
            )
            blocked_count += 1
            continue

        filtered.append(candidate)

    if blocked_count > 0:
        log.info(
            "Source filter: %d candidates blocked, %d passed",
            blocked_count,
            len(filtered),
        )

    return filtered


# ---------------------------------------------------------------------------
# Blog author detection
# ---------------------------------------------------------------------------


def is_blog_author_candidate(candidate: object, url: str) -> bool:
    """
    Detect if a candidate appears to be a blog author rather than leadership.

    Blog authors are often:
    - Content marketers
    - Guest contributors
    - Not in leadership positions

    This is a heuristic: if a candidate is sourced from /blog/* and does not have
    a leadership title, treat them as a likely blog-author-only mention.
    """
    if not url:
        return False

    path = _safe_path(url)
    if "/blog" not in path:
        return False

    title = _candidate_title(candidate)
    title_lower = title.lower()

    # If they have a leadership title, they're probably legitimate.
    leadership_titles = [
        "ceo",
        "cto",
        "cfo",
        "coo",
        "cmo",
        "cpo",
        "cro",
        "chief",
        "president",
        "founder",
        "co-founder",
        "cofounder",
        "vp",
        "vice president",
        "svp",
        "evp",
        "director",
        "head of",
    ]
    if any(t in title_lower for t in leadership_titles):
        return False

    return True


__all__ = [
    "is_blocked_source_url",
    "is_employee_page_url",
    "classify_page_for_people_extraction",
    "PageClassification",
    "filter_candidates_by_source",
    "is_blog_author_candidate",
]
