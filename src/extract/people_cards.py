# src/extract/people_cards.py
"""
People Cards Extractor v2 - Enhanced extraction for modern HTML patterns.

Key improvements over v1:
  1. Better URL filtering (blocks thought-leadership, product pages with "team" in name)
  2. Multiple extraction strategies for different HTML structures
  3. Webflow-style plain text detection
  4. Image grid detection for headshots
  5. Repeated structure detection (sibling divs with similar content)

IMPORTANT (P1/P2 polish wiring):
  - This extractor is now gated by the central page classifier in
    src.extract.source_filters.classify_page_for_people_extraction().
  - If the classifier says the page is not a likely people/leadership page,
    this extractor returns [] early (prevents noise from newsroom/blog/etc).

Usage:
    from src.extract.people_cards import extract_people_cards

    candidates = extract_people_cards(
        html="<html>...",
        source_url="https://example.com/about",
        official_domain="example.com",
    )
"""

from __future__ import annotations

import inspect
import logging
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

try:
    from bs4 import BeautifulSoup, NavigableString, Tag

    _HAS_BS4 = True
except ImportError:
    _HAS_BS4 = False
    BeautifulSoup = None  # type: ignore
    Tag = None  # type: ignore
    NavigableString = None  # type: ignore

# Import quality gates for validation
try:
    from src.extract.quality_gates import (
        is_geography_term,
        is_nav_boilerplate,
        validate_person_name,
        validate_title,
    )

    _HAS_QUALITY_GATES = True
except ImportError:
    _HAS_QUALITY_GATES = False

# Central source/page filters (preferred)
try:
    from src.extract.source_filters import (
        classify_page_for_people_extraction,
        is_blog_author_candidate,
    )

    _HAS_SOURCE_FILTERS = True
except ImportError:
    _HAS_SOURCE_FILTERS = False
    classify_page_for_people_extraction = None  # type: ignore[assignment]
    is_blog_author_candidate = None  # type: ignore[assignment]

# Import Candidate type
try:
    from src.extract.candidates import Candidate

    _HAS_CANDIDATE = True
except ImportError:
    _HAS_CANDIDATE = False

    @dataclass
    class Candidate:  # type: ignore[no-redef]
        """Fallback Candidate for when src.extract.candidates is unavailable."""

        email: str | None
        source_url: str | None = None
        page_url: str | None = None
        first_name: str | None = None
        last_name: str | None = None
        raw_name: str | None = None
        title: str | None = None
        raw_title: str | None = None
        source_type: str | None = None
        context_snippet: str | None = None
        is_role_address_guess: bool = False


log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Local env tuning
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


# Minimum score to allow people_cards extraction (classifier threshold)
_PEOPLE_CARDS_CLASSIFY_MIN_SCORE = max(1, _env_int("PEOPLE_CARDS_CLASSIFY_MIN_SCORE", 8))


# ---------------------------------------------------------------------------
# Candidate factory (compat across Candidate schema variants)
# ---------------------------------------------------------------------------


def _candidate_field_set() -> set[str]:
    # Prefer dataclass fields when available
    try:
        fields = getattr(Candidate, "__dataclass_fields__", None)
        if isinstance(fields, dict):
            return set(fields.keys())
    except Exception:
        pass

    # Fallback to signature introspection (best-effort)
    try:
        sig = inspect.signature(Candidate)  # type: ignore[arg-type]
        return {
            p.name
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        }
    except Exception:
        return set()


_CANDIDATE_FIELDS = _candidate_field_set()


def _set_first_present(kwargs: dict[str, Any], options: list[str], value: Any) -> None:
    for field in options:
        if field in _CANDIDATE_FIELDS:
            kwargs[field] = value
            return


def _set_if_present(kwargs: dict[str, Any], field: str, value: Any) -> None:
    if field in _CANDIDATE_FIELDS:
        kwargs[field] = value


def _make_candidate(
    *,
    source_url: str,
    full_name: str,
    first_name: str | None,
    last_name: str | None,
    title: str | None,
    source_type: str,
    context_snippet: str | None,
) -> Candidate:
    """
    Create a Candidate instance while tolerating schema differences across refactors.

    Known field name variations:
      - page_url vs source_url
      - title vs raw_title
    """
    kwargs: dict[str, Any] = {}

    _set_if_present(kwargs, "email", None)
    _set_first_present(kwargs, ["page_url", "source_url", "url"], source_url)
    _set_first_present(kwargs, ["raw_name", "full_name", "name"], full_name)
    _set_if_present(kwargs, "first_name", first_name)
    _set_if_present(kwargs, "last_name", last_name)
    _set_first_present(kwargs, ["title", "raw_title"], title)
    _set_if_present(kwargs, "source_type", source_type)
    _set_if_present(kwargs, "context_snippet", context_snippet)
    _set_if_present(kwargs, "is_role_address_guess", False)

    try:
        return Candidate(**kwargs)  # type: ignore[arg-type]
    except TypeError:
        # Last-resort minimal constructor attempts (keeps pipeline alive)
        try:
            return Candidate(  # type: ignore[call-arg]
                email=None,
                source_url=source_url,
                first_name=first_name,
                last_name=last_name,
                raw_name=full_name,
                title=title,
                source_type=source_type,
                context_snippet=context_snippet,
                is_role_address_guess=False,
            )
        except Exception:
            return Candidate(email=None)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# URL Pattern Filtering - legacy fallback only
# ---------------------------------------------------------------------------

# URLs that should NEVER be processed for people extraction
# These contain "leadership" or "team" but are NOT team pages
_BLOCKED_URL_SUBSTRINGS = (
    # Content pages with "leadership" in URL
    "/thought-leadership",
    "/thought_leadership",
    "/leadership-insights",
    "/leadership-blog",
    "/leadership-articles",
    "/leadership-resources",
    # Product pages with "team" in URL
    "/teams-phone",
    "/teams-chat",
    "/teams-meeting",
    "/teams-integration",
    "/team-collaboration",
    "/team-management",
    "/team-productivity",
    "/my-team",
    "/your-team",
    # Blog/content URLs
    "/blog/",
    "/article/",
    "/articles/",
    "/post/",
    "/posts/",
    "/news/",
    "/press-release/",
    "/press-releases/",
    # Customer/case study URLs
    "/customer-stor",
    "/case-stud",
    "/success-stor",
    "/testimonial",
    "/client-stor",
    "/review/",
    "/reviews/",
    # Job/career URLs
    "/career",
    "/job",
    "/opening",
    "/position",
    "/work-with-us",
    "/join-us",
    "/hiring",
    # Legal/support URLs
    "/legal/",
    "/terms",
    "/privacy",
    "/support/",
    "/help/",
    "/faq",
    "/docs/",
    "/documentation/",
    # Product/feature URLs
    "/pricing",
    "/product/",
    "/products/",
    "/feature/",
    "/features/",
    "/solution/",
    "/solutions/",
    "/platform/",
    "/demo/",
    "/signup/",
    "/login/",
    # Event URLs
    "/event/",
    "/events/",
    "/webinar/",
    "/webinars/",
    "/conference/",
    "/workshop/",
    # Podcast URLs
    "/podcast/",
    "/podcasts/",
    "/episode/",
    "/episodes/",
)

# URLs that ARE good sources for people extraction
_ALLOWED_URL_SUBSTRINGS = (
    # About pages
    "/about",
    "/about-us",
    "/about_us",
    "/our-story",
    "/our_story",
    "/who-we-are",
    "/who_we_are",
    # Team pages
    "/team",
    "/our-team",
    "/our_team",
    "/the-team",
    "/meet-the-team",
    "/meet-our-team",
    # Leadership pages
    "/leadership",
    "/leaders",
    "/executives",
    "/executive-team",
    "/management",
    "/board",
    "/directors",
    # People pages
    "/people",
    "/staff",
    "/founders",
    "/partners",
    # Company pages
    "/company",
    "/company/about",
    # Press pages (often have leadership bios)
    "/press-room",
    "/pressroom",
    "/newsroom",
    "/news-room",
    "/media",
)


def _is_blocked_url(url: str) -> tuple[bool, str | None]:
    if not url:
        return False, None
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False, None
    for pattern in _BLOCKED_URL_SUBSTRINGS:
        if pattern in path:
            return True, f"blocked:{pattern}"
    return False, None


def _is_allowed_url(url: str) -> bool:
    if not url:
        return False
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(pattern in path for pattern in _ALLOWED_URL_SUBSTRINGS)


def _is_people_page_url(url: str) -> bool:
    """
    Legacy URL-only conservative filter, used only when source_filters is missing.
    """
    is_blocked, reason = _is_blocked_url(url)
    if is_blocked:
        log.debug("Blocking URL from extraction: %s (reason=%s)", url, reason)
        return False
    if _is_allowed_url(url):
        return True
    log.debug("URL not in allowed list: %s", url)
    return False


# ---------------------------------------------------------------------------
# Name Validation
# ---------------------------------------------------------------------------

_NON_PERSON_PHRASES = frozenset(
    [
        "learn more",
        "read more",
        "see more",
        "view all",
        "show more",
        "contact us",
        "get in touch",
        "reach out",
        "talk to us",
        "about us",
        "our team",
        "our leadership",
        "meet the team",
        "join us",
        "work with us",
        "careers",
        "open positions",
        "sign up",
        "sign in",
        "log in",
        "register",
        "subscribe",
        "get started",
        "start free",
        "try free",
        "book demo",
        "request demo",
        "schedule call",
        "contact sales",
        "privacy policy",
        "terms of service",
        "cookie policy",
        "all rights reserved",
        "copyright",
    ]
)

_LEADERSHIP_TITLE_PATTERNS = (
    "ceo",
    "cto",
    "cfo",
    "coo",
    "cmo",
    "cpo",
    "cro",
    "ciso",
    "cao",
    "cdo",
    "clo",
    "chief",
    "president",
    "founder",
    "co-founder",
    "cofounder",
    "vice president",
    "vp ",
    " vp",
    "svp",
    "evp",
    "avp",
    "director",
    "head of",
    "lead",
    "principal",
    "partner",
    "managing director",
    "general manager",
    "gm ",
    "executive",
    "officer",
    "chairman",
    "chairwoman",
    "chair ",
)

_JOB_TITLE_PATTERNS = (
    *_LEADERSHIP_TITLE_PATTERNS,
    "manager",
    "senior",
    "staff",
    "engineer",
    "developer",
    "analyst",
    "consultant",
    "specialist",
    "coordinator",
    "associate",
    "advisor",
    "counsel",
    "attorney",
)


def _looks_like_person_name(text: str) -> bool:
    if not text or len(text) < 3:
        return False
    text = text.strip()

    lower = text.lower()
    if lower in _NON_PERSON_PHRASES:
        return False
    for phrase in _NON_PERSON_PHRASES:
        if phrase in lower:
            return False

    words = text.split()
    if len(words) < 2 or len(words) > 5:
        return False

    for word in words:
        clean = word.replace("-", "").replace("'", "").replace(".", "")
        if not clean:
            continue
        if not clean[0].isupper():
            return False
        if not all(c.isalpha() for c in clean):
            return False

    if _HAS_QUALITY_GATES:
        try:
            result = validate_person_name(text)
            if not getattr(result, "is_valid", False):
                return False
        except Exception:
            # Fail open to avoid hard dependency loops; AI + later gates can clean up
            pass

    return True


def _looks_like_title(text: str) -> bool:
    if not text:
        return False
    text = text.strip()
    if len(text) > 100:
        return False

    if _HAS_QUALITY_GATES:
        try:
            result = validate_title(text)
            if not getattr(result, "is_valid", False):
                return False
            if is_geography_term(text):
                return False
            if is_nav_boilerplate(text):
                return False
        except Exception:
            pass

    lower = text.lower()
    if any(pattern in lower for pattern in _JOB_TITLE_PATTERNS):
        return True

    if len(text) <= 40:
        nav_words = {"home", "about", "contact", "services", "products", "blog", "news"}
        if lower.strip() in nav_words:
            return False
        return True

    return False


def _has_leadership_title(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(pattern in lower for pattern in _LEADERSHIP_TITLE_PATTERNS)


def _split_name(full_name: str) -> tuple[str | None, str | None]:
    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if len(parts) < 2:
        return full_name.strip(), None
    return parts[0], parts[-1]


# ---------------------------------------------------------------------------
# Extraction Strategy 1: LinkedIn Anchors
# ---------------------------------------------------------------------------


def _is_linkedin_url(href: str) -> bool:
    if not href:
        return False
    lower = href.lower()
    return "linkedin.com" in lower or "linkedin." in lower


def _extract_from_linkedin_anchors(
    soup: Any,
    source_url: str,
    section_root: Any = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_names: set[str] = set()
    search_root = section_root if section_root else soup

    for anchor in search_root.find_all("a", href=True):
        href = anchor.get("href", "")
        if not _is_linkedin_url(href):
            continue

        name_text = anchor.get_text(strip=True)
        if not name_text or not _looks_like_person_name(name_text):
            continue

        name_key = name_text.lower().strip()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        title = _find_adjacent_title(anchor)
        first_name, last_name = _split_name(name_text)

        candidates.append(
            _make_candidate(
                source_url=source_url,
                full_name=name_text,
                first_name=first_name,
                last_name=last_name,
                title=title,
                source_type="people_card_linkedin",
                context_snippet=f"{name_text} - {title}" if title else name_text,
            )
        )

        log.debug("Extracted from LinkedIn anchor: %s (title=%s)", name_text, title)

    return candidates


def _find_adjacent_title(element: Any) -> str | None:
    if not _HAS_BS4 or element is None:
        return None

    name_text = element.get_text(strip=True)

    container = None
    for tag_name in ["li", "article", "section", "div"]:
        parent = element.find_parent(tag_name)
        if parent:
            text_len = len(parent.get_text(strip=True))
            if text_len < 500:
                container = parent
                break

    if not container:
        container = element.parent
    if not container:
        return None

    strings = list(container.stripped_strings)

    name_idx = None
    for i, s in enumerate(strings):
        if s.strip() == name_text.strip():
            name_idx = i
            break

    if name_idx is None:
        return None

    if name_idx + 1 < len(strings):
        potential_title = strings[name_idx + 1].strip()
        if _looks_like_title(potential_title):
            return potential_title

    if name_idx > 0:
        potential_title = strings[name_idx - 1].strip()
        if _looks_like_title(potential_title):
            return potential_title

    return None


# ---------------------------------------------------------------------------
# Extraction Strategy 2: Card Structures with CSS Classes
# ---------------------------------------------------------------------------


def _extract_from_card_structures(
    soup: Any,
    source_url: str,
    section_root: Any = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_names: set[str] = set()
    search_root = section_root if section_root else soup

    card_class_patterns = [
        re.compile(r"person|team|member|leader|executive|founder", re.I),
        re.compile(r"bio|profile|staff|employee", re.I),
        re.compile(r"card|tile|item", re.I),
    ]

    for pattern in card_class_patterns:
        for card in search_root.find_all(
            ["div", "li", "article", "section"],
            class_=pattern,
        ):
            name_elem = card.find(["h2", "h3", "h4", "h5", "h6", "strong", "b"])
            if not name_elem:
                strings = list(card.stripped_strings)
                name_text = None
                for s in strings[:3]:
                    if _looks_like_person_name(s):
                        name_text = s
                        break
            else:
                name_text = name_elem.get_text(strip=True)

            if not name_text or not _looks_like_person_name(name_text):
                continue

            name_key = name_text.lower().strip()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)

            title = _find_title_in_card(card, name_text)
            first_name, last_name = _split_name(name_text)

            candidates.append(
                _make_candidate(
                    source_url=source_url,
                    full_name=name_text,
                    first_name=first_name,
                    last_name=last_name,
                    title=title,
                    source_type="people_card_structure",
                    context_snippet=f"{name_text} - {title}" if title else name_text,
                )
            )

            log.debug("Extracted from card structure: %s (title=%s)", name_text, title)

    return candidates


def _find_title_in_card(card: Any, name_text: str) -> str | None:
    for tag in ["p", "span", "div"]:
        title_elem = card.find(
            tag,
            class_=re.compile(r"title|role|position|job|designation", re.I),
        )
        if title_elem:
            title_text = title_elem.get_text(strip=True)
            if _looks_like_title(title_text):
                return title_text

    strings = list(card.stripped_strings)
    for i, s in enumerate(strings):
        if s.strip() == name_text.strip() and i + 1 < len(strings):
            potential = strings[i + 1].strip()
            if _looks_like_title(potential):
                return potential

    return None


# ---------------------------------------------------------------------------
# Extraction Strategy 3: Repeated Sibling Structures
# ---------------------------------------------------------------------------


def _extract_from_repeated_siblings(
    soup: Any,
    source_url: str,
    section_root: Any = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_names: set[str] = set()
    search_root = section_root if section_root else soup

    for container in search_root.find_all(["div", "ul", "section", "article"]):
        children = [
            c
            for c in container.children
            if hasattr(c, "name") and c.name in ("div", "li", "article")
        ]

        if len(children) < 3:
            continue

        child_structures = []
        for child in children:
            strings = list(child.stripped_strings)
            child_structures.append(len(strings))

        if not child_structures:
            continue

        avg_strings = sum(child_structures) / len(child_structures)
        if avg_strings < 1.5 or avg_strings > 6:
            continue

        for child in children:
            strings = list(child.stripped_strings)
            if len(strings) < 2:
                continue

            name_text = None
            title_text = None

            for i, s in enumerate(strings):
                if not name_text and _looks_like_person_name(s):
                    name_text = s
                    if i + 1 < len(strings) and _looks_like_title(strings[i + 1]):
                        title_text = strings[i + 1]
                    break

            if not name_text:
                continue

            name_key = name_text.lower().strip()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)

            first_name, last_name = _split_name(name_text)

            candidates.append(
                _make_candidate(
                    source_url=source_url,
                    full_name=name_text,
                    first_name=first_name,
                    last_name=last_name,
                    title=title_text,
                    source_type="people_card_siblings",
                    context_snippet=f"{name_text} - {title_text}" if title_text else name_text,
                )
            )

            log.debug("Extracted from sibling structure: %s (title=%s)", name_text, title_text)

    return candidates


# ---------------------------------------------------------------------------
# Extraction Strategy 4: Image + Text Patterns (Headshots)
# ---------------------------------------------------------------------------


_ALT_SPLITS = [",", " - ", " – ", " | "]
_ALT_EXCLUDE_WORDS = ("logo", "icon", "banner", "photo", "image", "headshot")


def _extract_name_title_from_alt(alt_text: str) -> tuple[str | None, str | None]:
    alt = (alt_text or "").strip()
    if not alt:
        return None, None

    for sep in _ALT_SPLITS:
        if sep not in alt:
            continue
        left, right = alt.split(sep, 1)
        potential_name = left.strip()
        potential_title = right.strip()
        if _looks_like_person_name(potential_name):
            return potential_name, potential_title if _looks_like_title(potential_title) else None

    if _looks_like_person_name(alt) and not any(x in alt.lower() for x in _ALT_EXCLUDE_WORDS):
        return alt, None

    return None, None


def _extract_name_title_from_parent(parent: Any) -> tuple[str | None, str | None]:
    if not parent:
        return None, None

    strings = list(parent.stripped_strings)
    for i, s in enumerate(strings):
        if _looks_like_person_name(s):
            title = None
            if i + 1 < len(strings) and _looks_like_title(strings[i + 1]):
                title = strings[i + 1]
            return s, title
    return None, None


def _extract_name_from_next_sibling(img: Any) -> str | None:
    if not img:
        return None
    next_sib = img.find_next_sibling()
    if not next_sib:
        return None

    sib_text = next_sib.get_text(strip=True) if hasattr(next_sib, "get_text") else str(next_sib)
    return sib_text if _looks_like_person_name(sib_text) else None


def _extract_from_image_text_patterns(
    soup: Any,
    source_url: str,
    section_root: Any = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_names: set[str] = set()
    search_root = section_root if section_root else soup

    for img in search_root.find_all("img"):
        alt_name, alt_title = _extract_name_title_from_alt(img.get("alt", ""))
        adj_name, adj_title = _extract_name_title_from_parent(getattr(img, "parent", None))

        sib_name = None
        if not adj_name:
            sib_name = _extract_name_from_next_sibling(img)

        name_text = alt_name or adj_name or sib_name
        title_text = alt_title or adj_title

        if not name_text:
            continue

        name_key = name_text.lower().strip()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        first_name, last_name = _split_name(name_text)

        candidates.append(
            _make_candidate(
                source_url=source_url,
                full_name=name_text,
                first_name=first_name,
                last_name=last_name,
                title=title_text,
                source_type="people_card_image",
                context_snippet=f"{name_text} - {title_text}" if title_text else name_text,
            )
        )

        log.debug("Extracted from image pattern: %s (title=%s)", name_text, title_text)

    return candidates


# ---------------------------------------------------------------------------
# Extraction Strategy 5: Heading-based extraction
# ---------------------------------------------------------------------------


def _extract_from_headings(
    soup: Any,
    source_url: str,
    section_root: Any = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_names: set[str] = set()
    search_root = section_root if section_root else soup

    for heading in search_root.find_all(["h2", "h3", "h4", "h5", "h6"]):
        name_text = heading.get_text(strip=True)
        if not _looks_like_person_name(name_text):
            continue

        name_key = name_text.lower().strip()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        title = None
        next_elem = heading.find_next_sibling()
        if next_elem:
            next_text = next_elem.get_text(strip=True)
            if _looks_like_title(next_text):
                title = next_text

        if not title:
            parent = heading.parent
            if parent:
                strings = list(parent.stripped_strings)
                for i, s in enumerate(strings):
                    if s.strip() == name_text.strip() and i + 1 < len(strings):
                        potential = strings[i + 1].strip()
                        if _looks_like_title(potential):
                            title = potential
                            break

        first_name, last_name = _split_name(name_text)

        candidates.append(
            _make_candidate(
                source_url=source_url,
                full_name=name_text,
                first_name=first_name,
                last_name=last_name,
                title=title,
                source_type="people_card_heading",
                context_snippet=f"{name_text} - {title}" if title else name_text,
            )
        )

        log.debug("Extracted from heading: %s (title=%s)", name_text, title)

    return candidates


# ---------------------------------------------------------------------------
# Extraction Strategy 6: List Items (ul/ol > li patterns)
# ---------------------------------------------------------------------------


def _extract_from_list_items(
    soup: Any,
    source_url: str,
    section_root: Any = None,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_names: set[str] = set()
    search_root = section_root if section_root else soup

    for li in search_root.find_all("li"):
        strings = list(li.stripped_strings)
        if len(strings) < 1:
            continue

        name_text = None
        title_text = None

        for i, s in enumerate(strings):
            if len(s) < 3:
                continue
            if not name_text and _looks_like_person_name(s):
                name_text = s
                if i + 1 < len(strings):
                    potential_title = strings[i + 1]
                    if _looks_like_title(potential_title):
                        title_text = potential_title
                break

        if name_text and not title_text:
            strong = li.find(["strong", "b", "em"])
            if strong:
                strong_text = strong.get_text(strip=True)
                if _looks_like_title(strong_text) and strong_text != name_text:
                    title_text = strong_text

        if not name_text:
            continue

        name_key = name_text.lower().strip()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        first_name, last_name = _split_name(name_text)

        candidates.append(
            _make_candidate(
                source_url=source_url,
                full_name=name_text,
                first_name=first_name,
                last_name=last_name,
                title=title_text,
                source_type="people_card_list_item",
                context_snippet=f"{name_text} - {title_text}" if title_text else name_text,
            )
        )

        log.debug("Extracted from list item: %s (title=%s)", name_text, title_text)

    return candidates


# ---------------------------------------------------------------------------
# Section Detection
# ---------------------------------------------------------------------------

_LEADERSHIP_SECTION_PATTERNS = [
    r"meet\s+(?:the\s+)?(?:leadership|team|people|founders?)",
    r"(?:our\s+)?leadership\s*(?:team)?",
    r"(?:our\s+)?executive\s*(?:team|leadership)?",
    r"(?:our\s+)?management\s*(?:team)?",
    r"(?:our\s+)?team",
    r"(?:the\s+)?people\s+behind",
    r"(?:our\s+)?founders?",
    r"board\s+of\s+directors",
    r"advisory\s+board",
    r"senior\s+leadership",
    r"c-suite",
    r"executives?",
    r"(?:the\s+)?leadership",
    r"(?:our\s+)?company\s+leadership",
    r"(?:meet\s+)?(?:our\s+)?leaders?",
    r"(?:the\s+)?team\s+behind",
    r"(?:our\s+)?executive\s+officers",
    r"key\s+people",
    r"(?:our\s+)?management",
    r"who\s+we\s+are",
    r"(?:the\s+)?people\s+of",
    r"about\s+us",
    r"the\s+company",
]

_LEADERSHIP_SECTION_RE = re.compile(
    r"^(?:" + "|".join(_LEADERSHIP_SECTION_PATTERNS) + r")$",
    re.IGNORECASE,
)


def _find_leadership_sections(soup: Any) -> list[Any]:
    sections = []
    seen_elements = set()

    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        text = heading.get_text(strip=True)
        if not text:
            continue

        if _LEADERSHIP_SECTION_RE.search(text):
            log.debug("Found leadership heading: %s", text)

            parent = heading.find_parent(["section", "div", "article"])
            if parent and id(parent) not in seen_elements:
                sections.append(parent)
                seen_elements.add(id(parent))

            current = heading
            for _ in range(5):
                next_sib = current.find_next_sibling(
                    ["section", "div", "article", "ul"],
                )
                if not next_sib:
                    break

                if id(next_sib) not in seen_elements:
                    children = list(next_sib.children)
                    element_children = [c for c in children if hasattr(c, "name") and c.name]
                    if len(element_children) >= 2:
                        sections.append(next_sib)
                        seen_elements.add(id(next_sib))

                current = next_sib

    return sections


# ---------------------------------------------------------------------------
# Main Extraction Function
# ---------------------------------------------------------------------------


def _candidate_name_key(c: Candidate) -> str:
    raw_name = (
        getattr(c, "raw_name", None)
        or getattr(c, "full_name", None)
        or getattr(c, "name", None)
        or ""
    )
    return (raw_name or "").lower().strip()


def _should_skip_blog_author(c: Candidate, source_url: str) -> bool:
    if not (_HAS_SOURCE_FILTERS and is_blog_author_candidate is not None):
        return False
    try:
        if not is_blog_author_candidate(c, source_url):
            return False
        title = getattr(c, "title", None) or getattr(c, "raw_title", None) or ""
        return not _has_leadership_title(str(title or ""))
    except Exception:
        return False


def _merge_candidates(
    *,
    dst: list[Candidate],
    seen_names: set[str],
    new_candidates: list[Candidate],
    source_url: str,
) -> None:
    for c in new_candidates:
        name_key = _candidate_name_key(c)
        if not name_key or name_key in seen_names:
            continue
        if _should_skip_blog_author(c, source_url):
            continue
        seen_names.add(name_key)
        dst.append(c)


def _classifier_allows_people_cards(html: str, source_url: str) -> bool:
    if _HAS_SOURCE_FILTERS and classify_page_for_people_extraction is not None:
        try:
            verdict = classify_page_for_people_extraction(
                source_url,
                html,
                min_score=_PEOPLE_CARDS_CLASSIFY_MIN_SCORE,
            )
            if not getattr(verdict, "ok", False):
                log.debug(
                    "Skipping people_cards (classifier not ok): url=%s score=%s reasons=%s",
                    source_url,
                    getattr(verdict, "score", None),
                    getattr(verdict, "reasons", None),
                )
                return False
            return True
        except Exception as exc:
            log.debug(
                "Classifier error; falling back to legacy URL gating: url=%s err=%s",
                source_url,
                exc,
            )

    if not _is_people_page_url(source_url):
        log.debug("Skipping non-people page URL (legacy gating): %s", source_url)
        return False

    return True


def _extract_in_scope(soup: Any, source_url: str, section: Any) -> list[list[Candidate]]:
    return [
        _extract_from_linkedin_anchors(soup, source_url, section),
        _extract_from_card_structures(soup, source_url, section),
        _extract_from_repeated_siblings(soup, source_url, section),
        _extract_from_image_text_patterns(soup, source_url, section),
        _extract_from_headings(soup, source_url, section),
        _extract_from_list_items(soup, source_url, section),
    ]


def _extract_whole_page_primary(soup: Any, source_url: str) -> list[list[Candidate]]:
    return [
        _extract_from_linkedin_anchors(soup, source_url, None),
        _extract_from_card_structures(soup, source_url, None),
        _extract_from_repeated_siblings(soup, source_url, None),
    ]


def _extract_whole_page_secondary(soup: Any, source_url: str) -> list[list[Candidate]]:
    return [
        _extract_from_image_text_patterns(soup, source_url, None),
        _extract_from_headings(soup, source_url, None),
        _extract_from_list_items(soup, source_url, None),
    ]


def extract_people_cards(
    html: str,
    source_url: str,
    official_domain: str | None = None,
) -> list[Candidate]:
    """
    Extract people/leadership candidates from HTML using multiple strategies.

    Gating behavior:
      - Preferred: classify_page_for_people_extraction(url, html) and return []
        early if not ok. This prevents extraction on newsroom/blog/etc.
      - Fallback: legacy URL allow/block list if source_filters is unavailable.

    Args:
        html: Raw HTML content
        source_url: URL the HTML was fetched from
        official_domain: The company's official domain (currently unused here)

    Returns:
        List of Candidate objects with email=None
    """
    if not _HAS_BS4:
        log.warning("BeautifulSoup not available; skipping people cards extraction")
        return []

    if not html or not source_url:
        return []

    if not _classifier_allows_people_cards(html, source_url):
        return []

    _ = official_domain  # intentionally unused (reserved for future heuristics)

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[Candidate] = []
    seen_names: set[str] = set()

    sections = _find_leadership_sections(soup)
    for section in sections:
        for batch in _extract_in_scope(soup, source_url, section):
            _merge_candidates(
                dst=candidates,
                seen_names=seen_names,
                new_candidates=batch,
                source_url=source_url,
            )

    if len(candidates) < 3:
        for batch in _extract_whole_page_primary(soup, source_url):
            _merge_candidates(
                dst=candidates,
                seen_names=seen_names,
                new_candidates=batch,
                source_url=source_url,
            )

        if len(candidates) < 3:
            for batch in _extract_whole_page_secondary(soup, source_url):
                _merge_candidates(
                    dst=candidates,
                    seen_names=seen_names,
                    new_candidates=batch,
                    source_url=source_url,
                )

    log.info("Extracted %d people cards from %s", len(candidates), source_url)
    return candidates


__all__ = [
    "extract_people_cards",
    "Candidate",
]
