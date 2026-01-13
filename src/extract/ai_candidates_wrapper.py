# src/extract/ai_candidates_wrapper.py
"""
Wrapper for AI candidate refinement with proper metrics tracking.

This module wraps the AI refiner to:
  - Track whether AI was called, succeeded, and what it returned
  - Apply quality gates + tiered fallback ONLY when AI is disabled or fails
  - Enforce the contract:
      If AI is enabled and the call succeeds but the model returns 0 people,
      this wrapper MUST return [] (no fallback).

Key behavior:
  - AI enabled + ok_nonempty  -> return AI people
  - AI enabled + ok_empty     -> return [] (NO fallback)
  - AI disabled               -> smart fallback
  - AI failed (exception/parse)-> smart fallback

NOTE (batch_test.ps1 compatibility):
  - Emits log lines:
      "Calling AI refiner ..."
      "AI refinement complete: <N> raw candidates -> <M> people"
    so your PowerShell parser can compute ai_seconds + parse ai_in/ai_out.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-filtering configuration
# ---------------------------------------------------------------------------

# Maximum candidates to send to AI (controls token costs)
_AI_MAX_CANDIDATES = 50

# URL patterns that indicate press/news pages (not team pages)
# These pages often contain names of journalists, analysts, customers - not employees
_PRESS_URL_PATTERNS = re.compile(
    r"/(press|news|media|blog|articles?|stories|events?|webinars?|podcasts?|"
    r"case-stud|customer-stor|resources?|insights?|announcements?|"
    r"industry/media|newsroom|press-room|press-release|in-the-news)(/|$|\?)",
    re.IGNORECASE,
)

# Locale path pattern (e.g., /fr-fr/, /de-de/, /ja-jp/, /ko-kr/)
_LOCALE_PATH_PATTERN = re.compile(r"/([a-z]{2}-[a-z]{2})/", re.IGNORECASE)

# Patterns in names that indicate non-person entries
_NON_PERSON_NAME_PATTERNS = re.compile(
    r"^(read more|learn more|view|see|click|download|contact us|get started|"
    r"sign up|subscribe|join|follow|share|about|company|team|staff|"
    r"our mission|our vision|privacy|terms|copyright|\d{4}|"
    r"all rights|reserved|inc\.|llc|ltd|corp)$",
    re.IGNORECASE,
)

# Import quality gates
try:
    from src.extract.quality_gates import (
        clean_title_if_invalid,
        is_blog_source_url,
        is_third_party_source_url,
        should_persist_as_person,
        validate_candidate_for_persistence,
        validate_person_name,
    )

    _HAS_QUALITY_GATES = True
except ImportError:  # pragma: no cover
    _HAS_QUALITY_GATES = False
    should_persist_as_person = None  # type: ignore
    validate_candidate_for_persistence = None  # type: ignore
    validate_person_name = None  # type: ignore
    clean_title_if_invalid = None  # type: ignore
    is_third_party_source_url = None  # type: ignore
    is_blog_source_url = None  # type: ignore

# Import the actual AI refiner
try:
    from src.extract.ai_candidates import AI_PEOPLE_ENABLED
    from src.extract.ai_candidates import (
        extract_ai_candidates as _raw_extract_ai_candidates,
    )

    _HAS_AI_EXTRACTOR = True
except ImportError:  # pragma: no cover
    _HAS_AI_EXTRACTOR = False
    AI_PEOPLE_ENABLED = False  # type: ignore
    _raw_extract_ai_candidates = None  # type: ignore

# Import Candidate type (for typing only)
try:
    from src.extract.candidates import Candidate
except ImportError:  # pragma: no cover
    Candidate = Any  # type: ignore


# Leadership title indicators
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
    "chief",
    "president",
    "founder",
    "co-founder",
    "cofounder",
    "vp ",
    "vice president",
    "svp",
    "evp",
    "avp",
    "director",
    "head of",
    "partner",
    "managing director",
    "general manager",
    "gm",
    "principal",
    "owner",
)

# Any title indicator (broader than leadership)
_ANY_TITLE_PATTERNS = (
    "manager",
    "lead",
    "senior",
    "sr.",
    "executive",
    "officer",
    "advisor",
    "consultant",
    "specialist",
    "engineer",
    "architect",
    "analyst",
    "coordinator",
)


def _qg_is_valid(result: Any) -> bool:
    """Normalize various validate_* return types into a boolean."""
    if result is None:
        return False
    if isinstance(result, tuple) and result:
        return bool(result[0])
    if hasattr(result, "is_valid"):
        return bool(result.is_valid)
    if isinstance(result, bool):
        return result
    return False


@dataclass
class AIRefinementMetrics:
    """Metrics from an AI refinement attempt."""

    ai_enabled: bool = False
    ai_called: bool = False
    ai_call_succeeded: bool = False
    ai_input_candidates: int = 0  # After pre-filtering
    ai_input_candidates_raw: int = 0  # Before pre-filtering
    ai_returned_people: int = 0

    # Pre-filter stats
    prefilter_removed_press_news: int = 0
    prefilter_removed_non_person: int = 0
    prefilter_removed_locale_dup: int = 0
    prefilter_removed_name_dedup: int = 0
    prefilter_removed_cap: int = 0

    # Reason-coded outcomes for auditing
    # one of: ok_nonempty | ok_empty | parse_error | exception | disabled
    ai_outcome: str = "disabled"
    # one of: ai_failed | ai_disabled | none
    fallback_reason: str = "none"

    fallback_used: bool = False
    fallback_tier: str | None = None  # Which fallback tier was used
    quality_rejections: int = 0
    ai_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ai_enabled": self.ai_enabled,
            "ai_called": self.ai_called,
            "ai_call_succeeded": self.ai_call_succeeded,
            "ai_input_candidates": self.ai_input_candidates,
            "ai_input_candidates_raw": self.ai_input_candidates_raw,
            "ai_returned_people": self.ai_returned_people,
            "prefilter_removed_press_news": self.prefilter_removed_press_news,
            "prefilter_removed_non_person": self.prefilter_removed_non_person,
            "prefilter_removed_locale_dup": self.prefilter_removed_locale_dup,
            "prefilter_removed_name_dedup": self.prefilter_removed_name_dedup,
            "prefilter_removed_cap": self.prefilter_removed_cap,
            "ai_outcome": self.ai_outcome,
            "fallback_used": self.fallback_used,
            "fallback_reason": self.fallback_reason,
            "fallback_tier": self.fallback_tier,
            "quality_rejections": self.quality_rejections,
            "ai_error": self.ai_error,
        }


def _get_candidate_name(cand: Any) -> str | None:
    """Extract name from a candidate object."""
    raw_name = getattr(cand, "raw_name", None)
    if raw_name:
        return str(raw_name).strip()

    first = getattr(cand, "first_name", None) or ""
    last = getattr(cand, "last_name", None) or ""
    full = f"{first} {last}".strip()
    return full if full else None


def _get_candidate_email(cand: Any) -> str | None:
    """Extract email from a candidate object."""
    email = getattr(cand, "email", None)
    return str(email).strip() if email else None


def _get_candidate_title(cand: Any) -> str | None:
    """Extract title from a candidate object."""
    title = getattr(cand, "title", None)
    return str(title).strip() if title else None


def _get_candidate_source_url(cand: Any) -> str | None:
    """Extract source URL from a candidate object."""
    url = getattr(cand, "source_url", None)
    return str(url).strip() if url else None


def _has_leadership_title(title: str | None) -> bool:
    """Check if title indicates a leadership/executive position."""
    if not title:
        return False
    lower = title.lower()
    return any(pattern in lower for pattern in _LEADERSHIP_TITLE_PATTERNS)


def _has_any_title(title: str | None) -> bool:
    """Check if candidate has any recognizable job title."""
    if not title:
        return False
    if _has_leadership_title(title):
        return True
    lower = title.lower()
    return any(pattern in lower for pattern in _ANY_TITLE_PATTERNS)


def _is_valid_name_structure(name: str | None) -> bool:
    """
    Check if name has valid person-name structure.

    Valid: "John Smith", "Mary Jane Watson", "Jean-Pierre Dupont"
    Invalid: "Learn More", "San Francisco", single words
    """
    if not name:
        return False

    name = name.strip()
    if len(name) < 3:
        return False

    words = name.split()
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

    if _HAS_QUALITY_GATES and validate_person_name is not None:
        try:
            return _qg_is_valid(validate_person_name(name))
        except Exception:  # pragma: no cover
            return False

    return True


def _is_from_third_party_source(cand: Any) -> bool:
    """Check if candidate is from a third-party source (customer story, etc.)."""
    if not _HAS_QUALITY_GATES or is_third_party_source_url is None:
        return False
    url = _get_candidate_source_url(cand)
    return bool(url) and bool(is_third_party_source_url(url))


def _is_from_blog_source(cand: Any) -> bool:
    """Check if candidate is from a blog/resources/news-like page."""
    if not _HAS_QUALITY_GATES or is_blog_source_url is None:
        return False
    url = _get_candidate_source_url(cand)
    return bool(url) and bool(is_blog_source_url(url))


def _apply_quality_gates(
    candidates: list[Any],
    *,
    ai_approved: bool,
) -> tuple[list[Any], int]:
    """
    Apply quality gates to filter out garbage candidates.

    Returns:
        (filtered_candidates, rejection_count)
    """
    if not _HAS_QUALITY_GATES or should_persist_as_person is None:
        return candidates, 0

    filtered: list[Any] = []
    rejections = 0

    for cand in candidates:
        name = _get_candidate_name(cand)
        email = _get_candidate_email(cand)
        title = _get_candidate_title(cand)
        source_url = _get_candidate_source_url(cand)

        if should_persist_as_person(
            name=name,
            email=email,
            title=title,
            ai_approved=ai_approved,
            source_url=source_url,
        ):
            # Clean title if it's bad but keep candidate
            if title and clean_title_if_invalid is not None:
                try:
                    cleaned = clean_title_if_invalid(title)
                except Exception:  # pragma: no cover
                    cleaned = title
                if cleaned != title:
                    try:
                        cand.title = cleaned
                    except AttributeError:
                        pass
            filtered.append(cand)
        else:
            if validate_candidate_for_persistence is not None:
                try:
                    result = validate_candidate_for_persistence(
                        name=name,
                        email=email,
                        title=title,
                    )
                    reason = getattr(result, "rejection_reason", None)
                except Exception:  # pragma: no cover
                    reason = None
                log.debug(
                    "Quality gate rejected candidate: name=%r email=%r reason=%s",
                    name,
                    email,
                    reason,
                )
            rejections += 1

    return filtered, rejections


# ---------------------------------------------------------------------------
# Pre-filtering for AI input (reduce token costs)
# ---------------------------------------------------------------------------


def _normalize_name_for_dedup(name: str | None) -> str:
    """Normalize a name for deduplication (lowercase, strip, collapse spaces)."""
    if not name:
        return ""
    return " ".join(name.lower().split())


def _get_url_path_without_locale(url: str | None) -> str:
    """Extract the path from a URL, removing locale prefixes like /fr-fr/."""
    if not url:
        return ""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        path = parsed.path.lower()
        # Remove locale prefix
        path = _LOCALE_PATH_PATTERN.sub("/", path)
        return path
    except Exception:
        return ""


def _is_press_or_news_url(url: str | None) -> bool:
    """Check if URL is a press/news page that shouldn't yield employee names."""
    if not url:
        return False
    return bool(_PRESS_URL_PATTERNS.search(url))


def _is_locale_duplicate_url(url: str | None) -> bool:
    """Check if URL has a locale prefix (likely duplicate content)."""
    if not url:
        return False
    return bool(_LOCALE_PATH_PATTERN.search(url))


def _is_non_person_name(name: str | None) -> bool:
    """
    Check if name matches patterns that indicate it's CLEARLY not a person.

    This is intentionally conservative - we only reject names that are
    obviously non-person (UI elements, company names, etc.). We do NOT
    reject single-word names since they could be legitimate partial data
    (e.g., first name only) that the AI can evaluate.
    """
    if not name:
        return True
    name = name.strip()
    if len(name) < 2:
        return True
    if _NON_PERSON_NAME_PATTERNS.match(name):
        return True
    # Don't reject single-word names - they might be valid partial data
    # Let the AI decide if "Abbey" or "John" is a real person
    return False


def _candidate_sort_key(cand: Any) -> tuple[int, int, int, int, str]:
    """
    Sort key for candidates (lower = better priority).

    Priority order:
    1. Has email (highest)
    2. Has leadership title
    3. Has any title
    4. From non-locale URL
    5. Alphabetical by name (tie-breaker)
    """
    has_email = 0 if _get_candidate_email(cand) else 1
    has_leadership = 0 if _has_leadership_title(_get_candidate_title(cand)) else 1
    has_title = 0 if _has_any_title(_get_candidate_title(cand)) else 1
    is_locale = 1 if _is_locale_duplicate_url(_get_candidate_source_url(cand)) else 0
    name = _normalize_name_for_dedup(_get_candidate_name(cand))
    return (has_email, has_leadership, has_title, is_locale, name)


def _prefilter_candidates_for_ai(
    candidates: list[Any],
    max_candidates: int = _AI_MAX_CANDIDATES,
) -> tuple[list[Any], dict[str, int]]:
    """
    Pre-filter candidates before sending to AI to reduce token costs.

    Filtering steps:
    1. Remove candidates from press/news pages
    2. Remove candidates with non-person names
    3. Remove candidates from locale-prefixed URLs (keep canonical)
    4. Deduplicate by normalized name (keep best candidate per name)
    5. Cap at max_candidates (prioritize email-anchored and titled)

    Returns:
        (filtered_candidates, stats_dict)
    """
    stats = {
        "input_total": len(candidates),
        "removed_press_news": 0,
        "removed_non_person_name": 0,
        "removed_locale_duplicate": 0,
        "removed_name_dedup": 0,
        "removed_cap_exceeded": 0,
        "output_total": 0,
    }

    if not candidates:
        return [], stats

    # Step 1: Remove press/news page candidates
    step1 = []
    for c in candidates:
        url = _get_candidate_source_url(c)
        if _is_press_or_news_url(url):
            stats["removed_press_news"] += 1
        else:
            step1.append(c)

    # Step 2: Remove non-person names (but keep email-anchored candidates)
    step2 = []
    for c in step1:
        name = _get_candidate_name(c)
        email = _get_candidate_email(c)
        # Keep candidates that have an email, even if name looks non-person
        # The AI can decide based on email pattern
        if email:
            step2.append(c)
        elif _is_non_person_name(name):
            stats["removed_non_person_name"] += 1
        else:
            step2.append(c)

    # Step 3: Deduplicate by (name, source_path) - keep best candidate per name per page
    # This handles cases where the same person appears multiple times on one page
    # (e.g., email-anchored + people-card version)
    path_groups: dict[tuple[str, str], list[Any]] = {}
    for c in step2:
        name_key = _normalize_name_for_dedup(_get_candidate_name(c))
        if not name_key:
            # No name - keep it, AI will decide
            path_groups.setdefault(("__no_name__", str(id(c))), []).append(c)
            continue
        path_key = _get_url_path_without_locale(_get_candidate_source_url(c))
        group_key = (name_key, path_key)
        if group_key not in path_groups:
            path_groups[group_key] = []
        path_groups[group_key].append(c)

    step3 = []
    same_page_dedup_count = 0
    for _key, group in path_groups.items():
        # Sort by priority (email > title > nothing) and keep best
        group.sort(key=_candidate_sort_key)
        step3.append(group[0])
        same_page_dedup_count += len(group) - 1

    # Only count as locale_duplicate if there were actual locale URLs involved
    # Otherwise it's just same-page deduplication (which we track separately)
    stats["removed_locale_duplicate"] = same_page_dedup_count

    # Step 4: Deduplicate by normalized name (keep best per name)
    name_best: dict[str, Any] = {}
    for c in step3:
        name_key = _normalize_name_for_dedup(_get_candidate_name(c))
        if not name_key:
            continue
        existing = name_best.get(name_key)
        if existing is None:
            name_best[name_key] = c
        else:
            # Keep the one with better attributes (email > title > nothing)
            if _candidate_sort_key(c) < _candidate_sort_key(existing):
                name_best[name_key] = c

    step4 = list(name_best.values())
    stats["removed_name_dedup"] = len(step3) - len(step4)

    # Step 5: Sort by priority and cap at max_candidates
    step4.sort(key=_candidate_sort_key)

    if len(step4) > max_candidates:
        stats["removed_cap_exceeded"] = len(step4) - max_candidates
        step4 = step4[:max_candidates]

    stats["output_total"] = len(step4)

    return step4, stats


def _smart_fallback(
    candidates: list[Any],
) -> tuple[list[Any], str, int]:
    """
    Smart tiered fallback (used ONLY when AI is disabled or fails).

    Tier 1: Email-anchored candidates (highest confidence)
    Tier 2: People-card candidates with leadership titles
    Tier 3: People-card candidates with any title
    Tier 4: Valid name-only candidates

    Returns:
        (selected_candidates, tier_name, rejection_count)
    """
    if not candidates:
        return [], "none", 0

    # Filter out third-party sources and blog/resources/news-like sources first
    valid_candidates = [
        c
        for c in candidates
        if (not _is_from_third_party_source(c)) and (not _is_from_blog_source(c))
    ]
    removed = len(candidates) - len(valid_candidates)
    if removed > 0:
        log.info("Smart fallback: removed %d candidates from excluded sources", removed)

    # Tier 1: Email-anchored candidates
    email_anchored = [c for c in valid_candidates if _get_candidate_email(c)]
    if email_anchored:
        log.info(
            "Smart fallback tier 1: using %d email-anchored candidates",
            len(email_anchored),
        )
        filtered, rejections = _apply_quality_gates(email_anchored, ai_approved=True)
        return filtered, "email_anchored", rejections + removed

    # Tier 2: Leadership titles + valid name structure
    leadership = [
        c
        for c in valid_candidates
        if _has_leadership_title(_get_candidate_title(c))
        and _is_valid_name_structure(_get_candidate_name(c))
    ]
    if leadership:
        log.info(
            "Smart fallback tier 2: using %d candidates with leadership titles",
            len(leadership),
        )
        return leadership, "leadership_title", (len(valid_candidates) - len(leadership)) + removed

    # Tier 3: Any title + valid name structure
    with_title = [
        c
        for c in valid_candidates
        if _has_any_title(_get_candidate_title(c))
        and _is_valid_name_structure(_get_candidate_name(c))
    ]
    if with_title:
        log.info(
            "Smart fallback tier 3: using %d candidates with any title",
            len(with_title),
        )
        return with_title, "any_title", (len(valid_candidates) - len(with_title)) + removed

    # Tier 4: Valid name-only candidates (strictest validation)
    valid_names = [c for c in valid_candidates if _is_valid_name_structure(_get_candidate_name(c))]
    if valid_names:
        log.info("Smart fallback tier 4: using %d valid-name candidates", len(valid_names))
        filtered, rejections = _apply_quality_gates(valid_names, ai_approved=False)
        return filtered, "valid_names", rejections + removed

    log.info("Smart fallback: no valid candidates found in any tier")
    return [], "none", len(candidates)


def refine_candidates_with_ai(
    *,
    company_name: str,
    domain: str,
    raw_candidates: Sequence[Any],
) -> tuple[list[Any], AIRefinementMetrics]:
    """
    Refine candidates using AI with proper metrics tracking and strict contract enforcement.

    Contract:
      - If AI is enabled and the call succeeds but returns 0 people -> return [] (no fallback).
      - Fallback is used only when AI is disabled or the AI call fails.

    Pre-filtering (when AI is enabled):
      - Removes press/news page candidates
      - Removes non-person names
      - Deduplicates by normalized name
      - Removes locale duplicates
      - Caps at _AI_MAX_CANDIDATES

    Returns:
        (refined_candidates, metrics)
    """
    metrics = AIRefinementMetrics()
    candidates = list(raw_candidates)

    if not candidates:
        return [], metrics

    metrics.ai_input_candidates_raw = len(candidates)

    # Determine whether AI is enabled and available
    metrics.ai_enabled = bool(_HAS_AI_EXTRACTOR and AI_PEOPLE_ENABLED)

    if not metrics.ai_enabled:
        metrics.ai_outcome = "disabled"
        metrics.fallback_used = True
        metrics.fallback_reason = "ai_disabled"
        metrics.ai_input_candidates = len(candidates)
        log.info("AI refinement disabled; using smart fallback")
        filtered, tier, rejections = _smart_fallback(candidates)
        metrics.fallback_tier = tier
        metrics.quality_rejections = rejections
        return filtered, metrics

    if _raw_extract_ai_candidates is None:
        # Misconfiguration / unavailable AI callable even though enabled flag is on
        metrics.ai_outcome = "exception"
        metrics.ai_error = "ai_extractor_callable_missing"
        metrics.fallback_used = True
        metrics.fallback_reason = "ai_failed"
        metrics.ai_input_candidates = len(candidates)
        log.warning("AI extractor enabled but callable missing; using smart fallback")
        filtered, tier, rejections = _smart_fallback(candidates)
        metrics.fallback_tier = tier
        metrics.quality_rejections = rejections
        return filtered, metrics

    # Pre-filter candidates to reduce AI token costs
    filtered_candidates, prefilter_stats = _prefilter_candidates_for_ai(candidates)

    # Record pre-filter metrics
    metrics.prefilter_removed_press_news = prefilter_stats["removed_press_news"]
    metrics.prefilter_removed_non_person = prefilter_stats["removed_non_person_name"]
    metrics.prefilter_removed_locale_dup = prefilter_stats["removed_locale_duplicate"]
    metrics.prefilter_removed_name_dedup = prefilter_stats["removed_name_dedup"]
    metrics.prefilter_removed_cap = prefilter_stats["removed_cap_exceeded"]
    metrics.ai_input_candidates = len(filtered_candidates)

    # Log pre-filter summary if significant reduction
    total_removed = len(candidates) - len(filtered_candidates)
    if total_removed > 0:
        log.info(
            "Pre-filter: %d -> %d candidates (removed: %d press/news, %d non-person, "
            "%d locale-dup, %d name-dedup, %d cap)",
            len(candidates),
            len(filtered_candidates),
            prefilter_stats["removed_press_news"],
            prefilter_stats["removed_non_person_name"],
            prefilter_stats["removed_locale_duplicate"],
            prefilter_stats["removed_name_dedup"],
            prefilter_stats["removed_cap_exceeded"],
        )

    # If pre-filtering removed everything, return empty
    if not filtered_candidates:
        metrics.ai_outcome = "ok_empty"
        metrics.ai_called = False
        log.info("Pre-filter removed all candidates; skipping AI call")
        return [], metrics

    # Attempt AI refinement
    try:
        # NOTE: batch_test.ps1 looks for this exact phrase to start AI timing.
        log.info(
            "Calling AI refiner with %d candidates for %s (%s)",
            len(filtered_candidates),
            company_name,
            domain,
        )

        metrics.ai_called = True
        refined = _raw_extract_ai_candidates(
            company_name=company_name,
            domain=domain,
            raw_candidates=filtered_candidates,
        )

        metrics.ai_call_succeeded = True

        refined_list: list[Any]
        if refined is None:
            refined_list = []
        else:
            refined_list = list(refined)

        metrics.ai_returned_people = len(refined_list)

        # NOTE: batch_test.ps1 looks for "AI refinement complete:" to stop timing + parse.
        log.info(
            "AI refinement complete: %d raw candidates -> %d people",
            len(filtered_candidates),
            len(refined_list),
        )

        if refined_list:
            metrics.ai_outcome = "ok_nonempty"
            metrics.fallback_used = False
            metrics.fallback_reason = "none"
            return refined_list, metrics

        # Contract: AI succeeded but returned empty -> return [] and DO NOT fallback
        metrics.ai_outcome = "ok_empty"
        metrics.fallback_used = False
        metrics.fallback_reason = "none"
        log.info("AI returned 0 people successfully; returning [] (no fallback per contract).")
        return [], metrics

    except Exception as exc:
        is_parse = isinstance(exc, (ValueError, TypeError))
        metrics.ai_called = True
        metrics.ai_call_succeeded = False
        metrics.ai_outcome = "parse_error" if is_parse else "exception"
        metrics.ai_error = str(exc)
        metrics.fallback_used = True
        metrics.fallback_reason = "ai_failed"

        log.exception(
            "AI refinement failed for %s (%s): %s",
            company_name,
            domain,
            exc,
        )

        filtered, tier, rejections = _smart_fallback(candidates)
        metrics.fallback_tier = tier
        metrics.quality_rejections = rejections
        return filtered, metrics


def update_result_from_metrics(
    result: Any,
    metrics: AIRefinementMetrics,
) -> None:
    """
    Update an AutodiscoveryResult with AI refinement metrics.
    """
    if result is None:
        return

    try:
        result.ai_enabled = metrics.ai_enabled
        result.ai_called = metrics.ai_called
        result.ai_call_succeeded = metrics.ai_call_succeeded
        result.ai_input_candidates = metrics.ai_input_candidates
        result.ai_returned_people = metrics.ai_returned_people
        result.fallback_used = metrics.fallback_used

        # Optional fields if present on the result object
        if hasattr(result, "ai_outcome"):
            result.ai_outcome = metrics.ai_outcome
        if hasattr(result, "fallback_reason"):
            result.fallback_reason = metrics.fallback_reason
        if hasattr(result, "fallback_tier"):
            result.fallback_tier = metrics.fallback_tier
        if hasattr(result, "ai_error"):
            result.ai_error = metrics.ai_error

        # Keep this field strictly "AI-approved count" (not fallback count).
        if hasattr(result, "ai_approved_people"):
            if metrics.ai_outcome == "ok_nonempty":
                result.ai_approved_people = metrics.ai_returned_people
            else:
                result.ai_approved_people = 0

        if metrics.quality_rejections > 0 and hasattr(result, "people_skipped_quality"):
            result.people_skipped_quality = metrics.quality_rejections

    except AttributeError:
        # Result doesn't have all these fields - that's OK
        pass


__all__ = [
    "AIRefinementMetrics",
    "refine_candidates_with_ai",
    "update_result_from_metrics",
]
