# src/extract/quality_gates.py
"""
Quality gates for candidate name/title validation.

This module provides strict validation to prevent obviously wrong data from
being persisted as "people" records:

  - Compliance/standard acronyms (PCI DSS, SOC2, ISO27001) are not people
  - Geographic locations (San Francisco, Buenos Aires) are not names
  - Placeholder/test localparts (jdoe, test, example) should be filtered
  - Generic nav/boilerplate terms should not become person names
  - Marketing CTA phrases (Unlock Opportunities, See X in action, etc.) are not names
  - Third-party sources (customer stories, case studies) should not yield employees

Title validation is intentionally conservative to prevent marketing blurbs from
being interpreted as job titles.

NOTE: These gates are used across extractors and persistence/AI fallback paths.
"""

from __future__ import annotations

import re
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Compliance / standard acronyms that are NOT people
# ---------------------------------------------------------------------------

_COMPLIANCE_PATTERNS: set[str] = {
    # Security/privacy standards
    "pci",
    "pci dss",
    "pci-dss",
    "pcidss",
    "soc",
    "soc1",
    "soc2",
    "soc 1",
    "soc 2",
    "soc-1",
    "soc-2",
    "iso",
    "iso27001",
    "iso 27001",
    "iso-27001",
    "iso27k",
    "hipaa",
    "hitech",
    "gdpr",
    "ccpa",
    "cpra",
    "ferpa",
    "glba",
    "sox",
    "sarbanes-oxley",
    "sarbanes oxley",
    "fedramp",
    "fed-ramp",
    "fed ramp",
    "nist",
    "cis",
    "cmmc",
    "fisma",
    "itar",
    "hitrust",
    "aicpa",
    "ssae",
    "ssae16",
    "ssae18",
    # Other tech acronyms that appear as "names"
    "api",
    "sdk",
    "saas",
    "paas",
    "iaas",
    "roi",
    "kpi",
    "okr",
    "nps",
    "crm",
    "erp",
    "hrm",
    "hris",
    "gdp",
    "cagr",
    "arr",
    "mrr",
}

# ---------------------------------------------------------------------------
# Placeholder / test localparts
# ---------------------------------------------------------------------------

_PLACEHOLDER_LOCALPARTS: set[str] = {
    # Classic test/placeholder names
    "jdoe",
    "j.doe",
    "johndoe",
    "john.doe",
    "john_doe",
    "janedoe",
    "jane.doe",
    "jane_doe",
    "test",
    "testuser",
    "test.user",
    "test_user",
    "example",
    "sample",
    "demo",
    "demouser",
    "placeholder",
    "yourname",
    "your.name",
    "your_name",
    "firstname",
    "first.name",
    "first_name",
    "lastname",
    "last.name",
    "last_name",
    "firstname.lastname",
    "first.last",
    "user",
    "newuser",
    "tempuser",
    "admin",
    "administrator",
    "root",
    "sysadmin",
    "nobody",
    "null",
    "void",
    "anonymous",
    "anon",
    # Generic
    "name",
    "email",
    "address",
    "person",
}

# ---------------------------------------------------------------------------
# Role/shared inbox localparts (not personal emails)
# ---------------------------------------------------------------------------

_ROLE_LOCALPARTS: set[str] = {
    "info",
    "contact",
    "hello",
    "support",
    "sales",
    "team",
    "admin",
    "help",
    "office",
    "careers",
    "hr",
    "jobs",
    "press",
    "media",
    "marketing",
    "billing",
    "accounts",
    "service",
    "enquiries",
    "inquiries",
    "general",
    "feedback",
    "webmaster",
    "postmaster",
    "abuse",
    "security",
    "privacy",
    "legal",
    "compliance",
    "pr",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "mailer-daemon",
}

# ---------------------------------------------------------------------------
# Geographic locations (cities, states, countries)
# ---------------------------------------------------------------------------

_GEOGRAPHY_TERMS: set[str] = {
    # Major US cities
    "new york",
    "new york city",
    "nyc",
    "los angeles",
    "la",
    "chicago",
    "houston",
    "phoenix",
    "philadelphia",
    "san antonio",
    "san diego",
    "dallas",
    "san jose",
    "austin",
    "jacksonville",
    "san francisco",
    "sf",
    "seattle",
    "denver",
    "boston",
    "washington",
    "washington dc",
    "dc",
    "atlanta",
    "miami",
    "portland",
    "las vegas",
    "detroit",
    "minneapolis",
    # Directional-place commonly scraped from addresses
    "north bethesda",
    # International cities
    "london",
    "paris",
    "berlin",
    "tokyo",
    "sydney",
    "melbourne",
    "singapore",
    "hong kong",
    "shanghai",
    "beijing",
    "mumbai",
    "bangalore",
    "delhi",
    "dubai",
    "toronto",
    "vancouver",
    "montreal",
    "mexico city",
    "sao paulo",
    "buenos aires",
    "amsterdam",
    "dublin",
    "zurich",
    "geneva",
    "stockholm",
    "oslo",
    "copenhagen",
    "helsinki",
    "warsaw",
    "prague",
    "vienna",
    "brussels",
    "lisbon",
    "madrid",
    "barcelona",
    "milan",
    "rome",
    "tel aviv",
    "seoul",
    # US states
    "california",
    "texas",
    "florida",
    "new york state",
    "illinois",
    "pennsylvania",
    "ohio",
    "georgia",
    "michigan",
    "north carolina",
    "new jersey",
    "virginia",
    "washington state",
    "arizona",
    "massachusetts",
    "tennessee",
    "indiana",
    "maryland",
    "missouri",
    "wisconsin",
    "colorado",
    "minnesota",
    "south carolina",
    "alabama",
    "louisiana",
    "kentucky",
    "oregon",
    "connecticut",
    # Countries
    "usa",
    "united states",
    "us",
    "uk",
    "united kingdom",
    "canada",
    "australia",
    "germany",
    "france",
    "japan",
    "china",
    "india",
    "brazil",
    "mexico",
    "spain",
    "italy",
    "netherlands",
    "switzerland",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "ireland",
    "israel",
    "south korea",
    "korea",
    "taiwan",
    "argentina",
    # Regions
    "emea",
    "apac",
    "latam",
    "americas",
    "europe",
    "asia",
    "north america",
    "south america",
    "middle east",
    "africa",
    "asia pacific",
    "western europe",
    "eastern europe",
}

# Navigation / boilerplate terms that should never be person names
_NAV_BOILERPLATE_TERMS: set[str] = {
    # Navigation
    "home",
    "about",
    "about us",
    "contact",
    "contact us",
    "team",
    "our team",
    "leadership",
    "careers",
    "jobs",
    "blog",
    "news",
    "press",
    "media",
    "resources",
    "products",
    "services",
    "solutions",
    "pricing",
    "support",
    "help",
    "faq",
    "documentation",
    "docs",
    "login",
    "sign in",
    "sign up",
    "register",
    "account",
    "privacy",
    "terms",
    "legal",
    "cookies",
    "sitemap",
    # Generic role/team names (not people)
    "engineering",
    "marketing",
    "sales",
    "finance",
    "human resources",
    "hr",
    "operations",
    "customer success",
    "product",
    "design",
    "legal team",
    "executive team",
    "board of directors",
    "advisory board",
    "investors",
    # High-signal non-person labels that show up as “cards”
    "partners",
    "data partners",
    "premier partners",
    "technology partners",
    # Misc boilerplate
    "read more",
    "learn more",
    "see more",
    "view all",
    "get started",
    "request demo",
    "book a demo",
    "subscribe",
    "newsletter",
    "download",
    "share",
}

# Marketing/CTA-ish tokens and phrases that frequently get mis-scraped as names.
# These are intentionally high-signal; avoid turning this into a giant stoplist.
_NON_PERSON_NAME_TOKENS: set[str] = {
    "unlock",
    "opportunity",
    "opportunities",
    "see",
    "action",
    "demo",
    "trial",
    "request",
    "book",
    "schedule",
    "started",
    "start",
    "learn",
    "more",
    "logo",
    "circle",
    "cares",
    "care",
    # High-signal card/system labels that are not humans
    "partner",
    "partners",
    "solutions",
    "resources",
}

# ---------------------------------------------------------------------------
# Source URL patterns (for filtering third-party content)
# ---------------------------------------------------------------------------

_THIRD_PARTY_URL_PATTERNS: tuple[str, ...] = (
    "/customer-stor",  # customer-story, customer-stories
    "/case-stud",  # case-study, case-studies
    "/success-stor",  # success-story, success-stories
    "/testimonial",
    "/client-stor",
    "/customer-spotlight",
    "/review/",
    "/reviews/",
)

_BLOG_URL_PATTERNS: tuple[str, ...] = (
    "/blog/",
    "/article/",
    "/articles/",
    "/post/",
    "/posts/",
)

# Directional geography patterns (addresses / region labels)
_DIRECTION_WORDS = {"north", "south", "east", "west"}
_DIRECTIONAL_PLACE_RE = re.compile(
    r"^(north|south|east|west)\s+[a-z][a-z\s\-']+$",
    re.IGNORECASE,
)

_GEOGRAPHY_RE = re.compile(
    r"^(" + "|".join(re.escape(g) for g in sorted(_GEOGRAPHY_TERMS, key=len, reverse=True)) + r")$",
    re.IGNORECASE,
)

_NAME_TOKEN_RE = re.compile(r"^[A-Za-z][A-Za-z'\-\.]*$")

_ALLOWED_PARTICLES = {
    "van",
    "von",
    "de",
    "del",
    "da",
    "di",
    "du",
    "la",
    "le",
}

_IGNORED_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "md"}

# ---------------------------------------------------------------------------
# Title validation tightening
# ---------------------------------------------------------------------------

# Max allowed title length (marketing blurbs are typically much longer)
_MAX_TITLE_LEN = 80

# Must contain at least one role keyword token (case-insensitive)
_ROLE_KEYWORD_PATTERNS: tuple[str, ...] = (
    r"\bchief\b",
    r"\bceo\b",
    r"\bcfo\b",
    r"\bcto\b",
    r"\bcoo\b",
    r"\bcmo\b",
    r"\bcpo\b",
    r"\bcro\b",
    r"\bciso\b",
    r"\bpresident\b",
    r"\bfounder\b",
    r"\bco[-\s]?founder\b",
    r"\bchair\b",
    r"\bvp\b",
    r"\bsvp\b",
    r"\bevp\b",
    r"\bvice president\b",
    r"\bdirector\b",
    r"\bhead\b",
    r"\bmanager\b",
    r"\bprincipal\b",
    r"\bpartner\b",
    r"\bgeneral counsel\b",
    r"\bcounsel\b",
    r"\bofficer\b",
    r"\bexecutive\b",
    r"\badvisor\b",
)
_ROLE_KEYWORDS_RE = re.compile("|".join(_ROLE_KEYWORD_PATTERNS), re.IGNORECASE)

# Reject obvious marketing verbs/phrases
_MARKETING_TITLE_PHRASES: tuple[str, ...] = (
    "insights",
    "features",
    "built right",
    "built in",
    "learn",
    "get",
    "grow",
    "connect",
    "instantly",
    "out of the box",
)

_GLUE_WORDS = frozenset({"in", "for", "to", "with", "and", "the", "of"})

# ---------------------------------------------------------------------------
# Validation result type
# ---------------------------------------------------------------------------


class ValidationResult(NamedTuple):
    """Result of a validation check."""

    is_valid: bool
    rejection_reason: str | None


# ---------------------------------------------------------------------------
# Public validation functions
# ---------------------------------------------------------------------------


def is_placeholder_localpart(localpart: str) -> bool:
    lp = (localpart or "").lower().strip()
    return lp in _PLACEHOLDER_LOCALPARTS


def is_role_email(email: str | None) -> bool:
    """Return True if email looks like a role/shared inbox address."""
    if not email or "@" not in email:
        return False
    local = email.split("@", 1)[0].strip().lower()
    return local in _ROLE_LOCALPARTS


def is_compliance_term(text: str) -> bool:
    normalized = (text or "").lower().strip()
    return normalized in _COMPLIANCE_PATTERNS


def is_geography_term(text: str) -> bool:
    normalized = (text or "").lower().strip()

    if normalized in _GEOGRAPHY_TERMS or bool(_GEOGRAPHY_RE.match(normalized)):
        return True

    if bool(_DIRECTIONAL_PLACE_RE.match(normalized)):
        return True

    if normalized in _DIRECTION_WORDS:
        return True

    return False


def is_nav_boilerplate(text: str) -> bool:
    normalized = (text or "").lower().strip()
    return normalized in _NAV_BOILERPLATE_TERMS


# ---------------------------------------------------------------------------
# Source URL filtering (prevents extracting customers as employees)
# ---------------------------------------------------------------------------


def is_third_party_source_url(url: str) -> bool:
    """
    Check if URL indicates third-party content (customers, partners, testimonials).
    """
    if not url:
        return False
    lower = url.lower()
    return any(p in lower for p in _THIRD_PARTY_URL_PATTERNS)


def is_blog_source_url(url: str) -> bool:
    """Check if URL indicates blog/article content."""
    if not url:
        return False
    lower = url.lower()
    return any(p in lower for p in _BLOG_URL_PATTERNS)


def _has_leadership_title(title: str | None) -> bool:
    """Check if title indicates a leadership/executive position."""
    if not title:
        return False
    lower = title.lower()
    leadership_indicators = (
        "ceo",
        "cto",
        "cfo",
        "coo",
        "cmo",
        "cpo",
        "cro",
        "ciso",
        "chief",
        "president",
        "founder",
        "co-founder",
        "cofounder",
        "vp ",
        "vice president",
        "svp",
        "evp",
        "director",
        "head of",
        "partner",
        "managing",
        "chair",
        "chairman",
        "chairwoman",
    )
    return any(ind in lower for ind in leadership_indicators)


def _tokenize_name(name: str) -> list[str]:
    n = re.sub(r"\s+", " ", (name or "").strip())
    n = n.strip(" ,;:|/\\")
    if not n:
        return []
    toks = n.split(" ")
    out: list[str] = []
    for t in toks:
        tt = t.strip(" ,;:|/\\()[]{}<>")
        if tt:
            out.append(tt)
    return out


def _fast_reject_person_name(name_clean: str) -> ValidationResult | None:
    if is_compliance_term(name_clean):
        return ValidationResult(False, "compliance_term")
    if is_geography_term(name_clean):
        return ValidationResult(False, "geography_term")
    if is_nav_boilerplate(name_clean):
        return ValidationResult(False, "nav_boilerplate")
    return None


def _reject_numericish(name_clean: str) -> ValidationResult | None:
    if re.fullmatch(r"[\d\-\.\s]+", name_clean) is not None:
        return ValidationResult(False, "numeric_only")
    if re.search(r"\d", name_clean) is not None:
        return ValidationResult(False, "contains_digits")
    return None


def _strip_ignored_suffixes(tokens: list[str]) -> list[str]:
    out: list[str] = []
    for t in tokens:
        tl = t.strip(".").lower()
        if tl in _IGNORED_SUFFIXES:
            continue
        out.append(t)
    return out


def _reject_by_token_count(tokens: list[str]) -> ValidationResult | None:
    if len(tokens) > 5:
        return ValidationResult(False, "too_many_tokens")
    return None


def _reject_non_person_tokens(tokens: list[str]) -> ValidationResult | None:
    for t in tokens:
        tl = t.strip(".").lower()
        if tl in _NON_PERSON_NAME_TOKENS:
            return ValidationResult(False, f"non_person_token:{tl}")
    return None


def _looks_like_phrase(tokens: list[str]) -> bool:
    if len(tokens) < 3:
        return False
    return any(t.lower() in _GLUE_WORDS for t in tokens)


def _is_initial_token(token: str) -> bool:
    base = token.strip(".")
    return len(base) == 1 and base.isalpha()


def _is_name_alpha(token: str) -> bool:
    cleaned = token.replace("-", "").replace("'", "")
    return cleaned.isalpha() if cleaned else False


def _validate_name_tokens(tokens: list[str]) -> ValidationResult | None:
    for t in tokens:
        tl = t.lower()
        if tl in _ALLOWED_PARTICLES:
            continue
        if _is_initial_token(t):
            continue
        if _NAME_TOKEN_RE.match(t) is None:
            return ValidationResult(False, "bad_token_chars")
    return None


def _reject_upper_acronym(name_clean: str) -> ValidationResult | None:
    if not name_clean.isupper():
        return None
    words = name_clean.split()
    if len(words) <= 3 and all(len(w) <= 6 for w in words):
        return ValidationResult(False, "likely_acronym")
    return None


def _validate_single_token(token: str) -> ValidationResult:
    if len(token.strip(".")) < 3:
        return ValidationResult(False, "single_token_too_short")
    return ValidationResult(True, None)


def _has_two_name_parts(tokens: list[str]) -> bool:
    longish = [t for t in tokens if len(t.strip(".")) >= 2 and _is_name_alpha(t.strip("."))]
    return len(longish) >= 2


def validate_person_name(name: str) -> ValidationResult:
    """
    Validate that a name looks like a real person name.

    Primary goal: prevent garbage like "Unlock Opportunities", "Clari Logo",
    "North Bethesda", "PCI DSS" from being stored as person names.
    """
    if not name or not name.strip():
        return ValidationResult(False, "empty_name")

    name_clean = name.strip()

    fast = _fast_reject_person_name(name_clean)
    if fast is not None:
        return fast

    numericish = _reject_numericish(name_clean)
    if numericish is not None:
        return numericish

    if "@" in name_clean:
        return ValidationResult(False, "contains_at_sign")

    toks = _tokenize_name(name_clean)
    if not toks:
        return ValidationResult(False, "empty_tokens")

    toks_core = _strip_ignored_suffixes(toks)

    count_res = _reject_by_token_count(toks_core)
    if count_res is not None:
        return count_res

    non_person_res = _reject_non_person_tokens(toks_core)
    if non_person_res is not None:
        return non_person_res

    if _looks_like_phrase(toks_core):
        return ValidationResult(False, "looks_like_phrase")

    token_res = _validate_name_tokens(toks_core)
    if token_res is not None:
        return token_res

    upper_res = _reject_upper_acronym(name_clean)
    if upper_res is not None:
        return upper_res

    if len(toks_core) == 1:
        return _validate_single_token(toks_core[0])

    if not _has_two_name_parts(toks_core):
        return ValidationResult(False, "not_enough_name_parts")

    return ValidationResult(True, None)


def validate_title(title: str) -> ValidationResult:
    """
    Validate that a title looks like a job title, not a location or CTA/garbage.

    Tightened rules:
      - Max length: <= 80 characters
      - Must contain at least one role keyword (chief/CEO/CFO/CTO/president/founder/vp/etc.)
      - Reject marketing blurbs/verbs (insights/features/built right/learn/get/grow/connect/etc.)
    """
    if not title or not title.strip():
        return ValidationResult(True, None)

    title_clean = re.sub(r"\s+", " ", title.strip())

    if len(title_clean) > _MAX_TITLE_LEN:
        return ValidationResult(False, "title_too_long")

    if is_geography_term(title_clean):
        return ValidationResult(False, "geography_as_title")

    if is_nav_boilerplate(title_clean):
        return ValidationResult(False, "nav_boilerplate_as_title")

    tl = title_clean.lower()

    # Marketing blurb patterns that are not titles
    if any(p in tl for p in _MARKETING_TITLE_PHRASES):
        return ValidationResult(False, "marketing_buzzword_in_title")

    # CTA-ish titles that are almost never real job titles
    if " in action" in tl and (tl.startswith("see ") or tl.startswith("watch ")):
        return ValidationResult(False, "cta_see_in_action")
    if tl.startswith(("unlock ", "request ", "book ", "schedule ", "get started")):
        return ValidationResult(False, "cta_prefix")
    if "opportunities" in tl and "unlock" in tl:
        return ValidationResult(False, "cta_unlock_opportunities")

    # Require at least one role keyword
    if not _ROLE_KEYWORDS_RE.search(title_clean):
        return ValidationResult(False, "missing_role_keyword")

    return ValidationResult(True, None)


def validate_candidate_for_persistence(
    *,
    name: str | None,
    email: str | None,
    title: str | None = None,
) -> ValidationResult:
    """
    Comprehensive validation for a candidate before persisting.

    NOTE: This is a conservative check used when candidates are not AI-approved
    (and/or for fallback pathways). Email-only candidates can pass; name-only
    candidates must have a valid person name and some additional evidence.
    """
    # Check placeholder email localpart
    if email:
        localpart = email.split("@")[0] if "@" in email else email
        lp = localpart.lower().strip()
        if is_placeholder_localpart(lp):
            return ValidationResult(False, f"placeholder_email:{lp}")

    # Role email handling
    if email and is_role_email(email):
        if not name:
            return ValidationResult(False, "role_email_no_name")
        nm = validate_person_name(name)
        if not nm.is_valid:
            return ValidationResult(False, f"role_email_invalid_name:{nm.rejection_reason}")

    # Name validation when present
    if name:
        name_result = validate_person_name(name)
        if not name_result.is_valid:
            return ValidationResult(False, f"invalid_name:{name_result.rejection_reason}")

    # Title validation (informational here; callers may clear instead of rejecting)
    if title:
        title_result = validate_title(title)
        if not title_result.is_valid:
            # Do not hard-reject solely on title; the caller may clear title and keep.
            pass

    # If we have neither a name nor an email, reject
    if not name and not email:
        return ValidationResult(False, "no_name_or_email")

    return ValidationResult(True, None)


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def clean_title_if_invalid(title: str | None) -> str | None:
    if not title:
        return None
    result = validate_title(title)
    return title if result.is_valid else None


def clean_email_if_role(email: str | None, *, name: str | None) -> str | None:
    """
    If email is a role/shared inbox and we have a valid human name,
    return None (strip the role email from the person).
    """
    if not email:
        return None
    if not is_role_email(email):
        return email
    if not name:
        return None
    nm = validate_person_name(name)
    return None if nm.is_valid else None


def should_persist_as_person(
    *,
    name: str | None,
    email: str | None,
    title: str | None = None,
    ai_approved: bool = False,
    source_url: str | None = None,
    source_type: str | None = None,
) -> bool:
    """
    Determine if a candidate should be kept for persistence.

    Rules (high-level):
      - Always block third-party sources.
      - If a name is present, it must validate as a person name.
      - If title is invalid, treat it as absent (callers may clear it).
      - For non-AI candidates with no email:
          require a valid title OR a strong person signal (source_type indicates profile).
      - Email-only candidates are allowed (we may persist the email even if no name).

    Args:
        source_type: Optional extractor source type (e.g., "people_card_linkedin").
                     Callers can provide this for stronger gating, but it is optional.
    """
    # Source URL filtering - applied even for AI-approved candidates
    if source_url:
        if is_third_party_source_url(source_url):
            return False

        # Blog authors without leadership titles are likely content writers, not execs
        if is_blog_source_url(source_url) and not _has_leadership_title(title):
            return False

    # Validate name if present (always)
    if name:
        nm = validate_person_name(name)
        if not nm.is_valid:
            return False

    # Normalize title validity for decisioning
    title_valid = True
    if title:
        tr = validate_title(title)
        title_valid = tr.is_valid

    # AI-approved: trust after source-url + name validation
    if ai_approved:
        return True

    # Non-AI candidates:
    # If we have an email, allow (after placeholder checks handled elsewhere)
    if email:
        return True

    # No email: must have a valid title OR a strong person signal
    if title and title_valid:
        return True

    strong_signal = False
    if source_type:
        st = source_type.lower().strip()
        if "linkedin" in st or "profile" in st:
            strong_signal = True

    return strong_signal


__all__ = [
    "ValidationResult",
    "is_placeholder_localpart",
    "is_role_email",
    "is_compliance_term",
    "is_geography_term",
    "is_nav_boilerplate",
    "is_third_party_source_url",
    "is_blog_source_url",
    "validate_person_name",
    "validate_title",
    "validate_candidate_for_persistence",
    "clean_title_if_invalid",
    "clean_email_if_role",
    "should_persist_as_person",
]
