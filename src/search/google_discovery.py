# src/search/google_discovery.py
"""
Serper-powered LinkedIn lead discovery.

Searches Google (via Serper.dev) for C-suite LinkedIn profiles at target
companies, extracts names from URLs and result titles, and returns
structured DiscoveredPerson objects for downstream processing.

No database dependency -- this module is purely functional.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from urllib.parse import unquote

import httpx

log = logging.getLogger(__name__)

SERPER_ENDPOINT = "https://google.serper.dev/search"

CSUITE_ROLES = ["CEO", "CFO", "COO", "CTO", "CIO", "CHRO", "CMO"]

# Map abbreviations to their expanded forms for role validation.
# When we search for "CEO", a LinkedIn title might say "Chief Executive Officer".
_ROLE_EXPANSIONS: dict[str, list[str]] = {
    "CEO": ["ceo", "chief executive officer", "chief executive"],
    "CFO": ["cfo", "chief financial officer", "chief finance officer"],
    "COO": ["coo", "chief operating officer", "chief operations officer"],
    "CTO": ["cto", "chief technology officer", "chief technical officer"],
    "CIO": ["cio", "chief information officer"],
    "CHRO": ["chro", "chief human resources officer", "chief people officer", "chief hr officer"],
    "CMO": ["cmo", "chief marketing officer"],
}

# Words to strip from LinkedIn URL slugs (titles, credentials, etc.)
_SLUG_STRIP_WORDS = frozenset(
    {
        "ceo",
        "cfo",
        "coo",
        "cto",
        "cio",
        "chro",
        "cmo",
        "president",
        "founder",
        "cofounder",
        "co",
        "director",
        "vp",
        "svp",
        "evp",
        "chief",
        "officer",
        "executive",
        "managing",
        "partner",
        "chairman",
        "mba",
        "phd",
        "cpa",
        "mpa",
        "jd",
        "md",
        "pmp",
        "cfa",
        "cfp",
        "sphr",
        "shrm",
        "cissp",
        "inc",
        "llc",
        "ltd",
    }
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class DiscoveredPerson:
    first_name: str
    last_name: str
    title: str  # e.g. "CEO", "CFO"
    source_url: str  # LinkedIn URL
    confidence: str  # "high" | "medium" | "low"
    raw_snippet: str = ""  # Original search result text for audit


@dataclass
class CompanyDiscoveryResult:
    company_name: str
    domain: str
    people: list[DiscoveredPerson] = field(default_factory=list)
    queries_used: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def _get_api_key() -> str:
    """Return Serper API key from env var."""
    api_key = os.getenv("SERPER_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SERPER_API_KEY must be set.")
    return api_key


def is_api_configured() -> bool:
    """Check whether Serper API key is present (non-empty)."""
    return bool(os.getenv("SERPER_API_KEY", "").strip())


# ---------------------------------------------------------------------------
# Serper Search API
# ---------------------------------------------------------------------------


def search_linkedin_for_role(
    company_name: str,
    role: str,
    *,
    api_key: str,
    num_results: int = 3,
) -> list[dict]:
    """
    Search Google via Serper for LinkedIn profiles:
      site:linkedin.com/in trycents.com CEO OR Chief Executive Officer

    Nothing is quoted — Google matches flexibly and still returns the
    right LinkedIn profiles.

    Returns a list of result dicts with 'title', 'link', 'snippet' keys
    matching the shape expected by downstream code.
    """
    # Get the expanded title form (e.g. CEO -> "Chief Executive Officer")
    expansion = _ROLE_EXPANSIONS.get(role.upper(), [])
    expanded = expansion[1] if len(expansion) >= 2 else ""

    # Nothing quoted — let Google match flexibly
    if expanded:
        query = f"site:linkedin.com/in {company_name} {role} OR {expanded}"
    else:
        query = f"site:linkedin.com/in {company_name} {role}"
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "q": query,
        "num": min(num_results, 10),
        "gl": "us",
        "hl": "en",
    }

    with httpx.Client(timeout=15.0) as client:
        resp = client.post(
            SERPER_ENDPOINT,
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    # Serper returns results under "organic" key.
    # Normalize to match the field names downstream code expects.
    results = []
    for item in data.get("organic", []):
        results.append(
            {
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
            }
        )
    return results


# ---------------------------------------------------------------------------
# Name extraction from LinkedIn URLs
# ---------------------------------------------------------------------------


def parse_linkedin_name(url: str) -> tuple[str, str] | None:
    """
    Extract (first_name, last_name) from a LinkedIn URL slug.

    Example: /in/john-doe-ceo-12345 -> ("John", "Doe")

    Strips trailing numeric IDs and common title/credential words.
    """
    m = re.search(r"linkedin\.com/in/([^/?#]+)", url)
    if not m:
        return None

    slug = unquote(m.group(1)).lower()
    parts = slug.split("-")

    # Remove trailing numeric/hex ID segments (e.g., "1a2b3c4d")
    while parts and re.match(r"^[0-9a-f]{4,}$", parts[-1]):
        parts.pop()

    # Remove known title/credential suffixes from the end
    while parts and parts[-1] in _SLUG_STRIP_WORDS:
        parts.pop()

    # Remove known title/credential words from the beginning too
    while parts and parts[0] in _SLUG_STRIP_WORDS:
        parts.pop(0)

    if len(parts) >= 2:
        return (parts[0].capitalize(), parts[1].capitalize())

    return None


def parse_name_from_title(title: str) -> tuple[str, str] | None:
    """
    Extract name from a Google search result title.

    Typical format: "John Doe - CEO - Acme Corp | LinkedIn"
    """
    # Remove " | LinkedIn" or " - LinkedIn" suffix
    clean = re.split(r"\s*[|\u2013\u2014-]\s*LinkedIn", title, flags=re.IGNORECASE)[0]

    # Split on separators to get name portion (usually the first segment)
    parts = re.split(r"\s*[\u2013\u2014-]\s*", clean)
    if parts:
        name_str = parts[0].strip()
        name_parts = name_str.split()
        if len(name_parts) >= 2:
            first = name_parts[0]
            last = name_parts[-1]
            # Skip if they look like titles rather than names
            if first.lower() not in _SLUG_STRIP_WORDS and last.lower() not in _SLUG_STRIP_WORDS:
                return (first, last)

    return None


# ---------------------------------------------------------------------------
# Result validation
# ---------------------------------------------------------------------------


def _derive_search_name(company_name: str, domain: str) -> str:
    """
    Derive the best company name to use in the Serper search query.

    If the company name is a real multi-word name, use it as-is.
    Otherwise use the full domain (including .com) since Google still
    finds the right results — e.g. "ceo trycents.com" works fine.

    Examples:
      ("trycents.com", "trycents.com") → "trycents.com"
      ("camber.health", "camber.health") → "camber.health"
      ("Goldman Sachs", "goldmansachs.com") → "Goldman Sachs"
    """
    name_lower = company_name.strip().lower()

    # Check if the company name is a real multi-word name (not a domain).
    # Strip any TLD first to compare.
    name_no_tld = name_lower
    for tld in (".com", ".org", ".net", ".io", ".ai", ".co", ".dev", ".work"):
        name_no_tld = name_no_tld.removesuffix(tld)
    # Also strip the domain's TLD
    for tld in ("." + domain.lower().split(".")[-1],) if "." in domain else ():
        name_no_tld = name_no_tld.removesuffix(tld)

    domain_base = domain.lower().split(".")[0] if "." in domain else domain.lower()

    # If the name has spaces and doesn't look like the domain, it's a proper name
    if " " in name_no_tld and name_no_tld.replace(" ", "") != domain_base:
        return company_name.strip()

    # The name is basically the domain — use the full domain as the search term
    return domain.lower()


def _company_keywords(company_name: str, domain: str) -> list[str]:
    """
    Build a list of keywords to check search results against.

    For company_name="Camber Health" and domain="camber.health" this returns
    ["camber", "health"].
    For company_name="trycents.com" and domain="trycents.com" this returns
    ["trycents"].
    """
    keywords: list[str] = []

    # Common TLDs that are useless as matching keywords
    _skip_tlds = {"com", "org", "net", "io", "ai", "co", "dev", "work", "app"}

    # From the search name (multi-word company names)
    search_name = _derive_search_name(company_name, domain)
    for word in search_name.lower().replace(".", " ").split():
        word = word.strip(".,;:!?\"'()-")
        if (
            len(word) >= 2
            and word not in _SLUG_STRIP_WORDS
            and word not in _skip_tlds
            and word not in keywords
        ):
            keywords.append(word)

    # Domain base as fallback
    domain_base = domain.lower().split(".")[0] if "." in domain else domain.lower()
    if len(domain_base) >= 2 and domain_base not in keywords:
        keywords.append(domain_base)

    return keywords


def _extract_company_from_title(title: str) -> str:
    """
    Extract the company name portion from a LinkedIn search result title.

    LinkedIn titles typically follow:
      "John Doe - CEO - Acme Corp | LinkedIn"
      "Jane Smith - Chief Financial Officer at BigCo | LinkedIn"

    Returns the company portion lowercased, or empty string if unparseable.
    """
    # Remove " | LinkedIn" or " - LinkedIn" suffix
    clean = re.split(r"\s*[|\u2013\u2014-]\s*LinkedIn", title, flags=re.IGNORECASE)[0]

    # Split on separators (dash, en-dash, em-dash)
    segments = re.split(r"\s*[\u2013\u2014-]\s*", clean)

    # Company is usually the last segment (after name and title)
    # Format: "Name - Role - Company" → segments[-1] is company
    if len(segments) >= 3:
        return segments[-1].strip().lower()

    # Sometimes: "Name - Role at Company"
    if len(segments) >= 2:
        role_part = segments[-1].strip()
        at_match = re.search(r"\bat\s+(.+)$", role_part, re.IGNORECASE)
        if at_match:
            return at_match.group(1).strip().lower()

    return ""


def _result_matches_company(
    item: dict,
    keywords: list[str],
) -> bool:
    """
    Strictly validate that a LinkedIn result belongs to the target company.

    Extracts the company name from the LinkedIn title and checks if our
    company keywords appear in it. Falls back to checking the full
    title + snippet if the title can't be parsed.
    """
    if not keywords:
        return True  # Can't validate, let it through

    title = item.get("title", "")

    # Primary: check the company field extracted from the LinkedIn title
    company_from_title = _extract_company_from_title(title)
    if company_from_title:
        return any(kw in company_from_title for kw in keywords)

    # Fallback: if we couldn't parse a company from the title,
    # check the full title + snippet (less strict but better than nothing)
    text = (title + " " + item.get("snippet", "")).lower()
    return any(kw in text for kw in keywords)


def _extract_role_from_title(title: str) -> str:
    """
    Extract the role/title portion from a LinkedIn search result title.

    LinkedIn titles typically follow:
      "John Doe - CEO - Acme Corp | LinkedIn"
      "Jane Smith - Chief Financial Officer at BigCo | LinkedIn"

    The role is the SECOND segment (index 1) in a 3-segment title,
    or the last segment (which may contain "at Company") in a 2-segment title.

    Returns the role portion lowercased, or empty string if unparseable.
    """
    # Remove " | LinkedIn" or " - LinkedIn" suffix
    clean = re.split(r"\s*[|\u2013\u2014-]\s*LinkedIn", title, flags=re.IGNORECASE)[0]

    # Split on separators (dash, en-dash, em-dash)
    segments = re.split(r"\s*[\u2013\u2014-]\s*", clean)

    # "Name - Role - Company" → segments[1] is the role
    if len(segments) >= 3:
        return segments[1].strip().lower()

    # "Name - Role at Company" → strip the "at Company" part
    if len(segments) >= 2:
        role_part = segments[-1].strip()
        at_match = re.search(r"^(.+?)\s+at\s+", role_part, re.IGNORECASE)
        if at_match:
            return at_match.group(1).strip().lower()
        return role_part.lower()

    return ""


def _result_matches_role(item: dict, role: str) -> bool:
    """
    Validate that a LinkedIn result actually has the target C-suite role.

    Extracts the role segment from the title and checks if the target role
    abbreviation or its expanded form appears in it.
    """
    expansions = _ROLE_EXPANSIONS.get(role.upper(), [role.lower()])

    title = item.get("title", "")
    role_from_title = _extract_role_from_title(title)

    if role_from_title:
        return any(exp in role_from_title for exp in expansions)

    # Fallback: check full title + snippet
    text = (title + " " + item.get("snippet", "")).lower()
    return any(exp in text for exp in expansions)


# ---------------------------------------------------------------------------
# Person extraction helper
# ---------------------------------------------------------------------------


def _extract_person(
    item: dict,
    title: str,
    base_confidence: str,
    seen_names: set[str],
) -> DiscoveredPerson | None:
    """
    Try to extract a DiscoveredPerson from a search result item.

    Parses the name from the LinkedIn URL slug or the result title.
    Returns None if no name can be extracted or the person was already seen.
    Mutates *seen_names* on success.
    """
    link = item.get("link", "")

    # Try URL slug first (highest confidence)
    name = parse_linkedin_name(link)
    confidence = base_confidence if name else "low"

    # Fallback: try the result title
    if not name:
        name = parse_name_from_title(item.get("title", ""))
        confidence = "medium" if name else "low"

    if not name:
        return None

    # Deduplicate by name
    name_key = f"{name[0].lower()}:{name[1].lower()}"
    if name_key in seen_names:
        return None
    seen_names.add(name_key)

    return DiscoveredPerson(
        first_name=name[0],
        last_name=name[1],
        title=title,
        source_url=link,
        confidence=confidence,
        raw_snippet=item.get("snippet", ""),
    )


# ---------------------------------------------------------------------------
# Company-level discovery
# ---------------------------------------------------------------------------


def discover_people_for_company(
    company_name: str,
    domain: str,
    roles: list[str] | None = None,
) -> CompanyDiscoveryResult:
    """
    Search Serper for C-suite LinkedIn profiles at a company.

    Returns a CompanyDiscoveryResult with all discovered people.
    """
    api_key = _get_api_key()
    target_roles = roles or CSUITE_ROLES
    search_name = _derive_search_name(company_name, domain)
    result = CompanyDiscoveryResult(company_name=company_name, domain=domain)
    keywords = _company_keywords(company_name, domain)

    log.info(
        "Discovery for %s (domain=%s) search_name=%r keywords=%s",
        company_name,
        domain,
        search_name,
        keywords,
    )

    seen_urls: set[str] = set()
    seen_names: set[str] = set()

    for role in target_roles:
        try:
            items = search_linkedin_for_role(
                search_name,
                role,
                api_key=api_key,
                num_results=3,
            )
            result.queries_used += 1

            accepted_for_role = 0
            # Track the first company-matched result as a fallback
            # in case nobody passes the role check
            fallback_candidate: dict | None = None

            for item in items:
                # Only take the top matching result per role
                if accepted_for_role >= 1:
                    break

                link = item.get("link", "")

                # Skip non-LinkedIn URLs
                if "linkedin.com/in/" not in link:
                    continue

                # Skip duplicate URLs
                if link in seen_urls:
                    continue

                # Company in the LinkedIn title must match our target
                if not _result_matches_company(item, keywords):
                    log.debug(
                        "Skipping result (company mismatch): title=%r for %s %s",
                        item.get("title", ""),
                        search_name,
                        role,
                    )
                    continue

                # Remember first company-matched result as fallback
                if fallback_candidate is None:
                    fallback_candidate = item

                # Check if the role also matches
                if not _result_matches_role(item, role):
                    log.debug(
                        "Skipping result (role mismatch): title=%r expected %s",
                        item.get("title", ""),
                        role,
                    )
                    continue

                # Both company and role match — accept with the role
                seen_urls.add(link)
                person = _extract_person(item, role, "high", seen_names)
                if person:
                    result.people.append(person)
                    accepted_for_role += 1

            # Fallback: if no one passed the role check but we had a
            # company match, insert that person with an empty title
            # so we at least capture them for manual review
            if accepted_for_role == 0 and fallback_candidate is not None:
                link = fallback_candidate.get("link", "")
                if link not in seen_urls:
                    seen_urls.add(link)
                    person = _extract_person(fallback_candidate, "", "low", seen_names)
                    if person:
                        log.info(
                            "Fallback insert (no role match): %s %s for %s %s",
                            person.first_name,
                            person.last_name,
                            search_name,
                            role,
                        )
                        result.people.append(person)

        except Exception as exc:
            result.errors.append(f"{role}: {exc}")
            log.warning(
                "Serper search failed for %s %s: %s",
                search_name,
                role,
                exc,
            )

    return result
