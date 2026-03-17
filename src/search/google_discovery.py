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
      site:linkedin.com/in "Acme Corp" CEO

    Returns a list of result dicts with 'title', 'link', 'snippet' keys
    matching the shape expected by downstream code.
    """
    query = f'site:linkedin.com/in "{company_name}" {role}'
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


def _company_keywords(company_name: str, domain: str) -> list[str]:
    """
    Build a list of keywords to check search results against.

    For company_name="Traba" and domain="traba.work" this returns
    ["traba"] (deduplicated, lowercased, no TLD junk).
    """
    keywords: list[str] = []

    # From company name: split on whitespace, keep words 2+ chars
    for word in company_name.lower().split():
        word = word.strip(".,;:!?\"'()-")
        if len(word) >= 2 and word not in _SLUG_STRIP_WORDS:
            keywords.append(word)

    # From domain: strip TLD, split on dots/hyphens
    domain_base = domain.lower().split(".")[0] if "." in domain else domain.lower()
    for part in re.split(r"[-.]", domain_base):
        if len(part) >= 2 and part not in keywords:
            keywords.append(part)

    return keywords


def _result_mentions_company(
    item: dict,
    keywords: list[str],
) -> bool:
    """
    Check if a search result's title or snippet actually mentions the company.

    This filters out results where the person is at a different company but
    happened to appear in the search (e.g. someone who previously worked there).
    """
    if not keywords:
        return True  # Can't validate, let it through

    text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    return any(kw in text for kw in keywords)


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
    result = CompanyDiscoveryResult(company_name=company_name, domain=domain)
    keywords = _company_keywords(company_name, domain)

    seen_urls: set[str] = set()
    seen_names: set[str] = set()

    for role in target_roles:
        try:
            items = search_linkedin_for_role(
                company_name,
                role,
                api_key=api_key,
                num_results=3,
            )
            result.queries_used += 1

            accepted_for_role = 0
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
                seen_urls.add(link)

                # Validate: result must actually mention the company
                if not _result_mentions_company(item, keywords):
                    log.debug(
                        "Skipping result (no company match): %s for %s %s",
                        link,
                        company_name,
                        role,
                    )
                    continue

                # Try to extract name from URL slug first (highest confidence)
                name = parse_linkedin_name(link)
                confidence = "high" if name else "low"

                # Fallback: try the result title
                if not name:
                    name = parse_name_from_title(item.get("title", ""))
                    confidence = "medium" if name else "low"

                if not name:
                    continue

                # Deduplicate by name (same person might appear for
                # multiple roles)
                name_key = f"{name[0].lower()}:{name[1].lower()}"
                if name_key in seen_names:
                    continue
                seen_names.add(name_key)

                result.people.append(
                    DiscoveredPerson(
                        first_name=name[0],
                        last_name=name[1],
                        title=role,
                        source_url=link,
                        confidence=confidence,
                        raw_snippet=item.get("snippet", ""),
                    )
                )
                accepted_for_role += 1

        except Exception as exc:
            result.errors.append(f"{role}: {exc}")
            log.warning(
                "Serper search failed for %s %s: %s",
                company_name,
                role,
                exc,
            )

    return result
