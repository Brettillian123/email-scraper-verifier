"""
R11 / O05 — HTML candidate extractor (broad, AI-refiner friendly).

Given an HTML document, try to find:
  - email addresses that plausibly belong to the target org
  - any nearby human-looking name (when available)
  - lightweight context around the email

and return a list of Candidate objects.

This stage is now intentionally higher-recall: we keep most on-domain
emails and attach light metadata, then let the AI refiner (O27) decide
which rows are real people. We still apply a few hard guards to avoid
obvious garbage (wrong domains, clearly non-email strings, etc.).
"""

# src/extract/candidates.py
from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from html import unescape
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

from .stopwords import NAME_STOPWORDS

# --- Public data model -------------------------------------------------------


@dataclass
class Candidate:
    """
    A candidate email + optional person-ish metadata extracted from HTML.

    Fields:
      - email: normalized email address (lowercased) when available, else None
      - source_url: the page where the candidate was found
      - first_name / last_name: optional parsed names when confidently available
      - raw_name: the raw name text captured before normalization/splitting
      - title: optional nearby title/role text (not yet populated by R11)
      - source_type: rough type of HTML source (e.g., "mailto_link", "link", "text")
      - context_snippet: small snippet of nearby text for AI to reason about
      - is_role_address_guess: True when the local-part looks like a role/alias
    """

    email: str | None
    source_url: str
    first_name: str | None = None
    last_name: str | None = None
    raw_name: str | None = None
    title: str | None = None
    source_type: str | None = None
    context_snippet: str | None = None
    is_role_address_guess: bool = False


# --- Heuristics & regexes ----------------------------------------------------

EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)

_OBFUSCATION_MARKERS: tuple[str, ...] = (" at ", "[at]", "(at)", " dot ", "[dot]", "(dot)")

# Very generic localparts that are almost always role/placeholder inboxes.
ROLE_ALIASES: set[str] = {
    "info",
    "contact",
    "hello",
    "hi",
    "support",
    "help",
    "success",
    "customersuccess",
    "cs",
    "sales",
    "marketing",
    "growth",
    "press",
    "media",
    "pr",
    "jobs",
    "job",
    "careers",
    "career",
    "hiring",
    "recruiting",
    "recruiter",
    "talent",
    "hr",
    "billing",
    "payments",
    "invoices",
    "accounts",
    "accounting",
    "finance",
    "legal",
    "privacy",
    "security",
    "abuse",
    "postmaster",
    "webmaster",
    "admin",
    "administrator",
    "root",
    "system",
    "no-reply",
    "noreply",
    "donotreply",
    "newsletter",
    "news",
    "updates",
    "alerts",
    "notifications",
    "notify",
    "bounce",
    "mailer-daemon",
    "team",
    "office",
    "partners",
    "partner",
    "founders",
    "founder",
    "example",  # treat example@ as a placeholder/role inbox
}

# Tokens that usually indicate a business concept/section rather than a person.
_NON_NAME_TOKENS: set[str] = {
    "executive",
    "talent",
    "building",
    "deliver",
    "delivery",
    "pipeline",
    "revenue",
    "growth",
    "demand",
    "solutions",
    "solution",
    "ventures",
    "capital",
    "partners",
    "partner",
    "group",
    "agency",
    "studio",
    "media",
    "services",
    "service",
    "consulting",
    "advisors",
    "advisory",
    "operations",
    "success",
    "customer",
    "clients",
    "accounts",
    "accounting",
    "leadership",
    "management",
    "team",
    "office",
    "board",
    "committee",
    "council",
}

# Common name suffixes we should ignore when counting tokens
_NAME_SUFFIXES: set[str] = {
    "jr",
    "sr",
    "ii",
    "iii",
    "iv",
    "cpa",
    "mba",
    "phd",
    "esq",
}

# Extra "this is clearly not a person" words for generation purposes.
# We start with the global NAME_STOPWORDS loaded from name_stopwords.txt,
# and add a handful of very specific extras that may not appear there.
GENERATION_NAME_STOPWORDS: set[str] = set(NAME_STOPWORDS) | {
    # generic non-person / section words
    "welcome",
    "team",
    "office",
    "info",
    "support",
    "example",
    "contact",
    "admin",
    "marketing",
    "billing",
    "hello",
    "hi",
    "building",
    "deliver",
    "delivery",
    "executive",
    "executives",
    "talent",
    "solution",
    "solutions",
    "partner",
    "partners",
    "growth",
    "strategy",
    "strategic",
    "turnaround",
    "restructuring",
    "pricing",
    "price",
    "resources",
    "resource",
    "service",
    "services",
    "company",
    "about",
    "blog",
    "careers",
    "leadership",
    # navigation / section labels (Brandt + generic)
    "home",
    "our",
    "firm",
    "disclaimers",
    "privacy",
    "policy",
    "documents",
    "document",
    "payroll",
    "contractor",
    "testimonials",
    "testimonial",
    "more",
    "payment",
    "payments",
    "links",
    "link",
    "useful",
    "login",
    "log",
    "in",
    "out",
    # generic service / category words
    "individual",
    "business",
    "preparation",
    "tax",
    "bookkeeping",
    "clients",
    "client",
    # degrees / credentials / titles (we treat as non-name tokens)
    "cpa",
    "mba",
    "esq",
    "phd",
    "md",
    "jd",
    "certified",
    "public",
    "accountant",
    "advisor",
    "advisors",
    "manager",
    "director",
    "principal",
    "owner",
    "founder",
    # Crestwell-specific marketing/tagline words we've seen
    "ambitious",
    "efficient",
    "dedicated",
    "specialized",
    "expertise",
    "fractional",
    "readiness",
    "quiz",
    "founderled",
    "founder-led",
    "process",
    "how",
    "works",
    "take",
    "book",
    "call",
    "option",
    "options",
    "faq",
    "terms",
    # LinkedIn pseudo-person
    "linkedin",
    "view",
    # Community Advisor pseudo-person
    "community",
}

# Tokens that are common degree / credential suffixes on names.
_NAME_DEGREES: set[str] = {
    "cpa",
    "mba",
    "phd",
    "md",
    "esq",
    "jd",
    "llm",
    "llb",
    "cfa",
    "cma",
    "cisa",
    "pe",
}

# Used by some simple quick checks in other modules
_NAME_SHAPE_RE = re.compile(r"^[A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){0,3}$")


# =============================================================================
# ISSUE 2 FIX: Unicode escape decoder for emails from JSON/JS
# =============================================================================

# Pattern to match uXXXX at the start of a string (without backslash)
_UNICODE_ESCAPE_START_RE = re.compile(r"^u([0-9a-fA-F]{4})")


def _decode_unicode_escapes(s: str) -> str:
    """
    Decode Unicode escape sequences that appear in extracted emails.

    Handles two cases:
    1. Proper escapes: \\u003e -> >
    2. Broken escapes: u003e at start of string -> >

    This fixes emails extracted from inline JSON/JavaScript where
    characters like < and > are Unicode-escaped.

    Examples:
        "u003ehr@outreach.io" -> "hr@outreach.io"
        "\\u003chr@outreach.io" -> "<hr@outreach.io" -> "hr@outreach.io"
    """
    if not s:
        return s

    # Case 1: Handle proper \uXXXX escapes (backslash present)
    if "\\u" in s:
        try:
            s = s.encode("utf-8").decode("unicode_escape")
        except Exception:
            pass

    # Case 2: Handle "uXXXX" without backslash at START of string
    # Loop in case there are multiple (unlikely but defensive)
    max_iterations = 5  # Prevent infinite loop
    for _ in range(max_iterations):
        match = _UNICODE_ESCAPE_START_RE.match(s)
        if not match:
            break
        try:
            char = chr(int(match.group(1), 16))
            s = char + s[5:]  # Replace uXXXX with the character
        except Exception:
            break

    # Strip any decoded < or > characters from edges (these are HTML artifacts)
    s = s.strip("<>").strip()

    return s


def _deobfuscate_email_text(text: str) -> str:
    """
    De-obfuscate common anti-scraping email patterns.

    Examples:
        "john [at] acme [dot] com" -> "john@acme.com"
        "mary (at) acme dot co dot uk" -> "mary@acme.co.uk"
        "bob at acme dot com" -> "bob@acme.com"
    """
    if not text:
        return text

    s = text

    # Replace [at], (at), " at " with @
    s = re.sub(r"\s*[\[\(]?\s*at\s*[\]\)]?\s*", "@", s, flags=re.IGNORECASE)

    # Replace [dot], (dot), " dot " with .
    s = re.sub(r"\s*[\[\(]?\s*dot\s*[\]\)]?\s*", ".", s, flags=re.IGNORECASE)

    # Clean up whitespace
    s = " ".join(s.split())

    return s


# --- Internal helpers (also used by demo_autodiscovery.py) -------------------


def normalize_generated_name(raw: str) -> str | None:
    """
    Take a heading like 'BRITTANY BRANDT, CPA' or 'CERTIFIED PUBLIC ACCOUNTANT'
    and either:
      - return a cleaned person name like 'Brittany Brandt', or
      - return None if it's not a real person (Our Firm, Useful Links, etc).
    """
    if not raw:
        return None

    # Normalize whitespace + strip weird characters but keep letters, spaces, and comma
    cleaned = re.sub(r"[^A-Za-z ,]+", " ", raw)
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return None

    # If there is a comma, almost always "Name, CPA/MBA/..." → keep the left part
    if "," in cleaned:
        cleaned, _ = cleaned.split(",", 1)
        cleaned = cleaned.strip()
        if not cleaned:
            return None

    words = cleaned.split()
    if not words:
        return None

    kept: list[str] = []
    for w in words:
        wl = w.lower()
        # Drop obvious non-name tokens
        if wl in GENERATION_NAME_STOPWORDS:
            continue
        if len(wl) <= 1:
            continue
        if not wl.isalpha():
            continue
        kept.append(w.title())

    # Require at least first + last name to consider this a "real person"
    if len(kept) < 2:
        return None

    return " ".join(kept)


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_punctuation(text: str) -> str:
    # Keep apostrophes and hyphens, drop trailing commas, pipes, etc.
    return text.strip(" \t\r\n,;:|/·•")


def _is_concepty_token(tok: str) -> bool:
    t = tok.lower()
    if t in _NON_NAME_TOKENS or t in NAME_STOPWORDS:
        return True
    # Common endings for abstract/business nouns
    if len(t) >= 6 and (
        t.endswith("ing")
        or t.endswith("ment")
        or t.endswith("tion")
        or t.endswith("sion")
        or t.endswith("ship")
        or t.endswith("ness")
    ):
        return True
    # Obvious abbreviations / entities
    if t in {"llc", "inc", "ltd", "corp", "co", "gmbh", "sa", "plc", "srl"}:
        return True
    return False


def _looks_human_name(text: str) -> bool:
    """
    Very conservative "is this string plausibly a human full name?" check.

    Goals:
      - Accept: "Brett Anderson", "BRITTANY BRANDT, CPA", "Ana-Maria O'Neil"
      - Reject: "Executive Talent", "Our Firm", "Useful Links", "Log In",
                "Certified Public Accountant", "Accountant and Tax Advisor"
    """
    if not text:
        return False

    s = _normalize_space(text)
    if not s:
        return False

    # Quick hard guards
    if "@" in s:
        return False
    if any(ch.isdigit() for ch in s):
        return False

    # Strip any emails that slipped into the label
    s = EMAIL_RE.sub(" ", s)

    # Strip obvious trailing sections like "- VP Sales" or "| Sales"
    s = re.split(r"[|/·•]", s, maxsplit=1)[0]
    s = re.split(r"\s+-\s+", s, maxsplit=1)[0]
    s = re.split(r"\s+–\s+", s, maxsplit=1)[0]
    s = _strip_punctuation(s)

    raw_tokens = [t for t in s.split() if t]
    # We want "First Last" or "First Middle Last" + optional suffix (CPA, MBA)
    if len(raw_tokens) < 2 or len(raw_tokens) > 5:
        return False

    core_tokens: list[str] = []
    name_like_count = 0  # tokens that look like actual name parts

    for tok in raw_tokens:
        tok_clean = _strip_punctuation(tok)
        if not tok_clean:
            continue

        low = tok_clean.lower()

        # Ignore simple suffixes when counting tokens (Jr, Sr, II, III, IV, CPA, MBA, etc.).
        if low in _NAME_SUFFIXES:
            continue

        # Treat stopwords / concept tokens as non-name words, but do NOT
        # immediately reject the whole string; we'll require that at least
        # 2 tokens are *not* in these sets.
        if low in GENERATION_NAME_STOPWORDS or low in _NON_NAME_TOKENS:
            core_tokens.append(tok_clean)
            continue

        # Normalize all-caps names to title case:
        if tok_clean.isupper() and len(tok_clean) > 3:
            tok_norm = tok_clean.title()
        else:
            tok_norm = tok_clean

        # Basic charset / shape check: letters + ' . -
        if not re.match(r"^[A-Za-z][A-Za-z'.-]*$", tok_norm):
            return False

        # Strong bias toward title-case: "Brett", "Anderson"
        if not tok_norm[0].isupper():
            return False

        core_tokens.append(tok_norm)
        name_like_count += 1

    # After removing suffixes and non-name words, we want at least 2 "real"
    # name-like tokens (e.g., first + last).
    if name_like_count < 2:
        return False

    # And we still don't want absurdly long phrases after stripping suffixes.
    if len(core_tokens) > 4:
        return False

    return True


def _split_first_last(full_name: str) -> tuple[str | None, str | None]:
    """
    Simple first/last splitter.

    Handles middle initials: "John Q. Public" -> ("John", "Public")
    Strips trailing periods from tokens.

    Examples:
        "John Public" -> ("John", "Public")
        "John Q. Public" -> ("John", "Public")
        "John Q Public" -> ("John", "Public")
        "John A. B. Public" -> ("John", "Public")
        "Mary" -> ("Mary", None)
    """
    s = _normalize_space(full_name)
    if not s:
        return None, None
    parts = s.split()
    if len(parts) == 1:
        return parts[0].rstrip("."), None

    # First token is the first name
    first = parts[0].rstrip(".")

    # Find last name by skipping middle initials
    # A middle initial is a single character optionally followed by a period
    non_initials = []
    for p in parts[1:]:
        cleaned = p.rstrip(".")
        # Skip if it's a single character (middle initial)
        if len(cleaned) == 1 and cleaned.isalpha():
            continue
        non_initials.append(cleaned)

    if non_initials:
        return first, " ".join(non_initials)

    # All remaining parts were initials, return the last one
    return first, parts[-1].rstrip(".")


def _name_from_local_part(local: str) -> tuple[str | None, str | None]:
    """
    Extract first/last name from email local-part.

    Examples:
        "jane" -> ("Jane", None)
        "jane.doe" -> ("Jane", "Doe")
        "j.doe" -> ("J", "Doe")  # keep single initials
        "jane-doe" -> ("Jane", "Doe")
        "jane_doe" -> ("Jane", "Doe")
    """
    if not local:
        return None, None

    # Split on common separators
    parts = re.split(r"[._-]", local)
    parts = [p.strip() for p in parts if p.strip()]

    if not parts:
        return None, None

    if len(parts) == 1:
        # Single token: title case it
        return parts[0].title(), None

    # Multiple tokens: title case first and last
    return parts[0].title(), parts[-1].title()


def _choose_name_piece(text: str) -> str:
    """
    From a noisy label like:

        "Brett Anderson – VP Sales"
        "Brett Anderson, VP Sales"
        "VP Sales – Brett Anderson"

    try to pick the part that looks like a human name.

    Returns "" if we don't find a plausible name segment.
    """
    if not text:
        return ""

    t = unescape(text)
    t = _normalize_space(t)

    # Strip email addresses out of the text entirely.
    t = EMAIL_RE.sub(" ", t)

    # Split on hard separators first: pipes, slashes, bullets
    segments: list[str] = []
    for seg in re.split(r"[|/·•]", t):
        seg = seg.strip()
        if not seg:
            continue
        segments.append(seg)

    candidates: list[str] = []

    for seg in segments:
        # Within each segment, split on commas and dashes.
        pieces = re.split(r"[,;/]", seg)
        more: list[str] = []
        for p in pieces:
            more.extend(re.split(r"\s+-\s+|\s+–\s+", p))
        if not more:
            more = [seg]

        for piece in more:
            piece = _strip_punctuation(piece)
            if not piece:
                continue
            if _looks_human_name(piece):
                candidates.append(piece)

    if candidates:
        # Prefer the earliest name-y piece.
        return candidates[0]

    # No strong candidate → as a last resort, if *entire* string looks like a
    # name, take it; otherwise, return empty so callers can treat as "no name".
    if _looks_human_name(t):
        return t

    return ""


def _normalize_email(raw: str) -> str | None:
    """
    Extract and normalize an email from a string like:
      "mailto:brett.anderson@crestwellpartners.com?subject=Hello"

    Also handles Unicode-escaped emails from JSON/JS like:
      "u003ehr@outreach.io" -> "hr@outreach.io"
    """
    if not raw:
        return None

    # Decode Unicode escapes first
    s = _decode_unicode_escapes(raw)

    s = unquote(s).strip()
    # Strip mailto:
    if s.lower().startswith("mailto:"):
        s = s[7:]
    # Drop query params
    s = s.split("?", 1)[0].strip()

    # Strip any remaining < or > from edges
    s = s.strip("<>").strip()

    m = EMAIL_RE.search(s)
    if not m:
        return None
    return m.group(0).lower()


def _same_org(email_domain: str, official_domain: str | None, source_url: str) -> bool:
    """
    Decide whether an email domain is plausibly "part of" the target org.

    - If official_domain is None, skip filtering entirely (caller explicitly disabled it).
    - If official_domain is provided, require exact or subdomain match.
    - Otherwise, fall back to matching the source_url host.
    """
    # When official_domain is explicitly None, caller intends to disable
    # domain filtering entirely (used in tests and special cases).
    if official_domain is None:
        return True

    edom = email_domain.lower()
    if official_domain:
        base = official_domain.lower()
        return edom == base or edom.endswith("." + base)

    try:
        host = (urlparse(source_url).netloc or "").lower()
    except Exception:
        host = ""
    if not host:
        return True
    if host == edom:
        return True
    if host.endswith("." + edom) or edom.endswith("." + host):
        return True
    return False


def _text_has_obfuscation(text: str) -> bool:
    t = text.lower()
    return any(marker in t for marker in _OBFUSCATION_MARKERS)


def _iter_mailto_and_href_emails(soup: BeautifulSoup, seen: set[str]) -> Iterator[tuple[str, Tag]]:
    for a in soup.find_all("a", href=True):
        em = _normalize_email(a["href"])
        if em and em not in seen:
            seen.add(em)
            yield em, a
            continue

        href_decoded = _decode_unicode_escapes(a["href"])
        m = EMAIL_RE.search(href_decoded)
        if not m:
            continue
        em2 = m.group(0).lower()
        if em2 not in seen:
            seen.add(em2)
            yield em2, a


def _iter_attribute_emails(soup: BeautifulSoup, seen: set[str]) -> Iterator[tuple[str, Tag]]:
    for tag in soup.find_all(True):
        for attr_val in tag.attrs.values():
            if not isinstance(attr_val, str):
                continue
            if "@" not in attr_val:
                continue
            decoded_val = _decode_unicode_escapes(attr_val)
            for m in EMAIL_RE.finditer(decoded_val):
                em = m.group(0).lower()
                if em not in seen:
                    seen.add(em)
                    yield em, tag


def _iter_text_node_emails(
    soup: BeautifulSoup, *, deobfuscate: bool, seen: set[str]
) -> Iterator[tuple[str, NavigableString]]:
    for node in soup.find_all(string=True):
        text = str(node)

        has_at = "@" in text
        has_obfuscation = deobfuscate and (not has_at) and _text_has_obfuscation(text)

        if not has_at and not has_obfuscation:
            continue

        decoded_text = _decode_unicode_escapes(text)

        if deobfuscate and (has_obfuscation or _text_has_obfuscation(decoded_text)):
            decoded_text = _deobfuscate_email_text(decoded_text)

        for m in EMAIL_RE.finditer(decoded_text):
            em = m.group(0).lower()
            if em not in seen:
                seen.add(em)
                yield em, node


def _iter_email_nodes(
    soup: BeautifulSoup, *, deobfuscate: bool = False
) -> Iterator[tuple[str, Tag | NavigableString]]:
    """
    Yield (email, node) pairs from:
      - <a href="mailto:...">
      - any attribute containing an email
      - plain text nodes
    """
    seen: set[str] = set()

    yield from _iter_mailto_and_href_emails(soup, seen)
    yield from _iter_attribute_emails(soup, seen)
    yield from _iter_text_node_emails(soup, deobfuscate=deobfuscate, seen=seen)


def _extract_name_near_node(node: Tag | NavigableString) -> str | None:
    """
    Try a few cheap local-context tricks around the email node to find
    a human-looking name.

    Priority:
      1) Text of the parent element (minus the email itself).
      2) Previous sibling text.
      3) Previous heading in the same section.
    """

    def clean(txt: str) -> str:
        return _normalize_space(EMAIL_RE.sub(" ", unescape(txt or "")))

    # If <a> node or similar, use its text.
    if isinstance(node, Tag):
        text = clean(node.get_text(" ", strip=True))
        piece = _choose_name_piece(text)
        if piece:
            return piece

        # Look at parent
        parent = node.parent
        if parent is not None and isinstance(parent, Tag):
            text = clean(parent.get_text(" ", strip=True))
            piece = _choose_name_piece(text)
            if piece:
                return piece

            # Previous sibling text
            prev = parent.previous_sibling
            while isinstance(prev, NavigableString) and not prev.strip():
                prev = prev.previous_sibling
            if isinstance(prev, NavigableString):
                text = clean(str(prev))
                piece = _choose_name_piece(text)
                if piece:
                    return piece
            elif isinstance(prev, Tag):
                text = clean(prev.get_text(" ", strip=True))
                piece = _choose_name_piece(text)
                if piece:
                    return piece

            # Walk backwards to find the nearest heading tag
            for sib in parent.find_all_previous(["h1", "h2", "h3", "h4", "h5", "h6"]):
                text = clean(sib.get_text(" ", strip=True))
                piece = _choose_name_piece(text)
                if piece:
                    return piece

    # If node is just a NavigableString, see the parent.
    if isinstance(node, NavigableString):
        parent = node.parent
        if isinstance(parent, Tag):
            text = clean(parent.get_text(" ", strip=True))
            piece = _choose_name_piece(text)
            if piece:
                return piece

    return None


def _local_part_role_like(local: str) -> bool:
    """
    Heuristic: does this local-part look like a role/alias inbox?
    """
    local_lower = local.lower()
    if local_lower in ROLE_ALIASES:
        return True
    # Also handle e.g. "sales-team", "info_us", "support-emea"
    stripped = re.sub(r"[^a-z]", "", local_lower)
    for alias in ROLE_ALIASES:
        if alias in stripped:
            return True
    return False


def _local_context_snippet(node: Tag | NavigableString) -> str | None:
    """
    Build a small context snippet around the email for AI to reason about.
    """
    try:
        if isinstance(node, Tag):
            txt = node.get_text(" ", strip=True)
        else:
            txt = str(node)
        s = _normalize_space(unescape(txt or ""))
        if not s:
            return None
        # Truncate to keep tokens under control.
        return s[:280]
    except Exception:
        return None


def _source_type_for_node(node: Tag | NavigableString) -> str | None:
    """
    Rough source_type tag for diagnostics / AI context.
    """
    if isinstance(node, Tag):
        if node.name == "a":
            href = (node.get("href") or "").lower()
            if href.startswith("mailto:"):
                return "mailto_link"
            return "link"
        return node.name
    return "text"


# --- Main API ----------------------------------------------------------------


def extract_candidates(
    html: str,
    company_domain: str | None = None,
    *,
    deobfuscate: bool = False,
    source_url: str | None = None,
    official_domain: str | None = None,
) -> list[Candidate]:
    """
    Extract broad (email, optional name, context) candidates from a single HTML page.

    We:
      - Parse the DOM with BeautifulSoup.
      - Find emails via attributes + text.
      - Filter to the same org/domain when official_domain is supplied.
      - Attach:
          * is_role_address_guess based on local-part
          * best-effort human-looking name (when available)
          * small context snippet and source_type

    We deliberately do **not** try to decide if this is truly a person; that is
    the AI refiner's job. Downstream code uses:
      - role/placeholder heuristics to keep role inboxes at company-level, and
      - AI to filter/normalize person rows.
    """
    if not html:
        return []

    # Handle source_url default
    if source_url is None:
        source_url = "https://example.com/unknown"

    # Handle official_domain: prefer explicit, fall back to company_domain
    effective_domain = official_domain or company_domain

    soup = BeautifulSoup(html, "html.parser")

    # Per-page dedup: avoid blasting the AI with dozens of identical
    # office@example.com rows from the same page.
    by_key: dict[tuple[str | None, str, bool], Candidate] = {}

    for email, node in _iter_email_nodes(soup, deobfuscate=deobfuscate):
        local, _, dom = email.partition("@")
        if not local or not dom:
            continue

        # Filter by org/domain, if known.
        if not _same_org(dom, effective_domain, source_url):
            continue

        # Role-ness check - FILTER OUT role addresses entirely
        is_role_guess = _local_part_role_like(local)
        if is_role_guess:
            continue

        # Try to infer a name near this email (best-effort).
        raw_name = _extract_name_near_node(node)
        first_name: str | None = None
        last_name: str | None = None

        if raw_name and _looks_human_name(raw_name):
            first_name, last_name = _split_first_last(raw_name)
        else:
            # If the text we found does not pass the name heuristics,
            # try local-part fallback: jane.doe@domain.com -> Jane / Doe
            if not raw_name and local:
                first_name, last_name = _name_from_local_part(local)
                if first_name or last_name:
                    parts = [p for p in [first_name, last_name] if p]
                    raw_name = " ".join(parts) if parts else None
            else:
                raw_name = None

        context_snippet = _local_context_snippet(node)
        source_type = _source_type_for_node(node)

        candidate = Candidate(
            email=email.lower(),
            source_url=source_url,
            first_name=first_name,
            last_name=last_name,
            raw_name=raw_name,
            title=None,
            source_type=source_type,
            context_snippet=context_snippet,
            is_role_address_guess=is_role_guess,
        )

        key = (
            candidate.email,
            (candidate.raw_name or "").lower(),
            candidate.is_role_address_guess,
        )
        existing = by_key.get(key)

        if existing is None:
            by_key[key] = candidate
            continue

        # If we already have this (email, name, role-flag) on this page,
        # keep the "better" one by a simple heuristic:
        #   - prefer a candidate with a longer context snippet
        #   - otherwise keep the first one we saw
        existing_snip = existing.context_snippet or ""
        new_snip = candidate.context_snippet or ""
        if len(new_snip) > len(existing_snip):
            by_key[key] = candidate

    return list(by_key.values())
