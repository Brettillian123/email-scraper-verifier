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

import logging
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from html import unescape
from typing import Any
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

from src.extract.quality_gates import (
    is_placeholder_localpart,
    validate_person_name,
    validate_title,
)

from .stopwords import NAME_STOPWORDS

log = logging.getLogger(__name__)

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


def _qg_valid_reason(result: Any) -> tuple[bool, str | None]:
    """
    Normalize validate_* return types to (valid, reason).

    Supports:
      - (bool, reason)
      - (bool,)
      - objects with .is_valid and optional .rejection_reason/.reason
      - bool
    """
    if result is None:
        return False, None
    if isinstance(result, tuple):
        if not result:
            return False, None
        valid = bool(result[0])
        reason = None
        if len(result) >= 2:
            reason = str(result[1]) if result[1] is not None else None
        return valid, reason
    if isinstance(result, bool):
        return result, None
    if hasattr(result, "is_valid"):
        valid = bool(result.is_valid)
        reason = None
        if hasattr(result, "rejection_reason"):
            rr = result.rejection_reason
            reason = str(rr) if rr is not None else None
        elif hasattr(result, "reason"):
            rr = result.reason
            reason = str(rr) if rr is not None else None
        return valid, reason
    return False, None


def _should_keep_candidate(cand: Candidate) -> bool:
    """Apply quality gates to filter obvious garbage."""

    # Check placeholder email localparts (example@, test@, etc.)
    if cand.email:
        localpart = cand.email.split("@", 1)[0].lower()
        if is_placeholder_localpart(localpart):
            return False

    # Check name quality (if we have one)
    name = cand.raw_name or f"{cand.first_name or ''} {cand.last_name or ''}".strip()
    if name:
        valid, reason = _qg_valid_reason(validate_person_name(name))
        if not valid:
            log.debug("Rejecting candidate name=%r: %s", name, reason)
            return False

    # Check title quality (if present); clear bad title but keep candidate
    if cand.title:
        valid, reason = _qg_valid_reason(validate_title(cand.title))
        if not valid:
            log.debug("Rejecting candidate title=%r: %s", cand.title, reason)
            cand.title = None

    return True


# --- Heuristics & regexes ----------------------------------------------------

EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)

# Robust obfuscation detection (used to decide whether to attempt deobfuscation).
# We intentionally require BOTH an at-marker and dot-marker to reduce false positives.
_OBF_AT_RE = re.compile(r"(\b(at)\b|\[at\]|\(at\))", re.IGNORECASE)
_OBF_DOT_RE = re.compile(r"(\b(dot)\b|\[dot\]|\(dot\))", re.IGNORECASE)

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

    # Replace [at], (at), " at " with @ (allow no surrounding spaces)
    s = re.sub(r"\s*[\[\(]?\s*at\s*[\]\)]?\s*", "@", s, flags=re.IGNORECASE)

    # Replace [dot], (dot), " dot " with . (allow no surrounding spaces)
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
    """
    s = _normalize_space(full_name)
    if not s:
        return None, None
    parts = s.split()
    if len(parts) == 1:
        return parts[0].rstrip("."), None

    first = parts[0].rstrip(".")

    non_initials = []
    for p in parts[1:]:
        cleaned = p.rstrip(".")
        if len(cleaned) == 1 and cleaned.isalpha():
            continue
        non_initials.append(cleaned)

    if non_initials:
        return first, " ".join(non_initials)

    return first, parts[-1].rstrip(".")


def _name_from_local_part(local: str) -> tuple[str | None, str | None]:
    """
    Extract first/last name from email local-part, but ONLY when we have
    an explicit separator (., _, -). This avoids generating weak/incorrect
    "names" from single-token locals like 'john' or 'jdoe'.

    Examples:
        "jane.doe" -> ("Jane", "Doe")
        "jane-doe" -> ("Jane", "Doe")
        "jane_doe" -> ("Jane", "Doe")
        "jane" -> (None, None)
        "jdoe" -> (None, None)
    """
    if not local:
        return None, None

    if not any(sep in local for sep in (".", "_", "-")):
        return None, None

    parts = re.split(r"[._-]", local)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) < 2:
        return None, None

    first = parts[0].title()
    last = parts[-1].title()
    return first, last


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
        return candidates[0]

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
    if s.lower().startswith("mailto:"):
        s = s[7:]
    s = s.split("?", 1)[0].strip()
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
    t = (text or "").lower()
    if "@" in t:
        return False
    return bool(_OBF_AT_RE.search(t) and _OBF_DOT_RE.search(t))


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
            if "@" not in attr_val and not _text_has_obfuscation(attr_val):
                continue
            decoded_val = _decode_unicode_escapes(attr_val)
            if _text_has_obfuscation(decoded_val):
                decoded_val = _deobfuscate_email_text(decoded_val)
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
        has_obf = deobfuscate and (not has_at) and _text_has_obfuscation(text)

        if not has_at and not has_obf:
            continue

        decoded_text = _decode_unicode_escapes(text)

        if deobfuscate and (has_obf or _text_has_obfuscation(decoded_text)):
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
      - plain text nodes (optionally deobfuscated)
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
      1) Text of the node itself (minus the email).
      2) Text of the parent element.
      3) Previous sibling text.
      4) Previous heading in the same section.
    """

    def clean(txt: str) -> str:
        return _normalize_space(EMAIL_RE.sub(" ", unescape(txt or "")))

    if isinstance(node, Tag):
        text = clean(node.get_text(" ", strip=True))
        piece = _choose_name_piece(text)
        if piece:
            return piece

        parent = node.parent
        if parent is not None and isinstance(parent, Tag):
            text = clean(parent.get_text(" ", strip=True))
            piece = _choose_name_piece(text)
            if piece:
                return piece

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

            for sib in parent.find_all_previous(["h1", "h2", "h3", "h4", "h5", "h6"]):
                text = clean(sib.get_text(" ", strip=True))
                piece = _choose_name_piece(text)
                if piece:
                    return piece

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
        if alias and alias in stripped:
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


def _candidate_key(c: Candidate) -> tuple[str | None, str, bool]:
    return (c.email, (c.raw_name or "").lower(), c.is_role_address_guess)


def _keep_richer_context(existing: Candidate, incoming: Candidate) -> Candidate:
    existing_snip = existing.context_snippet or ""
    incoming_snip = incoming.context_snippet or ""
    if len(incoming_snip) > len(existing_snip):
        return incoming
    return existing


def _email_candidate_from_node(
    *,
    email: str,
    node: Tag | NavigableString,
    source_url: str,
    effective_domain: str | None,
) -> Candidate | None:
    local, _, dom = email.partition("@")
    if not local or not dom:
        return None

    if not _same_org(dom, effective_domain, source_url):
        return None

    is_role_guess = _local_part_role_like(local)

    raw_name = _extract_name_near_node(node)
    first_name: str | None = None
    last_name: str | None = None

    if raw_name and _looks_human_name(raw_name):
        first_name, last_name = _split_first_last(raw_name)
    else:
        raw_name = None
        if local and not is_role_guess:
            first_name, last_name = _name_from_local_part(local)
            if first_name and last_name:
                raw_name = f"{first_name} {last_name}"
            else:
                first_name = None
                last_name = None

    candidate = Candidate(
        email=email.lower(),
        source_url=source_url,
        first_name=first_name,
        last_name=last_name,
        raw_name=raw_name,
        title=None,
        source_type=_source_type_for_node(node),
        context_snippet=_local_context_snippet(node),
        is_role_address_guess=is_role_guess,
    )

    if not _should_keep_candidate(candidate):
        return None
    return candidate


def _load_optional_helpers() -> tuple[
    Callable[[str], Any] | None,
    Callable[[str], Any] | None,
    Callable[..., Any] | None,
    Callable[..., Any] | None,
]:
    try:
        from src.extract.source_filters import (  # type: ignore
            is_blocked_source_url,
            is_employee_page_url,
        )
    except Exception:  # pragma: no cover
        is_blocked_source_url = None
        is_employee_page_url = None

    try:
        # If you later add an HTML-aware classifier, we will use it when present.
        # Expected signature (recommendation):
        #   classify_page_for_people_extraction(
        #       url: str,
        #       html: str,
        #       official_domain: str | None,
        #   ) -> tuple[bool, str]
        from src.extract.url_filters import (  # type: ignore
            classify_page_for_people_extraction,
        )
    except Exception:  # pragma: no cover
        classify_page_for_people_extraction = None

    try:
        from src.extract.people_cards import extract_people_cards  # type: ignore
    except Exception:  # pragma: no cover
        extract_people_cards = None

    return (
        is_blocked_source_url,
        is_employee_page_url,
        classify_page_for_people_extraction,
        extract_people_cards,
    )


def _should_run_people_cards_page(
    *,
    source_url: str,
    html: str,
    effective_domain: str | None,
    extract_people_cards: Callable[..., Any] | None,
    is_blocked_source_url: Callable[[str], Any] | None,
    classify_page_for_people_extraction: Callable[..., Any] | None,
    is_employee_page_url: Callable[[str], Any] | None,
) -> tuple[bool, str]:
    if extract_people_cards is None:
        return False, "skip:no_people_cards_extractor"

    url = (source_url or "").strip()

    if is_blocked_source_url is not None:
        try:
            blocked, reason = is_blocked_source_url(url)  # type: ignore[misc]
        except Exception:
            blocked, reason = False, None
        if blocked:
            why = reason or "blocked"
            return False, f"skip:blocked_source:{why}"

    if classify_page_for_people_extraction is not None:
        try:
            ok, reason = classify_page_for_people_extraction(  # type: ignore[misc]
                url=url,
                html=html,
                official_domain=effective_domain,
            )
            if not ok:
                return False, f"skip:classifier:{reason}"
            return True, f"allow:classifier:{reason}"
        except Exception:
            pass

    if is_employee_page_url is not None:
        try:
            if is_employee_page_url(url):  # type: ignore[misc]
                return True, "allow:is_employee_page_url"
        except Exception:
            pass

    head = (html or "")[:60_000].lower()
    people_signals = (
        "leadership team",
        "executive team",
        "executive leadership",
        "management team",
        "board of directors",
        "our team",
        "meet the team",
        "our leadership",
        "founders",
        "executives",
        "directors",
        '"@type":"person"',
        '"@type": "person"',
        '"@type":"employee"',
        '"@type": "employee"',
    )
    if any(s in head for s in people_signals):
        return True, "allow:html_people_signal"

    return False, "skip:no_strong_people_signal"


def _merge_people_cards_into_map(
    *,
    by_key: dict[tuple[str | None, str, bool], Candidate],
    card_candidates: list[Any],
    source_url: str,
) -> int:
    added_cards = 0

    for c in card_candidates:
        try:
            c.source_url = source_url
        except Exception:
            pass

        if getattr(c, "email", None):
            try:
                c.email = None
            except Exception:
                pass

        if not _should_keep_candidate(c):
            continue

        name_key = (getattr(c, "raw_name", None) or "").lower().strip()
        if not name_key:
            continue

        key = (None, name_key, False)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = c
            added_cards += 1
            continue

        existing_title = getattr(existing, "title", None) or ""
        new_title = getattr(c, "title", None) or ""
        if (not existing_title and new_title) or (len(new_title) > len(existing_title)):
            by_key[key] = c

    return added_cards


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
      - Find emails via attributes + text (optionally deobfuscated).
      - Filter to the same org/domain when official_domain is supplied.
      - Attach:
          * is_role_address_guess based on local-part
          * best-effort human-looking name (when available)
          * small context snippet and source_type

    P1/P2 polish:
      - Extract same-org emails from any page (safe: customer emails won't pass _same_org()).
      - Run no-email people-cards extraction ONLY on pages that are likely employee pages
        and NOT third-party content (case studies, customer stories, events, podcasts, etc.).
        This prevents “customer names” being persisted as company employees.
    """
    if not html:
        return []

    if source_url is None:
        source_url = "https://example.com/unknown"

    effective_domain = official_domain or company_domain
    (
        is_blocked_source_url,
        is_employee_page_url,
        classify_page_for_people_extraction,
        extract_people_cards,
    ) = _load_optional_helpers()

    soup = BeautifulSoup(html, "html.parser")

    # Per-page dedup: avoid blasting the AI with dozens of identical rows.
    by_key: dict[tuple[str | None, str, bool], Candidate] = {}

    for email, node in _iter_email_nodes(soup, deobfuscate=deobfuscate):
        cand = _email_candidate_from_node(
            email=email,
            node=node,
            source_url=source_url,
            effective_domain=effective_domain,
        )
        if cand is None:
            continue

        k = _candidate_key(cand)
        existing = by_key.get(k)
        if existing is None:
            by_key[k] = cand
            continue
        by_key[k] = _keep_richer_context(existing, cand)

    run_cards, cards_reason = _should_run_people_cards_page(
        source_url=source_url,
        html=html,
        effective_domain=effective_domain,
        extract_people_cards=extract_people_cards,
        is_blocked_source_url=is_blocked_source_url,
        classify_page_for_people_extraction=classify_page_for_people_extraction,
        is_employee_page_url=is_employee_page_url,
    )

    if run_cards and extract_people_cards is not None:
        try:
            card_candidates = extract_people_cards(  # type: ignore[misc]
                html=html,
                source_url=source_url,
                official_domain=effective_domain,
            )
        except Exception as exc:  # pragma: no cover
            log.debug("people_cards extraction failed url=%s err=%s", source_url, exc)
            card_candidates = []

        added_cards = _merge_people_cards_into_map(
            by_key=by_key,
            card_candidates=list(card_candidates or []),
            source_url=source_url,
        )
        if added_cards:
            log.info(
                "people_cards added=%d url=%s reason=%s",
                added_cards,
                source_url,
                cards_reason,
            )
    else:
        log.debug("people_cards skipped url=%s reason=%s", source_url, cards_reason)

    return list(by_key.values())
