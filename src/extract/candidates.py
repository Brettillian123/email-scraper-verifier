# src/extract/candidates.py
from __future__ import annotations

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from html import unescape
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

# --- Public data model -------------------------------------------------------


@dataclass(frozen=True)
class Candidate:
    """
    A high-confidence person/email candidate extracted from HTML.

    Fields:
      - email: normalized email address (lowercased)
      - first_name / last_name: optional parsed names when confidently available
      - source_url: the page where the candidate was found
      - raw_name: the raw name text captured before normalization/splitting
    """

    email: str
    source_url: str
    first_name: str | None = None
    last_name: str | None = None
    raw_name: str | None = None


# --- Heuristics & regexes ----------------------------------------------------

EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)

# Splitters for "Name — Title" or "Name | Title", etc.
NAME_SPLIT_RE = re.compile(r"\s*[–—\-|,]\s+")

# Elements whose visible text we scan for plain emails
TEXTY_TAGS = {"a", "p", "li", "div", "address", "footer", "td", "th"}

# Ignore these containers entirely
SKIP_CONTAINERS = {"script", "style", "noscript", "template"}

# Aliases / role mailboxes to skip entirely
ROLE_ALIASES = {
    "info",
    "sales",
    "press",
    "support",
    "contact",
    "careers",
    # conservative extras that frequently show up as role mailboxes
    "hello",
    "hi",
    "help",
    "jobs",
    "hr",
    "team",
    "admin",
    "office",
    "marketing",
    "billing",
    "inquiries",
    "pr",
}

TOKEN_BLACKLIST = ROLE_ALIASES | {"it"}

# Token must be at least this long to be considered "namey"
MIN_NAME_TOKEN_LEN = 2

# O05 (optional): de-obfuscation is behind a flag; enable only where policies/ToS allow.
_DEOBFUSCATE = os.getenv("EXTRACT_DEOBFUSCATE", "0").strip().lower() in {"1", "true", "yes"}

# De-obfuscation components, e.g. "john [at] acme [dot] com", "mary (at) acme dot co dot uk"
_OB_AT = r"(?:@|\[at\]|\(at\)|\sat\s| at )"
_OB_DOT = r"(?:\.|\[dot\]|\(dot\)|\sdot\s| dot )"
_OB_EMAIL = re.compile(
    rf"""
    (?P<local>[A-Za-z0-9._%+\-]+)
    \s*{_OB_AT}\s*
    (?P<domain>[A-Za-z0-9\-]+(?:\s*{_OB_DOT}\s*[A-Za-z0-9\-]+)+)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _is_role_alias_email(email: str) -> bool:
    """
    Return True if the email's local-part matches a known role/distribution alias.
    """
    try:
        local = email.split("@", 1)[0].lower()
    except Exception:
        return False
    return local in ROLE_ALIASES


# --- Public API --------------------------------------------------------------


def extract_candidates(
    html: str,
    source_url: str,
    official_domain: str | None = None,
    *,
    deobfuscate: bool | None = None,
) -> list[Candidate]:
    """
    Extract (email, first_name, last_name, source_url[, raw_name]) records
    from an HTML document.

    Extraction order:
      1) mailto: links (high precision)
      2) conservative regex inside typical contact/person blocks
      3) cautious fallback name inference from email local-part
      4) (O05 when enabled) de-obfuscated patterns in page text

    Filtering:
      - If `official_domain` is provided, only return emails whose domain is the
        same domain or a subdomain of it (strict-to-org default).

    Dedup:
      - Dedup in-memory by email; prefer candidates that have a human name.
    """
    soup = BeautifulSoup(html or "", "html.parser")

    by_email: dict[str, Candidate] = {}

    # 0) (O05) De-obfuscation pass over full page text — only when enabled
    use_deob = _DEOBFUSCATE if deobfuscate is None else bool(deobfuscate)
    if use_deob:
        page_text = soup.get_text(" ", strip=True)
        for email in _scan_deobfuscated_emails(page_text):
            if _is_role_alias_email(email):
                continue
            if not _in_scope(email, official_domain):
                continue
            cand = _candidate_with_best_name(email, None, source_url)
            _insert_or_upgrade(by_email, cand)

    # 1) mailto: links (most precise)
    for email, name_guess in _scan_mailto_links(soup):
        if _is_role_alias_email(email):  # drop role/distribution addresses
            continue
        if not _in_scope(email, official_domain):
            continue
        cand = _candidate_with_best_name(email, name_guess, source_url)
        _insert_or_upgrade(by_email, cand)

    # 2) plain text emails inside texty blocks
    for email, ctx_name in _scan_text_blocks(soup):
        if _is_role_alias_email(email):  # drop role/distribution addresses
            continue
        if not _in_scope(email, official_domain):
            continue
        cand = _candidate_with_best_name(email, ctx_name, source_url)
        _insert_or_upgrade(by_email, cand)

    return list(by_email.values())


# --- Core scanners -----------------------------------------------------------


def _scan_mailto_links(soup: BeautifulSoup) -> Iterator[tuple[str, str | None]]:
    """
    Yield (email, name_text) pairs from <a href="mailto:...">.
    If the link text is just the email, name_text may be None.
    """
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if not href.lower().startswith("mailto:"):
            continue

        raw = href[len("mailto:") :]
        raw = raw.split("?")[0]  # strip query (subject, cc, etc.)
        raw = unquote(raw)

        # mailto may contain comma-separated addresses
        for addr in (t.strip() for t in raw.split(",") if t.strip()):
            m = EMAIL_RE.search(addr)
            if not m:
                continue
            email = m.group(0).lower()

            # Try to extract a human-looking name from link text or nearby labels
            link_text = (a.get_text(" ", strip=True) or "").strip()
            name_text = _extract_name_from_link_or_context(a, link_text, email)

            yield email, name_text


def _scan_text_blocks(soup: BeautifulSoup) -> Iterator[tuple[str, str | None]]:
    """
    Scan typical text containers and yield (email, nearby_name_guess).
    We avoid scanning script/style/noscript/template.
    """
    for el in soup.find_all(TEXTY_TAGS):
        if _is_inside_skip_container(el):
            continue
        # Avoid double-counting mailto anchors here
        if el.name == "a":
            continue

        text = el.get_text(" ", strip=True)
        if not text:
            continue

        # Decode HTML entities so patterns like "bob&#64;acme.com" become "bob@acme.com"
        text = unescape(text)

        for m in EMAIL_RE.finditer(text):
            email = m.group(0).lower()
            # Try to find a nearby label (strong/h* sibling or parent context)
            nearby_name = _nearest_label_name(el)
            yield email, nearby_name


def _scan_deobfuscated_emails(text: str) -> list[str]:
    """
    (O05) Find obfuscated emails like "name [at] acme [dot] com" within plain text.
    Also benefits from HTML entity unescaping to catch things like "user&#64;example.com".
    """
    txt = unescape(text or "")
    found: set[str] = set()

    for m in _OB_EMAIL.finditer(txt):
        local = (m.group("local") or "").strip()
        domain = _normalize_obfuscated_domain(m.group("domain") or "")
        if not local or not domain:
            continue
        email = f"{local}@{domain}".lower()
        # Validate with our standard email regex to avoid oddities
        if EMAIL_RE.fullmatch(email):
            found.add(email)

    # A light extra pass: after unescape, the standard regex will catch any plain emails
    # that were previously encoded (e.g., "&#64;"). We only add what wasn't already added.
    for m in EMAIL_RE.finditer(txt):
        found.add(m.group(0).lower())

    return sorted(found)


def _normalize_obfuscated_domain(dom_s: str) -> str:
    """Turn 'acme [dot] co [dot] uk' → 'acme.co.uk' and remove stray spaces."""
    s = re.sub(_OB_DOT, ".", dom_s, flags=re.IGNORECASE)
    return re.sub(r"\s+", "", s)


# --- Context/name helpers ----------------------------------------------------


def _extract_name_from_link_or_context(a: Tag, link_text: str, email: str) -> str | None:
    """
    Decide the best raw name to associate with an email found in an anchor.
    Preference order:
      - Link text, if not the email itself and looks like a name (optionally split off titles)
      - A nearest label: strong/b/h* sibling or within the same card/row
      - None
    """
    if link_text and link_text.lower() != email:
        candidate = _choose_name_piece(link_text)
        if _looks_human_name(candidate):
            return candidate

    # Try nearest labels if link text is the email or not name-like
    # 1) previous siblings like <strong>, <b>, headings
    for sib in a.previous_siblings:
        if isinstance(sib, Tag) and sib.name in {"strong", "b", "h1", "h2", "h3", "h4", "h5", "h6"}:
            t = sib.get_text(" ", strip=True)
            candidate = _choose_name_piece(t)
            if _looks_human_name(candidate):
                return candidate
        if isinstance(sib, NavigableString):
            t = str(sib).strip()
            if t:
                candidate = _choose_name_piece(t)
                if _looks_human_name(candidate):
                    return candidate

    # 2) labels within the same parent
    parent = a.parent
    if isinstance(parent, Tag):
        # Prefer strong/b/headings within the same block
        for label in parent.find_all({"strong", "b", "h1", "h2", "h3", "h4", "h5", "h6"}, limit=3):
            t = label.get_text(" ", strip=True)
            candidate = _choose_name_piece(t)
            if _looks_human_name(candidate):
                return candidate

        # Fallback: use parent text without the email itself
        pt = parent.get_text(" ", strip=True).replace(email, "").strip()
        candidate = _choose_name_piece(pt)
        if _looks_human_name(candidate):
            return candidate

    return None


def _nearest_label_name(node: Tag) -> str | None:
    """
    From a general text node containing an email, try to find a nearby label that
    reads like a person's name (e.g., a bold/heading sibling or parent).
    """
    # 1) immediate strong/b/heading children
    if isinstance(node, Tag):
        for label in node.find_all({"strong", "b", "h1", "h2", "h3", "h4", "h5", "h6"}, limit=3):
            t = label.get_text(" ", strip=True)
            candidate = _choose_name_piece(t)
            if _looks_human_name(candidate):
                return candidate

    # 2) previous siblings text
    for sib in node.previous_siblings:
        if isinstance(sib, Tag) and sib.name in {"strong", "b"} | {f"h{i}" for i in range(1, 7)}:
            t = sib.get_text(" ", strip=True)
            candidate = _choose_name_piece(t)
            if _looks_human_name(candidate):
                return candidate
        if isinstance(sib, NavigableString):
            t = str(sib).strip()
            candidate = _choose_name_piece(t)
            if _looks_human_name(candidate):
                return candidate

    # 3) parent context
    parent = node.parent
    if isinstance(parent, Tag):
        t = parent.get_text(" ", strip=True)
        candidate = _choose_name_piece(t)
        if _looks_human_name(candidate):
            return candidate

    return None


def _choose_name_piece(text: str) -> str:
    """
    If the text includes both name and title separated by a common separator,
    return the leading piece that is most likely the name.
    """
    if not text:
        return ""
    # Remove embedded emails from the label text
    text = EMAIL_RE.sub("", text).strip()

    # Split on common separators and pick the earliest "human-like" piece
    parts = [p.strip() for p in NAME_SPLIT_RE.split(text) if p.strip()]
    for p in parts:
        if _looks_human_name(p):
            return p
    # If none look clearly human, return the first chunk (may be empty)
    return parts[0] if parts else ""


def _looks_human_name(text: str | None) -> bool:
    """
    Very conservative check: looks like 1–3 tokens, alphabetic with min length.
    """
    if not text:
        return False
    tokens = _name_tokens(text)
    if not (1 <= len(tokens) <= 3):
        return False
    # Avoid pure roles/titles
    lowered = [t.lower() for t in tokens]
    if any(t in TOKEN_BLACKLIST for t in lowered):
        return False
    return all(_is_namey_token(t) for t in tokens)


def _is_namey_token(tok: str) -> bool:
    # Accept hyphens and apostrophes within tokens
    core = tok.replace("-", "").replace("’", "").replace("'", "")
    return core.isalpha() and len(core) >= MIN_NAME_TOKEN_LEN


def _name_tokens(text: str) -> list[str]:
    # Compact whitespace and split; drop empty tokens
    text = re.sub(r"\s+", " ", text.strip())
    return [t for t in text.split(" ") if t]


def _localpart_to_name(local: str) -> tuple[str, str, str] | None:
    """
    Cautious fallback: infer (raw_name, first, last) from the email local-part.
    - Split on dot/underscore/hyphen
    - Drop short and blacklisted tokens
    - Require at least two tokens that look like names
    """
    if local in ROLE_ALIASES:
        return None

    raw_tokens = re.split(r"[._\-]+", local)
    tokens = [t for t in raw_tokens if _is_namey_token(t) and t.lower() not in TOKEN_BLACKLIST]

    if len(tokens) < 2:
        return None

    # Keep at most two tokens (first, last)
    first_raw, last_raw = tokens[0], tokens[1]
    first, last = _smart_titlecase(first_raw), _smart_titlecase(last_raw)
    raw_name = f"{first} {last}"
    return raw_name, first, last


def _smart_titlecase(word: str) -> str:
    """
    Title-case with light handling of common surname patterns:
    - O'Connor, O’Malley
    - McDonald, MacDonald (keep simple 'Mc' rule)
    """
    if not word:
        return word

    w = word.lower()

    # O' / O’ prefix
    if w.startswith("o'") and len(w) > 2:
        return "O'" + w[2:].capitalize()
    if w.startswith("o’") and len(w) > 2:
        return "O’" + w[2:].capitalize()

    # Mc prefix
    if w.startswith("mc") and len(w) > 2:
        return "Mc" + w[2:3].upper() + w[3:]

    # Hyphenated parts
    if "-" in w:
        return "-".join(_smart_titlecase(p) for p in w.split("-"))

    return w.capitalize()


def _split_first_last(raw_name: str) -> tuple[str | None, str | None]:
    """
    Split a raw name string into (first, last) applying normalization.
    Keeps at most two tokens; if only one, last is None.
    """
    tokens = _name_tokens(raw_name)
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return _smart_titlecase(tokens[0]), None
    first, last = tokens[0], tokens[-1]
    return _smart_titlecase(first), _smart_titlecase(last)


# --- Domain & scope helpers --------------------------------------------------


def _in_scope(email: str, official_domain: str | None) -> bool:
    """
    If official_domain is provided, restrict to that domain or any subdomain.
    Otherwise, accept all.
    """
    if not official_domain:
        return True
    try:
        domain = email.split("@", 1)[1].lower()
    except Exception:
        return False

    official = official_domain.lower()
    return domain == official or domain.endswith("." + official)


def _is_inside_skip_container(el: Tag) -> bool:
    parent = el.parent
    while isinstance(parent, Tag):
        if parent.name in SKIP_CONTAINERS:
            return True
        parent = parent.parent
    return False


# --- Candidate assembly / preference ----------------------------------------


def _candidate_with_best_name(email: str, raw_name_hint: str | None, source_url: str) -> Candidate:
    """
    Build the richest candidate we can from a hint, else fallback from local-part.
    """
    first: str | None = None
    last: str | None = None
    raw_name: str | None = None

    # 1) Try contextual/raw hint (link text / nearby label)
    if raw_name_hint:
        raw = _choose_name_piece(raw_name_hint)
        if _looks_human_name(raw):
            raw_name = raw
            first, last = _split_first_last(raw_name)

    # 2) Fallback: infer from local-part (e.g., jane.doe@)
    if not first and not last:
        local = email.split("@", 1)[0]
        lp = _localpart_to_name(local)
        if lp:
            raw_name, first, last = lp

    return Candidate(
        email=email.lower().strip(),
        source_url=source_url,
        first_name=first,
        last_name=last,
        raw_name=raw_name,
    )


def _insert_or_upgrade(by_email: dict[str, Candidate], cand: Candidate) -> None:
    """
    Dedup by email. Prefer the richer record (i.e., the one that has a name).
    """
    existing = by_email.get(cand.email)
    if existing is None:
        by_email[cand.email] = cand
        return

    # If existing has no name but new has, upgrade.
    existing_has_name = bool(existing.first_name or existing.last_name)
    new_has_name = bool(cand.first_name or cand.last_name)

    if new_has_name and not existing_has_name:
        by_email[cand.email] = cand
        return

    # If both have names, keep the one with more filled fields (stability)
    if new_has_name and existing_has_name:
        existing_fields = int(bool(existing.first_name)) + int(bool(existing.last_name))
        new_fields = int(bool(cand.first_name)) + int(bool(cand.last_name))
        if new_fields > existing_fields:
            by_email[cand.email] = cand


# --- Tiny utility (not used externally, but handy for tests) -----------------


def _domain_of(url: str) -> str:
    """Extract host from a URL (lowercased), or empty string on failure."""
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""
