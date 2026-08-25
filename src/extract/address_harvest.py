# src/extract/address_harvest.py
"""
Harvest *real* (published) email addresses out of HTML we already crawled.

Why this exists
---------------
Pattern inference needs at least one genuine address per domain before it can
guess anything for that domain's colleagues. Hundreds of companies have zero
confirmed addresses, so inference has nothing to learn from -- yet their pages
are already sitting in the `sources` table. Anything we find there seeds the
domain's pattern for every colleague at **zero SMTP cost**.

This module is *pure*: HTML in, addresses out. It performs no I/O and touches
no database, matching the convention set by `src/extract/candidates.py`
(pure extractor) + `scripts/extract_candidates.py` (DB driver). The DB driver
for this module is `scripts/harvest_addresses.py`.

Sources scanned, in descending precision order:
  1. JSON-LD (`<script type="application/ld+json">`) Person/Organization/
     ContactPoint objects carrying an `email` field. Team and About pages
     commonly emit this and it is the highest-precision signal available,
     because the markup itself asserts "this is a person's email".
  2. `mailto:` hrefs (query string stripped).
  3. Plain-text addresses in visible content.
  4. Conservative obfuscations: "name [at] domain [dot] com", "name (at)
     domain.com", and HTML entity encoding (`&#64;`, `&commat;`), which the
     parser decodes for us before we ever see the text.

Deliberate limitations
----------------------
* JavaScript-assembled addresses (concatenated string fragments, base64/ROT13
  blobs, `document.write` splices) are NOT decoded. Reconstructing them is
  guesswork, and a wrong address here is worse than no address: it would seed
  a *false* pattern for the whole domain and mis-guess every colleague.
* Bare-word obfuscation ("jane at acme.com") is only accepted when a "dot"
  word marker also appears, otherwise ordinary prose ("look at example.com")
  would decode into a fabricated address.
* Addresses rendered as images are out of scope.

Filtering rules
---------------
* Domain match is mandatory. An address is only attributed to a company when
  its domain equals, or is a sub/parent of, one of that company's known hosts.
  A careers page linking a recruiter at another firm is dropped.
* Role addresses (RFC 2142 + the usual marketing aliases) are separated out,
  never treated as a person's address. They are still *reported*, because a
  live role inbox is evidence the domain accepts mail at all.
* Placeholder locals (test@, yourname@, and the classic doe surnames) are
  dropped outright.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from html import unescape
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Tag

# Reuse, do not re-implement: these already encode this project's decisions
# about what an email looks like and which locals are junk.
from src.extract.candidates import (
    EMAIL_RE,
    _extract_name_near_node,
    _local_part_role_like,
    _normalize_email,
)
from src.extract.quality_gates import is_placeholder_localpart
from src.generate.patterns import PATTERN_PRIORITY, build_localpart, norm_name

log = logging.getLogger(__name__)

__all__ = [
    "Attribution",
    "HarvestedAddress",
    "PageHarvest",
    "PersonRef",
    "attribute_address",
    "attribute_local_part",
    "build_allowed_domains",
    "domain_matches",
    "harvest_page",
    "is_role_local_part",
    "normalize_host",
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

# Lower rank == more trustworthy provenance. Used to pick a winner when the
# same address shows up more than once on a page.
_METHOD_RANK: dict[str, int] = {"jsonld": 0, "mailto": 1, "text": 2, "obfuscated": 3}


@dataclass(frozen=True)
class HarvestedAddress:
    """One real address found on one page."""

    email: str  # normalized, lowercased
    local_part: str
    domain: str
    source_url: str
    method: str  # "jsonld" | "mailto" | "text" | "obfuscated"
    display_name: str | None = None  # name text sitting next to the address


@dataclass(frozen=True)
class PersonRef:
    """Minimal view of an already-extracted person, for attribution."""

    person_id: int
    first_name: str | None
    last_name: str | None
    full_name: str | None = None


@dataclass(frozen=True)
class Attribution:
    """A confident local-part -> person match, plus the pattern that explains it."""

    person_id: int
    pattern: str


@dataclass
class PageHarvest:
    """Everything one page yielded."""

    personal: list[HarvestedAddress] = field(default_factory=list)
    role: list[HarvestedAddress] = field(default_factory=list)
    rejected_foreign: int = 0
    rejected_placeholder: int = 0

    def __len__(self) -> int:  # convenience for callers/tests
        return len(self.personal) + len(self.role)


# ---------------------------------------------------------------------------
# Host / domain matching
# ---------------------------------------------------------------------------


def normalize_host(value: str | None) -> str:
    """
    Reduce a host, URL, or email domain to a bare comparable host.

    "https://WWW.Acme.com:443/team" -> "acme.com"
    """
    s = (value or "").strip().lower()
    if not s:
        return ""
    if "//" in s:
        s = urlparse(s).netloc or s.split("//", 1)[1]
    s = s.split("/", 1)[0]
    s = s.split("@")[-1]  # tolerate being handed a whole address
    s = s.split(":", 1)[0]  # strip port
    if s.startswith("www."):
        s = s[4:]
    return s.strip(".")


def build_allowed_domains(*candidates: str | None) -> set[str]:
    """
    Build the set of hosts an address may live on to count as "this company".

    Feed it companies.domain / official_domain / user_supplied_domain and the
    crawled page URL. Empty and dotless inputs are ignored.
    """
    return {h for h in (normalize_host(c) for c in candidates) if h and "." in h}


def domain_matches(email_domain: str, allowed: Iterable[str]) -> bool:
    """
    True when `email_domain` belongs to the same org as one of `allowed`.

    Matching is bidirectional on the sub/parent axis -- the same rule
    `candidates._same_org()` uses -- so careers.acme.com and acme.com are
    treated as one org, while notacme.com is not.
    """
    edom = normalize_host(email_domain)
    if not edom:
        return False
    for raw in allowed:
        base = normalize_host(raw)
        if not base:
            continue
        if edom == base or edom.endswith("." + base) or base.endswith("." + edom):
            return True
    return False


# ---------------------------------------------------------------------------
# Role / placeholder classification
# ---------------------------------------------------------------------------

# RFC 2142 mailbox names that candidates.ROLE_ALIASES does not already carry.
# (info, marketing, sales, support, abuse, security, postmaster, webmaster and
# the hello/contact/team/careers/jobs/press aliases are already in that set.)
_RFC2142_EXTRA: frozenset[str] = frozenset({"hostmaster", "usenet", "noc", "ftp", "www", "uucp"})


def is_role_local_part(local: str) -> bool:
    """
    True when the local-part is a shared/role inbox rather than a named person.

    Delegates to `candidates._local_part_role_like()` (token-boundary aware, so
    "bioinformatics" is not flagged for containing "info") and adds the handful
    of RFC 2142 names that set is missing.
    """
    lp = (local or "").strip().lower()
    if not lp:
        return False
    if _local_part_role_like(lp):
        return True
    if lp in _RFC2142_EXTRA:
        return True
    tokens = [t for t in re.split(r"[^a-z]+", lp) if t]
    return any(t in _RFC2142_EXTRA for t in tokens)


# ---------------------------------------------------------------------------
# Obfuscation decoding (conservative by design)
# ---------------------------------------------------------------------------

_AT_BRACKET = r"(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\})"
_AT_WORD = r"(?:\s+at\s+)"
_DOT_MARK = r"(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\}|\s+dot\s+)"
_LOCAL = r"[A-Za-z0-9._%+-]{1,64}"
_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_TLD = r"[A-Za-z]{2,24}"
_NOT_LOCAL_CHAR = r"(?<![A-Za-z0-9._%+-])"

# Bracketed at-marker: unambiguous, so the domain may use plain dots.
#   "jane [at] acme.com" / "jane (at) acme [dot] com"
_OBF_BRACKET_RE = re.compile(
    rf"{_NOT_LOCAL_CHAR}({_LOCAL})\s*{_AT_BRACKET}\s*"
    rf"({_LABEL}(?:\s*(?:{_DOT_MARK}|\.)\s*{_LABEL})*\s*(?:{_DOT_MARK}|\.)\s*{_TLD})",
    re.IGNORECASE,
)

# Bare "at" word: ambiguous with prose, so we additionally require the final
# separator to be a spelled-out "dot". "look at example.com" therefore does
# NOT decode; "jane at acme dot com" does.
_OBF_WORD_RE = re.compile(
    rf"{_NOT_LOCAL_CHAR}({_LOCAL}){_AT_WORD}"
    rf"({_LABEL}(?:\s*(?:{_DOT_MARK}|\.)\s*{_LABEL})*\s*{_DOT_MARK}\s*{_TLD})",
    re.IGNORECASE,
)

_DOT_MARK_RE = re.compile(_DOT_MARK, re.IGNORECASE)


def _decode_obfuscated(local: str, domain_blob: str) -> str | None:
    """Turn a matched (local, domain) obfuscation pair into a real address."""
    domain = _DOT_MARK_RE.sub(".", domain_blob)
    domain = re.sub(r"\s+", "", domain)
    candidate = f"{local.strip()}@{domain}".lower()
    # Round-trip through the shared validator so we never emit a shape the
    # rest of the pipeline would reject.
    return _normalize_email(candidate)


def _iter_obfuscated(text: str) -> Iterator[str]:
    # Note: this runs page-wide, not only on pages that lack a literal "@".
    # Real team pages routinely mix plain addresses with obfuscated ones, and
    # both regexes above are anchored tightly enough (bracketed at-marker, or
    # bare "at" plus a spelled-out "dot") to run safely alongside prose.
    if not text:
        return
    for rx in (_OBF_BRACKET_RE, _OBF_WORD_RE):
        for m in rx.finditer(text):
            email = _decode_obfuscated(m.group(1), m.group(2))
            if email:
                yield email


# ---------------------------------------------------------------------------
# JSON-LD / schema.org
# ---------------------------------------------------------------------------

_JSONLD_TYPES: frozenset[str] = frozenset(
    {
        "person",
        "organization",
        "corporation",
        "localbusiness",
        "ngo",
        "educationalorganization",
        "contactpoint",
        "employee",
        "founder",
    }
)
_JSONLD_MAX_DEPTH = 12


def _jsonld_type_matches(raw: Any) -> bool:
    values = raw if isinstance(raw, list) else [raw]
    for v in values:
        if not isinstance(v, str):
            continue
        # "https://schema.org/Person" -> "person"
        leaf = v.rstrip("/").rsplit("/", 1)[-1].rsplit("#", 1)[-1].strip().lower()
        if leaf in _JSONLD_TYPES:
            return True
    return False


def _walk_jsonld(node: Any, depth: int = 0) -> Iterator[tuple[str, str | None]]:
    """Yield (raw_email, display_name) from Person/Organization-ish objects."""
    if depth > _JSONLD_MAX_DEPTH:
        return
    if isinstance(node, list):
        for item in node:
            yield from _walk_jsonld(item, depth + 1)
        return
    if not isinstance(node, dict):
        return

    if "email" in node and _jsonld_type_matches(node.get("@type")):
        name = node.get("name")
        display = name.strip() if isinstance(name, str) and name.strip() else None
        raw_emails = node["email"]
        for raw in raw_emails if isinstance(raw_emails, list) else [raw_emails]:
            if isinstance(raw, str) and raw.strip():
                yield raw, display

    for value in node.values():
        yield from _walk_jsonld(value, depth + 1)


def _iter_jsonld_emails(soup: BeautifulSoup) -> Iterator[tuple[str, str | None]]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        blob = script.string or script.get_text()
        if not blob or not blob.strip():
            continue
        try:
            data = json.loads(blob)
        except (ValueError, TypeError) as exc:
            log.debug("skipping malformed ld+json block: %s", exc)
            continue
        yield from _walk_jsonld(data)


# ---------------------------------------------------------------------------
# mailto: and plain text
# ---------------------------------------------------------------------------


def _display_name_for_anchor(a: Tag) -> str | None:
    text = a.get_text(" ", strip=True)
    if text and "@" not in text and 2 <= len(text) <= 80:
        return text
    try:
        return _extract_name_near_node(a)
    except Exception:  # pragma: no cover - defensive; helper is heuristic
        return None


def _iter_mailto_emails(soup: BeautifulSoup) -> Iterator[tuple[str, str | None]]:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not isinstance(href, str) or not href.strip().lower().startswith("mailto:"):
            continue
        # _normalize_email unquotes, drops the mailto: scheme and any
        # ?subject=/&cc= query string, and lowercases.
        email = _normalize_email(href)
        if not email:
            continue
        yield email, _display_name_for_anchor(a)


_STRIPPED_TAGS = ("script", "style", "noscript", "template")


def _visible_text(soup: BeautifulSoup) -> str:
    """Visible page text. Mutates `soup`, so call this last."""
    for el in soup.find_all(_STRIPPED_TAGS):
        el.decompose()
    return unescape(soup.get_text(" ", strip=True))


# ---------------------------------------------------------------------------
# Public extractor
# ---------------------------------------------------------------------------


def _with_display_name(addr: HarvestedAddress, display_name: str) -> HarvestedAddress:
    return HarvestedAddress(
        email=addr.email,
        local_part=addr.local_part,
        domain=addr.domain,
        source_url=addr.source_url,
        method=addr.method,
        display_name=display_name,
    )


def _replace(out: PageHarvest, old: HarvestedAddress, new: HarvestedAddress) -> None:
    for bucket in (out.personal, out.role):
        for i, existing in enumerate(bucket):
            if existing.email == old.email:
                bucket[i] = new
                return


def _classify(
    email: str,
    *,
    source_url: str,
    method: str,
    display_name: str | None,
    allowed: Iterable[str],
    out: PageHarvest,
    seen: dict[str, tuple[int, HarvestedAddress]],
) -> None:
    """Apply the domain/role/placeholder rules and file the address."""
    local, _, domain = email.partition("@")
    if not local or not domain:
        return

    if not domain_matches(domain, allowed):
        out.rejected_foreign += 1
        return

    if is_placeholder_localpart(local):
        out.rejected_placeholder += 1
        return

    rank = _METHOD_RANK.get(method, 99)
    prior = seen.get(email)
    if prior is not None and prior[0] <= rank:
        # Already recorded from an equal-or-better source; only backfill a name.
        if display_name and not prior[1].display_name:
            better = _with_display_name(prior[1], display_name)
            _replace(out, prior[1], better)
            seen[email] = (prior[0], better)
        return

    addr = HarvestedAddress(
        email=email,
        local_part=local,
        domain=normalize_host(domain),
        source_url=source_url,
        method=method,
        display_name=display_name or (prior[1].display_name if prior else None),
    )
    if prior is not None:
        _replace(out, prior[1], addr)
    else:
        (out.role if is_role_local_part(local) else out.personal).append(addr)
    seen[email] = (rank, addr)


def harvest_page(
    html: str,
    *,
    source_url: str,
    allowed_domains: Iterable[str],
) -> PageHarvest:
    """
    Extract on-domain email addresses from one stored page.

    Parameters
    ----------
    html:
        Raw HTML as stored in `sources.html`.
    source_url:
        The page URL (`sources.source_url`), recorded as provenance.
    allowed_domains:
        Hosts that count as "this company". Build it with
        `build_allowed_domains(company.domain, company.official_domain, source_url)`.
        An empty set means nothing can match and the page yields nothing --
        that is intentional: an address we cannot attribute to a company is
        worse than no address.

    Returns
    -------
    PageHarvest
        `.personal` -- named-person addresses, usable for pattern inference.
        `.role`     -- shared inboxes; evidence the domain accepts mail, but
                       never a person's address.
    """
    out = PageHarvest()
    if not html or not html.strip():
        return out

    # Re-run the dotless guard here too: a caller that hand-rolled its allow
    # list must not be able to smuggle in a bare TLD ("com"), which would make
    # domain_matches() accept every address on the page.
    allowed = build_allowed_domains(*allowed_domains)
    if not allowed:
        log.debug("harvest_page: no allowed domains for url=%s; skipping", source_url)
        return out

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # pragma: no cover - html.parser is very forgiving
        log.warning("harvest_page: unparseable html url=%s err=%s", source_url, exc)
        return out

    seen: dict[str, tuple[int, HarvestedAddress]] = {}

    def record(raw: str, method: str, display: str | None = None) -> None:
        email = _normalize_email(raw)
        if email:
            _classify(
                email,
                source_url=source_url,
                method=method,
                display_name=display,
                allowed=allowed,
                out=out,
                seen=seen,
            )

    # 1) JSON-LD, then 2) mailto: -- both must run before _visible_text(),
    # which destructively strips <script> from the tree.
    for raw, display in _iter_jsonld_emails(soup):
        record(raw, "jsonld", display)
    for raw, display in _iter_mailto_emails(soup):
        record(raw, "mailto", display)

    text = _visible_text(soup)
    # 3) plain text
    for m in EMAIL_RE.finditer(text):
        record(m.group(0), "text")
    # 4) obfuscated
    for email in _iter_obfuscated(text):
        record(email, "obfuscated")

    return out


# ---------------------------------------------------------------------------
# Attribution: local-part -> already-extracted person
# ---------------------------------------------------------------------------


def _candidate_locals(person: PersonRef) -> dict[str, str]:
    """Map every local-part this person could plausibly own -> pattern key."""
    first = (person.first_name or "").strip()
    last = (person.last_name or "").strip()
    if not first and not last and person.full_name:
        parts = person.full_name.split()
        if parts:
            first = parts[0]
            last = parts[-1] if len(parts) > 1 else ""

    fn, ln = norm_name(first, last)
    if not fn and not ln:
        return {}

    out: dict[str, str] = {}
    for key in PATTERN_PRIORITY:
        lp = build_localpart(key, first, last)
        # First match wins: PATTERN_PRIORITY is ordered most-common-first, so a
        # local-part several patterns can produce is labelled with the likelier
        # of them.
        if lp and lp not in out:
            out[lp] = key
    return out


def attribute_local_part(
    local_part: str,
    people: Sequence[PersonRef],
) -> Attribution | None:
    """
    Match a harvested local-part to exactly one known person.

    Returns None when nothing matches *or* when more than one person matches
    (ambiguity is not a match). Callers should still keep the address: an
    unattributed on-domain address is fully usable for pattern inference.
    """
    lp = (local_part or "").strip().lower()
    if not lp:
        return None
    lp = lp.split("+", 1)[0]  # drop plus-addressing tag
    if not lp:
        return None

    hits: list[Attribution] = []
    for person in people:
        pattern = _candidate_locals(person).get(lp)
        if pattern:
            hits.append(Attribution(person_id=person.person_id, pattern=pattern))

    if len(hits) == 1:
        return hits[0]
    return None


def _person_display_names(person: PersonRef) -> set[str]:
    names: set[str] = set()
    if person.full_name and person.full_name.strip():
        names.add(person.full_name.strip().lower())
    first = (person.first_name or "").strip()
    last = (person.last_name or "").strip()
    if first or last:
        names.add(f"{first} {last}".strip().lower())
    return {n for n in names if n}


def attribute_address(
    addr: HarvestedAddress,
    people: Sequence[PersonRef],
) -> Attribution | None:
    """
    Attribute a harvested address, using its display name to break ties.

    Local-part matching is the primary signal. When several people share a
    candidate local-part, an exact display-name match (e.g. the `name` field of
    the JSON-LD Person object) picks the winner; otherwise we stay unattributed.
    """
    direct = attribute_local_part(addr.local_part, people)
    if direct is not None:
        return direct

    display = (addr.display_name or "").strip().lower()
    if not display:
        return None

    lp = addr.local_part.strip().lower().split("+", 1)[0]
    named = [
        p for p in people if display in _person_display_names(p) and lp in _candidate_locals(p)
    ]
    if len(named) == 1:
        person = named[0]
        return Attribution(person_id=person.person_id, pattern=_candidate_locals(person)[lp])
    return None
