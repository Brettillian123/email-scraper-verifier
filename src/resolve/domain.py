from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache

import dns.resolver
import httpx
import idna
import tldextract

# Bump when resolver logic meaningfully changes
RESOLVER_VERSION = "r08.2"

# Tight, test-friendly timeouts (kept tiny by design)
_HTTP_TIMEOUT = httpx.Timeout(3.0)
_DNS_TIMEOUT = 2.0

# Public Suffix handling: use bundled list only (no network fetch)
_EXTRACT = tldextract.TLDExtract(cache_dir=False, suffix_list_urls=None)

# Free-mail/consumer and common hosting domains we will not accept as "official"
# (extend as needed; all lowercased punycode)
_DENY = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "zoho.com",
    "yandex.com",
    "pm.me",
    "hey.com",
    "mail.com",
}

# Small, deterministic TLD weighting to help pick the obvious choice on ties
_TLD_BONUS = {
    "com": 10,
    "io": 6,
    "co": 5,
    "ai": 5,
    "net": 4,
    "org": 3,
}

# Very light normalization of legal suffixes in names (not exhaustive on purpose)
_CORP_SUFFIX_RE = re.compile(
    r"""
    \b(
        inc\.?|ltd\.?|llc|llp|plc|gmbh|s\.?a\.?|s\.?r\.?l\.?|bv|ab|oyj?|pty|pte|
        co(mpany)?|corp(oration)?|limited|holdings?|group
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

DOMAIN_SPLIT = re.compile(r"^[a-z]+://", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class Candidate:
    raw: str  # as provided
    domain: str  # normalized, punycode ascii (registrable/apex)
    reason: str  # normalization reason
    base_conf: int = 30  # seed score


@dataclass(frozen=True, slots=True)
class Decision:
    chosen: str | None  # punycode ascii (registrable/apex)
    method: str
    confidence: int
    reason: str


def _strip_scheme_www(host_like: str) -> str:
    s = host_like.strip()
    # Lowercase early; strip scheme and leading wwwN.
    s = DOMAIN_SPLIT.sub("", s).lower()
    s = re.sub(r"^www\d*\.", "", s)
    # Remove port and path
    s = s.split("/")[0].split(":")[0]
    # Remove trailing dot (rooted FQDN)
    if s.endswith("."):
        s = s[:-1]
    return s


def _to_punycode(domain_like: str) -> str:
    # Accept unicode or ascii; return ascii/punycode (lowercase)
    return idna.encode(domain_like).decode("ascii").lower()


def _registrable(apex_or_host: str) -> str:
    """
    Collapse any subdomain to the registrable domain (apex),
    e.g. blog.acme.co.uk -> acme.co.uk
    """
    ext = _EXTRACT(apex_or_host)
    if not ext.suffix or not ext.domain:
        return apex_or_host
    return f"{ext.domain}.{ext.suffix}".lower()


def _valid_like_domain(s: str) -> bool:
    # very light sanity: at least one dot and no spaces/underscores
    return "." in s and " " not in s and "_" not in s


def normalize_hint(hint: str | None) -> Candidate | None:
    """
    Normalize a user-provided host/domain/url-like hint to a punycoded apex domain.
    Rejects denylisted consumer/hosting domains.
    """
    if not hint:
        return None
    host = _strip_scheme_www(hint)
    if not host or not _valid_like_domain(host):
        return None
    try:
        d_ascii = _to_punycode(host)
    except idna.IDNAError:
        return None
    apex = _registrable(d_ascii)
    if apex in _DENY:
        return None
    return Candidate(raw=hint, domain=apex, reason="hint_normalized", base_conf=70)


def _ascii_slug(s: str) -> str:
    """
    Aggressively but safely produce an ascii-only slug from a company name.
    - lowercase
    - strip corporate suffixes
    - remove accents/diacritics
    - keep [a-z0-9], drop others
    """
    s = _CORP_SUFFIX_RE.sub(" ", s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "", s.lower())
    return s


def candidates_from_name(name: str) -> list[Candidate]:
    """
    Heuristic guesses from company name → domain candidates.
    e.g., "Acme Inc" -> acme.com, acme.co, acme.io, ...
    """
    base = _ascii_slug(name)
    if not base:
        return []
    tlds = (".com", ".co", ".io", ".ai", ".net", ".org")
    out: list[Candidate] = []
    for t in tlds:
        dom = f"{base}{t}"
        out.append(Candidate(raw=dom, domain=dom, reason="name_heuristic", base_conf=25))
    return out


@lru_cache(maxsize=1024)
def _dns_any(host: str) -> bool:
    """
    True if the domain has any of MX/A/AAAA. Cached for the process lifetime.
    """
    r = dns.resolver.Resolver(configure=True)
    r.lifetime = _DNS_TIMEOUT
    try:
        for rtype in ("MX", "A", "AAAA"):
            try:
                ans = r.resolve(host, rtype, lifetime=_DNS_TIMEOUT)
                if ans:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


@lru_cache(maxsize=1024)
def _http_head_ok(host: str) -> tuple[bool, str | None]:
    """
    Probe https://host (HEAD with no redirects). If 405, fall back to GET.
    If 3xx, return the Location host (registrable) as the redirect target.
    If HTTPS fails, retry over HTTP.
    """
    headers = {"User-Agent": f"EmailScraperResolver/{RESOLVER_VERSION}"}

    def _probe(scheme: str) -> tuple[bool, str | None]:
        url = f"{scheme}://{host}"
        try:
            with httpx.Client(headers=headers, timeout=_HTTP_TIMEOUT, follow_redirects=False) as c:
                r = c.head(url)
                if 200 <= r.status_code < 300:
                    return True, None
                if 300 <= r.status_code < 400:
                    loc = r.headers.get("location")
                    if loc:
                        # Pull host from the Location, tolerate relative forms
                        loc_host = _strip_scheme_www(loc)
                        if _valid_like_domain(loc_host):
                            try:
                                return True, _registrable(_to_punycode(loc_host))
                            except idna.IDNAError:
                                return True, None
                    return True, None
                if r.status_code == 405:
                    # Some origins block HEAD; try a tiny GET
                    r = c.get(url, headers={"Range": "bytes=0-0"})
                    if 200 <= r.status_code < 400:
                        return True, None
        except Exception:
            return False, None
        return False, None

    ok, loc = _probe("https")
    if ok:
        return True, loc
    # Fallback to http:// if TLS fails
    ok, loc = _probe("http")
    return ok, loc


def _score_base(candidate: Candidate) -> int:
    # Start from base_conf; add small TLD preference if known
    ext = _EXTRACT(candidate.domain)
    bonus = _TLD_BONUS.get(ext.suffix or "", 0)
    return candidate.base_conf + bonus


def decide(cands: Iterable[Candidate]) -> Decision:
    """
    Score-and-pick the most plausible official domain from candidates.
    Tie-breakers are deterministic: hint > higher score > .com bias > lexicographic.
    """
    # Dedup by normalized domain, keep the strongest base_conf per domain
    by_domain: dict[str, Candidate] = {}
    for c in cands:
        if c.domain in _DENY:
            continue
        existing = by_domain.get(c.domain)
        if existing is None or c.base_conf > existing.base_conf:
            by_domain[c.domain] = c

    if not by_domain:
        return Decision(chosen=None, method="none", confidence=0, reason="no_viable_candidate")

    best_domain = None
    best_score = -1
    best_method = "fallback"
    best_reason = "scored_best"

    # Deterministic iteration order to make ties stable
    for domain in sorted(by_domain.keys()):
        c = by_domain[domain]
        score = _score_base(c)

        dns_ok = _dns_any(c.domain)
        if dns_ok:
            score += 25
        http_ok, loc = _http_head_ok(c.domain)
        if http_ok:
            score += 25

        # If there is an external redirect to a different apex, consider it (lightly)
        picked_domain = c.domain
        if http_ok:
            picked_method = "http_ok"
            picked_reason = "dns+http_ok"
        elif dns_ok:
            picked_method = "dns_valid"
            picked_reason = "dns_ok"
        else:
            picked_method = "candidate"
            picked_reason = c.reason

        if http_ok and loc and loc != c.domain and loc not in _DENY:
            # Only trust redirect if the target resolves too
            if _dns_any(loc):
                # Prefer the redirect slightly over the original
                picked_domain = loc
                picked_method = "http_redirect"
                picked_reason = f"redirect->{loc}"
                score += 5

        # Deterministic tie-breakers:
        # 1) higher score
        # 2) candidate whose reason started from hint
        # 3) .com bias (already in score via _TLD_BONUS, but if still tied)
        # 4) lexicographic domain
        def tiebreak_key(d: str, cand: Candidate, sc: int) -> tuple[int, int, int, str]:
            ext = _EXTRACT(d)
            is_hint = 1 if cand.reason.startswith("hint") else 0
            is_com = 1 if (ext.suffix or "") == "com" else 0
            return (sc, is_hint, is_com, f"{d}")

        new_key = tiebreak_key(picked_domain, c, score)
        best_key = (
            None
            if best_domain is None
            else tiebreak_key(best_domain, by_domain[best_domain], best_score)
        )
        if (
            best_domain is None
            or score > best_score
            or (score == best_score and new_key > best_key)
        ):
            best_domain = picked_domain
            best_score = score
            best_method = picked_method
            best_reason = picked_reason

    confidence = max(0, min(best_score, 100))
    return Decision(
        chosen=best_domain,
        method=best_method,
        confidence=confidence,
        reason=best_reason,
    )


def resolve(company_name: str, user_hint: str | None) -> Decision:
    """
    Public API: produce a Decision with a chosen apex domain (punycode) if any.
    """
    items: list[Candidate] = []
    h = normalize_hint(user_hint)
    if h:
        items.append(h)
    items.extend(candidates_from_name(company_name))
    return decide(items)
