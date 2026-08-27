"""Cheap signals about a candidate company, from its own front page.

Two fetches at most -- homepage, and /about if the homepage links one. That is
deliberate: this runs over hundreds of candidates most of which will be
discarded, so it must cost far less than the crawl that follows for the few that
survive. Anything needing a real crawl happens after the company is promoted.

Everything here is extracted from HTML the company published about itself. No
third-party data source, no service that would need a subscription.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_META_DESC = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']', re.I
)
_TAG = re.compile(r"<[^>]+>")

#: Phrases that mean "this company is at the stage that hires fractional execs".
_STAGE = {
    "series a": 12, "seed round": 12, "seed-stage": 10, "pre-seed": 8,
    "series b": 6, "backed by": 8, "raised": 6, "yc ": 8, "y combinator": 8,
    "techstars": 6,
}

#: Phrases that mean the opposite -- too big, or not a company we sell to.
_DISQUALIFY = {
    "fortune 500": -25, "nasdaq": -20, "nyse:": -20, "publicly traded": -20,
    "our franchise": -15, "law firm": -15, "recruiting agency": -20,
    "staffing agency": -20, "executive search": -25, "we place executives": -30,
}

#: Sector words that match the book Crestwell actually sells into.
_SECTOR = {
    "saas": 8, "b2b": 6, "platform": 4, "api": 4, "fintech": 8, "healthtech": 8,
    "infrastructure": 5, "developer": 5, "analytics": 5, "security": 6,
    "compliance": 6, "logistics": 5, "marketplace": 5, "ai": 4, "automation": 4,
}


@dataclass
class Profile:
    domain: str
    reachable: bool = False
    title: str = ""
    description: str = ""
    has_careers: bool = False
    has_team_page: bool = False
    stage_hits: list[str] = field(default_factory=list)
    sector_hits: list[str] = field(default_factory=list)
    disqualifiers: list[str] = field(default_factory=list)
    text_len: int = 0

    def as_dict(self) -> dict:
        return {
            "reachable": self.reachable,
            "title": self.title[:200],
            "description": self.description[:300],
            "has_careers": self.has_careers,
            "has_team_page": self.has_team_page,
            "stage_hits": self.stage_hits,
            "sector_hits": self.sector_hits,
            "disqualifiers": self.disqualifiers,
            "text_len": self.text_len,
        }


def _visible_text(html: str) -> str:
    return _TAG.sub(" ", html or "").lower()


def build_profile(domain: str, *, max_bytes: int = 600_000, timeout: float = 15.0) -> Profile:
    """Fetch the homepage and read what the company says about itself."""
    import httpx

    from src.fetch.ssrf import safe_get

    p = Profile(domain=domain)
    html = ""
    for scheme in ("https", "http"):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=False) as client:
                resp = safe_get(client, f"{scheme}://{domain}/", max_bytes=max_bytes)
        except Exception:
            continue
        if getattr(resp, "status_code", 0) < 400:
            html = getattr(resp, "text", "") or ""
            p.reachable = True
            break
    if not p.reachable:
        return p

    m = _TITLE.search(html)
    if m:
        p.title = _TAG.sub(" ", m.group(1)).strip()[:200]
    m = _META_DESC.search(html)
    if m:
        p.description = m.group(1).strip()[:300]

    low = html.lower()
    p.has_careers = any(
        k in low for k in ("/careers", "/jobs", "greenhouse.io", "lever.co", "ashbyhq")
    )
    p.has_team_page = any(
        k in low for k in ("/team", "/about", "/leadership", "/people", "/company")
    )

    text = _visible_text(html)
    p.text_len = len(text)
    p.stage_hits = [k for k in _STAGE if k in text]
    p.sector_hits = [k for k in _SECTOR if k in text]
    p.disqualifiers = [k for k in _DISQUALIFY if k in text]
    return p


def score_profile(p: Profile) -> tuple[int, list[str]]:
    """Score a profile against Crestwell's ICP. Returns (0-100, reasons).

    Company-level, deliberately separate from src/scoring/icp.py -- that scorer
    grades a PERSON at a known company (role family, seniority) and needs a
    person to work at all. This grades whether the company is worth entering the
    pipeline in the first place, which is a different question with different
    inputs.

    A disqualifier is decisive rather than weighted. An executive search firm is
    not a weak prospect, it is a competitor, and no amount of sector match should
    outvote that.
    """
    reasons: list[str] = []
    score = 0

    if not p.reachable:
        return 0, ["unreachable"]

    for k in p.disqualifiers:
        w = _DISQUALIFY[k]
        score += w
        reasons.append(f"disqualify:{k}{w}")
    if score <= -20:
        return 0, reasons + ["disqualified"]

    # Hiring is the strongest single signal that an executive gap exists.
    if p.has_careers:
        score += 20
        reasons.append("careers_page+20")
    if p.has_team_page:
        # Also a precondition for the pipeline working at all: no team page,
        # no people to extract, no addresses to build.
        score += 15
        reasons.append("team_page+15")

    for k in p.stage_hits:
        w = _STAGE[k]
        score += w
        reasons.append(f"stage:{k}+{w}")

    sector_total = 0
    for k in p.sector_hits:
        sector_total += _SECTOR[k]
    if sector_total:
        capped = min(25, sector_total)  # a keyword-stuffed page is not 10x better
        score += capped
        reasons.append(f"sector:{','.join(p.sector_hits[:4])}+{capped}")

    # A page with almost no text is usually a parked domain or a splash screen.
    if p.text_len < 400:
        score -= 15
        reasons.append("thin_page-15")

    return max(0, min(100, score)), reasons
