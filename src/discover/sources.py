"""Where new company domains come from.

The pipeline has always been able to find PEOPLE at a company. It has never been
able to find the company. That was a business input -- a CSV -- and this module
is the part that removes the CSV.

Three adapters, all returning bare domains plus provenance. Deliberately dumb:
they find candidates, they do not judge them. Judging happens in icp_company.py
after a profile crawl, because a domain on its own says almost nothing.

    vc_portfolio  A seed/Series-A fund's portfolio page is a curated list of
                  exactly the stage that buys fractional executives. One page is
                  worth dozens of search queries.
    searxng       Self-hosted metasearch (no key, no quota, no subscription),
                  asked about funding events. The weakest of the three: a funding
                  query returns the PRESS, not the company that raised.
    link_graph    Customer, partner and integration pages of companies already in
                  the database. Cheapest, no external dependency, and biased
                  toward whoever the existing set already links to -- which is
                  why it is the third source and not the first.

Every fetch goes through src.fetch.ssrf.safe_get: robots-respecting, SSRF-guarded
and size-capped, the same path the crawler uses.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse

log = logging.getLogger(__name__)

#: Hosts that appear on nearly every page and are never a prospect.
_NEVER = {
    "twitter.com", "x.com", "linkedin.com", "facebook.com", "instagram.com",
    "youtube.com", "github.com", "medium.com", "substack.com", "crunchbase.com",
    "apple.com", "google.com", "microsoft.com", "amazon.com", "aws.amazon.com",
    "wikipedia.org", "producthunt.com", "angel.co", "wellfound.com", "notion.so",
    "docs.google.com", "forms.gle", "calendly.com", "zoom.us", "vimeo.com",
    "reddit.com", "tiktok.com", "threads.net", "bsky.app", "mastodon.social",
}

#: Suffixes that indicate infrastructure rather than a company's own site.
_NEVER_SUFFIX = (
    ".cloudfront.net", ".amazonaws.com", ".googleapis.com", ".gstatic.com",
    ".cdn.com", ".typekit.net", ".fontawesome.com", ".w3.org", ".schema.org",
)

#: Publishers, funds, accelerators and advice sites. A funding-news query returns
#: these instead of the company that raised, so without this filter the search
#: source proposes the press as prospects.
_PRESS_HINTS = (
    ".vc", "vc.com", "ventures.com", "capital.com", "partners.com",
    "techcrunch", "businesswire", "prnewswire", "globenewswire", "forbes",
    "bloomberg", "reuters", "axios", "venturebeat", "sifted", "eu-startups",
    "finsmes", "pitchbook", "cbinsights", "accelerator", "ycombinator",
    "seedtable", "startupsavant", "failory", "saastr",
)


def _is_press(domain: str) -> bool:
    """True for a publisher/fund/advice domain rather than an operating company."""
    d = (domain or "").lower()
    return any(h in d for h in _PRESS_HINTS)


_HREF = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)


#: The crawler already identifies itself by this name in robots.txt terms; a
#: discovery fetch should not be more anonymous than a crawl fetch, and
#: several funds 403 an unnamed client outright.
_UA = {"User-Agent": "EmailVerifierBot/1.0 (+https://crestwellpartners.com)"}


@dataclass(frozen=True)
class Candidate:
    domain: str
    source: str
    source_detail: str


def _registrable(host: str) -> str | None:
    """Reduce a hostname to the domain we would probe, or None if unusable."""
    h = (host or "").strip().lower().rstrip(".")
    if not h or "." not in h:
        return None
    if h.startswith("www."):
        h = h[4:]
    if h in _NEVER or any(h.endswith(s) for s in _NEVER_SUFFIX):
        return None
    # Strip obvious subdomains that are never the company's mail domain.
    # `jobs.` was missing and let jobs.bvp.com -- the FUND's own job board --
    # through as a prospect on the first live run.
    for pre in ("blog.", "docs.", "help.", "support.", "status.", "app.",
                "careers.", "jobs.", "www2.", "info.", "go.", "get."):
        if h.startswith(pre):
            h = h[len(pre):]
            break
    if h in _NEVER or h.count(".") > 3:
        return None
    return h


def _domains_from_html(html: str) -> list[str]:
    """Every distinct outbound domain linked from a page, in order of appearance."""
    out: list[str] = []
    seen: set[str] = set()
    for href in _HREF.findall(html or ""):
        if not href.lower().startswith(("http://", "https://")):
            continue
        try:
            host = urlparse(href).netloc
        except Exception:
            continue
        d = _registrable(host)
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return out


def from_vc_portfolio(url: str, *, max_bytes: int = 2_000_000) -> list[Candidate]:
    """Every company linked from a fund's portfolio page.

    Portfolio pages are mostly a grid of logos, each wrapped in an outbound link
    to the company. That structure is why this yields so much per fetch: the page
    IS the list, already filtered to the fund's stage and thesis.
    """
    import httpx

    from src.fetch.ssrf import safe_get

    try:
        with httpx.Client(timeout=20.0, follow_redirects=False) as client:
            resp = safe_get(client, url, max_bytes=max_bytes, headers=_UA)
    except Exception as exc:
        log.warning("vc_portfolio fetch failed", extra={"url": url, "err": str(exc)})
        return []
    if getattr(resp, "status_code", 0) >= 400:
        log.warning("vc_portfolio HTTP %s", getattr(resp, "status_code", "?"), extra={"url": url})
        return []

    host = _registrable(urlparse(url).netloc) or ""
    out = []
    for d in _domains_from_html(getattr(resp, "text", "") or ""):
        # Exact match is not enough: a fund's own job board and blog live on
        # subdomains, and jobs.bvp.com was promoted as a prospect because of it.
        if host and (d == host or d.endswith("." + host) or host.endswith("." + d)):
            continue
        out.append(Candidate(domain=d, source="vc_portfolio", source_detail=url))
    log.info("vc_portfolio found %s candidates", len(out), extra={"url": url})
    return out


def from_searxng(query: str, *, num: int = 20) -> list[Candidate]:
    """Companies named by a web search, via SELF-HOSTED SearXNG.

    Replaces the Serper SaaS API: no key, no quota, no subscription, and no
    account to run out of credits -- which is exactly what Serper had done.
    SearXNG runs as `cw-iq-searxng` on the isolated iq network, is not published
    to the internet, and metasearches DuckDuckGo/Bing/Brave rather than scraping
    any one of them.

    KNOW THE WEAKNESS BEFORE TRUSTING THIS SOURCE. A query like "seed round B2B
    SaaS" returns ARTICLES ABOUT funding, not the companies that raised: the
    first live results were redbud.vc, payproglobal.com and forumvc.com -- a VC,
    an advice blog and an accelerator. Taking the result host gives you the
    publisher, not the subject.

    _PRESS filters the worst of that, but the honest position is that search is
    the weakest of the three sources and works best with queries naming a
    company-listing PAGE ("portfolio", "our companies", "backed by") rather than
    an event. Extracting the subject of a funding article needs the article read,
    which is an LLM job and not this function's.
    """
    import httpx

    base = (os.getenv("SEARXNG_URL") or "http://iq-searxng:8080").rstrip("/")
    try:
        r = httpx.get(
            f"{base}/search",
            params={"q": query, "format": "json", "language": "en"},
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning(
            "searxng query failed -- is cw-iq-searxng up and is JSON enabled in "
            "its settings.yml? (JSON is off by default)",
            extra={"q": query, "err": str(exc)},
        )
        return []

    out: list[Candidate] = []
    seen: set[str] = set()
    for item in (data.get("results") or [])[: int(num)]:
        url = item.get("url") or ""
        if not url:
            continue
        try:
            host = urlparse(url).netloc
        except Exception:
            continue
        d = _registrable(host)
        if not d or d in seen or _is_press(d):
            continue
        seen.add(d)
        out.append(Candidate(domain=d, source="searxng", source_detail=query))
    log.info("searxng found %s candidates", len(out), extra={"q": query})
    return out


def from_link_graph(page_url: str, *, max_bytes: int = 1_000_000) -> list[Candidate]:
    """Companies linked from a known company's customers/partners page.

    Lowest yield of the three and the most biased -- it can only surface whoever
    the existing set already points at -- but it needs no external service and
    the pages are already in reach of the crawler.
    """
    import httpx

    from src.fetch.ssrf import safe_get

    try:
        with httpx.Client(timeout=20.0, follow_redirects=False) as client:
            resp = safe_get(client, page_url, max_bytes=max_bytes, headers=_UA)
    except Exception as exc:
        log.debug("link_graph fetch failed", extra={"url": page_url, "err": str(exc)})
        return []
    if getattr(resp, "status_code", 0) >= 400:
        return []

    host = _registrable(urlparse(page_url).netloc) or ""
    return [
        Candidate(domain=d, source="link_graph", source_detail=page_url)
        for d in _domains_from_html(getattr(resp, "text", "") or "")
        if d != host
    ]


#: Adapter table. `kind` in discovery_source selects one of these, so a new fund
#: or search phrase is a row in the database, not a deploy.
ADAPTERS = {
    "vc_portfolio": from_vc_portfolio,
    "searxng": from_searxng,
    "link_graph": from_link_graph,
}
