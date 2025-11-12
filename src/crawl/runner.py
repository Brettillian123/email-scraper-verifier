# src/crawl/runner.py
from __future__ import annotations

import re
import time
from collections import deque
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from src.config import (
    CRAWL_FOLLOW_KEYWORDS,
    CRAWL_HTML_MAX_BYTES,
    CRAWL_MAX_DEPTH,
    CRAWL_MAX_PAGES_PER_DOMAIN,
    CRAWL_SEED_PATHS,
)
from src.fetch import fetch_url  # R09 exposed this for R10 use

from .targets import is_internal_url, looks_relevant, seed_urls


@dataclass
class Page:
    url: str
    html: bytes  # store bytes; decode later when parsing
    fetched_at: float


# Schemes we never enqueue (R10: skip mailto/offsite/JS/etc.)
_SKIP_SCHEMES: tuple[str, ...] = ("mailto:", "tel:", "javascript:")


def _header_get(headers, name: str) -> str | None:
    """Case-insensitive header getter that tolerates plain dicts or requests-like objects."""
    if not headers:
        return None
    try:
        v = headers.get(name)  # type: ignore[attr-defined]
        if v is not None:
            return v
    except Exception:
        pass
    try:
        for k, v in headers.items():  # type: ignore[attr-defined]
            if isinstance(k, str) and k.lower() == name.lower():
                return v
    except Exception:
        pass
    return None


def _header_int(headers, name: str) -> int | None:
    v = _header_get(headers, name)
    if v is None:
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _is_html(headers, body: bytes) -> bool:
    """Allow only HTML in R10; header-led with a small sniff fallback."""
    ctype = (_header_get(headers, "Content-Type") or "").lower()
    if "text/html" in ctype:
        return True
    # Fallback sniff (very light): look for an <html or <!doctype
    head = body[:4096].lstrip().lower()
    return head.startswith(b"<!doctype html") or b"<html" in head


def crawl_domain(domain: str) -> list[Page]:
    """BFS from seed paths; single-host, robots-aware via fetch_url()."""
    base = f"https://{domain}/"
    base_host = urlparse(base).netloc.lower()

    max_pages = CRAWL_MAX_PAGES_PER_DOMAIN
    max_depth = CRAWL_MAX_DEPTH

    seeds = [s.strip() for s in CRAWL_SEED_PATHS.split(",") if s.strip()]
    hints = [h.strip() for h in CRAWL_FOLLOW_KEYWORDS.split(",") if h.strip()]

    start_urls = seed_urls(base, seeds)

    seen: set[str] = set()
    pages: list[Page] = []
    q: deque[tuple[str, int]] = deque((s, 0) for s in start_urls)

    while q and len(pages) < max_pages:
        url, depth = q.popleft()
        if url in seen:
            continue
        seen.add(url)

        # All fetching goes through the robots-aware client (R09)
        res = fetch_url(url)

        # Support both real client (status/body) and older stub (status_code/content)
        status = getattr(res, "status", getattr(res, "status_code", None))
        if not (isinstance(status, int) and 200 <= status < 300):
            continue

        headers = getattr(res, "headers", {}) or {}
        # If the server advertises a huge body, skip before we parse (guardrail)
        cl = _header_int(headers, "Content-Length")
        if cl is not None and cl > CRAWL_HTML_MAX_BYTES:
            continue

        content = getattr(res, "body", getattr(res, "content", None))
        if not content:
            continue

        # Normalize to bytes
        body = content.encode("utf-8", "ignore") if isinstance(content, str) else bytes(content)

        # Enforce HTML-only + size cap (R10)
        if not _is_html(headers, body):
            continue
        if len(body) > CRAWL_HTML_MAX_BYTES:
            # Hard cap to avoid memory/FD pressure; do not store or parse
            continue

        pages.append(Page(url=url, html=body, fetched_at=time.time()))

        # Depth guardrail: do not enqueue children beyond max_depth
        if depth >= max_depth:
            continue

        # Very small, dependency-free link pull
        # Only consider hrefs; skip fragments and query-heavy paginators by ignoring '?' and '#'
        text = body.decode("utf-8", "ignore")
        for m in re.finditer(
            r'href\s*=\s*[\'"](?P<h>[^\'"#?]+)',
            text,
            flags=re.I,
        ):
            href = m.group("h").strip()
            # Skip disallowed schemes fast (mailto, tel, javascript, etc.)
            if href.startswith(_SKIP_SCHEMES):
                continue

            cand = urljoin(url, href)

            # Only HTTP(S) links; everything else is ignored in R10
            scheme = urlparse(cand).scheme.lower()
            if scheme not in ("http", "https"):
                continue

            # Same-host only (guardrail against offsite traversal)
            if not is_internal_url(base_host, cand):
                continue

            # Heuristic relevance filter to prioritize likely profile/contact pages
            if looks_relevant(cand, hints):
                q.append((cand, depth + 1))

    return pages
