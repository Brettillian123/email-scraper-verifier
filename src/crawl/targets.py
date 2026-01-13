# src/crawl/targets.py
from __future__ import annotations

from urllib.parse import urljoin, urlparse


def seed_urls(base: str, seed_paths: list[str]) -> list[str]:
    """
    Build absolute seed URLs from a base URL and a list of seed paths.

    Args:
        base: Base URL like "https://example.com/"
        seed_paths: List of paths like ["/about", "team", "/company/leadership"]

    Returns:
        Absolute URLs with the original order preserved, de-duplicated.
    """
    out: list[str] = []
    seen: set[str] = set()
    base_norm = base if base.endswith("/") else f"{base}/"

    for raw in seed_paths:
        p = (raw or "").strip()
        if not p:
            continue
        if not p.startswith("/"):
            p = f"/{p}"

        u = urljoin(base_norm, p)
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out


def _normalize_host(host: str) -> str:
    """
    Normalize host for internal-link checks:
      - lowercase + strip whitespace
      - treat example.com and www.example.com as equivalent
    """
    h = (host or "").lower().strip()
    if h.startswith("www."):
        return h[4:]
    return h


def is_internal_url(base_host: str, href: str) -> bool:
    """
    Return True if href is internal to base_host (treating www variants as internal).

    Notes:
      - Relative URLs (no netloc) are considered internal.
      - Absolute URLs are internal if normalized host matches.
    """
    try:
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        if host == "":
            return True
        return _normalize_host(host) == _normalize_host(base_host)
    except Exception:
        return False


def looks_relevant(url: str, keyword_hints: list[str]) -> bool:
    """
    Return True if the URL looks relevant based on keyword hints.

    This is a cheap substring filter intended to constrain the crawl graph.
    """
    low = (url or "").lower()
    return any(k in low for k in keyword_hints if k)
