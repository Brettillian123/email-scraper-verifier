# src/crawl/targets.py
from __future__ import annotations

from urllib.parse import urljoin, urlparse


def seed_urls(base: str, seed_paths: list[str]) -> list[str]:
    # base like "https://example.com/"
    return [urljoin(base, p if p.startswith("/") else f"/{p}") for p in seed_paths]


def is_internal_url(base_host: str, href: str) -> bool:
    try:
        host = urlparse(href).netloc.lower()
        return host == "" or host == base_host
    except Exception:
        return False


def looks_relevant(url: str, keyword_hints: list[str]) -> bool:
    low = url.lower()
    return any(k in low for k in keyword_hints)
