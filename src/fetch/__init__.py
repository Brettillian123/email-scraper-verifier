# src/fetch/__init__.py
"""
Tiny fetcher package: robots enforcement, politeness throttling, HTTP cache, and an httpx client.

Crawler-facing API:
  - fetch_url(url: str) -> FetchResult

Other public entry points (advanced/internal use):
  - FetcherClient, FetchResult, RobotsDisallowed
  - robots helpers: get_crawl_delay, is_allowed
  - throttle helpers: wait_for_turn, after_response, penalize, mark_ok
  - cache: Cache, CacheEntry, default_cache()
"""

from .cache import (
    Cache,
    CacheEntry,
)
from .cache import (
    default as default_cache,
)
from .client import (
    FetcherClient,
    FetchResult,
    RobotsDisallowed,
    base_backoff_s,
    cache_ttl_s,
    fetch_url,  # <-- re-export the single function the R10 crawler will use
    min_gap_s,
    robots_ttl_s,
)
from .robots import (
    clear_cache as clear_robots_cache,
)
from .robots import (
    get_crawl_delay,
    is_allowed,
)
from .throttle import (
    after_response,
    mark_ok,
    next_allowed_at,
    penalize,
    waf_strikes,
    wait_for_turn,
)
from .throttle import (
    clear as clear_throttle,
)

__all__ = [
    # crawler-facing facade
    "fetch_url",
    # client
    "FetcherClient",
    "FetchResult",
    "RobotsDisallowed",
    "min_gap_s",
    "robots_ttl_s",
    "cache_ttl_s",
    "base_backoff_s",
    # robots
    "get_crawl_delay",
    "is_allowed",
    "clear_robots_cache",
    # throttle
    "wait_for_turn",
    "after_response",
    "penalize",
    "mark_ok",
    "next_allowed_at",
    "waf_strikes",
    "clear_throttle",
    # cache
    "Cache",
    "CacheEntry",
    "default_cache",
]

__version__ = "0.1.0"
