# src/fetch/client.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from . import cache as http_cache
from . import robots, throttle
from .ssrf import SSRFBlocked, safe_get

# --------------------------------------------------------------------------------------------------
# Public constants (the testkit may probe these by alternate names)
# --------------------------------------------------------------------------------------------------

# Minimum politeness gap (fallback when no Crawl-delay); mirror throttle's default
min_gap_s = float(os.getenv("THROTTLE_DEFAULT_MIN_GAP_SECONDS", "1.0"))
# Robots cache TTL
robots_ttl_s = float(os.getenv("ROBOTS_TTL_SECONDS", "86400"))
# HTTP cache default TTL
cache_ttl_s = float(os.getenv("FETCH_CACHE_TTL_SEC", "900"))
# Base backoff used for WAF cooloff (first = 2*base, e.g., 6.0 when base=3.0)
base_backoff_s = float(os.getenv("THROTTLE_BASE_BACKOFF_SECONDS", "3.0"))

# --------------------------------------------------------------------------------------------------
# Module configuration
# --------------------------------------------------------------------------------------------------

# Import the canonical UA from config to ensure consistency with robots.txt checks.
# Previously this module had its own default ("Email-Scraper/0.1") which diverged
# from config.py's "EmailVerifierBot/0.9", causing robots.txt group mismatches.
try:
    from src.config import FETCH_USER_AGENT
except ImportError:
    FETCH_USER_AGENT = os.getenv(
        "FETCH_USER_AGENT",
        (
            "EmailVerifierBot/0.9 (+https://verifier.crestwellpartners.com; "
            "contact: banderson@crestwellpartners.com)"
        ),
    )

# httpx client tuning
CONNECT_TIMEOUT_S = float(os.getenv("FETCH_CONNECT_TIMEOUT_S", "5.0"))
READ_TIMEOUT_S = float(os.getenv("FETCH_READ_TIMEOUT_S", "10.0"))
MAX_REDIRECTS = int(os.getenv("FETCH_MAX_REDIRECTS", "5"))

# Retry policy for transient 5xx
FETCH_MAX_RETRIES = int(os.getenv("FETCH_MAX_RETRIES", "2"))
RETRY_BASE_S = float(os.getenv("FETCH_RETRY_BASE_SECONDS", "0.5"))  # for 5xx only, not WAF

# Response/body handling
FETCH_ACCEPT = os.getenv("FETCH_ACCEPT", "text/html, */*")
FETCH_MAX_READ_BYTES = int(os.getenv("FETCH_MAX_READ_BYTES", str(2_000_000)))  # 2 MB cap

# --------------------------------------------------------------------------------------------------
# Results / Exceptions
# --------------------------------------------------------------------------------------------------


@dataclass
class FetchResult:
    status: int
    url: str
    effective_url: str
    content_type: str | None
    body: bytes | None
    from_cache: bool
    reason: str  # "fresh-cache" | "validated-cache" | "network"
    #        | "blocked-by-robots" | "blocked-ssrf:*" | "waf-throttle" | "error"
    #        | "throttle-budget"
    # Seconds this call spent asleep (per-host pacing + retry backoff). Callers that
    # enforce a wall-clock budget need this so politeness waiting is not charged
    # against time meant for productive work.
    waited_s: float = 0.0
    # True when the raw body exceeded max_body_bytes. `body` is truncated to the cap;
    # callers that would rather skip an oversized page than parse a partial one check
    # this flag instead of measuring len(body).
    oversize: bool = False


class RobotsDisallowed(RuntimeError):
    """Raised if raise_on_disallow=True and a path is blocked by robots.txt."""


# --------------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------------


def _split_host_path(url: str) -> tuple[str, str]:
    parts = urlsplit(url)
    host = parts.netloc.lower()
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    return host, path


def _now() -> float:
    return time.monotonic()


def _cap_body(body: bytes | None, limit: int = FETCH_MAX_READ_BYTES) -> tuple[bytes | None, bool]:
    """Return (body_capped_to_limit, was_oversize)."""
    if body is None:
        return None, False
    if len(body) > limit:
        return body[:limit], True
    return body, False


def _retry_after_seconds(headers: Any) -> float | None:
    """
    Parse a Retry-After header into seconds, accepting both the delta-seconds and the
    HTTP-date forms. Returns None when absent or unparseable so the caller falls back
    to its own backoff. The value is a *floor* for backoff, not a ceiling -- callers
    clamp it (throttle.penalize caps at MAX_BACKOFF_S).
    """
    raw = headers.get("retry-after") or headers.get("Retry-After")
    if not raw:
        return None
    raw = str(raw).strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return max(0.0, (dt - datetime.now(UTC)).total_seconds())


# --------------------------------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------------------------------


class FetcherClient:
    """
    Small wrapper around httpx that enforces robots + politeness throttling + lightweight cache.

    Flow:
      1) robots.is_allowed(host, path) → if False: blocked result (or raise)
         (skipped when enforce_robots=False, for callers that gate robots themselves)
      2) cache.get(url) → if fresh: return without sleeping or touching the network
      3) per attempt: throttle.wait_for_turn(host), then http GET with conditionals
      4) On 200: cache.store_200; throttle.after_response(2xx)
         On 304: cache.store_304; throttle.after_response(304)
         On 403/429: throttle.penalize (honoring Retry-After), then retry
         On >=500: throttle + exponential retry backoff
    """

    def __init__(
        self,
        *,
        user_agent: str | None = None,
        cache_db: str | None = None,
        raise_on_disallow: bool = False,
        enforce_robots: bool = True,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        max_body_bytes: int | None = None,
    ) -> None:
        self.user_agent = user_agent or FETCH_USER_AGENT
        self.raise_on_disallow = raise_on_disallow
        # enforce_robots=False is for callers that already run their own robots gate
        # (the crawler does, with block explainability logging) and would otherwise pay
        # for the check twice. It never means "ignore robots" -- see fetch().
        self.enforce_robots = enforce_robots
        self.max_body_bytes = int(max_body_bytes) if max_body_bytes else FETCH_MAX_READ_BYTES
        # Use a singleton cache unless a specific db is requested
        self.cache = http_cache.default() if cache_db is None else http_cache.Cache(cache_db)

        self._client = httpx.Client(
            headers={"User-Agent": self.user_agent, "Accept": FETCH_ACCEPT},
            timeout=httpx.Timeout(
                READ_TIMEOUT_S if read_timeout is None else read_timeout,
                connect=CONNECT_TIMEOUT_S if connect_timeout is None else connect_timeout,
            ),
            # Redirects are driven by ssrf.safe_get() instead, so that every hop
            # is re-validated. URLs here come from scraped, attacker-controlled
            # domains; httpx would follow a 302 into the private network blind.
            follow_redirects=False,
            max_redirects=MAX_REDIRECTS,
        )

    # ---- core fetch ------------------------------------------------------------------

    def fetch(self, url: str, *, max_wait_s: float | None = None) -> FetchResult:
        """
        Fetch `url` politely.

        `max_wait_s` bounds how long this call may sleep waiting for the host's pacing
        window. When the required wait exceeds it, the call returns immediately with
        reason="throttle-budget" instead of sleeping, so a caller under a wall-clock
        budget can give up on a host that has told us to back off for a long time
        rather than burning its whole budget asleep.
        """
        host, path = _split_host_path(url)

        # (1) robots gate
        if self.enforce_robots and not robots.is_allowed(host, path):
            if self.raise_on_disallow:
                raise RobotsDisallowed(f"Blocked by robots.txt for host={host} path={path}")
            return FetchResult(
                status=451,  # using 451 (Unavailable For Legal Reasons) as a "blocked" sentinel
                url=url,
                effective_url=url,
                content_type=None,
                body=None,
                from_cache=False,
                reason="blocked-by-robots",
            )

        # (2) cache check -- BEFORE the politeness wait. A fresh hit touches no socket,
        # so pacing it would slow re-crawls to the speed of a cold crawl and defeat the
        # point of the cache. It also must not advance the host's window.
        entry, is_fresh = self.cache.get(url)
        if is_fresh and entry is not None:
            body, oversize = _cap_body(entry.body, self.max_body_bytes)
            return FetchResult(
                status=entry.status,
                url=url,
                effective_url=(entry.effective_url or f"{entry.scheme}://{entry.host}{entry.path}"),
                content_type=entry.content_type,
                body=body,
                from_cache=True,
                reason="fresh-cache",
                oversize=oversize,
            )

        # Prepare conditional headers if we have a cache entry
        headers = self.cache.conditionals(url) if entry is not None else {}

        # (3) politeness wait + network fetch / response handling. The wait lives inside
        # the retry loop so that a retry after a 429 actually serves out the backoff.
        return self._do_request_with_retries(url, headers, host, max_wait_s=max_wait_s)

    # ----------------------------------------------------------------------------------
    # Convenience methods
    # ----------------------------------------------------------------------------------

    def get(self, url: str) -> FetchResult:
        return self.fetch(url)

    # ----------------------------------------------------------------------------------
    # Internals
    # ----------------------------------------------------------------------------------

    def _do_request_with_retries(
        self,
        url: str,
        headers: dict[str, str],
        host: str,
        *,
        max_wait_s: float | None = None,
    ) -> FetchResult:
        attempt = 0
        waited = 0.0
        while True:
            # Politeness wait, re-evaluated on every attempt. Doing this inside the loop
            # (rather than once before it) is what makes a retry after a 429/5xx actually
            # serve out the backoff that penalize()/after_response() just scheduled.
            pending = throttle.next_allowed_at(host) - _now()
            if max_wait_s is not None and pending > max_wait_s:
                return FetchResult(
                    status=429,
                    url=url,
                    effective_url=url,
                    content_type=None,
                    body=None,
                    from_cache=False,
                    reason="throttle-budget",
                    waited_s=waited,
                )
            waited += throttle.wait_for_turn(host)

            try:
                resp = safe_get(
                    self._client,
                    url,
                    max_hops=MAX_REDIRECTS,
                    headers=headers,
                    # Bound the DECOMPRESSED read. _cap_body below only
                    # truncates what is already in memory, which is too late
                    # against a gzip bomb.
                    max_bytes=self.max_body_bytes,
                )
            except SSRFBlocked as exc:
                return FetchResult(
                    status=451,
                    url=url,
                    effective_url=url,
                    content_type=None,
                    body=None,
                    from_cache=False,
                    reason=f"blocked-ssrf:{exc}",
                    waited_s=waited,
                )
            except httpx.RequestError as exc:
                status = 599
                throttle.after_response(host, status)
                if attempt >= FETCH_MAX_RETRIES:
                    return FetchResult(
                        status=status,
                        url=url,
                        effective_url=url,
                        content_type=None,
                        body=None,
                        from_cache=False,
                        reason=f"error:{type(exc).__name__}",
                        waited_s=waited,
                    )
                waited += self._sleep_retry(attempt)
                attempt += 1
                continue

            status = int(resp.status_code)
            retry_after = _retry_after_seconds(resp.headers)

            if status == 304:
                self.cache.store_304(url, resp.headers)
                throttle.after_response(host, 304)
                fresh_entry, _ = self.cache.get(url)
                body, oversize = _cap_body(
                    fresh_entry.body if (fresh_entry and fresh_entry.body) else None,
                    self.max_body_bytes,
                )
                content_type = (
                    fresh_entry.content_type if fresh_entry else resp.headers.get("Content-Type")
                )
                return FetchResult(
                    status=fresh_entry.status if fresh_entry else 304,
                    url=url,
                    effective_url=(
                        (fresh_entry.effective_url if fresh_entry else None) or str(resp.url)
                    ),
                    content_type=content_type,
                    body=body,
                    from_cache=True,
                    reason="validated-cache",
                    waited_s=waited,
                    oversize=oversize,
                )

            if 200 <= status < 300:
                body, oversize = _cap_body(resp.content or b"", self.max_body_bytes)
                ct = resp.headers.get("Content-Type")
                # An oversized response is deliberately NOT cached: we only hold a
                # truncated prefix, and replaying that as a cache hit would hand the
                # caller a partial page that no longer looks oversized.
                if not oversize:
                    self.cache.store_200(
                        url, status, ct, body, resp.headers, effective_url=str(resp.url)
                    )
                throttle.after_response(host, status)
                return FetchResult(
                    status=status,
                    url=url,
                    effective_url=str(resp.url),
                    content_type=ct,
                    body=body,
                    from_cache=False,
                    reason="network",
                    waited_s=waited,
                    oversize=oversize,
                )

            if status in (403, 429):
                # WAF / rate limit. Schedule an exponential cool-off (honoring
                # Retry-After when the server sent one), then retry -- the wait at the
                # top of the loop serves the cool-off. Previously this returned straight
                # away, which permanently lost the page on a transient 429.
                throttle.penalize(host, min_seconds=retry_after)
                if attempt >= FETCH_MAX_RETRIES:
                    return FetchResult(
                        status=status,
                        url=url,
                        effective_url=str(resp.url),
                        content_type=resp.headers.get("Content-Type"),
                        body=None,
                        from_cache=False,
                        reason="waf-throttle",
                        waited_s=waited,
                    )
                attempt += 1
                continue

            if status >= 500:
                if retry_after is not None:
                    throttle.penalize(host, min_seconds=retry_after)
                else:
                    throttle.after_response(host, status)
                if attempt >= FETCH_MAX_RETRIES:
                    return FetchResult(
                        status=status,
                        url=url,
                        effective_url=str(resp.url),
                        content_type=resp.headers.get("Content-Type"),
                        body=None,
                        from_cache=False,
                        reason="error:server",
                        waited_s=waited,
                    )
                waited += self._sleep_retry(attempt)
                attempt += 1
                continue

            # Other statuses (1xx/3xx except 304 / 4xx non-WAF):
            throttle.after_response(host, status)
            return FetchResult(
                status=status,
                url=url,
                effective_url=str(resp.url),
                content_type=resp.headers.get("Content-Type"),
                body=None,
                from_cache=False,
                reason="network",
                waited_s=waited,
            )

    def _sleep_retry(self, attempt: int) -> float:
        # Basic exponential backoff for 5xx/RequestError; tests can monkeypatch time.sleep
        delay = RETRY_BASE_S * (2**attempt)
        time.sleep(delay)
        return delay

    # ----------------------------------------------------------------------------------
    # Context manager
    # ----------------------------------------------------------------------------------

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def __enter__(self) -> FetcherClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# --- R10 facade --------------------------------------------------------------


def fetch_url(url: str) -> FetchResult:
    """
    Convenience wrapper for the R10 crawler.

    Usage:
        from src.fetch import fetch_url
        res = fetch_url("https://example.com/")
    """
    client = FetcherClient()
    return client.get(url)


__all__ = [
    "FetcherClient",
    "FetchResult",
    "RobotsDisallowed",
    "min_gap_s",
    "robots_ttl_s",
    "cache_ttl_s",
    "base_backoff_s",
    "fetch_url",
]
