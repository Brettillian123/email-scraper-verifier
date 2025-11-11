# src/fetch/throttle.py
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

from . import robots

# --------------------------------------------------------------------------------------
# Configuration (env-overridable)
# --------------------------------------------------------------------------------------

# Base backoff (first WAF cool-off will be 2 * base, e.g., 2 * 3s = 6s)
BASE_BACKOFF_S = float(os.getenv("THROTTLE_BASE_BACKOFF_SECONDS", "3.0"))
# Maximum backoff cap
MAX_BACKOFF_S = float(os.getenv("THROTTLE_MAX_BACKOFF_SECONDS", "60.0"))
# Safety minimum per-host gap if robots has no Crawl-delay
DEFAULT_MIN_GAP_S = float(os.getenv("THROTTLE_DEFAULT_MIN_GAP_SECONDS", "1.0"))

# --------------------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------------------


@dataclass
class _HostState:
    next_allowed_at: float = 0.0  # monotonic seconds when host can be hit again
    waf_strikes: int = 0  # consecutive 403/429 counters


_MEMO: dict[str, _HostState] = {}
_LOCKS: dict[str, threading.Lock] = {}
_GLOBAL_LOCK = threading.Lock()


def _now() -> float:
    # Use monotonic so tests can monkeypatch the clock
    return time.monotonic()


def _sleep(dt: float) -> None:
    # Calls real time.sleep, but tests can monkeypatch it.
    time.sleep(dt)


def _host_lock(host: str) -> threading.Lock:
    host = host.strip().lower()
    with _GLOBAL_LOCK:
        lk = _LOCKS.get(host)
        if lk is None:
            lk = threading.Lock()
            _LOCKS[host] = lk
        return lk


def _state(host: str) -> _HostState:
    host = host.strip().lower()
    st = _MEMO.get(host)
    if st is None:
        st = _HostState()
        _MEMO[host] = st
    return st


# --------------------------------------------------------------------------------------
# Core API
# --------------------------------------------------------------------------------------


def wait_for_turn(host: str) -> float:
    """
    Block (sleep) until this host is eligible to be hit.
    Returns the number of seconds slept (0 if no wait).
    """
    host = host.strip().lower()
    with _host_lock(host):
        st = _state(host)
        now = _now()
        if st.next_allowed_at <= now:
            return 0.0
        dt = st.next_allowed_at - now
        if dt > 0:
            _sleep(dt)
            return dt
        return 0.0


def _resolved_crawl_delay(host: str, override: float | None) -> float:
    if override is not None:
        try:
            return max(0.0, float(override))
        except Exception:
            return DEFAULT_MIN_GAP_S
    # Ask robots for Crawl-delay; fall back to default
    try:
        cd = robots.get_crawl_delay(host)
        return max(0.0, float(cd))
    except Exception:
        return DEFAULT_MIN_GAP_S


def mark_ok(host: str, crawl_delay_s: float | None = None) -> float:
    """
    Record a successful (2xx/304) response and schedule the next allowed time.
    Returns the new per-host delay that was scheduled.
    """
    host = host.strip().lower()
    delay = _resolved_crawl_delay(host, crawl_delay_s)
    with _host_lock(host):
        st = _state(host)
        st.waf_strikes = 0  # reset consecutive WAF counters
        now = _now()
        # Never move next_allowed backwards; success sets gap = crawl-delay
        st.next_allowed_at = max(st.next_allowed_at, now) + delay
        return delay


def penalize(host: str) -> float:
    """
    Record a WAF block (429 or 403). Increase backoff exponentially and schedule it.
    Returns the cool-off seconds that were scheduled.
    Policy: backoff = min(MAX_BACKOFF_S, BASE_BACKOFF_S * 2**strikes)
      where strikes increments on each penalize() call.
      This yields first backoff of 2 * BASE (e.g., 6s for base=3s).
    """
    host = host.strip().lower()
    with _host_lock(host):
        st = _state(host)
        st.waf_strikes += 1
        backoff = BASE_BACKOFF_S * (2**st.waf_strikes)
        if backoff > MAX_BACKOFF_S:
            backoff = MAX_BACKOFF_S
        now = _now()
        st.next_allowed_at = max(st.next_allowed_at, now) + backoff
        return backoff


def after_response(host: str, status: int, crawl_delay_s: float | None = None) -> float:
    """
    Convenience: update throttling state based on HTTP status.
    Returns the scheduled delay applied (crawl-delay for success, or cool-off for WAF).
    """
    if 200 <= int(status) <= 299 or int(status) == 304:
        return mark_ok(host, crawl_delay_s=crawl_delay_s)
    if int(status) in (403, 429):
        return penalize(host)
    # For other statuses (e.g., 3xx except 304, 4xx non-WAF, 5xx), keep the current window.
    # Still apply at least the crawl-delay gap, but don't count as success (no reset of strikes).
    host = host.strip().lower()
    delay = _resolved_crawl_delay(host, crawl_delay_s)
    with _host_lock(host):
        st = _state(host)
        now = _now()
        st.next_allowed_at = max(st.next_allowed_at, now) + delay
    return delay


# --------------------------------------------------------------------------------------
# Introspection / test helpers
# --------------------------------------------------------------------------------------


def next_allowed_at(host: str) -> float:
    """Return the monotonic timestamp when this host is next eligible."""
    host = host.strip().lower()
    with _host_lock(host):
        return _state(host).next_allowed_at


def waf_strikes(host: str) -> int:
    """Return current consecutive WAF strike count."""
    host = host.strip().lower()
    with _host_lock(host):
        return _state(host).waf_strikes


def clear(host: str | None = None) -> None:
    """Clear throttling state (all hosts or a single host)."""
    if host is None:
        with _GLOBAL_LOCK:
            _MEMO.clear()
            _LOCKS.clear()
        return
    host = host.strip().lower()
    with _host_lock(host):
        _MEMO.pop(host, None)
        _LOCKS.pop(host, None)


# Backwards-friendly aliases (some tests/users prefer these names)
record_ok = mark_ok
cooloff = penalize
before_request = wait_for_turn
update_after_response = after_response
