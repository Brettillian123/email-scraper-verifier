# src/fetch/robots.py
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field

import httpx

# --------------------------------------------------------------------------------------
# Configuration (env-overridable)
# --------------------------------------------------------------------------------------

FETCH_USER_AGENT = os.getenv(
    "FETCH_USER_AGENT",
    "Email-Scraper/0.1 (+contact: verifier@crestwellpartners.com)",
)

# Cache a successful robots fetch/parse for 24h by default
ROBOTS_TTL_SECONDS = float(os.getenv("ROBOTS_TTL_SECONDS", "86400"))
# If origin is down (>=500 or timeout), deny by default for this window
ROBOTS_DENY_TTL_SECONDS = float(os.getenv("ROBOTS_DENY_TTL_SECONDS", "600"))
# Network timeout for fetching robots.txt
ROBOTS_TIMEOUT_SECONDS = float(os.getenv("ROBOTS_TIMEOUT_SECONDS", "10"))
# Default crawl delay when none specified
ROBOTS_DEFAULT_DELAY_SECONDS = float(os.getenv("ROBOTS_DEFAULT_DELAY_SECONDS", "1"))

# Enforce HTTPS per spec here; if you later want to probe HTTP fallback, make it configurable
_ROBOTS_SCHEME = "https"

# --------------------------------------------------------------------------------------
# Data structures
# --------------------------------------------------------------------------------------


@dataclass
class _Rule:
    allow: bool
    path: str  # stored as-is, evaluated with simple prefix match


@dataclass
class _Policy:
    """The resolved policy for *our* UA (or *) on a host."""

    kind: str  # "allow_all" | "deny_all" | "rules"
    rules: list[_Rule] = field(default_factory=list)
    crawl_delay: float | None = None
    status_code: int | None = None
    reason: str = ""
    # monotonic-based expiry; tests can fake time.monotonic()
    expires_at: float = 0.0
    fetched_at: float = 0.0


# host → _Policy (for our UA)
_MEMO: dict[str, _Policy] = {}
_LOCKS: dict[str, threading.Lock] = {}
_GLOBAL_LOCK = threading.Lock()


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def _now() -> float:
    # use monotonic so tests can freeze time via monkeypatch
    return time.monotonic()


def _host_lock(host: str) -> threading.Lock:
    with _GLOBAL_LOCK:
        lk = _LOCKS.get(host)
        if lk is None:
            lk = threading.Lock()
            _LOCKS[host] = lk
        return lk


def _strip_comment(line: str) -> str:
    # remove comments starting with '#'
    idx = line.find("#")
    return line if idx < 0 else line[:idx]


def _split_kv(line: str) -> tuple[str, str] | None:
    if ":" not in line:
        return None
    k, v = line.split(":", 1)
    return k.strip().lower(), v.strip()


@dataclass
class _Group:
    uas: list[str] = field(default_factory=list)  # lowercased UA tokens
    rules: list[_Rule] = field(default_factory=list)
    crawl_delay: float | None = None


@dataclass
class _ParsedRobots:
    groups: list[_Group] = field(default_factory=list)


def _parse_robots(text: str) -> _ParsedRobots:
    """
    Minimal robots.txt parser supporting:
      - User-agent
      - Allow
      - Disallow
      - Crawl-delay
    Groups are contiguous UA lines followed by directives (RFC 9309 style).
    We use simple prefix matching for paths (no wildcards).
    """
    groups: list[_Group] = []
    current = _Group()
    seen_any_directive = False  # to decide when a new UA starts a new group

    for raw in text.splitlines():
        line = _strip_comment(raw).strip()
        if not line:
            # blank line does not force flush; spec allows
            continue
        kv = _split_kv(line)
        if not kv:
            continue
        key, val = kv

        if key == "user-agent":
            val_lc = val.lower()
            if not current.uas and not seen_any_directive:
                # first UA for this (yet-empty) group
                current.uas.append(val_lc)
                # keep collecting UAs until a directive appears
                continue
            if current.uas and not seen_any_directive:
                # additional UA for the same group
                current.uas.append(val_lc)
                continue
            # starting a new group (previous had directives already)
            if current.uas or current.rules or current.crawl_delay is not None:
                groups.append(current)
            current = _Group(uas=[val_lc])
            seen_any_directive = False
            continue

        # From here, we’re in directive territory
        seen_any_directive = True

        if key == "allow":
            if val == "":
                # empty allow is effectively a no-op
                continue
            current.rules.append(_Rule(True, val))
        elif key == "disallow":
            if val == "":
                # empty Disallow means "allow all", which is a no-op
                continue
            current.rules.append(_Rule(False, val))
        elif key == "crawl-delay":
            try:
                cd = float(val)
                if cd >= 0:
                    current.crawl_delay = cd
            except ValueError:
                # ignore invalid values
                pass
        else:
            # ignore Sitemap and any other directives for this MVP
            pass

    # flush last group
    if current.uas or current.rules or current.crawl_delay is not None:
        groups.append(current)

    return _ParsedRobots(groups=groups)


def _best_group_for_ua(parsed: _ParsedRobots, ua: str) -> _Group | None:
    """
    RFC-ish matching:
      - UA tokens are matched case-insensitively against the *prefix* of our UA string.
      - Choose the group with the longest matching token (most specific).
      - If none match, fall back to a group whose UA is '*'.
      - If multiple '*' groups exist, take the first encountered.
    """
    ua_lc = ua.lower()
    best: _Group | None = None
    best_len = -1
    star_fallback: _Group | None = None

    for g in parsed.groups:
        for tok in g.uas:
            if tok == "*":
                if star_fallback is None:
                    star_fallback = g
                continue
            if ua_lc.startswith(tok) and len(tok) > best_len:
                best = g
                best_len = len(tok)

    if best is not None:
        return best
    return star_fallback


def _evaluate_rules(path: str, rules: list[_Rule]) -> bool:
    """
    Apply longest-prefix rule wins.
    Tie-breaker: if equal length, 'Allow' beats 'Disallow'.
    If no rule matches, allow.
    """
    # normalize to path-only (strip query/fragment)
    if not path.startswith("/"):
        # defensive: ensure we evaluate a path
        # accept full URLs but only use their path component
        # very small, dependency-free parse:
        # find first '/', then take until '?' or '#'
        idx = path.find("/")
        path = path[idx:] if idx >= 0 else "/"
    q = path.split("?", 1)[0].split("#", 1)[0]

    best_rule: _Rule | None = None
    best_len = -1

    for r in rules:
        # simple prefix match
        if q.startswith(r.path):
            plen = len(r.path)
            if plen > best_len or (
                plen == best_len and (best_rule is None or r.allow and not best_rule.allow)
            ):
                best_rule = r
                best_len = plen

    if best_rule is None:
        return True
    return best_rule.allow


def _build_policy_from_text(text: str) -> _Policy:
    parsed = _parse_robots(text)
    grp = _best_group_for_ua(parsed, FETCH_USER_AGENT)
    if grp is None:
        # no applicable group → allow all with default delay
        return _Policy(
            kind="allow_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason="no-applicable-group",
            status_code=200,
        )
    # rules present for chosen group
    return _Policy(
        kind="rules",
        rules=grp.rules[:],
        crawl_delay=grp.crawl_delay
        if grp.crawl_delay is not None
        else ROBOTS_DEFAULT_DELAY_SECONDS,
        reason="parsed",
        status_code=200,
    )


def _fetch_and_resolve(host: str) -> _Policy:
    """
    Fetch https://{host}/robots.txt and return a resolved policy for our UA.
    Status handling:
      - 200 → parse and enforce rules; cache for ROBOTS_TTL_SECONDS
      - 404/401/403 → treat as no robots (allow_all); cache for ROBOTS_TTL_SECONDS
      - >=500 or timeout → deny_all for ROBOTS_DENY_TTL_SECONDS
    """
    url = f"{_ROBOTS_SCHEME}://{host}/robots.txt"
    try:
        with httpx.Client(
            timeout=ROBOTS_TIMEOUT_SECONDS,
            follow_redirects=True,
            headers={"User-Agent": FETCH_USER_AGENT},
        ) as client:
            resp = client.get(url)
    except (httpx.RequestError, httpx.TimeoutException):
        pol = _Policy(
            kind="deny_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason="timeout",
            status_code=None,
        )
        now = _now()
        pol.fetched_at = now
        pol.expires_at = now + ROBOTS_DENY_TTL_SECONDS
        return pol

    status = resp.status_code
    now = _now()

    if status == 200:
        pol = _build_policy_from_text(resp.text or "")
        pol.status_code = 200
        pol.reason = "ok"
        pol.fetched_at = now
        pol.expires_at = now + ROBOTS_TTL_SECONDS
        return pol

    if status in (401, 403, 404):
        pol = _Policy(
            kind="allow_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason=f"{status}-treat-as-no-robots",
            status_code=status,
        )
        pol.fetched_at = now
        pol.expires_at = now + ROBOTS_TTL_SECONDS
        return pol

    if status >= 500:
        pol = _Policy(
            kind="deny_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason=f"{status}-server-error",
            status_code=status,
        )
        pol.fetched_at = now
        pol.expires_at = now + ROBOTS_DENY_TTL_SECONDS
        return pol

    # Other odd statuses: be conservative but not punitive → allow_all
    pol = _Policy(
        kind="allow_all",
        crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
        reason=f"{status}-treated-as-allow",
        status_code=status,
    )
    pol.fetched_at = now
    pol.expires_at = now + ROBOTS_TTL_SECONDS
    return pol


def _get_policy(host: str, *, force_refresh: bool = False) -> _Policy:
    host = host.strip().lower()
    if not host:
        # Defensive: empty host → allow all with default delay (non-caching)
        return _Policy(
            kind="allow_all", crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS, reason="empty-host"
        )

    with _host_lock(host):
        pol = _MEMO.get(host)
        now = _now()
        if force_refresh or pol is None or pol.expires_at <= now:
            pol = _fetch_and_resolve(host)
            # stamp expiry/fetched times if missing (shouldn’t be)
            if pol.fetched_at == 0.0:
                pol.fetched_at = now
            if pol.expires_at == 0.0:
                # default to TTL if not set
                ttl = ROBOTS_TTL_SECONDS if pol.kind != "deny_all" else ROBOTS_DENY_TTL_SECONDS
                pol.expires_at = now + ttl
            _MEMO[host] = pol
        return pol


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def get_crawl_delay(host: str, *, refresh: bool = False) -> float:
    """
    Return crawl-delay (seconds) for this host resolved for our UA.
    Falls back to ROBOTS_DEFAULT_DELAY_SECONDS if unspecified.
    """
    pol = _get_policy(host, force_refresh=refresh)
    # Always return a non-negative delay
    cd = pol.crawl_delay if pol.crawl_delay is not None else ROBOTS_DEFAULT_DELAY_SECONDS
    try:
        # guard against NaN/negatives sneaking in
        return max(0.0, float(cd))
    except Exception:
        return ROBOTS_DEFAULT_DELAY_SECONDS


def is_allowed(host: str, url_path: str, *, refresh: bool = False) -> bool:
    """
    Check if path is allowed to be fetched for our UA on this host.
    During outage windows (deny_all policy), this returns False.
    """
    pol = _get_policy(host, force_refresh=refresh)

    if pol.kind == "deny_all":
        return False
    if pol.kind == "allow_all":
        return True
    # rules
    return _evaluate_rules(url_path, pol.rules)


# --------------------------------------------------------------------------------------
# Test/ops helpers (safe to use in tests)
# --------------------------------------------------------------------------------------


def _debug_peek_policy(host: str) -> _Policy | None:
    """Return the cached policy for inspection (or None)."""
    return _MEMO.get(host.strip().lower())


def clear_cache(host: str | None = None) -> None:
    """Clear robots memoization cache (all or one host)."""
    with _GLOBAL_LOCK:
        if host is None:
            _MEMO.clear()
            _LOCKS.clear()
        else:
            _MEMO.pop(host.strip().lower(), None)
            _LOCKS.pop(host.strip().lower(), None)
