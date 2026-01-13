# src/fetch/robots.py
"""
Robots.txt enforcement layer (R10 compliance).

This module:
  - Fetches and caches robots.txt per host.
  - Applies Allow/Disallow rules for our configured user-agent.
  - Respects Crawl-delay.
  - Provides explainability for blocked URLs (Task A).

Status handling:
  - 200 → parse and enforce rules; cache for ROBOTS_TTL_SECONDS
  - 404/401/403 → treat as no robots (allow_all); cache for ROBOTS_TTL_SECONDS
  - >=500 or timeout → deny_all for ROBOTS_DENY_TTL_SECONDS
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from src.config import FETCH_USER_AGENT

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------

ROBOTS_TTL_SECONDS: float = 3600.0  # 1 hour cache for successful fetches
ROBOTS_DENY_TTL_SECONDS: float = 300.0  # 5 min cache for server errors (be conservative)
ROBOTS_TIMEOUT_SECONDS: float = 10.0
ROBOTS_DEFAULT_DELAY_SECONDS: float = 1.25  # polite default when no crawl-delay

_ROBOTS_SCHEME = "https"

# --------------------------------------------------------------------------------------
# Data structures
# --------------------------------------------------------------------------------------


@dataclass
class _Rule:
    allow: bool
    path: str


@dataclass
class _Policy:
    kind: str  # "allow_all" | "deny_all" | "rules"
    rules: list[_Rule] = field(default_factory=list)
    crawl_delay: float | None = None
    status_code: int | None = None
    reason: str = ""
    # monotonic-based expiry; tests can fake time.monotonic()
    expires_at: float = 0.0
    fetched_at: float = 0.0
    # For explainability: the URL we fetched robots.txt from
    robots_url: str = ""


@dataclass
class RobotsBlockInfo:
    """
    Structured explanation of why a URL was blocked by robots.txt.

    Used for logging, diagnostics, and queue result payloads.
    """

    blocked_url: str
    robots_url: str
    user_agent: str
    allowed: bool
    reason: str  # "disallow_rule", "deny_all", "allow_all", etc.
    matched_rule: str | None  # The Disallow/Allow directive path, or None if unavailable
    notes: str | None = None  # Additional context

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict for queue payloads."""
        return {
            "blocked_url": self.blocked_url,
            "robots_url": self.robots_url,
            "user_agent": self.user_agent,
            "allowed": self.allowed,
            "reason": self.reason,
            "matched_rule": self.matched_rule,
            "notes": self.notes,
        }


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

        # From here, we're in directive territory
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
                pass
        else:
            # ignore other directives for this MVP
            pass

    # flush last group
    if current.uas or current.rules or current.crawl_delay is not None:
        groups.append(current)

    return _ParsedRobots(groups=groups)


def _ua_product_tokens(ua: str) -> list[str]:
    """
    Extract likely product tokens from a User-Agent string.

    RFC 9309 matching is defined against the UA string prefix, but in practice
    UA strings may include multiple product tokens and/or leading branding.
    To avoid incorrectly falling back to '*' groups, we consider *product tokens*
    separated by whitespace and ignore parenthetical comment chunks.

    Example:
      "Email-Scraper/1.0 (+https://x; mailto:y)" -> ["email-scraper/1.0"]
      "CrestwellPartners Email-Scraper/1.0 (+...)" -> ["crestwellpartners", "email-scraper/1.0"]
    """
    out: list[str] = []
    for raw in (ua or "").strip().lower().split():
        if not raw:
            continue
        if raw.startswith("("):
            # parenthetical comments; ignore entirely
            continue
        tok = raw.strip(" \t\r\n;,)\"'")
        if tok:
            out.append(tok)
    return out


def _ua_token_matches(ua: str, tok: str) -> bool:
    """
    Match a robots UA token against our UA string.

    We accept:
      - strict prefix match against full UA string
      - OR prefix match against any extracted product token
    """
    ua_lc = (ua or "").strip().lower()
    tok_lc = (tok or "").strip().lower()
    if not tok_lc:
        return False
    if tok_lc == "*":
        return True

    if ua_lc.startswith(tok_lc):
        return True

    for p in _ua_product_tokens(ua_lc):
        if p.startswith(tok_lc):
            return True

    return False


def _best_group_for_ua(parsed: _ParsedRobots, ua: str) -> _Group | None:
    """
    Choose the best matching group for our UA:
      - Prefer the group with the longest matching UA token (most specific).
      - If none match, fall back to the first '*' group if present.
    """
    best: _Group | None = None
    best_len = -1
    star_fallback: _Group | None = None

    for g in parsed.groups:
        for tok in g.uas:
            if tok == "*":
                if star_fallback is None:
                    star_fallback = g
                continue
            if _ua_token_matches(ua, tok) and len(tok) > best_len:
                best = g
                best_len = len(tok)

    if best is not None:
        return best
    return star_fallback


def _evaluate_rules(path: str, rules: list[_Rule]) -> tuple[bool, _Rule | None]:
    """
    Apply longest-prefix rule wins.
    Tie-breaker: if equal length, 'Allow' beats 'Disallow'.
    If no rule matches, allow.

    Returns (allowed, matched_rule) where matched_rule is the rule that determined
    the outcome, or None if no rule matched (default allow).
    """
    if not path.startswith("/"):
        idx = path.find("/")
        path = path[idx:] if idx >= 0 else "/"
    q = path.split("?", 1)[0].split("#", 1)[0]

    best_rule: _Rule | None = None
    best_len = -1

    for r in rules:
        if q.startswith(r.path):
            plen = len(r.path)
            if plen > best_len or (
                plen == best_len and (best_rule is None or (r.allow and not best_rule.allow))
            ):
                best_rule = r
                best_len = plen

    if best_rule is None:
        return True, None
    return best_rule.allow, best_rule


def _build_policy_from_text(text: str) -> _Policy:
    parsed = _parse_robots(text)
    grp = _best_group_for_ua(parsed, FETCH_USER_AGENT)
    if grp is None:
        return _Policy(
            kind="allow_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason="no-applicable-group",
            status_code=200,
        )

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
        now = _now()
        pol = _Policy(
            kind="deny_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason="timeout",
            status_code=None,
            robots_url=url,
            fetched_at=now,
            expires_at=now + ROBOTS_DENY_TTL_SECONDS,
        )
        return pol

    status = resp.status_code
    now = _now()

    if status == 200:
        pol = _build_policy_from_text(resp.text or "")
        pol.status_code = 200
        pol.reason = "ok"
        pol.fetched_at = now
        pol.expires_at = now + ROBOTS_TTL_SECONDS
        pol.robots_url = url
        return pol

    if status in (401, 403, 404):
        pol = _Policy(
            kind="allow_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason=f"{status}-treat-as-no-robots",
            status_code=status,
            robots_url=url,
            fetched_at=now,
            expires_at=now + ROBOTS_TTL_SECONDS,
        )
        return pol

    if status >= 500:
        pol = _Policy(
            kind="deny_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason=f"{status}-server-error",
            status_code=status,
            robots_url=url,
            fetched_at=now,
            expires_at=now + ROBOTS_DENY_TTL_SECONDS,
        )
        return pol

    pol = _Policy(
        kind="allow_all",
        crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
        reason=f"{status}-treated-as-allow",
        status_code=status,
        robots_url=url,
        fetched_at=now,
        expires_at=now + ROBOTS_TTL_SECONDS,
    )
    return pol


def _get_policy(host: str, *, force_refresh: bool = False) -> _Policy:
    host = host.strip().lower()
    if not host:
        return _Policy(
            kind="allow_all",
            crawl_delay=ROBOTS_DEFAULT_DELAY_SECONDS,
            reason="empty-host",
        )

    lock = _host_lock(host)
    with lock:
        cached = _MEMO.get(host)
        if cached is not None and not force_refresh:
            if _now() < cached.expires_at:
                return cached

        pol = _fetch_and_resolve(host)
        _MEMO[host] = pol
        return pol


def _extract_path(url_or_path: str) -> str:
    """Extract just the path from a URL or path string."""
    if url_or_path.startswith("/"):
        return url_or_path.split("?", 1)[0].split("#", 1)[0]
    if "://" in url_or_path:
        idx = url_or_path.find("/", url_or_path.find("://") + 3)
        if idx >= 0:
            path = url_or_path[idx:]
            return path.split("?", 1)[0].split("#", 1)[0]
        return "/"
    return "/" + url_or_path.lstrip("/")


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def is_allowed(host: str, path: str) -> bool:
    """
    Return True if fetching the given path on host is allowed per robots.txt.
    """
    pol = _get_policy(host)

    if pol.kind == "allow_all":
        return True
    if pol.kind == "deny_all":
        return False

    allowed, _ = _evaluate_rules(path, pol.rules)
    return allowed


def explain_block(host: str, path: str) -> RobotsBlockInfo:
    """
    Return a structured explanation of whether a URL is blocked and why.
    """
    pol = _get_policy(host)

    normalized_path = _extract_path(path)
    full_url = f"https://{host}{normalized_path}"
    robots_url = pol.robots_url or f"https://{host}/robots.txt"

    if pol.kind == "allow_all":
        return RobotsBlockInfo(
            blocked_url=full_url,
            robots_url=robots_url,
            user_agent=FETCH_USER_AGENT,
            allowed=True,
            reason=f"allow_all ({pol.reason})",
            matched_rule=None,
            notes=None,
        )

    if pol.kind == "deny_all":
        return RobotsBlockInfo(
            blocked_url=full_url,
            robots_url=robots_url,
            user_agent=FETCH_USER_AGENT,
            allowed=False,
            reason=f"deny_all ({pol.reason})",
            matched_rule=None,
            notes="All paths blocked due to server error or timeout fetching robots.txt",
        )

    allowed, matched_rule = _evaluate_rules(normalized_path, pol.rules)

    if matched_rule is None:
        return RobotsBlockInfo(
            blocked_url=full_url,
            robots_url=robots_url,
            user_agent=FETCH_USER_AGENT,
            allowed=True,
            reason="no_matching_rule",
            matched_rule=None,
            notes="No Disallow/Allow rule matched this path; default is allow",
        )

    rule_type = "Allow" if matched_rule.allow else "Disallow"
    rule_str = f"{rule_type}: {matched_rule.path}"

    return RobotsBlockInfo(
        blocked_url=full_url,
        robots_url=robots_url,
        user_agent=FETCH_USER_AGENT,
        allowed=allowed,
        reason="disallow_rule" if not allowed else "allow_rule",
        matched_rule=rule_str,
        notes=None,
    )


def get_crawl_delay(host: str) -> float:
    """
    Return the crawl-delay for host, or default if not specified.
    """
    pol = _get_policy(host)
    return pol.crawl_delay if pol.crawl_delay is not None else ROBOTS_DEFAULT_DELAY_SECONDS


def clear_cache() -> None:
    """Clear the in-memory robots cache (useful for testing)."""
    with _GLOBAL_LOCK:
        _MEMO.clear()
        _LOCKS.clear()


def force_refresh(host: str) -> None:
    """Force a re-fetch of robots.txt for the given host."""
    _get_policy(host, force_refresh=True)
