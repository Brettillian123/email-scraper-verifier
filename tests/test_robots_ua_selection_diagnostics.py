from __future__ import annotations

import inspect
from dataclasses import asdict, is_dataclass
from typing import Any

import pytest
import respx
from httpx import Response

robots = pytest.importorskip("src.fetch.robots")


# ----------------------------- helpers ---------------------------------


def _maybe_clear_cache() -> None:
    """Clear robots cache if the module exposes a method."""
    clear = getattr(robots, "clear_cache", None)
    if callable(clear):
        clear()


def _collect_user_agent_candidates() -> list[tuple[str, str, str]]:
    """
    Heuristically discover where the effective User-Agent string might live.

    Returns: list of (module_name, attr_name, value)
    """
    candidates: list[tuple[str, str, str]] = []

    # Look on the robots module itself first.
    for attr in (
        "USER_AGENT",
        "USER_AGENT_STRING",
        "DEFAULT_USER_AGENT",
        "FETCH_USER_AGENT",
        "HTTP_USER_AGENT",
        "UA",
    ):
        if hasattr(robots, attr):
            val = getattr(robots, attr)
            if isinstance(val, str) and val.strip():
                candidates.append(("src.fetch.robots", attr, val))

    # Also check src.config if present (common pattern).
    try:
        import src.config as config  # type: ignore
    except Exception:
        config = None

    if config is not None:
        for attr in (
            "USER_AGENT",
            "USER_AGENT_STRING",
            "DEFAULT_USER_AGENT",
            "FETCH_USER_AGENT",
            "HTTP_USER_AGENT",
            "UA",
        ):
            if hasattr(config, attr):
                val = getattr(config, attr)
                if isinstance(val, str) and val.strip():
                    candidates.append(("src.config", attr, val))

    return candidates


def _patch_user_agent(monkeypatch: pytest.MonkeyPatch, ua: str) -> list[tuple[str, str]]:
    """
    Try hard to patch the UA in the most likely places.

    Returns: list of (where, name) patched.
    """
    patched: list[tuple[str, str]] = []

    # Patch likely attributes on robots module.
    for name in (
        "USER_AGENT",
        "USER_AGENT_STRING",
        "DEFAULT_USER_AGENT",
        "FETCH_USER_AGENT",
        "HTTP_USER_AGENT",
        "UA",
    ):
        if hasattr(robots, name):
            monkeypatch.setattr(robots, name, ua, raising=False)
            patched.append(("src.fetch.robots", name))

    # Patch src.config if available.
    try:
        import src.config as config  # type: ignore
    except Exception:
        config = None

    if config is not None:
        for name in (
            "USER_AGENT",
            "USER_AGENT_STRING",
            "DEFAULT_USER_AGENT",
            "FETCH_USER_AGENT",
            "HTTP_USER_AGENT",
            "UA",
        ):
            if hasattr(config, name):
                monkeypatch.setattr(config, name, ua, raising=False)
                patched.append(("src.config", name))

    # Patch common env vars (in case UA is read dynamically).
    for env_key in (
        "USER_AGENT",
        "EMAIL_SCRAPER_USER_AGENT",
        "SCRAPER_USER_AGENT",
        "ROBOTS_USER_AGENT",
    ):
        monkeypatch.setenv(env_key, ua)
        patched.append(("env", env_key))

    return patched


def _safe_repr(obj: Any) -> str:
    try:
        if is_dataclass(obj):
            return f"{obj.__class__.__name__}({asdict(obj)})"
        return repr(obj)
    except Exception:
        return f"<unreprable {type(obj).__name__}>"


def _try_explain(host: str, path: str) -> str | None:
    """
    If explain_block exists, call it in a signature-tolerant way and stringify.
    This is best-effort; the diagnostics are still useful even if this fails.
    """
    fn = getattr(robots, "explain_block", None)
    if not callable(fn):
        return None

    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())

        # Common patterns we might encounter:
        #   explain_block(host, path)
        #   explain_block(host, path, user_agent=...)
        #   explain_block(url)  (less likely)
        if len(params) >= 2:
            out = fn(host, path)  # type: ignore[misc]
        else:
            out = fn(path)  # type: ignore[misc]
        return _safe_repr(out)
    except Exception as e:
        return f"<explain_block call failed: {type(e).__name__}: {e}>"


def _diagnostic_header(monkeypatch: pytest.MonkeyPatch, ua: str) -> str:
    found = _collect_user_agent_candidates()
    patched = _patch_user_agent(monkeypatch, ua)

    lines = []
    lines.append("=== robots UA diagnostics ===")
    lines.append(f"Requested UA to set: {ua!r}")
    lines.append(f"Discovered UA candidates pre-call: {found!r}")
    lines.append(f"Patched locations: {patched!r}")
    return "\n".join(lines)


def _infer_group_used(delay_value: float | None) -> str:
    """
    In these tests we set:
      Email-Scraper group -> Crawl-delay 3
      Wildcard group      -> Crawl-delay 10
    """
    if delay_value is None:
        return "UNKNOWN(None)"
    if abs(delay_value - 3.0) < 1e-9:
        return "Email-Scraper group"
    if abs(delay_value - 10.0) < 1e-9:
        return "Wildcard(*) group"
    return f"UNKNOWN({delay_value})"


ROBOTS_TXT_UA_VS_WILDCARD = """User-agent: Email-Scraper
Disallow: /private
Allow: /
Crawl-delay: 3

User-agent: *
Disallow: /
Crawl-delay: 10
"""


# ----------------------------- fixtures ---------------------------------


@pytest.fixture(autouse=True)
def _clean_cache_each_test():
    _maybe_clear_cache()
    yield
    _maybe_clear_cache()


# ----------------------------- diagnostic tests --------------------------


@respx.mock
@pytest.mark.parametrize(
    "ua_variant",
    [
        "Email-Scraper",
        "Email-Scraper/1.0",
        "Email-Scraper (+mailto:test@example.com)",
        "email-scraper/2.0 (+https://example.com/contact)",
        "EMAIL-SCRAPER",
    ],
)
def test_diag_user_agent_should_match_group_token(
    monkeypatch: pytest.MonkeyPatch,
    ua_variant: str,
):
    """
    Purpose:
      Determine whether the UA matching logic is:
        - strict equality (buggy)
        - case-sensitive (buggy)
        - fails on version/comment suffixes (buggy)
        - or correctly matches 'Email-Scraper' token (desired)

    Expected outcome:
      /public should be allowed AND crawl-delay should be 3.0 (UA-specific group).
    """
    dbg = _diagnostic_header(monkeypatch, ua_variant)

    host = f"ua-match-{abs(hash(ua_variant))}.example.test"
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text=ROBOTS_TXT_UA_VS_WILDCARD)
    )

    allowed_public = robots.is_allowed(host, "/public")
    delay = robots.get_crawl_delay(host)

    explain_public = _try_explain(host, "/public")

    # If this fails, your failure message tells you *exactly* what was selected.
    assert allowed_public is True and delay == pytest.approx(3.0, rel=0, abs=1e-6), (
        f"{dbg}\n\n"
        f"Observed is_allowed('/public')={allowed_public!r}\n"
        f"Observed crawl_delay={delay!r} -> inferred: {_infer_group_used(delay)}\n"
        f"explain_block('/public')={explain_public}\n\n"
        "Interpretation guide:\n"
        "- If crawl_delay is 10 and allowed_public is False, wildcard group is being chosen.\n"
        "- If UA='Email-Scraper' works but UA='Email-Scraper/1.0' fails, matching is strict "
        "equality.\n"
        "- If case variants fail, matching is case-sensitive.\n"
    )


@respx.mock
def test_diag_specific_group_should_win_even_if_wildcard_is_later(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Purpose:
      Detect a 'last group wins' parser bug where later '*' overrides earlier specific groups.

    This reproduces your current failure pattern:
      - Specific group appears first
      - Wildcard group appears later
    """
    ua = "Email-Scraper/9.9 (+mailto:test@example.com)"
    dbg = _diagnostic_header(monkeypatch, ua)

    host = "group-order.example.test"
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text=ROBOTS_TXT_UA_VS_WILDCARD)
    )

    # /private should be disallowed under BOTH groups, so it's not diagnostic by itself.
    allowed_private = robots.is_allowed(host, "/private")

    # /public distinguishes the groups.
    allowed_public = robots.is_allowed(host, "/public")
    delay = robots.get_crawl_delay(host)

    explain_private = _try_explain(host, "/private")
    explain_public = _try_explain(host, "/public")

    assert allowed_private is False, (
        f"{dbg}\n\nUnexpected: /private allowed. That would indicate Allow/Disallow "
        "evaluation is broken.\n"
        f"explain_block('/private')={explain_private}\n"
    )

    assert allowed_public is True and delay == pytest.approx(3.0, rel=0, abs=1e-6), (
        f"{dbg}\n\n"
        f"Observed is_allowed('/public')={allowed_public!r}\n"
        f"Observed crawl_delay={delay!r} -> inferred: {_infer_group_used(delay)}\n"
        f"explain_block('/private')={explain_private}\n"
        f"explain_block('/public')={explain_public}\n\n"
        "If this fails with delay=10 and allowed_public=False:\n"
        "  You likely have a group selection bug (e.g., wildcard overriding specific),\n"
        "  OR UA token matching is not recognizing 'Email-Scraper' inside the UA string.\n"
    )


@respx.mock
def test_diag_is_allowed_and_crawl_delay_must_use_same_group(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Purpose:
      Detect split-brain behavior where is_allowed() uses one group but get_crawl_delay()
      uses another (or where crawl-delay is accidentally global rather than per-group).

    We do that by setting:
      Specific group: allow /public, crawl-delay 3
      Wildcard group: disallow everything, crawl-delay 10

    If is_allowed is True but delay is 10, crawl-delay storage/selection is buggy.
    """
    ua = "Email-Scraper/1.0 (+mailto:test@example.com)"
    dbg = _diagnostic_header(monkeypatch, ua)

    host = "consistency.example.test"
    respx.get(f"https://{host}/robots.txt").mock(
        return_value=Response(200, text=ROBOTS_TXT_UA_VS_WILDCARD)
    )

    allowed_public = robots.is_allowed(host, "/public")
    delay = robots.get_crawl_delay(host)
    explain_public = _try_explain(host, "/public")

    assert (allowed_public is True) == (delay == pytest.approx(3.0, rel=0, abs=1e-6)), (
        f"{dbg}\n\n"
        "Observed inconsistency between allow/deny and crawl-delay group selection:\n"
        f"  is_allowed('/public')={allowed_public!r}\n"
        f"  crawl_delay={delay!r} -> inferred: {_infer_group_used(delay)}\n"
        f"  explain_block('/public')={explain_public}\n\n"
        "Interpretation:\n"
        "- If is_allowed is True but delay is 10, crawl-delay is being read from '*' "
        "even when rules are not.\n"
        "- If is_allowed is False but delay is 3, allow/deny is using '*' while delay "
        "uses specific.\n"
    )
