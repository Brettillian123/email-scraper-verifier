# tests/test_o05_deobfuscation.py
"""
O05 Deobfuscation Tests

Tests email address deobfuscation (e.g., [at] -> @, [dot] -> .)

NOTE: Role aliases are now marked with is_role_address_guess=True rather than
being filtered out. Tests updated to reflect this behavior change.
"""

from __future__ import annotations

import pytest

from src.extract.candidates import extract_candidates

OFFICIAL = "acme.com"


def _emails(html: str, **kwargs) -> set[str]:
    """
    Helper to run extraction and return set of emails only.

    Allows overriding official_domain and deobfuscate per-call so tests
    don't rely on environment variables evaluated at import time.
    """
    # Default to OFFICIAL unless explicitly overridden
    official = kwargs.pop("official_domain", OFFICIAL)
    cands = extract_candidates(
        html,
        source_url="https://example.com/page",
        official_domain=official,
        **kwargs,
    )
    return {c.email for c in cands}


def _non_role_emails(html: str, **kwargs) -> set[str]:
    """
    Helper to run extraction and return set of emails that are NOT role aliases.
    """
    official = kwargs.pop("official_domain", OFFICIAL)
    cands = extract_candidates(
        html,
        source_url="https://example.com/page",
        official_domain=official,
        **kwargs,
    )
    return {c.email for c in cands if not c.is_role_address_guess}


HTML_BRACKET = "<p>Contact: John Doe — john [at] acme [dot] com</p>"
HTML_PARENS_WORDS = "<p>Mary (at) acme dot co dot uk</p>"
HTML_ROLE_ALIAS = "<p>General inbox: info [at] acme [dot] com</p>"
HTML_ENTITY_AT = "<p>Bob&#64;acme.com</p>"  # &#64; == '@'


def test_flag_off_no_deobfuscation(monkeypatch: pytest.MonkeyPatch) -> None:
    """When EXTRACT_DEOBFUSCATE=0, obfuscated forms should not be found."""
    monkeypatch.setenv("EXTRACT_DEOBFUSCATE", "0")
    # Do NOT pass deobfuscate=True here; we want the default OFF behavior.
    assert "john@acme.com" not in _emails(HTML_BRACKET)
    # sanity: scope still enforced, nothing else leaks in
    assert _emails(HTML_BRACKET) == set()


def test_flag_on_bracket_at_dot() -> None:
    """Basic '[at] ... [dot] ...' should be de-obfuscated when enabled."""
    emails = _emails(HTML_BRACKET, deobfuscate=True)  # explicit flag, no env dependence
    assert "john@acme.com" in emails


def test_flag_on_parentheses_and_word_dot() -> None:
    """'(at) ... dot ...' with multi-label TLD should be handled."""
    # This email is outside OFFICIAL ("acme.com"), so disable scoping for this test.
    emails = _emails(HTML_PARENS_WORDS, deobfuscate=True, official_domain=None)
    assert "mary@acme.co.uk" in emails


def test_role_alias_marked_when_deobfuscated() -> None:
    """
    Role/distribution aliases are now marked but not filtered.
    Using _non_role_emails helper to filter them downstream.
    """
    non_role = _non_role_emails(HTML_ROLE_ALIAS, deobfuscate=True)
    assert "info@acme.com" not in non_role


def test_html_entity_at_is_captured() -> None:
    """
    After HTML entity decoding, regular email regex should catch addresses like
    'bob&#64;acme.com' -> 'bob@acme.com'.
    """
    # Works regardless of the deobfuscation flag; we pass True to be explicit.
    assert "bob@acme.com" in _emails(HTML_ENTITY_AT, deobfuscate=True)


def test_html_entity_at_captured_even_if_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    The text-block scanner unescapes entities before applying EMAIL_RE, so we
    should capture entity-encoded '@' even with de-obfuscation disabled.
    """
    monkeypatch.setenv("EXTRACT_DEOBFUSCATE", "0")
    assert "bob@acme.com" in _emails(HTML_ENTITY_AT)
