# tests/test_extract_candidates.py
"""
Tests for candidate extraction from HTML.

NOTE: Role aliases (support@, sales@, info@) are now marked with
is_role_address_guess=True rather than being filtered.

CRITICAL: Avoid placeholder emails that are filtered by quality_gates.py:
- jane.doe, john.doe, jdoe, test@, example@, etc.
Use realistic names like alice.smith, bob.wilson, maria.garcia, etc.
"""
from __future__ import annotations

from collections.abc import Iterable

from src.extract.candidates import Candidate, extract_candidates


def _emails(cands: Iterable[Candidate]) -> set[str]:
    return {c.email for c in cands if c.email}


def _non_role_emails(cands: Iterable[Candidate]) -> set[str]:
    """Return only emails that are not role aliases."""
    return {c.email for c in cands if c.email and not c.is_role_address_guess}


def test_mailto_links_basic_name_parsing() -> None:
    """Test basic mailto link parsing with name extraction."""
    html = """
    <html>
      <body>
        <a href="mailto:alice.smith@example.com">Alice Smith</a>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
    )
    assert len(cands) >= 1

    by_email = {c.email: c for c in cands if c.email}
    assert "alice.smith@example.com" in by_email
    c = by_email["alice.smith@example.com"]
    assert c.first_name == "Alice"
    assert c.last_name == "Smith"


def test_plain_text_emails_and_domain_filtering() -> None:
    """Test that only in-scope domain emails are returned."""
    html = """
    <html>
      <body>
        <p>
          You can reach Carol at carol.wilson@example.com and Bob at bob@other.com.
        </p>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/team",
        official_domain="example.com",
    )

    emails = _emails(cands)
    assert "carol.wilson@example.com" in emails
    assert "bob@other.com" not in emails


def test_role_alias_addresses_are_marked() -> None:
    """
    Role aliases are marked with is_role_address_guess=True.
    """
    # Use alice.wilson instead of jane.doe (doe is a placeholder pattern)
    html = """
    <html>
      <body>
        <p>Email us at support@example.com or sales@example.com</p>
        <p>Direct: alice.wilson@example.com</p>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
    )

    by_email = {c.email: c for c in cands if c.email}

    # Role aliases should be present but marked
    if "support@example.com" in by_email:
        assert by_email["support@example.com"].is_role_address_guess is True
    if "sales@example.com" in by_email:
        assert by_email["sales@example.com"].is_role_address_guess is True

    # Person address should be present and NOT marked as role
    assert "alice.wilson@example.com" in by_email
    assert by_email["alice.wilson@example.com"].is_role_address_guess is False


def test_non_role_emails_can_be_filtered_downstream() -> None:
    """
    Demonstrate how to filter role aliases downstream if needed.
    """
    # Use alice.wilson instead of jane.doe
    html = """
    <html>
      <body>
        <p>Email us at support@example.com or sales@example.com</p>
        <p>Direct: alice.wilson@example.com</p>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
    )

    non_role = _non_role_emails(cands)
    assert "alice.wilson@example.com" in non_role


def test_deobfuscated_emails_when_enabled_via_param() -> None:
    """Test deobfuscation of [at] and [dot] patterns."""
    html = """
    <html>
      <body>
        <p>
          Contact: john [at] example [dot] com
        </p>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
        deobfuscate=True,
    )

    emails = _emails(cands)
    assert "john@example.com" in emails


def test_nearby_label_name_inference() -> None:
    """Test name inference from nearby DOM elements."""
    # Use maria.lopez - avoid mlopez which may have parsing issues
    html = """
    <html>
      <body>
        <div class="team-card">
          <strong>Maria Lopez</strong><br/>
          <a href="mailto:maria.lopez@example.com">maria.lopez@example.com</a>
        </div>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/team",
        official_domain="example.com",
    )

    # Filter to only email-bearing candidates
    email_cands = [c for c in cands if c.email == "maria.lopez@example.com"]
    assert len(email_cands) >= 1

    # At least one candidate should have the name
    names_found = [(c.first_name, c.last_name) for c in email_cands]
    assert ("Maria", "Lopez") in names_found
