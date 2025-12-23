# tests/test_extract_candidates.py
from __future__ import annotations

from collections.abc import Iterable

from src.extract.candidates import Candidate, extract_candidates


def _emails(cands: Iterable[Candidate]) -> set[str]:
    return {c.email for c in cands}


def test_mailto_links_basic_name_parsing() -> None:
    html = """
    <html>
      <body>
        <a href="mailto:alice.doe@example.com">Alice Doe</a>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
    )
    assert len(cands) == 1

    c = cands[0]
    assert c.email == "alice.doe@example.com"
    # Name should be parsed from link text
    assert c.first_name == "Alice"
    assert c.last_name == "Doe"
    assert c.raw_name in {"Alice Doe", "Alice  Doe"}  # robust to whitespace


def test_plain_text_emails_and_domain_filtering() -> None:
    html = """
    <html>
      <body>
        <p>
          You can reach Carol at carol@example.com and Bob at bob@other.com.
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
    # Only in-scope domain should be returned
    assert "carol@example.com" in emails
    assert "bob@other.com" not in emails


def test_role_alias_addresses_are_filtered() -> None:
    html = """
    <html>
      <body>
        <p>Email us at support@example.com or sales@example.com</p>
        <p>Direct: jane.doe@example.com</p>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
    )

    emails = _emails(cands)
    # Role aliases should be dropped entirely
    assert "support@example.com" not in emails
    assert "sales@example.com" not in emails
    # But a person address remains
    assert "jane.doe@example.com" in emails


def test_deobfuscated_emails_when_enabled_via_param() -> None:
    html = """
    <html>
      <body>
        <p>
          Contact: john [at] example [dot] com
        </p>
      </body>
    </html>
    """

    # Explicit deobfuscate=True should enable O05 path regardless of env vars.
    cands = extract_candidates(
        html,
        source_url="https://example.com/contact",
        official_domain="example.com",
        deobfuscate=True,
    )

    emails = _emails(cands)
    assert "john@example.com" in emails
    # No out-of-scope domains should sneak in
    assert all(e.endswith("@example.com") for e in emails)


def test_nearby_label_name_inference() -> None:
    html = """
    <html>
      <body>
        <div class="team-card">
          <strong>Maria Lopez</strong><br/>
          <a href="mailto:mlopez@example.com">mlopez@example.com</a>
        </div>
      </body>
    </html>
    """

    cands = extract_candidates(
        html,
        source_url="https://example.com/team",
        official_domain="example.com",
    )

    assert len(cands) == 1
    c = cands[0]
    # Name should be inferred from nearby <strong> label even though link text is the email
    assert c.email == "mlopez@example.com"
    assert c.first_name == "Maria"
    assert c.last_name == "Lopez"
    assert c.raw_name is not None
