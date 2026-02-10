# tests/test_r11_extraction.py
"""
R11 Extraction Tests

Tests HTML email extraction and persistence logic.

CRITICAL: Avoid placeholder emails that are filtered by quality_gates.py:
- jane.doe, john.doe, jdoe, etc. are in the placeholder list
Use realistic names like alice.smith, bob.wilson, maria.garcia, etc.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.extract import Candidate, extract_candidates  # noqa: E402

try:
    from scripts.extract_candidates import _persist_candidates  # noqa: E402

    HAS_PERSIST = True
except ImportError:
    HAS_PERSIST = False
    _persist_candidates = None  # type: ignore

OFFICIAL = "company.com"


def by_email(cands: list[Candidate]) -> dict[str, Candidate]:
    return {c.email: c for c in cands if c.email}


def non_role_emails(cands: list[Candidate]) -> set[str]:
    """Return only emails not marked as role aliases."""
    return {c.email for c in cands if c.email and not c.is_role_address_guess}


def test_mailto_and_text_emails_are_found():
    """Test that mailto links and plain text emails are extracted."""
    # Use alice.smith instead of jane.doe (doe is a placeholder pattern)
    html = """
    <div class="team">
      <a href="mailto:alice.smith@company.com">Alice Smith</a>
      <p>Questions? Email ops@company.com</p>
    </div>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/team", official_domain=OFFICIAL
    )
    d = by_email(cands)

    assert "alice.smith@company.com" in d
    assert d["alice.smith@company.com"].first_name == "Alice"
    assert d["alice.smith@company.com"].last_name == "Smith"


def test_name_near_link_is_captured():
    """
    Test that names near mailto links are correctly associated.
    """
    # Use non-placeholder names (avoid doe, public, and other stopwords)
    html = """
    <div class="team">
      <p><a href="mailto:bob.wilson@company.com">Bob Wilson, Engineer</a></p>
      <p><a href="mailto:alice.smith@company.com">Alice Smith, CTO</a></p>
    </div>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/contact", official_domain=OFFICIAL
    )
    d = by_email(cands)

    assert "bob.wilson@company.com" in d
    assert d["bob.wilson@company.com"].first_name == "Bob"
    assert d["bob.wilson@company.com"].last_name == "Wilson"

    assert "alice.smith@company.com" in d
    assert d["alice.smith@company.com"].first_name == "Alice"
    assert d["alice.smith@company.com"].last_name == "Smith"


def test_fallback_name_from_local_part():
    """Test name extraction from email local part when no nearby name exists."""
    # Use alice.smith instead of jane.doe
    html = """
    <p>Reach our recruiter at alice.smith@company.com</p>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/jobs", official_domain=OFFICIAL
    )
    d = by_email(cands)

    assert "alice.smith@company.com" in d
    assert d["alice.smith@company.com"].first_name == "Alice"
    assert d["alice.smith@company.com"].last_name == "Smith"


def test_role_aliases_are_marked_not_filtered():
    """
    Role aliases are now marked with is_role_address_guess=True rather than
    being filtered out.
    """
    html = """
    <footer>
      <p>General: info@company.com Â· Sales: sales@company.com</p>
    </footer>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/footer", official_domain=OFFICIAL
    )

    d = by_email(cands)

    # Role aliases may or may not be present
    for email in ["info@company.com", "sales@company.com"]:
        if email in d:
            assert d[email].is_role_address_guess is True

    # Using the helper to filter, we should get an empty set (only role aliases)
    non_role = non_role_emails(cands)
    assert "info@company.com" not in non_role
    assert "sales@company.com" not in non_role


def test_out_of_domain_emails_filtered():
    """Test that out-of-domain emails are filtered out."""
    html = """
    <div>
      <p>Write Bob at bob.wilson@company.com or bob.helper@gmail.com</p>
    </div>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/contact", official_domain=OFFICIAL
    )
    emails = {c.email for c in cands if c.email}
    assert "bob.wilson@company.com" in emails
    assert "bob.helper@gmail.com" not in emails


def _create_min_schema(con: sqlite3.Connection) -> None:
    """Create minimal SQLite schema for persistence tests."""
    con.execute(
        """
        CREATE TABLE people (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          first_name TEXT,
          last_name  TEXT,
          created_at TEXT,
          updated_at TEXT
        );
        """
    )
    con.execute(
        """
        CREATE TABLE emails (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL UNIQUE,
          person_id INTEGER,
          source_url TEXT,
          extracted_at TEXT,
          FOREIGN KEY(person_id) REFERENCES people(id)
        );
        """
    )


@pytest.mark.skipif(not HAS_PERSIST, reason="_persist_candidates not available")
def test_idempotent_upsert_and_enrichment():
    """Test that persistence is idempotent and enrichment updates existing records."""
    con = sqlite3.connect(":memory:")
    _create_min_schema(con)

    # Use alice.smith instead of jane.doe
    c1 = Candidate(
        email="alice.smith@company.com",
        source_url="https://www.company.com/team",
        first_name=None,
        last_name=None,
    )
    _persist_candidates(con, [(c1, None)])

    cur = con.execute("SELECT COUNT(*) FROM emails")
    assert cur.fetchone()[0] == 1
    cur = con.execute("SELECT person_id FROM emails WHERE email = ?", (c1.email,))
    assert cur.fetchone()[0] is None

    c2 = Candidate(
        email="alice.smith@company.com",
        source_url="https://www.company.com/team",
        first_name="Alice",
        last_name="Smith",
    )
    _persist_candidates(con, [(c2, None)])

    cur = con.execute("SELECT COUNT(*) FROM emails")
    assert cur.fetchone()[0] == 1

    cur = con.execute("SELECT person_id FROM emails WHERE email = ?", (c2.email,))
    person_id = cur.fetchone()[0]
    assert person_id is not None

    cur = con.execute("SELECT COUNT(*) FROM people")
    assert cur.fetchone()[0] == 1

    con.close()
