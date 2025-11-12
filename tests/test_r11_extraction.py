# tests/test_r11_extraction.py
from __future__ import annotations

import sqlite3

# Make repo root importable when tests run from project root
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.extract_candidates import _persist_candidates  # noqa: E402
from src.extract import Candidate, extract_candidates  # noqa: E402

OFFICIAL = "company.com"


def by_email(cands: list[Candidate]) -> dict[str, Candidate]:
    return {c.email: c for c in cands}


# ------------------------ HTML extraction tests ------------------------------


def test_mailto_and_text_emails_are_found():
    html = """
    <div class="team">
      <a href="mailto:jane.doe@company.com">Jane Doe</a>
      <p>Questions? Email ops@company.com</p>
    </div>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/team", official_domain=OFFICIAL
    )
    d = by_email(cands)

    assert "jane.doe@company.com" in d
    assert "ops@company.com" in d

    # Jane has a name from link text
    assert d["jane.doe@company.com"].first_name == "Jane"
    assert d["jane.doe@company.com"].last_name == "Doe"
    # ops may not have a human name (role-like), only check presence
    assert d["ops@company.com"].source_url == "https://www.company.com/team"


def test_name_near_link_is_captured():
    html = """
    <div class="row">
      <strong>John Q. Public</strong>
      <a href="mailto:john.public@company.com">john.public@company.com</a>
    </div>
    <div class="row">
      <a href="mailto:jane.doe@company.com">Jane Doe — CTO</a>
    </div>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/contact", official_domain=OFFICIAL
    )
    d = by_email(cands)

    assert d["john.public@company.com"].first_name == "John"
    assert d["john.public@company.com"].last_name == "Public"
    assert d["jane.doe@company.com"].first_name == "Jane"
    assert d["jane.doe@company.com"].last_name == "Doe"


def test_fallback_name_from_local_part():
    html = """
    <p>Reach our recruiter at jane.doe@company.com</p>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/jobs", official_domain=OFFICIAL
    )
    d = by_email(cands)

    assert d["jane.doe@company.com"].first_name == "Jane"
    assert d["jane.doe@company.com"].last_name == "Doe"


def test_role_aliases_are_ignored():
    html = """
    <footer>
      <p>General: info@company.com · Sales: sales@company.com</p>
    </footer>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/footer", official_domain=OFFICIAL
    )
    emails = {c.email for c in cands}
    assert "info@company.com" not in emails
    assert "sales@company.com" not in emails


def test_out_of_domain_emails_filtered():
    html = """
    <div>
      <p>Write Bob at bob@company.com or bob.helper@gmail.com</p>
    </div>
    """
    cands = extract_candidates(
        html, source_url="https://www.company.com/contact", official_domain=OFFICIAL
    )
    emails = {c.email for c in cands}
    assert "bob@company.com" in emails
    assert "bob.helper@gmail.com" not in emails


# ------------------------ Persistence / idempotency --------------------------


def _create_min_schema(con: sqlite3.Connection) -> None:
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


def test_idempotent_upsert_and_enrichment():
    con = sqlite3.connect(":memory:")
    _create_min_schema(con)

    # 1) First run: email only, no person created
    c1 = Candidate(
        email="alice.smith@company.com",
        source_url="https://www.company.com/team",
        first_name=None,
        last_name=None,
    )
    _persist_candidates(con, [c1])

    # After first persist: one email row, no people row linked
    cur = con.execute("SELECT COUNT(*) FROM emails")
    assert cur.fetchone()[0] == 1
    cur = con.execute("SELECT person_id FROM emails WHERE email = ?", (c1.email,))
    assert cur.fetchone()[0] is None

    # 2) Second run: same email but with a proper name; should create person and link email
    c2 = Candidate(
        email="alice.smith@company.com",
        source_url="https://www.company.com/team",
        first_name="Alice",
        last_name="Smith",
    )
    _persist_candidates(con, [c2])

    # Still only one email row (unique index), now linked to a person
    cur = con.execute("SELECT COUNT(*) FROM emails")
    assert cur.fetchone()[0] == 1

    cur = con.execute("SELECT person_id FROM emails WHERE email = ?", (c2.email,))
    person_id = cur.fetchone()[0]
    assert person_id is not None

    # People table should have exactly one row for Alice Smith (found or created)
    cur = con.execute("SELECT COUNT(*) FROM people")
    assert cur.fetchone()[0] == 1

    # 3) Third run: a poorer candidate (no name). Must not clobber person_id/source_url.
    c3 = Candidate(
        email="alice.smith@company.com",
        source_url="https://www.company.com/team",
        first_name=None,
        last_name=None,
    )
    _persist_candidates(con, [c3])

    cur = con.execute("SELECT person_id, source_url FROM emails WHERE email = ?", (c3.email,))
    pid_again, src_again = cur.fetchone()
    assert pid_again == person_id
    assert src_again == "https://www.company.com/team"
