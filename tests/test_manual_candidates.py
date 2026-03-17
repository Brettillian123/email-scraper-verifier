# tests/test_manual_candidates.py
"""
Tests for manual candidate submission feature.

Covers:
  - Pydantic validation (names, emails, batch limits)
  - Audit trail persistence after person/email cleanup
  - RQ task logic with mocked SMTP verification
  - Edge cases (no domain, duplicate emails)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mca_table(db_conn):
    """Ensure the manual_candidate_attempts table exists for tests."""
    try:
        db_conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_candidate_attempts (
              id BIGSERIAL PRIMARY KEY,
              tenant_id TEXT NOT NULL DEFAULT 'dev',
              company_id BIGINT NOT NULL,
              batch_id TEXT NOT NULL,
              first_name TEXT, last_name TEXT, full_name TEXT, title TEXT,
              submitted_email TEXT,
              outcome TEXT NOT NULL DEFAULT 'pending',
              verified_email TEXT, verify_status TEXT, verify_reason TEXT,
              error_detail TEXT, person_id BIGINT, email_id BIGINT,
              submitted_by TEXT,
              submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              processed_at TEXT
            )
        """)
        db_conn.commit()
    except Exception:
        try:
            db_conn.rollback()
        except Exception:
            pass
    yield db_conn


@pytest.fixture
def seed_company(mca_table):
    """Insert a test company with a domain and return its id."""
    con = mca_table
    row = con.execute(
        "INSERT INTO companies (tenant_id, name, domain, official_domain)"
        " VALUES ('dev', 'Acme Corp', 'acme.com', 'acme.com')"
        " RETURNING id",
    ).fetchone()
    con.commit()
    return int(row[0])


# ---------------------------------------------------------------------------
# Pydantic validation
# ---------------------------------------------------------------------------


class TestManualCandidateValidation:
    def test_valid_name_only(self):
        from src.api.browser import ManualCandidateInput

        c = ManualCandidateInput(first_name="Jane", last_name="Doe")
        assert c.first_name == "Jane"
        assert c.email is None

    def test_email_normalized(self):
        from src.api.browser import ManualCandidateInput

        c = ManualCandidateInput(first_name="Jane", email="Jane@Acme.COM")
        assert c.email == "jane@acme.com"

    def test_email_without_at_rejected(self):
        from src.api.browser import ManualCandidateInput

        with pytest.raises(Exception):
            ManualCandidateInput(first_name="Jane", email="notanemail")

    def test_whitespace_only_becomes_none(self):
        from src.api.browser import ManualCandidateInput

        c = ManualCandidateInput(first_name="  ", last_name="Doe")
        assert c.first_name is None
        assert c.last_name == "Doe"

    def test_batch_max_50(self):
        from src.api.browser import ManualCandidateInput, ManualCandidateRequest

        candidates = [
            ManualCandidateInput(first_name=f"Person{i}", last_name="Test") for i in range(51)
        ]
        with pytest.raises(Exception):
            ManualCandidateRequest(candidates=candidates)

    def test_empty_batch_rejected(self):
        from src.api.browser import ManualCandidateRequest

        with pytest.raises(Exception):
            ManualCandidateRequest(candidates=[])


# ---------------------------------------------------------------------------
# Audit trail persistence
# ---------------------------------------------------------------------------


class TestAuditTrail:
    def test_audit_survives_person_delete(self, seed_company, mca_table):
        """Audit row must remain after the person row is deleted."""
        con = mca_table
        batch_id = str(uuid.uuid4())

        person_row = con.execute(
            "INSERT INTO people (tenant_id, company_id, full_name, source_url)"
            " VALUES ('dev', ?, 'Test Person', 'manual:user_added')"
            " RETURNING id",
            (seed_company,),
        ).fetchone()
        person_id = int(person_row[0])

        con.execute(
            "INSERT INTO manual_candidate_attempts"
            " (tenant_id, company_id, batch_id, full_name, outcome, person_id)"
            " VALUES ('dev', ?, ?, 'Test Person', 'invalid', ?)",
            (seed_company, batch_id, person_id),
        )
        con.commit()

        # Simulate cleanup: delete person
        con.execute("DELETE FROM people WHERE id = ?", (person_id,))
        con.commit()

        # Audit row should still exist
        audit = con.execute(
            "SELECT outcome, full_name FROM manual_candidate_attempts WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        assert audit is not None
        assert audit[0] == "invalid"
        assert audit[1] == "Test Person"

    def test_audit_preserves_submitted_email(self, seed_company, mca_table):
        """Audit row remembers submitted_email even after cleanup."""
        con = mca_table
        batch_id = str(uuid.uuid4())

        con.execute(
            "INSERT INTO manual_candidate_attempts"
            " (tenant_id, company_id, batch_id, full_name, submitted_email, outcome)"
            " VALUES ('dev', ?, ?, 'Jane Doe', 'jane@acme.com', 'invalid')",
            (seed_company, batch_id),
        )
        con.commit()

        audit = con.execute(
            "SELECT submitted_email FROM manual_candidate_attempts WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        assert audit[0] == "jane@acme.com"


# ---------------------------------------------------------------------------
# RQ task integration tests
# ---------------------------------------------------------------------------


class TestManualCandidateTask:
    @patch("src.queueing.manual_candidates._verify_submitted_email")
    def test_valid_email_keeps_person(self, mock_verify, seed_company, mca_table):
        """Valid email → person stays in DB, audit says 'valid'."""
        from src.queueing.manual_candidates import task_verify_manual_candidates

        con = mca_table
        batch_id = str(uuid.uuid4())
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        person_row = con.execute(
            "INSERT INTO people (tenant_id, company_id, full_name, source_url)"
            " VALUES ('dev', ?, 'Valid Person', 'manual:user_added')"
            " RETURNING id",
            (seed_company,),
        ).fetchone()
        person_id = int(person_row[0])

        con.execute(
            "INSERT INTO manual_candidate_attempts"
            " (tenant_id, company_id, batch_id, full_name, submitted_email,"
            "  outcome, person_id, submitted_at)"
            " VALUES ('dev', ?, ?, 'Valid Person', 'valid@acme.com',"
            "  'pending', ?, ?)",
            (seed_company, batch_id, person_id, now),
        )
        con.commit()

        mock_verify.return_value = {
            "status": "valid",
            "reason": "rcpt_2xx_non_catchall",
            "email": "valid@acme.com",
            "email_id": 999,
            "mx_host": "mx.acme.com",
        }

        result = task_verify_manual_candidates(
            tenant_id="dev",
            company_id=seed_company,
            batch_id=batch_id,
        )

        assert result["ok"] is True
        assert result["valid"] == 1
        assert result["invalid"] == 0

        # Person should still exist
        person = con.execute("SELECT id FROM people WHERE id = ?", (person_id,)).fetchone()
        assert person is not None

        # Audit should say valid
        audit = con.execute(
            "SELECT outcome, verified_email FROM manual_candidate_attempts WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        assert audit[0] == "valid"
        assert audit[1] == "valid@acme.com"

    @patch("src.queueing.manual_candidates._verify_submitted_email")
    def test_invalid_email_deletes_person(self, mock_verify, seed_company, mca_table):
        """Invalid email → person deleted, audit persists with 'invalid'."""
        from src.queueing.manual_candidates import task_verify_manual_candidates

        con = mca_table
        batch_id = str(uuid.uuid4())
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        person_row = con.execute(
            "INSERT INTO people (tenant_id, company_id, full_name, source_url)"
            " VALUES ('dev', ?, 'Invalid Person', 'manual:user_added')"
            " RETURNING id",
            (seed_company,),
        ).fetchone()
        person_id = int(person_row[0])

        con.execute(
            "INSERT INTO manual_candidate_attempts"
            " (tenant_id, company_id, batch_id, full_name, submitted_email,"
            "  outcome, person_id, submitted_at)"
            " VALUES ('dev', ?, ?, 'Invalid Person', 'nope@acme.com',"
            "  'pending', ?, ?)",
            (seed_company, batch_id, person_id, now),
        )
        con.commit()

        mock_verify.return_value = {
            "status": "invalid",
            "reason": "rcpt_5xx",
            "email": "nope@acme.com",
            "email_id": None,
            "mx_host": "mx.acme.com",
        }

        result = task_verify_manual_candidates(
            tenant_id="dev",
            company_id=seed_company,
            batch_id=batch_id,
        )

        assert result["ok"] is True
        assert result["valid"] == 0
        assert result["invalid"] == 1

        # Person should be deleted
        person = con.execute("SELECT id FROM people WHERE id = ?", (person_id,)).fetchone()
        assert person is None

        # Audit should persist
        audit = con.execute(
            "SELECT outcome, submitted_email FROM manual_candidate_attempts WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        assert audit[0] == "invalid"
        assert audit[1] == "nope@acme.com"

    @patch("src.queueing.manual_candidates._generate_and_verify_for_person")
    def test_name_only_generates_and_verifies(self, mock_gen, seed_company, mca_table):
        """Name-only candidate → generate + verify is called."""
        from src.queueing.manual_candidates import task_verify_manual_candidates

        con = mca_table
        batch_id = str(uuid.uuid4())
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        person_row = con.execute(
            "INSERT INTO people"
            " (tenant_id, company_id, first_name, last_name, full_name, source_url)"
            " VALUES ('dev', ?, 'John', 'Smith', 'John Smith', 'manual:user_added')"
            " RETURNING id",
            (seed_company,),
        ).fetchone()
        person_id = int(person_row[0])

        con.execute(
            "INSERT INTO manual_candidate_attempts"
            " (tenant_id, company_id, batch_id, first_name, last_name,"
            "  full_name, outcome, person_id, submitted_at)"
            " VALUES ('dev', ?, ?, 'John', 'Smith', 'John Smith',"
            "  'pending', ?, ?)",
            (seed_company, batch_id, person_id, now),
        )
        con.commit()

        mock_gen.return_value = {
            "status": "valid",
            "reason": "valid_found",
            "email": "john.smith@acme.com",
            "attempts": [],
            "total_probes": 2,
        }

        result = task_verify_manual_candidates(
            tenant_id="dev",
            company_id=seed_company,
            batch_id=batch_id,
        )

        assert result["ok"] is True
        assert result["valid"] == 1

        mock_gen.assert_called_once_with(
            person_id=person_id,
            first_name="John",
            last_name="Smith",
            domain="acme.com",
        )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_domain_returns_error(self, mca_table):
        """Company without a domain → batch fails gracefully."""
        from src.queueing.manual_candidates import task_verify_manual_candidates

        con = mca_table
        row = con.execute(
            "INSERT INTO companies (tenant_id, name) VALUES ('dev', 'No Domain Inc') RETURNING id",
        ).fetchone()
        company_id = int(row[0])
        con.commit()

        batch_id = str(uuid.uuid4())
        result = task_verify_manual_candidates(
            tenant_id="dev",
            company_id=company_id,
            batch_id=batch_id,
        )

        assert result["ok"] is False
        assert result["error"] == "no_domain"

    def test_duplicate_email_upserts_cleanly(self, seed_company, mca_table):
        """Two inserts of the same email should not cause constraint errors."""
        from src.queueing.manual_candidates import _insert_email_for_manual

        con = mca_table

        # Create two people
        p1 = con.execute(
            "INSERT INTO people (tenant_id, company_id, full_name, source_url)"
            " VALUES ('dev', ?, 'Alice', 'manual:user_added')"
            " RETURNING id",
            (seed_company,),
        ).fetchone()
        p2 = con.execute(
            "INSERT INTO people (tenant_id, company_id, full_name, source_url)"
            " VALUES ('dev', ?, 'Bob', 'manual:user_added')"
            " RETURNING id",
            (seed_company,),
        ).fetchone()
        con.commit()

        eid1 = _insert_email_for_manual(
            con,
            tenant_id="dev",
            company_id=seed_company,
            person_id=int(p1[0]),
            email="dupe@acme.com",
        )
        eid2 = _insert_email_for_manual(
            con,
            tenant_id="dev",
            company_id=seed_company,
            person_id=int(p2[0]),
            email="dupe@acme.com",
        )

        # Both should succeed and return the same email_id
        assert eid1 is not None
        assert eid1 == eid2

    @patch("src.queueing.manual_candidates._verify_submitted_email")
    def test_mixed_batch(self, mock_verify, seed_company, mca_table):
        """Batch with valid + invalid candidates processes both correctly."""
        from src.queueing.manual_candidates import task_verify_manual_candidates

        con = mca_table
        batch_id = str(uuid.uuid4())
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        person_ids = []
        for name, email in [("Good Person", "good@acme.com"), ("Bad Person", "bad@acme.com")]:
            row = con.execute(
                "INSERT INTO people (tenant_id, company_id, full_name, source_url)"
                " VALUES ('dev', ?, ?, 'manual:user_added')"
                " RETURNING id",
                (seed_company, name),
            ).fetchone()
            pid = int(row[0])
            person_ids.append(pid)
            con.execute(
                "INSERT INTO manual_candidate_attempts"
                " (tenant_id, company_id, batch_id, full_name, submitted_email,"
                "  outcome, person_id, submitted_at)"
                " VALUES ('dev', ?, ?, ?, ?, 'pending', ?, ?)",
                (seed_company, batch_id, name, email, pid, now),
            )
        con.commit()

        # First call valid, second call invalid
        mock_verify.side_effect = [
            {
                "status": "valid",
                "reason": "rcpt_2xx",
                "email": "good@acme.com",
                "email_id": 1,
                "mx_host": "mx",
            },
            {
                "status": "invalid",
                "reason": "rcpt_5xx",
                "email": "bad@acme.com",
                "email_id": None,
                "mx_host": "mx",
            },
        ]

        result = task_verify_manual_candidates(
            tenant_id="dev",
            company_id=seed_company,
            batch_id=batch_id,
        )

        assert result["ok"] is True
        assert result["valid"] == 1
        assert result["invalid"] == 1

        # Good person should exist, bad person should be deleted
        good = con.execute("SELECT id FROM people WHERE id = ?", (person_ids[0],)).fetchone()
        bad = con.execute("SELECT id FROM people WHERE id = ?", (person_ids[1],)).fetchone()
        assert good is not None
        assert bad is None
