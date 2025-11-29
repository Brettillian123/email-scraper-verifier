# tests/test_r20_export_pipeline.py
from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.export_leads as export_cli
import src.export.exporter as exporter_mod
from src.export.exporter import ExportLead

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def stub_export_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Replace ExportPolicy.with a simple fake so R20 tests don't depend on the
    real export policy YAML/config. We only care that:

      - verify_status "valid" with icp_score >= 50 is accepted
      - other statuses or lower scores are rejected
    """

    class FakePolicy:
        def __init__(self, name: str = "default") -> None:
            self.name = name

        @classmethod
        def from_config(cls, name: str) -> FakePolicy:
            return cls(name=name)

        def is_exportable_row(
            self,
            *,
            email: str,
            verify_status: str | None,
            icp_score: int | None,
            extra,
        ) -> tuple[bool, str]:
            if verify_status != "valid":
                return False, "status_blocked"
            if icp_score is None or icp_score < 50:
                return False, "icp_too_low"
            return True, "ok"

    monkeypatch.setattr(exporter_mod, "ExportPolicy", FakePolicy)


@pytest.fixture
def memory_db() -> SimpleNamespace:
    """
    In-memory SQLite DB that provides just enough schema for iter_exportable_leads:

      - v_emails_latest: the "view" the exporter reads from (implemented as a table here)
      - suppression: used by src.db_suppression.is_email_suppressed()
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.execute(
        """
        CREATE TABLE v_emails_latest (
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            title_norm TEXT,
            title_raw TEXT,
            company_name TEXT,
            company_domain TEXT,
            source_url TEXT,
            icp_score INTEGER,
            verify_status TEXT,
            verified_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE suppression (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            reason TEXT,
            source TEXT,
            created_at TEXT
        )
        """
    )

    def seed_row(
        email: str,
        *,
        verify_status: str = "valid",
        icp_score: int | None = 80,
        first_name: str | None = "Alice",
        last_name: str | None = "Example",
        title_norm: str | None = "CTO",
        title_raw: str | None = None,
        company_name: str | None = "Acme Corp",
        company_domain: str | None = "acme.test",
        source_url: str | None = "https://example.com",
        verified_at: str | None = "2025-01-01T00:00:00Z",
    ) -> None:
        conn.execute(
            """
            INSERT INTO v_emails_latest (
                email,
                first_name,
                last_name,
                title_norm,
                title_raw,
                company_name,
                company_domain,
                source_url,
                icp_score,
                verify_status,
                verified_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                email,
                first_name,
                last_name,
                title_norm,
                title_raw,
                company_name,
                company_domain,
                source_url,
                icp_score,
                verify_status,
                verified_at,
            ),
        )
        conn.commit()

    def suppress_email(
        email: str,
        *,
        reason: str = "test_suppression",
        source: str = "test_r20",
    ) -> None:
        conn.execute(
            """
            INSERT INTO suppression (email, reason, source, created_at)
            VALUES (?, ?, ?, '2025-01-01T00:00:00Z')
            """,
            (email, reason, source),
        )
        conn.commit()

    return SimpleNamespace(conn=conn, seed_row=seed_row, suppress_email=suppress_email)


# ---------------------------------------------------------------------------
# iter_exportable_leads behaviour
# ---------------------------------------------------------------------------


def _collect_leads(memory_db: SimpleNamespace) -> list[ExportLead]:
    return list(exporter_mod.iter_exportable_leads(memory_db.conn, policy_name="default"))


def test_good_lead_is_exported(memory_db: SimpleNamespace) -> None:
    """
    A straightforward "good" lead:
      - valid verify_status
      - icp_score comfortably above threshold
      - not suppressed

    should be exported as a single ExportLead.
    """
    memory_db.seed_row("good@example.com", verify_status="valid", icp_score=90)

    leads = _collect_leads(memory_db)
    assert len(leads) == 1

    lead = leads[0]
    assert lead.email == "good@example.com"
    assert lead.icp_score == 90
    assert lead.verify_status == "valid"
    assert lead.company == "Acme Corp"
    assert lead.domain == "acme.test"


def test_suppressed_email_is_not_exported(memory_db: SimpleNamespace) -> None:
    """
    Any email present in the suppression table should be filtered out by
    iter_exportable_leads, regardless of verify_status/icp_score.
    """
    email = "suppressed@example.com"
    memory_db.seed_row(email, verify_status="valid", icp_score=90)
    memory_db.suppress_email(email)

    leads = _collect_leads(memory_db)
    assert leads == []


def test_invalid_status_is_not_exported(memory_db: SimpleNamespace) -> None:
    """
    Rows with verify_status != 'valid' are rejected by the fake policy stub
    (standing in for the real ExportPolicy config).
    """
    memory_db.seed_row(
        "invalid@example.com",
        verify_status="invalid",
        icp_score=90,
    )

    leads = _collect_leads(memory_db)
    assert leads == []


def test_low_icp_score_is_not_exported(memory_db: SimpleNamespace) -> None:
    """
    Even with verify_status 'valid', a low icp_score should be blocked by
    the policy stub.
    """
    memory_db.seed_row(
        "low-icp@example.com",
        verify_status="valid",
        icp_score=10,
    )

    leads = _collect_leads(memory_db)
    assert leads == []


def test_csv_injection_sanitization(memory_db: SimpleNamespace) -> None:
    """
    R20 must guard against CSV/Excel formula injection. Any string fields that
    start with '=', '+', '-', or '@' are prefixed with a single quote.

    This test seeds multiple potentially dangerous values and checks that the
    exported ExportLead has them escaped.
    """
    raw_email = '=HYPERLINK("http://bad")'
    memory_db.seed_row(
        raw_email,
        verify_status="valid",
        icp_score=90,
        first_name="+Bob",
        last_name="@Mallory",
        title_norm="-CFO",
        company_name="=Evil Corp",
        source_url="@tracker",
    )

    leads = _collect_leads(memory_db)
    assert len(leads) == 1
    lead = leads[0]

    # Email is required, so we always get a non-empty string with leading "'"
    assert lead.email.startswith("'=")
    assert raw_email in lead.email

    # Other text fields should also be escaped
    assert lead.first_name.startswith("'+")
    assert lead.last_name.startswith("'@")
    assert lead.title.startswith("'-")
    assert lead.company.startswith("'=")
    assert lead.source_url.startswith("'@")


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------


def test_cli_exports_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Basic smoke test for scripts/export_leads.py:

      - patch get_connection to return a dummy marker object
      - patch iter_exportable_leads to yield a small, fixed set of leads
      - run main() with CSV output
      - assert that the file exists with a header and one data row
    """

    out_path = tmp_path / "leads.csv"
    dummy_conn = object()

    def fake_get_connection(db_path: str) -> object:  # type: ignore[override]
        # Ensure the CLI passes through the path, but we ignore it here.
        assert isinstance(db_path, str)
        return dummy_conn

    def fake_iter_exportable_leads(
        conn: object,
        policy_name: str = "default",
    ) -> Iterable[ExportLead]:
        assert conn is dummy_conn
        assert policy_name == "default"
        yield ExportLead(
            email="cli@example.com",
            first_name="Cli",
            last_name="Test",
            title="Head of Testing",
            company="CLI Corp",
            domain="cli.test",
            source_url="https://cli.example.com",
            icp_score=100,
            verify_status="valid",
            verified_at="2025-01-01T00:00:00Z",
        )

    monkeypatch.setattr(export_cli, "get_connection", fake_get_connection)
    monkeypatch.setattr(export_cli, "iter_exportable_leads", fake_iter_exportable_leads)

    export_cli.main(
        [
            "--db",
            "ignored.db",
            "--output",
            str(out_path),
            "--format",
            "csv",
        ]
    )

    assert out_path.exists()
    lines = out_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= 2

    header = lines[0]
    row = lines[1]

    assert header.startswith(
        "email,first_name,last_name,title,company,domain,source_url,icp_score,verify_status,verified_at"
    )
    assert "cli@example.com" in row
    assert "CLI Corp" in row
