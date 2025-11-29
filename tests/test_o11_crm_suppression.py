from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.import_crm_suppression import import_csv

# ---------------------------------------------------------------------------
# Fixtures: on-disk SQLite DB with suppression table
# ---------------------------------------------------------------------------


@pytest.fixture
def suppression_db_file(tmp_path: Path) -> Path:
    """
    Create an on-disk SQLite database with a suppression table that matches
    what src.db_suppression expects for the plaintext-email case.

    Returns the path to the DB file so tests can pass it into import_csv().
    """
    db_path = tmp_path / "suppression.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE suppression (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            domain TEXT,
            reason TEXT,
            source TEXT,
            created_at TEXT,
            expires_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_suppression_rows(db_path: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT email, reason, source FROM suppression ORDER BY email").fetchall()
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_import_crm_suppression_inserts_rows(
    tmp_path: Path,
    suppression_db_file: Path,
) -> None:
    """
    Basic happy-path: import_csv should insert one row per CSV line with a
    non-empty email, and propagate reason/source correctly.
    """
    csv_path = tmp_path / "crm_suppression.csv"
    csv_path.write_text(
        "email,reason\no11test1@example.com,bounced\no11test2@example.com,complaint\n",
        encoding="utf-8",
    )

    import_csv(
        db_path=str(suppression_db_file),
        csv_path=str(csv_path),
        source="crm_test",
    )

    rows = _read_suppression_rows(suppression_db_file)
    assert [r["email"] for r in rows] == [
        "o11test1@example.com",
        "o11test2@example.com",
    ]
    assert [r["reason"] for r in rows] == ["bounced", "complaint"]
    assert all(r["source"] == "crm_test" for r in rows)


def test_import_crm_suppression_skips_blank_emails(
    tmp_path: Path,
    suppression_db_file: Path,
) -> None:
    """
    Rows with an empty email field should be ignored and not cause errors.
    """
    csv_path = tmp_path / "crm_suppression_blank.csv"
    csv_path.write_text(
        "email,reason\n,bounced\n   ,complaint\nvalid@example.com,bounced\n",
        encoding="utf-8",
    )

    import_csv(
        db_path=str(suppression_db_file),
        csv_path=str(csv_path),
        source="crm_test_blank",
    )

    rows = _read_suppression_rows(suppression_db_file)
    assert len(rows) == 1
    row = rows[0]
    assert row["email"] == "valid@example.com"
    assert row["reason"] == "bounced"
    assert row["source"] == "crm_test_blank"


def test_import_crm_suppression_default_reason_when_missing(
    tmp_path: Path,
    suppression_db_file: Path,
) -> None:
    """
    If the CSV has no 'reason' column or the value is empty, import_csv
    should fall back to 'crm_sync' as the reason.
    """
    csv_path = tmp_path / "crm_suppression_default_reason.csv"
    # No explicit reason column for the second row, empty value on third.
    csv_path.write_text(
        "email,reason\nno-reason-1@example.com,\nno-reason-2@example.com,\n",
        encoding="utf-8",
    )

    import_csv(
        db_path=str(suppression_db_file),
        csv_path=str(csv_path),
        source="crm_test_default_reason",
    )

    rows = _read_suppression_rows(suppression_db_file)
    assert len(rows) == 2
    for row in rows:
        assert row["reason"] == "crm_sync"
        assert row["source"] == "crm_test_default_reason"
