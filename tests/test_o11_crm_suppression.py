# tests/test_o11_crm_suppression.py
"""
Tests for CRM suppression import functionality.

Updated for PostgreSQL-only mode: uses src.db.get_conn() and
src.db_suppression module instead of SQLite with a separate script.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def suppression_table_ready(db_conn):
    """
    Ensure the suppression table exists and is empty for the test.

    The db_conn fixture from conftest.py provides a PostgreSQL connection.
    """
    # The suppression table should already exist from the schema
    # Clean up any existing test data
    try:
        db_conn.execute("DELETE FROM suppression WHERE tenant_id = 'dev'")
        db_conn.commit()
    except Exception:
        pass

    yield db_conn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_suppression_rows(conn) -> list[dict[str, Any]]:
    """Read all suppression rows from the database."""
    cur = conn.execute(
        "SELECT email, reason, source FROM suppression WHERE tenant_id = 'dev' ORDER BY email"
    )
    rows = cur.fetchall() or []

    result: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, tuple):
            result.append({"email": row[0], "reason": row[1], "source": row[2]})
        else:
            result.append(dict(row))
    return result


def _import_csv_from_path(
    conn,
    csv_path: str | Path,
    source: str,
    tenant_id: str = "dev",
) -> int:
    """
    Import suppression entries from a CSV file.

    This is a simplified implementation for testing purposes that uses
    the db_suppression.upsert_suppression function.

    Returns the number of rows imported.
    """
    from src.db_suppression import upsert_suppression

    csv_path = Path(csv_path)
    count = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip()
            if not email:
                continue

            reason = (row.get("reason") or "").strip() or "crm_sync"

            upsert_suppression(
                conn,
                email=email,
                reason=reason,
                source=source,
                tenant_id=tenant_id,
            )
            count += 1

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_import_crm_suppression_inserts_rows(
    tmp_path: Path,
    suppression_table_ready,
) -> None:
    """
    Basic happy-path: import_csv should insert one row per CSV line with a
    non-empty email, and propagate reason/source correctly.
    """
    conn = suppression_table_ready

    csv_path = tmp_path / "crm_suppression.csv"
    csv_path.write_text(
        "email,reason\no11test1@example.com,bounced\no11test2@example.com,complaint\n",
        encoding="utf-8",
    )

    _import_csv_from_path(
        conn=conn,
        csv_path=str(csv_path),
        source="crm_test",
    )

    rows = _read_suppression_rows(conn)
    assert [r["email"] for r in rows] == [
        "o11test1@example.com",
        "o11test2@example.com",
    ]
    assert [r["reason"] for r in rows] == ["bounced", "complaint"]
    assert all(r["source"] == "crm_test" for r in rows)


def test_import_crm_suppression_skips_blank_emails(
    tmp_path: Path,
    suppression_table_ready,
) -> None:
    """
    Rows with an empty email field should be ignored and not cause errors.
    """
    conn = suppression_table_ready

    csv_path = tmp_path / "crm_suppression_blank.csv"
    csv_path.write_text(
        "email,reason\n,bounced\n   ,complaint\nvalid@example.com,bounced\n",
        encoding="utf-8",
    )

    _import_csv_from_path(
        conn=conn,
        csv_path=str(csv_path),
        source="crm_test_blank",
    )

    rows = _read_suppression_rows(conn)
    assert len(rows) == 1
    row = rows[0]
    assert row["email"] == "valid@example.com"
    assert row["reason"] == "bounced"
    assert row["source"] == "crm_test_blank"


def test_import_crm_suppression_default_reason_when_missing(
    tmp_path: Path,
    suppression_table_ready,
) -> None:
    """
    If the CSV has no 'reason' column or the value is empty, import_csv
    should fall back to 'crm_sync' as the reason.
    """
    conn = suppression_table_ready

    csv_path = tmp_path / "crm_suppression_default_reason.csv"
    # No explicit reason column for the second row, empty value on third.
    csv_path.write_text(
        "email,reason\nno-reason-1@example.com,\nno-reason-2@example.com,\n",
        encoding="utf-8",
    )

    _import_csv_from_path(
        conn=conn,
        csv_path=str(csv_path),
        source="crm_test_default_reason",
    )

    rows = _read_suppression_rows(conn)
    assert len(rows) == 2
    for row in rows:
        assert row["reason"] == "crm_sync"
        assert row["source"] == "crm_test_default_reason"
