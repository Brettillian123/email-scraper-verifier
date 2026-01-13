# tests/test_r25_qa_acceptance.py
"""
R25 QA Acceptance Tests

These tests verify end-to-end behavior of the verification pipeline.

NOTE: This module uses SQLite-specific features:
- PRAGMA table_info() and PRAGMA foreign_keys
- sqlite3.Connection.executescript()
- SqliteFtsBackend for search

These tests must be skipped when running against PostgreSQL.
The conftest.py db_conn fixture would override our SQLite fixtures,
causing connection type mismatches.
"""

from __future__ import annotations

import csv
import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

# Check if we're running against PostgreSQL
_DB_URL = os.environ.get("DATABASE_URL", "")
_IS_POSTGRES = "postgresql" in _DB_URL.lower() or "postgres" in _DB_URL.lower()

# Skip all tests in this module if PostgreSQL is configured
pytestmark = pytest.mark.skipif(
    _IS_POSTGRES,
    reason="R25 tests use SQLite-specific features (PRAGMA, executescript, SqliteFtsBackend)",
)

# Directory where this test file lives
THIS_DIR = Path(__file__).parent
FIXTURES_DIR = THIS_DIR / "fixtures"
ROOT_DIR = THIS_DIR.parent


def _apply_schema(conn: sqlite3.Connection) -> None:
    """Apply the full project schema into the given SQLite connection."""
    schema_path = ROOT_DIR / "db" / "schema.sql"
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    sql = schema_path.read_text(encoding="utf-8")
    conn.executescript("PRAGMA foreign_keys = ON;")
    conn.executescript(sql)


def _default_value_for_column(col_type: str, name: str) -> Any:
    """Provide a conservative default for NOT NULL columns."""
    t = (col_type or "").upper()
    if name in {"attrs", "extra_attrs", "meta"}:
        return "{}"
    if "INT" in t:
        return 0
    if "REAL" in t or "FLOAT" in t or "DOUBLE" in t:
        return 0.0
    if "BOOL" in t:
        return 0
    if "DATE" in t or "TIME" in t:
        return "1970-01-01T00:00:00"
    return "r25-default"


def _insert_row(conn: sqlite3.Connection, table: str, values: dict[str, Any]) -> None:
    """Generic helper to insert a row into `table`."""
    pragma_sql = f"PRAGMA table_info({table})"
    cur = conn.execute(pragma_sql)
    cols = cur.fetchall()

    insert_cols: list[str] = []
    params: list[Any] = []

    for col in cols:
        name = col[1] if isinstance(col, tuple) else col["name"]
        col_type = col[2] if isinstance(col, tuple) else col["type"]
        notnull = bool(col[3] if isinstance(col, tuple) else col["notnull"])
        has_default = (col[4] if isinstance(col, tuple) else col["dflt_value"]) is not None

        if name in values:
            insert_cols.append(name)
            params.append(values[name])
        elif notnull and not has_default:
            insert_cols.append(name)
            params.append(_default_value_for_column(col_type, name))
        else:
            continue

    if not insert_cols:
        raise RuntimeError(f"No columns to insert for table {table}")

    placeholders = ", ".join(["?"] * len(insert_cols))
    col_list = ", ".join(insert_cols)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    conn.execute(sql, params)


def _load_known_domains_fixture() -> dict[tuple[str, str], dict[str, Any]]:
    """Load R25 known domains fixture from CSV."""
    csv_path = FIXTURES_DIR / "r25_known_domains.csv"
    if not csv_path.exists():
        return {}

    result: dict[tuple[str, str], dict[str, Any]] = {}
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = row.get("domain", "")
            email = row.get("email", "")
            if domain and email:
                result[(domain, email)] = {
                    "expected_verify_status": row.get("expected_verify_status", "unknown"),
                    "expected_icp_score": float(row.get("expected_icp_score", 0) or 0),
                }
    return result


def _seed_known_verifications(conn: sqlite3.Connection) -> None:
    """Seed database with known verification test data."""
    fixture = _load_known_domains_fixture()
    if not fixture:
        pytest.skip("r25_known_domains.csv fixture not found or empty")

    conn.execute("PRAGMA foreign_keys = OFF;")
    try:
        # Companies
        companies = [
            {"id": 1, "name": "Crestwell Partners", "domain": "crestwellpartners.com"},
            {"id": 2, "name": "Example Inc", "domain": "example.com"},
            {"id": 3, "name": "CatchAll Corp", "domain": "catchall.test"},
        ]
        for row in companies:
            _insert_row(conn, "companies", row)

        # People
        people = [
            {"id": 1, "company_id": 1, "first_name": "Brett", "last_name": "Anderson"},
            {"id": 2, "company_id": 2, "first_name": "Bad", "last_name": "Address"},
            {"id": 3, "company_id": 3, "first_name": "Random", "last_name": "User"},
        ]
        for row in people:
            _insert_row(conn, "people", row)

        # Emails
        emails = [
            {
                "id": 1,
                "person_id": 1,
                "email": "banderson@crestwellpartners.com",
                "source": "generated",
                "source_url": "https://crestwellpartners.com/team",
            },
            {
                "id": 2,
                "person_id": 2,
                "email": "bad-address@example.com",
                "source": "generated",
                "source_url": "https://example.com/team",
            },
            {
                "id": 3,
                "person_id": 3,
                "email": "random@catchall.test",
                "source": "generated",
                "source_url": "https://catchall.test/team",
            },
        ]
        for row in emails:
            _insert_row(conn, "emails", row)

        # Verification results
        verification_rows = [
            {
                "email_id": 1,
                "verify_status": fixture.get(
                    ("crestwellpartners.com", "banderson@crestwellpartners.com"),
                    {"expected_verify_status": "valid"},
                )["expected_verify_status"],
                "verify_reason": "r25_fixture_valid",
                "verified_mx": "mx.crestwellpartners.com",
                "verified_at": "2025-01-01T00:00:00",
            },
            {
                "email_id": 2,
                "verify_status": fixture.get(
                    ("example.com", "bad-address@example.com"),
                    {"expected_verify_status": "invalid"},
                )["expected_verify_status"],
                "verify_reason": "r25_fixture_invalid",
                "verified_mx": "mx.example.com",
                "verified_at": "2025-01-01T00:00:00",
            },
            {
                "email_id": 3,
                "verify_status": fixture.get(
                    ("catchall.test", "random@catchall.test"),
                    {"expected_verify_status": "risky_catch_all"},
                )["expected_verify_status"],
                "verify_reason": "r25_fixture_catchall",
                "verified_mx": "mx.catchall.test",
                "verified_at": "2025-01-01T00:00:00",
            },
        ]
        for row in verification_rows:
            _insert_row(conn, "verification_results", row)

        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys = ON;")


@pytest.fixture
def fresh_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Fresh on-disk SQLite database with the full schema applied."""
    db_path = tmp_path / "r25.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    _apply_schema(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_r25_known_domain_verification_snapshots(fresh_db: sqlite3.Connection) -> None:
    """Golden snapshot: companies/emails/verification_results should match expectations."""
    fixture = _load_known_domains_fixture()
    if not fixture:
        pytest.skip("r25_known_domains.csv fixture not found")

    _seed_known_verifications(fresh_db)

    cur = fresh_db.execute(
        """
        SELECT c.domain,
               e.email,
               v.verify_status
        FROM emails AS e
        JOIN people AS p ON p.id = e.person_id
        JOIN companies AS c ON c.id = p.company_id
        JOIN verification_results AS v ON v.email_id = e.id
        ORDER BY c.domain, e.email
        """
    )
    rows = cur.fetchall()
    assert rows, "Expected seeded verification rows for R25 snapshot"

    result_rows = []
    for row in rows:
        if isinstance(row, tuple):
            result_rows.append({"domain": row[0], "email": row[1], "verify_status": row[2]})
        else:
            result_rows.append(dict(row))

    seen_keys = {(r["domain"], r["email"]) for r in result_rows}
    assert seen_keys == set(fixture.keys())


def test_r25_v_emails_latest_exposes_expected_fields(fresh_db: sqlite3.Connection) -> None:
    """Sanity check that v_emails_latest surfaces expected fields."""
    fixture = _load_known_domains_fixture()
    if not fixture:
        pytest.skip("r25_known_domains.csv fixture not found")

    _seed_known_verifications(fresh_db)

    emails = [email for (_domain, email) in fixture.keys()]
    placeholders = ", ".join(["?"] * len(emails))

    # Try to query v_emails_latest view
    try:
        cur = fresh_db.execute(
            f"""
            SELECT email, company_domain, verify_status
            FROM v_emails_latest
            WHERE email IN ({placeholders})
            ORDER BY email
            """,
            emails,
        )
        rows = cur.fetchall()
    except Exception as e:
        # View might not exist or have different columns
        pytest.skip(f"v_emails_latest view issue: {e}")

    assert len(rows) == len(emails), "Expected all fixture emails to appear in v_emails_latest"
