from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any

import pytest

from src.db_suppression import (
    hash_email,
    is_domain_suppressed,
    is_email_suppressed,
    upsert_suppression,
)
from src.export.policy import ExportPolicy
from src.export.roles import is_role_address

# ---------------------------------------------------------------------------
# Fixtures: in-memory suppression tables
# ---------------------------------------------------------------------------


@pytest.fixture
def suppression_db_plaintext() -> sqlite3.Connection:
    """
    In-memory suppression table using plaintext emails.

    Matches the expectations of src.db_suppression for the "email" case:
      - email TEXT UNIQUE
      - domain TEXT
      - reason/source/created_at/expires_at (minimal extras for helpers)
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
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
    return conn


@pytest.fixture
def suppression_db_hash() -> sqlite3.Connection:
    """
    In-memory suppression table using hashed emails.

    Matches the expectations of src.db_suppression for the "email_hash" case:
      - email_hash TEXT UNIQUE
      - reason/source/created_at/expires_at
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE suppression (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_hash TEXT UNIQUE,
            reason TEXT,
            source TEXT,
            created_at TEXT,
            expires_at TEXT
        )
        """
    )
    return conn


# ---------------------------------------------------------------------------
# Role-address classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("email", "expected"),
    [
        ("info@example.com", True),
        ("INFO@example.com", True),
        ("info+tag@example.com", True),
        ("support@example.com", True),
        ("customer.service@example.com", True),
        ("hello@example.com", True),
        ("billing@example.com", True),
        ("jobs@example.com", True),
        ("hr@example.com", True),
        ("office@example.com", True),
        # Non-role / person-like addresses
        ("ceo@example.com", False),
        ("firstname.lastname@example.com", False),
        ("salesperson@example.com", False),
        ("student@example.edu", False),
        ("", False),
        ("not-an-email", False),
    ],
)
def test_is_role_address_basic(email: str, expected: bool) -> None:
    assert is_role_address(email) is expected


def test_is_role_address_normalizes_plus_and_dots() -> None:
    # Plus-tagged role address
    assert is_role_address("support+newsletter@example.com") is True
    # Dotted variant should still be treated as role
    assert is_role_address("customer.service+tag@example.com") is True
    # Dotted personal address should remain non-role
    assert is_role_address("first.last@example.com") is False


# ---------------------------------------------------------------------------
# Suppression lookups (plaintext schema)
# ---------------------------------------------------------------------------


def test_is_email_suppressed_plaintext_email_and_domain(
    suppression_db_plaintext: sqlite3.Connection,
) -> None:
    conn = suppression_db_plaintext

    # Email-level suppression via upsert_suppression()
    upsert_suppression(
        conn,
        email="blocked@example.com",
        reason="manual_test",
        source="r19_plaintext",
    )

    # Domain-level suppression via direct insert
    conn.execute(
        """
        INSERT INTO suppression (domain, reason, source, created_at, expires_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, NULL)
        """,
        ("suppressed-domain.test", "domain_test", "r19_plaintext"),
    )
    conn.commit()

    # Email-level check
    assert is_email_suppressed(conn, "blocked@example.com") is True
    # Domain-level check via is_domain_suppressed()
    assert is_domain_suppressed(conn, "suppressed-domain.test") is True
    # Domain-level should also apply when checking a full address
    assert is_email_suppressed(conn, "user@suppressed-domain.test") is True

    # Non-suppressed addresses/domains should be False
    assert is_email_suppressed(conn, "ok@example.com") is False
    assert is_domain_suppressed(conn, "other-domain.test") is False


def test_is_email_suppressed_respects_expires_at(
    suppression_db_plaintext: sqlite3.Connection,
) -> None:
    conn = suppression_db_plaintext

    # Expired suppression: expires_at in the past relative to CURRENT_TIMESTAMP.
    conn.execute(
        """
        INSERT INTO suppression (email, reason, source, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "expired@example.com",
            "old",
            "r19_plaintext",
            "2000-01-01 00:00:00",
            "2000-01-01 00:00:00",
        ),
    )
    # Active suppression: expires_at NULL
    conn.execute(
        """
        INSERT INTO suppression (email, reason, source, created_at, expires_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, NULL)
        """,
        (
            "active@example.com",
            "active",
            "r19_plaintext",
        ),
    )
    conn.commit()

    assert is_email_suppressed(conn, "expired@example.com") is False
    assert is_email_suppressed(conn, "active@example.com") is True


# ---------------------------------------------------------------------------
# Suppression lookups (hashed schema)
# ---------------------------------------------------------------------------


def test_is_email_suppressed_hashed_email(
    suppression_db_hash: sqlite3.Connection,
) -> None:
    conn = suppression_db_hash

    target = "hashed@example.com"
    h = hash_email(target)

    conn.execute(
        """
        INSERT INTO suppression (email_hash, reason, source, created_at, expires_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, NULL)
        """,
        (h, "bounced", "r19_hashed"),
    )
    conn.commit()

    # Our helper should find the row using the plaintext email.
    assert is_email_suppressed(conn, target) is True
    # Different email → different hash → not suppressed.
    assert is_email_suppressed(conn, "other@example.com") is False


def test_upsert_suppression_insert_and_update_plaintext(
    suppression_db_plaintext: sqlite3.Connection,
) -> None:
    conn = suppression_db_plaintext

    upsert_suppression(conn, email="dup@example.com", reason="first", source="s1")
    upsert_suppression(conn, email="dup@example.com", reason="second", source="s2")
    conn.commit()

    rows = conn.execute(
        "SELECT email, reason, source FROM suppression WHERE email = ?",
        ("dup@example.com",),
    ).fetchall()

    assert len(rows) == 1
    row = rows[0]
    assert row["email"] == "dup@example.com"
    # Latest reason/source should win due to ON CONFLICT UPDATE.
    assert row["reason"] == "second"
    assert row["source"] == "s2"


# ---------------------------------------------------------------------------
# R19-style integration: suppression + role-address + O10 ExportPolicy
# ---------------------------------------------------------------------------


def _build_demo_policy() -> ExportPolicy:
    """
    Small helper to construct an ExportPolicy similar to the examples
    in the R19/O10 instructions.
    """
    cfg: dict[str, object] = {
        "allowed_statuses": ["valid", "risky_catch_all"],
        "min_icp_score_valid": 70,
        "min_icp_score_catch_all": 80,
        "exclude_roles": ["student", "intern"],
        "exclude_seniority": ["junior"],
        "exclude_industries": ["education", "government"],
    }
    return ExportPolicy.from_config("demo_r19", cfg)


def _decide_with_r19(
    conn: sqlite3.Connection,
    policy: ExportPolicy,
    lead: Mapping[str, Any],
) -> tuple[bool, str]:
    """
    Apply the R19 wiring:

    1) Global suppressions (email/domain)
    2) Role-address policy
    3) O10 ExportPolicy.should_export() for remaining leads
    """
    email = str(lead["email"])

    if is_email_suppressed(conn, email):
        return False, "suppressed"

    if is_role_address(email):
        return False, "role_address"

    return policy.should_export(lead)


def test_r19_end_to_end_decisions(
    suppression_db_plaintext: sqlite3.Connection,
) -> None:
    conn = suppression_db_plaintext
    policy = _build_demo_policy()

    # Seed suppression for one email + one domain.
    upsert_suppression(
        conn,
        email="blocked@example.com",
        reason="manual_test",
        source="r19_integration",
    )
    conn.execute(
        """
        INSERT INTO suppression (domain, reason, source, created_at, expires_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, NULL)
        """,
        ("suppressed-domain.test", "domain_test", "r19_integration"),
    )
    conn.commit()

    leads = [
        {
            "email": "ceo@crestwellpartners.com",
            "verify_status": "valid",
            "icp_score": 90,
            "role_family": "Executive",
            "seniority": "C",
            "industry": "saas",
        },
        {
            "email": "info@crestwellpartners.com",  # role address → blocked
            "verify_status": "valid",
            "icp_score": 95,
            "role_family": "Operations",
            "seniority": "staff",
            "industry": "saas",
        },
        {
            "email": "blocked@example.com",  # email-suppressed
            "verify_status": "valid",
            "icp_score": 99,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "email": "someone@suppressed-domain.test",  # domain-suppressed
            "verify_status": "valid",
            "icp_score": 99,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "email": "invalid-status@example.com",  # invalid verify_status
            "verify_status": "invalid",
            "icp_score": 95,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "email": "low-icp@example.com",  # ICP below threshold
            "verify_status": "valid",
            "icp_score": 30,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "email": "student@example.com",  # excluded by role_family
            "verify_status": "valid",
            "icp_score": 90,
            "role_family": "student",
            "seniority": "staff",
            "industry": "saas",
        },
        {
            "email": "junior@example.com",  # excluded by seniority
            "verify_status": "valid",
            "icp_score": 90,
            "role_family": "Engineering",
            "seniority": "junior",
            "industry": "saas",
        },
        {
            "email": "gov@example.com",  # excluded by industry
            "verify_status": "valid",
            "icp_score": 90,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "government",
        },
    ]

    expected = [
        (True, "ok"),
        (False, "role_address"),
        (False, "suppressed"),
        (False, "suppressed"),
        (False, "status_not_allowed"),
        (False, "icp_below_threshold"),
        (False, "role_excluded"),
        (False, "seniority_excluded"),
        (False, "industry_excluded"),
    ]

    for lead, (exp_ok, exp_reason) in zip(leads, expected, strict=True):
        ok, reason = _decide_with_r19(conn, policy, lead)
        assert ok is exp_ok
        assert reason == exp_reason
