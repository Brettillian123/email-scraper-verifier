# tests/test_r21_search_indexing.py
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime

import pytest

from scripts import migrate_r21_search_indexing as migrate_r21
from src.search import LeadSearchParams, fuzzy_company_lookup, search_people_leads


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    """
    Fresh in-memory SQLite DB with the minimal schema needed for R21 tests.

    We intentionally create a small subset of the real schema:
      - companies
      - people
      - emails
      - v_emails_latest (view over emails)
      - R21 FTS tables + triggers via migrate_r21 helpers
    """
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")

    # Minimal base tables.
    connection.executescript(
        """
        CREATE TABLE companies (
            id INTEGER PRIMARY KEY,
            name TEXT,
            name_norm TEXT,
            domain TEXT,
            official_domain TEXT
        );

        CREATE TABLE people (
            id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            title TEXT,
            title_norm TEXT,
            role_family TEXT,
            seniority TEXT,
            icp_score INTEGER,
            source_url TEXT
        );

        CREATE TABLE emails (
            id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
            email TEXT NOT NULL,
            verify_status TEXT,
            verified_at TEXT,
            source_url TEXT
        );

        -- For tests we define v_emails_latest as a simple view over emails.
        CREATE VIEW v_emails_latest AS
        SELECT
            emails.id AS id,
            emails.person_id AS person_id,
            emails.email AS email,
            emails.verify_status AS verify_status,
            emails.verified_at AS verified_at,
            emails.source_url AS source_url
        FROM emails;
        """
    )

    # Apply R21 FTS tables + triggers.
    migrate_r21.create_fts_tables(connection)
    migrate_r21.create_people_fts_triggers(connection)
    migrate_r21.create_companies_fts_triggers(connection)

    yield connection
    connection.close()


def _insert_company(
    conn: sqlite3.Connection,
    name: str,
    *,
    name_norm: str | None = None,
    domain: str | None = None,
    official_domain: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO companies (name, name_norm, domain, official_domain)
        VALUES (?, ?, ?, ?)
        """,
        (name, name_norm, domain, official_domain),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_person(
    conn: sqlite3.Connection,
    company_id: int,
    *,
    first_name: str,
    last_name: str,
    title: str | None = None,
    title_norm: str | None = None,
    role_family: str | None = None,
    seniority: str | None = None,
    icp_score: int | None = None,
    source_url: str | None = None,
) -> int:
    full_name = f"{first_name} {last_name}"
    cur = conn.execute(
        """
        INSERT INTO people (
            company_id,
            first_name,
            last_name,
            full_name,
            title,
            title_norm,
            role_family,
            seniority,
            icp_score,
            source_url
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            first_name,
            last_name,
            full_name,
            title,
            title_norm,
            role_family,
            seniority,
            icp_score,
            source_url,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_email(
    conn: sqlite3.Connection,
    person_id: int,
    email: str,
    *,
    verify_status: str | None = None,
    verified_at: str | None = None,
    source_url: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO emails (person_id, email, verify_status, verified_at, source_url)
        VALUES (?, ?, ?, ?, ?)
        """,
        (person_id, email, verify_status, verified_at, source_url),
    )
    conn.commit()
    return int(cur.lastrowid)


def test_search_by_title_finds_expected_person(conn: sqlite3.Connection) -> None:
    """
    A basic FTS query on title_norm should return the matching person.
    """
    company_id = _insert_company(
        conn,
        name="Crestwell Partners",
        name_norm="crestwell partners",
        domain="crestwellpartners.com",
        official_domain="crestwellpartners.com",
    )

    cto_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="Alice",
        last_name="Anderson",
        title="Chief Technology Officer",
        title_norm="cto",
        role_family="engineering",
        seniority="c_level",
        icp_score=95,
    )
    _insert_email(
        conn,
        person_id=cto_id,
        email="alice.anderson@crestwellpartners.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    sales_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="Bob",
        last_name="Brown",
        title="Head of Sales",
        title_norm="head of sales",
        role_family="sales",
        seniority="director",
        icp_score=70,
    )
    _insert_email(
        conn,
        person_id=sales_id,
        email="bob.brown@crestwellpartners.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    params = LeadSearchParams(query="cto")
    results = search_people_leads(conn, params)

    assert len(results) == 1
    row = results[0]
    assert row["email"] == "alice.anderson@crestwellpartners.com"
    assert "Alice" in row["full_name"]
    assert row["verify_status"] == "valid"


def test_search_by_company_name_returns_people(conn: sqlite3.Connection) -> None:
    """
    Searching by company name should find people employed there, since
    company_name is included in the people_fts index.
    """
    crestwell_id = _insert_company(
        conn,
        name="Crestwell Partners",
        name_norm="crestwell partners",
        domain="crestwellpartners.com",
        official_domain="crestwellpartners.com",
    )
    other_id = _insert_company(
        conn,
        name="Other Company",
        name_norm="other company",
        domain="other.com",
        official_domain="other.com",
    )

    alice_id = _insert_person(
        conn,
        company_id=crestwell_id,
        first_name="Alice",
        last_name="Smith",
        title="VP of Sales",
        title_norm="vp sales",
        role_family="sales",
        seniority="vp",
        icp_score=88,
    )
    _insert_email(
        conn,
        person_id=alice_id,
        email="alice.smith@crestwellpartners.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    bob_id = _insert_person(
        conn,
        company_id=other_id,
        first_name="Bob",
        last_name="Jones",
        title="VP of Sales",
        title_norm="vp sales",
        role_family="sales",
        seniority="vp",
        icp_score=75,
    )
    _insert_email(
        conn,
        person_id=bob_id,
        email="bob.jones@other.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    params = LeadSearchParams(query="crestwell")
    results = search_people_leads(conn, params)

    assert len(results) == 1
    row = results[0]
    assert row["company"] == "Crestwell Partners"
    assert row["email"].endswith("@crestwellpartners.com")


def test_icp_min_filter_limits_results(conn: sqlite3.Connection) -> None:
    """
    icp_min should filter out low-ICP contacts even if they match the text query.
    """
    company_id = _insert_company(
        conn,
        name="Acme Corp",
        name_norm="acme corp",
        domain="acme.com",
        official_domain="acme.com",
    )

    high_icp_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="High",
        last_name="ICP",
        title="VP Engineering",
        title_norm="vp engineering",
        role_family="engineering",
        seniority="vp",
        icp_score=90,
    )
    _insert_email(
        conn,
        person_id=high_icp_id,
        email="high.icp@acme.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    low_icp_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="Low",
        last_name="ICP",
        title="VP Engineering",
        title_norm="vp engineering",
        role_family="engineering",
        seniority="vp",
        icp_score=30,
    )
    _insert_email(
        conn,
        person_id=low_icp_id,
        email="low.icp@acme.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    params = LeadSearchParams(query="vp engineering", icp_min=80)
    results = search_people_leads(conn, params)

    assert len(results) == 1
    assert results[0]["email"] == "high.icp@acme.com"
    assert results[0]["icp_score"] >= 80


def test_verify_status_filter_limits_results(conn: sqlite3.Connection) -> None:
    """
    verify_status filter should exclude leads that do not match the allowed
    statuses, even if they match the text query.
    """
    company_id = _insert_company(
        conn,
        name="Status Co",
        name_norm="status co",
        domain="statusco.com",
        official_domain="statusco.com",
    )

    valid_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="Val",
        last_name="ID",
        title="Engineer",
        title_norm="engineer",
        role_family="engineering",
        seniority="ic",
        icp_score=60,
    )
    _insert_email(
        conn,
        person_id=valid_id,
        email="valid@statusco.com",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    invalid_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="In",
        last_name="Valid",
        title="Engineer",
        title_norm="engineer",
        role_family="engineering",
        seniority="ic",
        icp_score=60,
    )
    _insert_email(
        conn,
        person_id=invalid_id,
        email="invalid@statusco.com",
        verify_status="invalid",
        verified_at=datetime.utcnow().isoformat(),
    )

    params = LeadSearchParams(
        query="engineer",
        verify_status=["valid"],
    )
    results = search_people_leads(conn, params)

    assert len(results) == 1
    assert results[0]["email"] == "valid@statusco.com"
    assert results[0]["verify_status"] == "valid"


def test_people_fts_triggers_update_and_delete(conn: sqlite3.Connection) -> None:
    """
    Updating or deleting a person should be reflected in FTS search results
    via the R21 triggers.
    """
    company_id = _insert_company(
        conn,
        name="Trigger Co",
        name_norm="trigger co",
        domain="trigger.co",
        official_domain="trigger.co",
    )

    person_id = _insert_person(
        conn,
        company_id=company_id,
        first_name="Trigg",
        last_name="Erson",
        title="Engineer",
        title_norm="engineer",
        role_family="engineering",
        seniority="ic",
        icp_score=50,
    )
    _insert_email(
        conn,
        person_id=person_id,
        email="trigger@trigger.co",
        verify_status="valid",
        verified_at=datetime.utcnow().isoformat(),
    )

    # Initially searchable by "engineer".
    params = LeadSearchParams(query="engineer")
    initial_results = search_people_leads(conn, params)
    assert len(initial_results) == 1

    # Update title_norm to "cto" and ensure FTS reflects the change.
    conn.execute(
        "UPDATE people SET title_norm = ? WHERE id = ?",
        ("cto", person_id),
    )
    conn.commit()

    params_cto = LeadSearchParams(query="cto")
    cto_results = search_people_leads(conn, params_cto)
    assert len(cto_results) == 1
    assert cto_results[0]["email"] == "trigger@trigger.co"

    params_engineer = LeadSearchParams(query="engineer")
    engineer_results = search_people_leads(conn, params_engineer)
    assert engineer_results == []

    # Now delete the person and ensure they disappear from FTS results.
    conn.execute("DELETE FROM emails WHERE person_id = ?", (person_id,))
    conn.execute("DELETE FROM people WHERE id = ?", (person_id,))
    conn.commit()

    params_deleted = LeadSearchParams(query="cto")
    deleted_results = search_people_leads(conn, params_deleted)
    assert deleted_results == []


def test_fuzzy_company_lookup_ranks_similar_names_higher(conn: sqlite3.Connection) -> None:
    """
    O21: fuzzy_company_lookup should rank near-duplicate company names above
    unrelated companies.
    """
    crestwell_1_id = _insert_company(
        conn,
        name="Crestwell Partners",
        name_norm="crestwell partners",
        domain="crestwellpartners.com",
        official_domain="crestwellpartners.com",
    )
    crestwell_2_id = _insert_company(
        conn,
        name="Crest-Well Partner Group",
        name_norm="crest well partner group",
        domain="crestwellgroup.com",
        official_domain="crestwellgroup.com",
    )
    other_id = _insert_company(
        conn,
        name="Completely Different Co",
        name_norm="completely different co",
        domain="different.com",
        official_domain="different.com",
    )

    assert crestwell_1_id
    assert crestwell_2_id
    assert other_id

    query = "Crestwell Partner"
    results = fuzzy_company_lookup(conn, query, limit=5)

    # We expect at least our two Crestwell variants to be present.
    ids = [row["id"] for row in results]
    assert crestwell_1_id in ids
    assert crestwell_2_id in ids

    # And we expect their similarity scores to be >= the unrelated one.
    other_rows = [row for row in results if row["id"] == other_id]
    if other_rows:
        other_similarity = other_rows[0]["similarity"]
        crestwell_sims = [
            row["similarity"] for row in results if row["id"] in (crestwell_1_id, crestwell_2_id)
        ]
        assert min(crestwell_sims) >= other_similarity
