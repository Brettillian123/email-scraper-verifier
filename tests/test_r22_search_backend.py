# tests/test_r22_search_backend.py
from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

from src.search.backend import SqliteFtsBackend
from src.search.indexing import LeadSearchParams, search_people_leads

# ---------------------------------------------------------------------------
# In-memory DB fixture for search tests
# ---------------------------------------------------------------------------


@pytest.fixture
def memory_db() -> SimpleNamespace:
    """
    Lightweight in-memory SQLite DB for R22 search backend tests.

    This sets up just enough schema to exercise search_people_leads() and the
    SqliteFtsBackend wrapper:

      - companies
      - people
      - v_emails_latest
      - people_fts (FTS5)

    It also exposes a helper seed_lead() that inserts a company + person +
    v_emails_latest row and wires up the FTS entry.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")

    conn.executescript(
        """
        CREATE TABLE companies (
          id INTEGER PRIMARY KEY,
          name TEXT,
          domain TEXT,
          official_domain TEXT,
          attrs TEXT
        );

        CREATE TABLE people (
          id INTEGER PRIMARY KEY,
          company_id INTEGER NOT NULL,
          first_name TEXT,
          last_name TEXT,
          full_name TEXT,
          title TEXT,
          title_norm TEXT,
          role_family TEXT,
          seniority TEXT,
          icp_score INTEGER,
          FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
        );

        CREATE TABLE v_emails_latest (
          person_id INTEGER NOT NULL,
          email TEXT NOT NULL,
          source TEXT,
          source_url TEXT,
          verify_status TEXT,
          verified_at TEXT,
          FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE
        );

        CREATE VIRTUAL TABLE people_fts USING fts5(
          full_name,
          title,
          company_name
        );
        """,
    )

    def seed_lead(
        *,
        full_name: str,
        title: str,
        role_family: str = "sales",
        seniority: str = "vp",
        icp_score: int = 80,
        company_name: str = "Acme Corp",
        company_domain: str = "acme.test",
        company_attrs: dict[str, Any] | None = None,
        email: str = "lead@example.com",
        verify_status: str = "valid",
        verified_at: str | None = "2025-01-01 00:00:00",
        source: str = "generated",
    ) -> int:
        """
        Insert a single lead (company + person + v_emails_latest + FTS).

        Returns the new person_id.
        """
        if company_attrs is None:
            company_attrs = {}

        attrs_json = json.dumps(company_attrs)

        cur = conn.execute(
            """
            INSERT INTO companies (name, domain, official_domain, attrs)
            VALUES (?, ?, ?, ?)
            """,
            (company_name, company_domain, company_domain, attrs_json),
        )
        company_id = cur.lastrowid

        first_name, _, last_name = full_name.partition(" ")
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
              icp_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                company_id,
                first_name or None,
                last_name or None,
                full_name,
                title,
                title,
                role_family,
                seniority,
                icp_score,
            ),
        )
        person_id = cur.lastrowid

        # FTS row: rowid must match people.id for the join in search_people_leads.
        conn.execute(
            """
            INSERT INTO people_fts (rowid, full_name, title, company_name)
            VALUES (?, ?, ?, ?)
            """,
            (person_id, full_name, title, company_name),
        )

        conn.execute(
            """
            INSERT INTO v_emails_latest (
              person_id,
              email,
              source,
              source_url,
              verify_status,
              verified_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                person_id,
                email,
                source,
                "https://example.test/source",
                verify_status,
                verified_at,
            ),
        )

        return person_id

    return SimpleNamespace(conn=conn, seed_lead=seed_lead)


# ---------------------------------------------------------------------------
# Filter behavior tests
# ---------------------------------------------------------------------------


def test_icp_min_filters_results(memory_db: SimpleNamespace) -> None:
    """
    icp_min should exclude leads whose icp_score is below the threshold.
    """
    db = memory_db

    db.seed_lead(
        full_name="Alice High",
        title="Sales Leader",
        icp_score=90,
        email="high@example.com",
    )
    db.seed_lead(
        full_name="Bob Mid",
        title="Sales Leader",
        icp_score=80,
        email="mid@example.com",
    )
    db.seed_lead(
        full_name="Charlie Low",
        title="Sales Leader",
        icp_score=60,
        email="low@example.com",
    )

    params = LeadSearchParams(
        query="sales",  # matches "Sales Leader"
        icp_min=80,
        sort="icp_desc",
        limit=10,
    )
    rows = search_people_leads(db.conn, params)

    emails = {row["email"] for row in rows}
    scores = {row["icp_score"] for row in rows}

    assert emails == {"high@example.com", "mid@example.com"}
    assert scores == {90, 80}


def test_verify_status_filters_results(memory_db: SimpleNamespace) -> None:
    """
    verify_status filter should only return leads with matching statuses.
    """
    db = memory_db

    db.seed_lead(
        full_name="Valid Lead",
        title="Sales Exec",
        icp_score=85,
        email="valid@example.com",
        verify_status="valid",
    )
    db.seed_lead(
        full_name="Invalid Lead",
        title="Sales Exec",
        icp_score=85,
        email="invalid@example.com",
        verify_status="invalid",
    )

    params = LeadSearchParams(
        query="sales",
        verify_status=["valid"],
        sort="icp_desc",
        limit=10,
    )
    rows = search_people_leads(db.conn, params)

    emails = {row["email"] for row in rows}
    statuses = {row["verify_status"] for row in rows}

    assert emails == {"valid@example.com"}
    assert statuses == {"valid"}


def test_role_and_seniority_filters(memory_db: SimpleNamespace) -> None:
    """
    roles + seniority filters should intersect correctly.
    """
    db = memory_db

    db.seed_lead(
        full_name="Sales VP",
        title="VP Sales",
        role_family="sales",
        seniority="vp",
        icp_score=90,
        email="vp_sales@example.com",
    )
    db.seed_lead(
        full_name="Marketing Director",
        title="Director Marketing",
        role_family="marketing",
        seniority="director",
        icp_score=90,
        email="dir_marketing@example.com",
    )

    params = LeadSearchParams(
        query="sales OR marketing",
        roles=["sales"],
        seniority=["vp"],
        sort="icp_desc",
        limit=10,
    )
    rows = search_people_leads(db.conn, params)

    emails = {row["email"] for row in rows}
    assert emails == {"vp_sales@example.com"}


def test_industry_size_and_tech_filters(memory_db: SimpleNamespace) -> None:
    """
    industries, sizes, and tech filters should combine to isolate the right lead.
    """
    db = memory_db

    # Matching company: B2B SaaS, 51-200, tech_keywords include salesforce.
    db.seed_lead(
        full_name="SaaS Sales",
        title="Account Executive",
        icp_score=88,
        company_name="SaaS Co",
        company_domain="saasco.test",
        company_attrs={
            "industry": "B2B SaaS",
            "size_bucket": "51-200",
            "tech_keywords": ["salesforce", "hubspot"],
        },
        email="saas@example.com",
    )

    # Non-matching company: different attributes.
    db.seed_lead(
        full_name="Other",
        title="Account Executive",
        icp_score=88,
        company_name="Other Co",
        company_domain="other.test",
        company_attrs={
            "industry": "Manufacturing",
            "size_bucket": "201-500",
            "tech_keywords": ["sap"],
        },
        email="other@example.com",
    )

    params = LeadSearchParams(
        query="executive",
        industries=["B2B SaaS"],
        sizes=["51-200"],
        tech=["salesforce"],
        sort="icp_desc",
        limit=10,
    )
    rows = search_people_leads(db.conn, params)

    emails = {row["email"] for row in rows}
    industries = {row["industry"] for row in rows}
    sizes = {row["company_size"] for row in rows}

    assert emails == {"saas@example.com"}
    assert industries == {"B2B SaaS"}
    assert sizes == {"51-200"}


def test_recency_days_excludes_old_records(memory_db: SimpleNamespace) -> None:
    """
    recency_days should exclude leads whose verified_at is older than the cutoff.

    We use extreme timestamps so the test is stable regardless of the current date:
      - very old: 1970-01-01
      - very recent/future: 9999-12-31
    """
    db = memory_db

    # Old lead
    db.seed_lead(
        full_name="Old Lead",
        title="Sales Exec",
        icp_score=80,
        email="old@example.com",
        verified_at="1970-01-01 00:00:00",
    )

    # Recent/future lead
    db.seed_lead(
        full_name="New Lead",
        title="Sales Exec",
        icp_score=80,
        email="new@example.com",
        verified_at="9999-12-31 00:00:00",
    )

    params = LeadSearchParams(
        query="sales",
        recency_days=30,
        sort="verified_desc",
        limit=10,
    )
    rows = search_people_leads(db.conn, params)

    emails = {row["email"] for row in rows}
    assert emails == {"new@example.com"}


# ---------------------------------------------------------------------------
# Sort + keyset pagination tests via SqliteFtsBackend
# ---------------------------------------------------------------------------


def test_icp_desc_keyset_pagination(memory_db: SimpleNamespace) -> None:
    """
    sort=icp_desc with cursor_icp/cursor_person_id should yield stable ordering
    and no duplicates across pages.
    """
    db = memory_db

    # Seed four leads with distinct ICP scores so ordering is deterministic.
    for score, email in [
        (95, "a@example.com"),
        (90, "b@example.com"),
        (85, "c@example.com"),
        (80, "d@example.com"),
    ]:
        db.seed_lead(
            full_name=f"Lead {score}",
            title="Sales Exec",
            icp_score=score,
            email=email,
        )

    backend = SqliteFtsBackend(db.conn)

    # First page: top 2 by icp_score desc.
    params_page1 = LeadSearchParams(
        query="sales",
        sort="icp_desc",
        limit=2,
    )
    page1 = backend.search_leads(params_page1)
    assert len(page1) == 2

    # Build cursor from last row of page1.
    last_row = page1[-1]
    cursor_icp = last_row["icp_score"]
    cursor_person_id = last_row["person_id"]

    # Second page: continue after cursor.
    params_page2 = LeadSearchParams(
        query="sales",
        sort="icp_desc",
        limit=2,
        cursor_icp=cursor_icp,
        cursor_person_id=cursor_person_id,
    )
    page2 = backend.search_leads(params_page2)
    assert len(page2) == 2

    all_emails = [row["email"] for row in page1 + page2]
    all_scores = [row["icp_score"] for row in page1 + page2]

    # No duplicates across pages.
    assert len(all_emails) == len(set(all_emails)) == 4

    # Scores are sorted descending overall.
    assert all_scores == sorted(all_scores, reverse=True)
