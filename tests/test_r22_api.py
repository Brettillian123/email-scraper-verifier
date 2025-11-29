# tests/test_r22_api.py
from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.search.backend import SqliteFtsBackend

# ---------------------------------------------------------------------------
# In-memory DB + TestClient fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def api_memory_db() -> SimpleNamespace:
    """
    Lightweight in-memory SQLite DB for /leads/search API tests.

    Schema mirrors the subset used by search_people_leads():

      - companies
      - people
      - v_emails_latest
      - people_fts (FTS5)

    Exposes seed_lead() to create a company + person + v_emails_latest row and
    wire up the FTS entry.
    """
    # NOTE: allow this connection to be used from FastAPI's TestClient thread
    conn = sqlite3.connect(":memory:", check_same_thread=False)
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
        source_url: str = "https://example.test/source",
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
                source_url,
                verify_status,
                verified_at,
            ),
        )

        return person_id

    return SimpleNamespace(conn=conn, seed_lead=seed_lead)


@pytest.fixture
def api_client(api_memory_db: SimpleNamespace) -> TestClient:
    """
    FastAPI TestClient backed by an in-memory SqliteFtsBackend.

    We inject the backend via app.state.search_backend so that the HTTP layer
    uses our test DB instead of opening a real file on disk.
    """
    backend = SqliteFtsBackend(api_memory_db.conn)
    app.state.search_backend = backend
    return TestClient(app)


# ---------------------------------------------------------------------------
# /leads/search behavior tests
# ---------------------------------------------------------------------------


def test_basic_query_filters_icp_and_verify_status(
    api_client: TestClient,
    api_memory_db: SimpleNamespace,
) -> None:
    """
    Basic GET /leads/search should respect icp_min and verify_status filters.
    """
    db = api_memory_db

    db.seed_lead(
        full_name="Alice High",
        title="Sales Exec",
        icp_score=90,
        email="high@example.com",
        verify_status="valid",
    )
    db.seed_lead(
        full_name="Bob Mid",
        title="Sales Exec",
        icp_score=80,
        email="mid@example.com",
        verify_status="valid",
    )
    db.seed_lead(
        full_name="Charlie Low",
        title="Sales Exec",
        icp_score=60,
        email="low@example.com",
        verify_status="valid",
    )
    db.seed_lead(
        full_name="Dave Invalid",
        title="Sales Exec",
        icp_score=95,
        email="invalid@example.com",
        verify_status="invalid",
    )

    resp = api_client.get(
        "/leads/search",
        params={
            "q": "sales",
            "icp_min": "80",
            "verify_status": "valid",
            "limit": "10",
        },
    )
    assert resp.status_code == 200
    body = resp.json()

    assert body["sort"] == "icp_desc"
    assert body["limit"] == 10

    results = body["results"]
    emails = {r["email"] for r in results}
    scores = {r["icp_score"] for r in results}
    statuses = {r["verify_status"] for r in results}

    assert emails == {"high@example.com", "mid@example.com"}
    assert scores == {90, 80}
    assert statuses == {"valid"}


def test_roles_and_seniority_filters_api(
    api_client: TestClient,
    api_memory_db: SimpleNamespace,
) -> None:
    """
    roles + seniority query params should intersect correctly in the HTTP layer.
    """
    db = api_memory_db

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

    resp = api_client.get(
        "/leads/search",
        params={
            "q": "sales OR marketing",
            "roles": "sales",
            "seniority": "vp",
            "limit": "10",
        },
    )
    assert resp.status_code == 200
    body = resp.json()

    results = body["results"]
    emails = {r["email"] for r in results}
    role_families = {r["role_family"] for r in results}
    seniorities = {r["seniority"] for r in results}

    assert emails == {"vp_sales@example.com"}
    assert role_families == {"sales"}
    assert seniorities == {"vp"}


def test_industry_size_tech_filters_api(
    api_client: TestClient,
    api_memory_db: SimpleNamespace,
) -> None:
    """
    industries, sizes, and tech query params should isolate the expected SaaS lead.
    """
    db = api_memory_db

    # Matching company
    db.seed_lead(
        full_name="SaaS AE",
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

    # Non-matching company
    db.seed_lead(
        full_name="Other AE",
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

    resp = api_client.get(
        "/leads/search",
        params={
            "q": "executive",
            "industries": "B2B SaaS",
            "sizes": "51-200",
            "tech": "salesforce",
            "limit": "10",
        },
    )
    assert resp.status_code == 200
    body = resp.json()

    results = body["results"]
    emails = {r["email"] for r in results}
    industries = {r["industry"] for r in results}
    sizes = {r["company_size"] for r in results}
    tech_lists = [r["tech"] for r in results]

    assert emails == {"saas@example.com"}
    assert industries == {"B2B SaaS"}
    assert sizes == {"51-200"}
    # At least one of the results should mention "salesforce" in its tech list.
    assert any("salesforce" in tech for tech in tech_lists)


def test_pagination_with_cursor_icp_desc(
    api_client: TestClient,
    api_memory_db: SimpleNamespace,
) -> None:
    """
    /leads/search with sort=icp_desc should support keyset pagination via cursor
    without duplicates and with stable ordering.
    """
    db = api_memory_db

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

    # Page 1
    resp1 = api_client.get(
        "/leads/search",
        params={
            "q": "sales",
            "sort": "icp_desc",
            "limit": "2",
        },
    )
    assert resp1.status_code == 200
    body1 = resp1.json()
    results1 = body1["results"]
    next_cursor = body1["next_cursor"]

    assert len(results1) == 2
    assert next_cursor is not None

    # Page 2 using cursor
    resp2 = api_client.get(
        "/leads/search",
        params={
            "q": "sales",
            "sort": "icp_desc",
            "limit": "2",
            "cursor": next_cursor,
        },
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    results2 = body2["results"]

    assert len(results2) == 2

    all_emails = [r["email"] for r in results1 + results2]
    all_scores = [r["icp_score"] for r in results1 + results2]

    # No duplicates across pages
    assert len(all_emails) == len(set(all_emails)) == 4
    # Scores are sorted descending overall
    assert all_scores == sorted(all_scores, reverse=True)


def test_invalid_sort_returns_400(api_client: TestClient) -> None:
    """
    Unsupported sort parameter should yield a 400 with an appropriate error code.
    """
    resp = api_client.get(
        "/leads/search",
        params={
            "q": "sales",
            "sort": "created_desc",
        },
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"] == "invalid_sort"


def test_malformed_cursor_returns_400(api_client: TestClient) -> None:
    """
    Malformed cursor should yield a 400 with error=invalid_cursor.
    """
    resp = api_client.get(
        "/leads/search",
        params={
            "q": "sales",
            "cursor": "not-base64!!",
        },
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"] == "invalid_cursor"
