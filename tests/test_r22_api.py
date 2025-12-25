# tests/test_r22_api.py
from __future__ import annotations

import json
import sqlite3
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.search.backend import SqliteFtsBackend
from src.verify.labels import VerifyLabel

# ---------------------------------------------------------------------------
# In-memory DB + TestClient fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def api_test_env(monkeypatch) -> Generator[SimpleNamespace, None, None]:
    """
    Combined fixture that creates in-memory DB, patches all DB access,
    and returns TestClient + seed helper.

    Uses FastAPI's dependency_overrides to ensure our backend is used.
    """
    import src.api.app as app_mod
    import src.db as db_mod
    import src.search.backend as backend_mod
    import src.search.indexing as indexing_mod

    # CRITICAL: Clear any stale backend from previous tests first.
    app.state.search_backend = None

    # Create fresh in-memory database
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
    conn.commit()

    # Create backend with our connection
    backend = SqliteFtsBackend(conn)

    # Named functions to avoid lambda closure issues
    def _get_conn():
        return conn

    # Patch db.get_conn AND db.get_connection at BOTH definition and all import sites
    monkeypatch.setattr(db_mod, "get_conn", _get_conn)
    if hasattr(db_mod, "get_connection"):
        monkeypatch.setattr(db_mod, "get_connection", lambda path=None: conn)

    # Patch anywhere get_conn might have been imported
    for mod in (backend_mod, indexing_mod, app_mod):
        if hasattr(mod, "get_conn"):
            monkeypatch.setattr(mod, "get_conn", _get_conn)

    # Patch backend getters everywhere
    def _return_backend(*args, **kwargs):
        _ = (args, kwargs)
        return backend

    for mod in (backend_mod, indexing_mod, app_mod):
        for attr in (
            "get_backend",
            "get_search_backend",
            "_get_default_backend",
            "_get_search_backend",
        ):
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, _return_backend)

    # CRITICAL: Patch _get_search_backend in app module specifically.
    monkeypatch.setattr(app_mod, "_get_search_backend", _return_backend)

    # Set app.state as backup
    if hasattr(app.state, "_state"):
        app.state._state.clear()
    app.state.search_backend = backend

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
              company_id, first_name, last_name, full_name, title, title_norm,
              role_family, seniority, icp_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                person_id, email, source, source_url, verify_status, verified_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (person_id, email, source, source_url, verify_status, verified_at),
        )
        conn.commit()
        return person_id

    # Create client AFTER all patches are applied
    client = TestClient(app)

    yield SimpleNamespace(client=client, conn=conn, seed_lead=seed_lead)

    # Cleanup - reset app state
    app.state.search_backend = None
    conn.close()


# Legacy fixtures for backward compatibility with existing tests
@pytest.fixture(scope="function")
def api_memory_db(api_test_env) -> SimpleNamespace:
    """Backward-compatible fixture returning conn and seed_lead."""
    return SimpleNamespace(conn=api_test_env.conn, seed_lead=api_test_env.seed_lead)


@pytest.fixture(scope="function")
def api_client(api_test_env) -> TestClient:
    """Backward-compatible fixture returning TestClient."""
    return api_test_env.client


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

    assert len(all_emails) == len(set(all_emails)) == 4
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


# ---------------------------------------------------------------------------
# O26 — verify_label + primary/alternate enrichment tests (API surface)
# ---------------------------------------------------------------------------


def test_api_returns_verify_label_for_single_valid_lead(
    api_client: TestClient,
    api_memory_db: SimpleNamespace,
) -> None:
    """
    For a person with a single valid email, /leads/search should surface a
    primary-native verify_label and is_primary_for_person=True.
    """
    db = api_memory_db

    db.seed_lead(
        full_name="Solo Lead",
        title="Sales Exec",
        icp_score=80,
        email="solo@example.com",
        verify_status="valid",
    )

    resp = api_client.get(
        "/leads/search",
        params={
            "q": "sales",
            "sort": "icp_desc",
            "limit": "10",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    results = body["results"]

    assert len(results) == 1
    row = results[0]

    assert row["email"] == "solo@example.com"
    assert row["verify_label"] == VerifyLabel.VALID_NATIVE_PRIMARY
    assert row["is_primary_for_person"] is True


def test_api_primary_and_alternate_labels_for_multiple_valids(monkeypatch) -> None:
    """
    When a person has multiple valid emails, /leads/search should expose a
    single primary and mark the others as alternates via verify_label and
    is_primary_for_person.

    This test disables cache and patches the backend getter to ensure complete
    isolation from other tests.
    """
    from unittest.mock import patch

    monkeypatch.setenv("LEAD_SEARCH_CACHE_ENABLED", "0")

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
    conn.commit()

    backend = SqliteFtsBackend(conn)

    cur = conn.execute(
        """
        INSERT INTO companies (name, domain, official_domain, attrs)
        VALUES (?, ?, ?, ?)
        """,
        ("Acme Corp", "acme.test", "acme.test", "{}"),
    )
    company_id = cur.lastrowid

    cur = conn.execute(
        """
        INSERT INTO people (
          company_id, first_name, last_name, full_name, title, title_norm,
          role_family, seniority, icp_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            "Brett",
            "Anderson",
            "Brett Anderson",
            "Sales Exec",
            "Sales Exec",
            "sales",
            "vp",
            80,
        ),
    )
    person_id = cur.lastrowid

    conn.execute(
        """
        INSERT INTO people_fts (rowid, full_name, title, company_name)
        VALUES (?, ?, ?, ?)
        """,
        (person_id, "Brett Anderson", "Sales Exec", "Acme Corp"),
    )

    conn.execute(
        """
        INSERT INTO v_emails_latest (
            person_id, email, source, source_url, verify_status, verified_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            "brett.anderson@example.com",
            "generated",
            "https://example.test/source",
            "valid",
            "2025-01-01 00:00:00",
        ),
    )

    conn.execute(
        """
        INSERT INTO v_emails_latest (
            person_id, email, source, source_url, verify_status, verified_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            "info@example.com",
            "generated",
            "https://example.test/source2",
            "valid",
            "2025-01-02 00:00:00",
        ),
    )
    conn.commit()

    app.state.search_backend = None

    with patch("src.api.app._get_search_backend", return_value=backend):
        with patch("src.search.cache.LEAD_SEARCH_CACHE_ENABLED", False):
            client = TestClient(app)

            resp = client.get(
                "/leads/search",
                params={
                    "q": "sales",
                    "sort": "icp_desc",
                    "limit": "10",
                },
            )
            assert resp.status_code == 200
            body = resp.json()
            results = body["results"]

            emails = {r["email"] for r in results}
            assert emails == {"brett.anderson@example.com", "info@example.com"}

            by_email = {r["email"]: r for r in results}
            primary = by_email["brett.anderson@example.com"]
            alternate = by_email["info@example.com"]

            assert primary["is_primary_for_person"] is True
            assert alternate["is_primary_for_person"] is False

            assert primary["verify_label"] == VerifyLabel.VALID_NATIVE_PRIMARY
            assert alternate["verify_label"] == VerifyLabel.VALID_NATIVE_ALTERNATE

    conn.close()
