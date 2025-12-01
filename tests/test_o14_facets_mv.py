# tests/test_o14_facets_mv.py
from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

import src.search.indexing as indexing
from src.search.backend import SqliteFtsBackend
from src.search.indexing import LeadSearchParams


def _setup_memory_db_with_mv() -> SimpleNamespace:
    """
    In-memory SQLite DB with just enough schema to exercise O14:

      - companies / people / emails / verification_results
      - v_emails_latest view (minimal fields used by indexing)
      - people_fts virtual table for MATCH/bm25
      - lead_search_docs materialized table populated with denormalized docs

    We intentionally keep this isolated from migrations so the test is fast and
    deterministic.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

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
          source_url TEXT
        );

        CREATE TABLE emails (
          id INTEGER PRIMARY KEY,
          person_id INTEGER NOT NULL,
          company_id INTEGER NOT NULL,
          email TEXT NOT NULL
        );

        CREATE TABLE verification_results (
          id INTEGER PRIMARY KEY,
          email_id INTEGER NOT NULL,
          verify_status TEXT,
          verified_at TEXT
        );

        DROP VIEW IF EXISTS v_emails_latest;
        CREATE VIEW v_emails_latest AS
        SELECT
          e.id          AS email_id,
          e.email       AS email,
          e.company_id  AS company_id,
          e.person_id   AS person_id,
          vr.verify_status AS verify_status,
          vr.verified_at   AS verified_at,
          NULL AS source,
          NULL AS source_url
        FROM emails AS e
        LEFT JOIN verification_results AS vr
          ON vr.email_id = e.id;

        CREATE VIRTUAL TABLE people_fts USING fts5(
          full_name,
          title,
          company,
          content=''
        );

        CREATE TABLE lead_search_docs (
          person_id INTEGER PRIMARY KEY,
          email TEXT,
          verify_status TEXT,
          icp_score INTEGER,
          role_family TEXT,
          seniority TEXT,
          company_size_bucket TEXT,
          company_industry TEXT,
          icp_bucket TEXT,
          created_at TEXT,
          updated_at TEXT
        );
        """
    )

    # Seed companies with attrs so industry/size could be derived if needed.
    companies = [
        (
            1,
            "Alpha Inc",
            "alpha.example",
            "alpha.example",
            '{"industry": "B2B SaaS", "size_bucket": "51-200"}',
        ),
        (
            2,
            "Beta LLC",
            "beta.example",
            "beta.example",
            '{"industry": "Fintech", "size_bucket": "1-10"}',
        ),
    ]
    conn.executemany(
        """
        INSERT INTO companies (id, name, domain, official_domain, attrs)
        VALUES (?, ?, ?, ?, ?)
        """,
        companies,
    )

    # Seed people: same 3 personas used across R23 facets tests.
    people = [
        # High-ICP valid sales VP at Alpha
        (
            1,
            1,
            "Alice",
            "Nguyen",
            "Alice Nguyen",
            "VP Sales",
            "VP Sales",
            "sales",
            "vp",
            85,
            "https://alpha.example/team/alice",
        ),
        # Mid-ICP invalid sales director at Alpha
        (
            2,
            1,
            "Bob",
            "Jones",
            "Bob Jones",
            "Sales Director",
            "Sales Director",
            "sales",
            "director",
            65,
            "https://alpha.example/team/bob",
        ),
        # Lower-ICP valid marketing director at Beta
        (
            3,
            2,
            "Carol",
            "Smith",
            "Carol Smith",
            "Director of Marketing",
            "Director of Marketing",
            "marketing",
            "director",
            55,
            "https://beta.example/team/carol",
        ),
    ]
    conn.executemany(
        """
        INSERT INTO people (
          id, company_id, first_name, last_name, full_name,
          title, title_norm, role_family, seniority, icp_score, source_url
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        people,
    )

    # Emails and verification results.
    emails = [
        (1, 1, 1, "alice@alpha.example"),
        (2, 2, 1, "bob@alpha.example"),
        (3, 3, 2, "carol@beta.example"),
    ]
    conn.executemany(
        "INSERT INTO emails (id, person_id, company_id, email) VALUES (?, ?, ?, ?)",
        emails,
    )

    verif_results = [
        (1, 1, "valid", "2025-01-01 00:00:00"),
        (2, 2, "invalid", "2025-01-02 00:00:00"),
        (3, 3, "valid", "2025-01-03 00:00:00"),
    ]
    conn.executemany(
        """
        INSERT INTO verification_results (id, email_id, verify_status, verified_at)
        VALUES (?, ?, ?, ?)
        """,
        verif_results,
    )

    # Populate FTS table: put a common token "sales" in all docs so the query
    # matches everyone regardless of actual title/company text.
    for person_id, company_id, _first_name, _last_name, full_name, title, *_rest in people:
        search_text = f"{full_name} {title} sales"
        company_name = "Alpha Inc" if company_id == 1 else "Beta LLC"
        conn.execute(
            """
            INSERT INTO people_fts (rowid, full_name, title, company)
            VALUES (?, ?, ?, ?)
            """,
            (person_id, search_text, search_text, company_name),
        )

    # Seed the materialized view (lead_search_docs) with denormalized docs.
    docs = [
        # person_id, email, verify_status, icp_score, role_family, seniority,
        # size_bucket, industry, icp_bucket, created_at, updated_at
        (
            1,
            "alice@alpha.example",
            "valid",
            85,
            "sales",
            "vp",
            "51-200",
            "B2B SaaS",
            "80-100",
            "2025-01-10 00:00:00",
            "2025-01-10 00:00:00",
        ),
        (
            2,
            "bob@alpha.example",
            "invalid",
            65,
            "sales",
            "director",
            "51-200",
            "B2B SaaS",
            "60-79",
            "2025-01-10 00:00:00",
            "2025-01-10 00:00:00",
        ),
        (
            3,
            "carol@beta.example",
            "valid",
            55,
            "marketing",
            "director",
            "1-10",
            "Fintech",
            "40-59",
            "2025-01-10 00:00:00",
            "2025-01-10 00:00:00",
        ),
    ]
    conn.executemany(
        """
        INSERT INTO lead_search_docs (
          person_id,
          email,
          verify_status,
          icp_score,
          role_family,
          seniority,
          company_size_bucket,
          company_industry,
          icp_bucket,
          created_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        docs,
    )

    conn.commit()
    return SimpleNamespace(conn=conn)


@pytest.fixture
def memory_db_mv() -> SimpleNamespace:
    db = _setup_memory_db_with_mv()
    try:
        yield db
    finally:
        db.conn.close()


def _run_search_with_facets(
    conn: sqlite3.Connection,
    facets: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """
    Helper to run a search against the SqliteFtsBackend and return the facets
    dict from the SearchResult.
    """
    backend = SqliteFtsBackend(conn)
    params = LeadSearchParams(
        query="sales",
        verify_status=None,
        icp_min=None,
        roles=None,
        seniority=None,
        industries=None,
        sizes=None,
        tech=None,
        source=None,
        recency_days=None,
        sort="icp_desc",
        limit=50,
        cursor_icp=None,
        cursor_verified_at=None,
        cursor_person_id=None,
        facets=facets,
    )
    result = backend.search(params)
    return result.facets or {}


def test_o14_facets_mv_and_join_match(memory_db_mv: SimpleNamespace, monkeypatch: Any) -> None:
    """
    O14: When FACET_USE_MV is toggled, facet counts should be identical.

    This ensures that:
      - The materialized view path (lead_search_docs) returns the same facet
        counts as the original join-based path.
      - Turning the feature flag off still yields correct facets.
    """
    # Fallback/join path (FACET_USE_MV = False)
    monkeypatch.setattr(indexing, "FACET_USE_MV", False)
    facets_join = _run_search_with_facets(memory_db_mv.conn, ["verify_status", "icp_bucket"])
    vs_join = {row["value"]: row["count"] for row in facets_join.get("verify_status", [])}
    icp_join = {row["value"]: row["count"] for row in facets_join.get("icp_bucket", [])}

    # Materialized-view path (FACET_USE_MV = True)
    monkeypatch.setattr(indexing, "FACET_USE_MV", True)
    facets_mv = _run_search_with_facets(memory_db_mv.conn, ["verify_status", "icp_bucket"])
    vs_mv = {row["value"]: row["count"] for row in facets_mv.get("verify_status", [])}
    icp_mv = {row["value"]: row["count"] for row in facets_mv.get("icp_bucket", [])}

    assert vs_mv == vs_join
    assert icp_mv == icp_join
