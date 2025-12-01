# tests/test_r23_facets_diagnostics.py
from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest
from test_r23_facets_backend import _setup_memory_db

import src.search.indexing as indexing  # noqa: F401  (imported for interactive diagnostics if needed)
from src.search.backend import SearchResult, SqliteFtsBackend
from src.search.indexing import LeadSearchParams


@pytest.fixture
def memory_db() -> SimpleNamespace:
    """
    Shared in-memory SQLite DB for R23 facet diagnostics.

    Reuses the minimal schema + seed data from test_r23_facets_backend so
    backend and diagnostics tests exercise the same setup.
    """
    return _setup_memory_db()


def _dump_rows(conn: sqlite3.Connection, sql: str, label: str) -> None:
    print(f"\n=== {label} ===")
    try:
        cur = conn.execute(sql)
        cur.row_factory = sqlite3.Row  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - diagnostics only
        print(f"[{label}] ERROR executing SQL:", exc)
        return

    rows = cur.fetchall()
    if not rows:
        print(f"[{label}] (no rows)")
        return

    for row in rows:
        # Convert Row -> dict for nicer printing
        try:
            d = dict(zip(row.keys(), row, strict=False))  # type: ignore[attr-defined]
        except Exception:
            d = dict(row)
        print(d)


@pytest.mark.parametrize(
    "facets",
    [
        ["verify_status"],
        ["verify_status", "icp_bucket"],
    ],
)
def test_r23_facet_diagnostics(memory_db: SimpleNamespace, facets: list[str]) -> None:
    """
    Diagnostic test for R23 facets.

    Run this with:
        pytest tests/test_r23_facets_diagnostics.py -s

    It will print:
      - tables/views in the test DB
      - v_emails_latest contents (email, person_id, icp_score, verify_status)
      - lead_search_docs contents (if present)
      - search results for query='sales'
      - facet results for the given facet set
    """
    conn: sqlite3.Connection = memory_db.conn
    conn.row_factory = sqlite3.Row

    print("\n\n================ R23 FACET DIAGNOSTICS ================")
    print(f"FACETS REQUESTED: {facets}")

    # 1) List tables and views
    _dump_rows(
        conn,
        """
        SELECT type, name
        FROM sqlite_master
        WHERE type IN ('table', 'view')
        ORDER BY type, name
        """,
        "sqlite_master (tables/views)",
    )

    # 2) v_emails_latest snapshot
    _dump_rows(
        conn,
        """
        SELECT
          email,
          person_id,
          verify_status,
          icp_score
        FROM v_emails_latest
        ORDER BY email
        """,
        "v_emails_latest (email, person_id, verify_status, icp_score)",
    )

    # 3) lead_search_docs snapshot, if it exists
    _dump_rows(
        conn,
        """
        SELECT
          person_id,
          email,
          verify_status,
          icp_score,
          icp_bucket,
          company_industry,
          company_size_bucket
        FROM lead_search_docs
        ORDER BY person_id
        """,
        "lead_search_docs (if present)",
    )

    # 4) Run search via SqliteFtsBackend, first without facets, then with facets
    backend = SqliteFtsBackend(conn)

    base_kwargs: dict[str, Any] = {
        "query": "sales",
        "verify_status": None,
        "icp_min": None,
        "roles": None,
        "seniority": None,
        "industries": None,
        "sizes": None,
        "tech": None,
        "source": None,
        "recency_days": None,
        "sort": "icp_desc",
        "limit": 50,
        "cursor_icp": None,
        "cursor_verified_at": None,
        "cursor_person_id": None,
    }

    print("\n=== Search (no facets) ===")
    params_no_facets = LeadSearchParams(**base_kwargs, facets=None)
    result_no_facets: SearchResult = backend.search(params_no_facets)  # type: ignore[assignment]

    print(f"search.leads count: {len(result_no_facets.leads)}")
    for row in result_no_facets.leads:
        print(
            {
                "email": row.get("email"),
                "person_id": row.get("person_id"),
                "icp_score": row.get("icp_score"),
                "verify_status": row.get("verify_status"),
            },
        )

    print("\n=== Search (with facets) ===")
    params_with_facets = LeadSearchParams(**base_kwargs, facets=facets)
    result_with_facets: SearchResult = backend.search(params_with_facets)  # type: ignore[assignment]

    facets_dict = result_with_facets.facets or {}
    print("facets dict:", facets_dict)

    # Break out verify_status counts explicitly for quick reading
    vs_list = facets_dict.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}
    print("verify_status facet counts:", vs_counts)

    icp_list = facets_dict.get("icp_bucket") or []
    icp_counts = {row["value"]: row["count"] for row in icp_list}
    print("icp_bucket facet counts:", icp_counts)

    # This is a pure diagnostic; no strict asserts here so it won't fail
    # your suite. Use the printed output to compare against expectations.
    assert True
