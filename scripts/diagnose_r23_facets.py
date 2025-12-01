#!/usr/bin/env python
from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from src.search.backend import SearchResult, SqliteFtsBackend
from src.search.indexing import LeadSearchParams


def _dump_rows(conn: sqlite3.Connection, sql: str, label: str) -> None:
    print(f"\n=== {label} ===")
    try:
        cur = conn.execute(sql)
    except Exception as exc:  # pragma: no cover - diagnostics only
        print(f"[{label}] ERROR executing SQL:", exc)
        return

    rows = cur.fetchall()
    if not rows:
        print(f"[{label}] (no rows)")
        return

    # Try to treat as Row objects for nicer printing; fall back to tuples.
    if isinstance(rows[0], sqlite3.Row):  # type: ignore[attr-defined]
        for row in rows:
            d = {k: row[k] for k in row.keys()}  # type: ignore[attr-defined]
            print(d)
    else:
        for row in rows:
            print(row)


def _dump_expected_verify_status_counts(conn: sqlite3.Connection) -> None:
    print("\n=== Expected verify_status counts from v_emails_latest ===")
    try:
        cur = conn.execute(
            """
            SELECT verify_status, COUNT(*) AS count
            FROM v_emails_latest
            GROUP BY verify_status
            ORDER BY verify_status
            """
        )
    except Exception as exc:
        print("[expected verify_status] ERROR executing SQL:", exc)
        return

    rows = cur.fetchall()
    if not rows:
        print("[expected verify_status] (no rows)")
        return

    for row in rows:
        if isinstance(row, sqlite3.Row):  # type: ignore[attr-defined]
            print({"verify_status": row["verify_status"], "count": row["count"]})
        else:
            print(row)


def _run_backend_search_with_facets(conn: sqlite3.Connection) -> None:
    print("\n=== Backend search + facets (R23) ===")

    backend = SqliteFtsBackend(conn)

    # Base params mimicking the R23 tests' intent: no explicit filters,
    # just "show me everything the query matches".
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

    # First, run without facets to see which leads the search considers.
    print("\n--- Search (no facets) ---")
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

    # Now run with verify_status + icp_bucket facets.
    print("\n--- Search (with facets: verify_status, icp_bucket) ---")
    params_with_facets = LeadSearchParams(
        **base_kwargs,
        facets=["verify_status", "icp_bucket"],
    )
    result_with_facets: SearchResult = backend.search(params_with_facets)  # type: ignore[assignment]

    facets_dict = result_with_facets.facets or {}
    print("facets dict:", facets_dict)

    vs_list = facets_dict.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}
    print("verify_status facet counts:", vs_counts)

    icp_list = facets_dict.get("icp_bucket") or []
    icp_counts = {row["value"]: row["count"] for row in icp_list}
    print("icp_bucket facet counts:", icp_counts)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose R23 facet behavior against a specific SQLite DB.",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite DB file (e.g. pytest fallback.db)",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row  # type: ignore[attr-defined]

    print("\n================ R23 FACET DIAGNOSTICS ================")
    print(f"DB PATH: {args.db}")

    # 1) List tables/views
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
    try:
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
    except Exception as exc:
        print("\n[lead_search_docs] ERROR or missing table:", exc)

    # 4) Raw expected verify_status counts independent of R23 logic
    _dump_expected_verify_status_counts(conn)

    # 5) Actual R23 backend facets
    _run_backend_search_with_facets(conn)


if __name__ == "__main__":
    main()
