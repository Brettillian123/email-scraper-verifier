# tests/test_r23_facets_backend.py
from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

from src.search.backend import SqliteFtsBackend
from src.search.indexing import LeadSearchParams


def _setup_memory_db() -> SimpleNamespace:
    """
    Create an in-memory SQLite DB with the minimal schema needed to exercise
    R23 facets over the SqliteFtsBackend.

    We intentionally keep this schema small and self-contained instead of
    relying on migrations, so the tests are fast and deterministic.
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

        -- Minimal v_emails_latest view exposing the fields used by indexing.py
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

        -- Simple FTS table; we just need MATCH + bm25() to work and rowid to
        -- line up with people.id.
        CREATE VIRTUAL TABLE people_fts USING fts5(
          full_name,
          title,
          company,
          content=''
        );
        """
    )

    # Seed companies with attrs so industry / size buckets can be derived.
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

    # Seed people with different role_family / seniority / icp_score.
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

    # Populate FTS table: rowid must match people.id.
    for person_id, company_id, _first_name, _last_name, full_name, title, *_rest in people:
        company_name = "Alpha Inc" if company_id == 1 else "Beta LLC"
        conn.execute(
            """
            INSERT INTO people_fts (rowid, full_name, title, company)
            VALUES (?, ?, ?, ?)
            """,
            (person_id, full_name, title, company_name),
        )

    conn.commit()
    return SimpleNamespace(conn=conn)


@pytest.fixture
def memory_db() -> SimpleNamespace:
    db = _setup_memory_db()
    try:
        yield db
    finally:
        db.conn.close()


def _search_with_facets(
    conn: sqlite3.Connection,
    facets: list[str],
    verify_status: list[str] | None = None,
    icp_min: int | None = None,
) -> Any:
    backend = SqliteFtsBackend(conn)
    params = LeadSearchParams(
        # Broad FTS query that matches all three seeded people
        # (Alice/Bob via "sales", Carol via "marketing").
        query="sales OR marketing",
        verify_status=verify_status,
        icp_min=icp_min,
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
    # New R23 path: backend.search returns a SearchResult with .facets
    return backend.search(params)


def test_facets_verify_status_counts(memory_db: SimpleNamespace) -> None:
    """
    Basic verify_status facet counts under the current filters.

    With the seeded data:
      - valid:   Alice, Carol  -> 2
      - invalid: Bob           -> 1
    """
    result = _search_with_facets(
        memory_db.conn,
        facets=["verify_status"],
    )
    facets = result.facets or {}
    vs_list = facets.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}

    assert vs_counts["valid"] == 2
    assert vs_counts["invalid"] == 1


def test_facets_respect_verify_status_filter(memory_db: SimpleNamespace) -> None:
    """
    When we filter to verify_status=['valid'], facet counts should only reflect
    the valid leads (Alice + Carol).
    """
    result = _search_with_facets(
        memory_db.conn,
        facets=["verify_status"],
        verify_status=["valid"],
    )
    facets = result.facets or {}
    vs_list = facets.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}

    assert vs_counts["valid"] == 2
    # invalid should either be absent or have count 0; in either case, not 1.
    assert "invalid" not in vs_counts or vs_counts["invalid"] == 0


def test_facets_multiple_dimensions_icp_bucket_and_verify_status(
    memory_db: SimpleNamespace,
) -> None:
    """
    Request multiple facet dimensions at once and verify basic shapes.

    Seed recap:
      - Alice: icp_score=85  -> bucket 80-100, verify_status=valid
      - Bob:   icp_score=65  -> bucket 60-79, verify_status=invalid
      - Carol: icp_score=55  -> bucket 40-59, verify_status=valid
    """
    result = _search_with_facets(
        memory_db.conn,
        facets=["verify_status", "icp_bucket"],
    )
    facets = result.facets or {}

    # verify_status facet
    vs_list = facets.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}
    assert vs_counts["valid"] == 2
    assert vs_counts["invalid"] == 1

    # icp_bucket facet
    icp_list = facets.get("icp_bucket") or []
    icp_counts = {row["value"]: row["count"] for row in icp_list}

    # We expect one lead in each bucket: 80-100 (Alice), 60-79 (Bob), 40-59 (Carol).
    assert icp_counts["80-100"] == 1
    assert icp_counts["60-79"] == 1
    assert icp_counts["40-59"] == 1

    # No-one in the lowest bucket.
    assert "0-39" not in icp_counts or icp_counts["0-39"] == 0


def test_facets_respect_icp_min_filter(memory_db: SimpleNamespace) -> None:
    """
    If we apply an icp_min filter (e.g. >= 70), only Alice (85) and Bob (65)
    vs Carol (55) should be filtered accordingly in facets.

    With icp_min=70:
      - Alice (85, valid) stays
      - Bob   (65, invalid) is filtered out
      - Carol (55, valid) is filtered out

    So verify_status facets should show:
      - valid: 1
      - invalid: absent or 0
    """
    result = _search_with_facets(
        memory_db.conn,
        facets=["verify_status", "icp_bucket"],
        icp_min=70,
    )
    facets = result.facets or {}
    vs_list = facets.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}

    assert vs_counts["valid"] == 1
    assert "invalid" not in vs_counts or vs_counts["invalid"] == 0


# ---------------------------------------------------------------------------
# DEBUG test – full facet diagnostics on the same in-memory DB
# ---------------------------------------------------------------------------


def test_r23_facets_debug(memory_db: SimpleNamespace) -> None:
    """
    Diagnostic test for R23 facets using the same in-memory DB as the other tests.

    Run with:
        pytest tests/test_r23_facets_backend.py::test_r23_facets_debug -s

    It will print:
      - tables/views in the test DB
      - v_emails_latest contents (email, person_id, icp_score, verify_status)
      - (if present) lead_search_docs contents
      - raw verify_status counts from SQL
      - search results and facet payload from the SqliteFtsBackend
    """
    conn: sqlite3.Connection = memory_db.conn
    conn.row_factory = sqlite3.Row  # type: ignore[attr-defined]

    def dump_rows(sql: str, label: str) -> None:
        print(f"\n=== {label} ===")
        try:
            cur = conn.execute(sql)
        except Exception as exc:
            print(f"[{label}] ERROR executing SQL:", exc)
            return

        rows = cur.fetchall()
        if not rows:
            print(f"[{label}] (no rows)")
            return

        if isinstance(rows[0], sqlite3.Row):  # type: ignore[attr-defined]
            for row in rows:
                d = {k: row[k] for k in row.keys()}  # type: ignore[attr-defined]
                print(d)
        else:
            for row in rows:
                print(row)

    print("\n================ R23 FACET DEBUG ================")

    # 1) List tables/views
    dump_rows(
        """
        SELECT type, name
        FROM sqlite_master
        WHERE type IN ('table', 'view')
        ORDER BY type, name
        """,
        "sqlite_master (tables/views)",
    )

    # 2) v_emails_latest snapshot
    dump_rows(
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

    # 3) lead_search_docs snapshot, if it exists (O14 MV)
    dump_rows(
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

    # 4) Raw expected verify_status counts independent of facet logic
    dump_rows(
        """
        SELECT verify_status, COUNT(*) AS count
        FROM v_emails_latest
        GROUP BY verify_status
        ORDER BY verify_status
        """,
        "Expected verify_status counts (raw SQL from v_emails_latest)",
    )

    # 5) Actual backend search + facets
    backend = SqliteFtsBackend(conn)

    base_kwargs: dict[str, Any] = {
        # Match all three seeded people, just like _search_with_facets
        "query": "sales OR marketing",
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

    print("\n--- Search (no facets) ---")
    params_no_facets = LeadSearchParams(**base_kwargs, facets=None)
    result_no_facets = backend.search(params_no_facets)

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

    print("\n--- Search (with facets: verify_status, icp_bucket) ---")
    params_with_facets = LeadSearchParams(
        **base_kwargs,
        facets=["verify_status", "icp_bucket"],
    )
    result_with_facets = backend.search(params_with_facets)

    facets = result_with_facets.facets or {}
    print("facets dict:", facets)

    vs_list = facets.get("verify_status") or []
    vs_counts = {row["value"]: row["count"] for row in vs_list}
    print("verify_status facet counts:", vs_counts)

    icp_list = facets.get("icp_bucket") or []
    icp_counts = {row["value"]: row["count"] for row in icp_list}
    print("icp_bucket facet counts:", icp_counts)

    # No strict assertions: this is purely diagnostic.
    assert True
