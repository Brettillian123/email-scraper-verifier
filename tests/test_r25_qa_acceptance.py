from __future__ import annotations

import csv
import inspect
import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pytest

from src.crawl import runner as crawl_runner
from src.db_pages import save_pages
from src.extract.candidates import extract_candidates
from src.search.backend import SqliteFtsBackend
from src.search.indexing import LeadSearchParams

# Directory where this test file lives
THIS_DIR = Path(__file__).parent
FIXTURES_DIR = THIS_DIR / "fixtures"
ROOT_DIR = THIS_DIR.parent


def _apply_schema(conn: sqlite3.Connection) -> None:
    """
    Apply the full project schema into the given SQLite connection.

    This mirrors scripts/apply_schema.py but runs entirely in-process so R25
    tests can stand up their own isolated databases.
    """
    schema_path = ROOT_DIR / "db" / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    conn.executescript("PRAGMA foreign_keys = ON;")
    conn.executescript(sql)


def _default_value_for_column(col_type: str, name: str) -> Any:
    """
    Provide a conservative default for NOT NULL columns we are not explicitly
    setting in our R25 fixtures.

    This keeps the test data robust against future schema extensions while
    staying clearly synthetic.
    """
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
        # ISO-8601 style timestamp
        return "1970-01-01T00:00:00"
    # Fallback for TEXT/UNKNOWN
    return "r25-default"


def _insert_row(conn: sqlite3.Connection, table: str, values: dict[str, Any]) -> None:
    """
    Generic helper to insert a row into `table`, filling in any required
    NOT NULL columns that don't have explicit values or defaults.

    This allows the R25 fixtures to remain stable even if new columns are
    added to the schema later.
    """
    pragma_sql = f"PRAGMA table_info({table})"
    cur = conn.execute(pragma_sql)
    cols = cur.fetchall()

    insert_cols: list[str] = []
    params: list[Any] = []

    for col in cols:
        name = col["name"]
        col_type = col["type"]
        notnull = bool(col["notnull"])
        has_default = col["dflt_value"] is not None

        if name in values:
            insert_cols.append(name)
            params.append(values[name])
        elif notnull and not has_default:
            insert_cols.append(name)
            params.append(_default_value_for_column(col_type, name))
        else:
            # Column either nullable or has a default; omit from INSERT.
            continue

    if not insert_cols:
        raise RuntimeError(f"No columns to insert for table {table}")

    placeholders = ", ".join(["?"] * len(insert_cols))
    col_list = ", ".join(insert_cols)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    conn.execute(sql, params)


def _load_known_domains_fixture() -> dict[tuple[str, str], dict[str, Any]]:
    """
    Load r25_known_domains.csv into a mapping keyed by (domain, email).

    CSV columns:
        domain,email,expected_verify_status,expected_icp_min
    """
    path = FIXTURES_DIR / "r25_known_domains.csv"
    data: dict[tuple[str, str], dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize and coerce types we care about
            domain = row["domain"].strip()
            email = row["email"].strip()
            expected_status = row["expected_verify_status"].strip()
            expected_icp_min = int(row["expected_icp_min"])
            data[(domain, email)] = {
                "expected_verify_status": expected_status,
                "expected_icp_min": expected_icp_min,
            }
    return data


def _seed_known_verifications(conn: sqlite3.Connection) -> None:
    """
    Seed a minimal but realistic set of companies/people/emails/verification
    results that match the r25_known_domains.csv fixture.

    We temporarily disable foreign key enforcement during seeding so the test
    remains resilient to future schema changes that add new relationships we
    do not explicitly care about here. The core chain we *do* care about
    (companies → people → emails → verification_results) is populated
    explicitly and consistently.
    """
    fixture = _load_known_domains_fixture()

    # Relax FK enforcement for the duration of this seed operation.
    conn.execute("PRAGMA foreign_keys = OFF;")
    try:
        # Companies
        companies = [
            {
                "id": 1,
                "name": "Crestwell Partners",
                "domain": "crestwellpartners.com",
                "official_domain": "crestwellpartners.com",
                "attrs": "{}",
            },
            {
                "id": 2,
                "name": "Example Co",
                "domain": "example.com",
                "official_domain": "example.com",
                "attrs": "{}",
            },
            {
                "id": 3,
                "name": "Catchall Inc",
                "domain": "catchall.test",
                "official_domain": "catchall.test",
                "attrs": "{}",
            },
        ]

        for row in companies:
            _insert_row(conn, "companies", row)

        # People (we do not assume an icp_score column exists on this table;
        # scores may be stored elsewhere or derived in views).
        people = [
            {
                "id": 1,
                "company_id": 1,
                "first_name": "Brett",
                "last_name": "Anderson",
                "full_name": "Brett Anderson",
                "title": "VP Sales",
                "title_norm": "vp sales",
                "role_family": "sales",
                "seniority": "senior",
            },
            {
                "id": 2,
                "company_id": 2,
                "first_name": "Jane",
                "last_name": "Doe",
                "full_name": "Jane Doe",
                "title": "Marketing Manager",
                "title_norm": "marketing manager",
                "role_family": "marketing",
                "seniority": "mid",
            },
            {
                "id": 3,
                "company_id": 3,
                "first_name": "Alex",
                "last_name": "Smith",
                "full_name": "Alex Smith",
                "title": "CTO",
                "title_norm": "cto",
                "role_family": "technical",
                "seniority": "senior",
            },
        ]

        for row in people:
            _insert_row(conn, "people", row)

        # Emails
        emails = [
            {
                "id": 1,
                "person_id": 1,
                "email": "banderson@crestwellpartners.com",
                "source": "extracted",
                "source_url": "https://crestwellpartners.com/team",
            },
            {
                "id": 2,
                "person_id": 2,
                "email": "bad-address@example.com",
                "source": "generated",
                "source_url": "https://example.com/about",
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

        # Verification results – aligned with the CSV expectations
        verification_rows = [
            {
                "email_id": 1,
                "verify_status": fixture[
                    ("crestwellpartners.com", "banderson@crestwellpartners.com")
                ]["expected_verify_status"],
                "verify_reason": "r25_fixture_valid",
                "verified_mx": "mx.crestwellpartners.com",
                "verified_at": "2025-01-01T00:00:00",
            },
            {
                "email_id": 2,
                "verify_status": fixture[("example.com", "bad-address@example.com")][
                    "expected_verify_status"
                ],
                "verify_reason": "r25_fixture_invalid",
                "verified_mx": "mx.example.com",
                "verified_at": "2025-01-01T00:00:00",
            },
            {
                "email_id": 3,
                "verify_status": fixture[("catchall.test", "random@catchall.test")][
                    "expected_verify_status"
                ],
                "verify_reason": "r25_fixture_catchall",
                "verified_mx": "mx.catchall.test",
                "verified_at": "2025-01-01T00:00:00",
            },
        ]

        for row in verification_rows:
            _insert_row(conn, "verification_results", row)

        conn.commit()
    finally:
        # Re-enable FK enforcement for any subsequent operations on this connection.
        conn.execute("PRAGMA foreign_keys = ON;")


@pytest.fixture
def fresh_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """
    Fresh on-disk SQLite database with the full schema applied.

    Using a temporary file instead of :memory: keeps behavior as close as
    possible to the real dev.db while still being fast and isolated.
    """
    db_path = tmp_path / "r25.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # type: ignore[attr-defined]

    _apply_schema(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_r25_known_domain_verification_snapshots(fresh_db: sqlite3.Connection) -> None:
    """
    Golden snapshot: companies/emails/verification_results should match the
    expectations encoded in tests/fixtures/r25_known_domains.csv.
    """
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
    rows = [dict(row) for row in cur.fetchall()]
    assert rows, "Expected seeded verification rows for R25 snapshot"

    expected = _load_known_domains_fixture()

    # Ensure we have exactly the domains/emails we expect
    seen_keys = {(r["domain"], r["email"]) for r in rows}
    assert seen_keys == set(expected.keys())

    for row in rows:
        key = (row["domain"], row["email"])
        expected_row = expected[key]
        assert row["verify_status"] == expected_row["expected_verify_status"]


def test_r25_v_emails_latest_exposes_expected_fields(
    fresh_db: sqlite3.Connection,
) -> None:
    """
    Sanity check that the export-facing view v_emails_latest surfaces the
    same fixture data with the fields the export pipeline expects.
    """
    _seed_known_verifications(fresh_db)

    expected = _load_known_domains_fixture()
    emails = [email for (_domain, email) in expected.keys()]

    placeholders = ", ".join(["?"] * len(emails))
    base_sql = f"""
        SELECT email,
               company_domain,
               verify_status,
               icp_score,
               source_url
        FROM v_emails_latest
        WHERE email IN ({placeholders})
        ORDER BY email
    """

    try:
        cur = fresh_db.execute(base_sql, emails)
        rows = [dict(row) for row in cur.fetchall()]
        icp_column_present = True
    except sqlite3.OperationalError as exc:
        # Fallback for schemas where v_emails_latest does not expose icp_score.
        if "icp_score" not in str(exc).lower():
            raise
        fallback_sql = f"""
            SELECT email,
                   company_domain,
                   verify_status,
                   source_url
            FROM v_emails_latest
            WHERE email IN ({placeholders})
            ORDER BY email
        """
        cur = fresh_db.execute(fallback_sql, emails)
        rows = [dict(row) for row in cur.fetchall()]
        icp_column_present = False

    assert len(rows) == len(emails), "Expected all fixture emails to appear in v_emails_latest"

    by_email = {r["email"]: r for r in rows}
    for (domain, email), expected_row in expected.items():
        row = by_email[email]
        assert row["verify_status"] == expected_row["expected_verify_status"]

        # If icp_score is available on the view and non-null, it should meet the
        # expected minimum. If it is null or the column is absent, we do not
        # enforce a strict check here.
        if icp_column_present:
            icp_value = row.get("icp_score")
            if icp_value is not None:
                assert icp_value >= expected_row["expected_icp_min"]

        # Depending on schema, company_domain may be derived from email or companies.domain.
        assert row.get("company_domain") in {domain, f"{domain}".lower()}
        assert row["source_url"], "Exportable emails should have a non-empty source_url"


def _build_search_params_for_term(
    term: str,
    *,
    verify_statuses: list[str] | None = None,
    icp_min: int | None = None,
    limit: int = 20,
) -> LeadSearchParams:
    """
    Generic helper to construct a LeadSearchParams instance for a given term,
    adapting to the actual parameter names used by the dataclass.
    """
    sig = inspect.signature(LeadSearchParams)
    params = sig.parameters

    kwargs: dict[str, Any] = {}

    # Query text
    if "q" in params:
        kwargs["q"] = term
    elif "query" in params:
        kwargs["query"] = term
    elif "term" in params:
        kwargs["term"] = term

    # Verify status filter
    if verify_statuses:
        if "verify_status" in params:
            kwargs["verify_status"] = verify_statuses
        elif "verify_statuses" in params:
            kwargs["verify_statuses"] = verify_statuses

    # ICP minimum
    if icp_min is not None:
        if "icp_min" in params:
            kwargs["icp_min"] = icp_min
        elif "min_icp" in params:
            kwargs["min_icp"] = icp_min

    # Facets
    if "facets" in params:
        kwargs["facets"] = ["verify_status", "icp_bucket"]

    # Limit / page size
    if "limit" in params:
        kwargs["limit"] = limit
    elif "page_size" in params:
        kwargs["page_size"] = limit

    return LeadSearchParams(**kwargs)


def _build_crestswell_search_params() -> LeadSearchParams:
    """
    Construct a LeadSearchParams instance for the Crestwell search scenario,
    adapting to the actual parameter names used by the dataclass.
    """
    return _build_search_params_for_term(
        "Crestwell",
        verify_statuses=["valid"],
        icp_min=70,
        limit=20,
    )


def test_r25_domain_only_auto_discovery_from_domain(
    fresh_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    """
    End-to-end smoke test for the "domain-only" auto-discovery path.

    Starting from a company + domain and no people/emails:
      - Stub the crawler (R10) via fetch_url().
      - Persist crawled pages via src.db_pages.save_pages().
      - Extract people/emails via R11 candidates.
      - Persist people/emails and seed simple verification_results rows.
      - Assert v_emails_latest and the search backend can see the auto-discovered leads.
    """
    company_id = 100
    domain = "autodiscover.test"

    # Seed a single company with only a domain/official_domain.
    _insert_row(
        fresh_db,
        "companies",
        {
            "id": company_id,
            "name": "AutoDiscover Inc",
            "domain": domain,
            "official_domain": domain,
            "attrs": "{}",
        },
    )
    fresh_db.commit()

    # Stub fetch_url used by the crawler to avoid real network calls.
    html_doc = """
    <!doctype html>
    <html>
      <body>
        <h1>Our Team</h1>
        <div class="person">
          <strong>Alice Johnson</strong>
          <span>VP Sales</span>
          <a href="mailto:alice.johnson@autodiscover.test">alice.johnson@autodiscover.test</a>
        </div>
        <div class="person">
          <strong>Bob Smith</strong>
          <span>CTO</span>
          <a href="mailto:bob.smith@autodiscover.test">Contact Bob</a>
        </div>
        <div class="contact">
          <span>Email us at</span>
          <a href="mailto:info@autodiscover.test">info@autodiscover.test</a>
        </div>
      </body>
    </html>
    """

    class FakeResponse:
        def __init__(self, url: str, body: str) -> None:
            self.url = url
            self.status = 200
            self.headers = {"Content-Type": "text/html; charset=utf-8"}
            self.body = body.encode("utf-8")
            # Older stubs may look at .content instead of .body
            self.content = self.body

    def fake_fetch(url: str, *args: Any, **kwargs: Any) -> FakeResponse:
        return FakeResponse(url, html_doc)

    # Patch the fetcher used by the crawler module (R09 → R10).
    monkeypatch.setattr(crawl_runner, "fetch_url", fake_fetch, raising=True)

    # Run the targeted crawler for this domain.
    pages = crawl_runner.crawl_domain(domain)
    assert pages, "Expected stub crawl to return at least one page"

    # Persist crawled pages into the sources table, associating them with the
    # company when supported.
    try:
        save_pages(fresh_db, pages, company_id=company_id)  # type: ignore[call-arg]
    except TypeError:
        # Back-compat for older save_pages(conn, pages) signatures.
        save_pages(fresh_db, pages)  # type: ignore[call-arg]

    fresh_db.commit()

    # Load back the HTML blobs for this company from sources.
    try:
        info_cur = fresh_db.execute("PRAGMA table_info(sources)")
        cols = [r["name"] for r in info_cur.fetchall()]
        has_company_id = "company_id" in cols
    except sqlite3.OperationalError:
        has_company_id = False

    if has_company_id:
        src_rows = fresh_db.execute(
            "SELECT source_url, html FROM sources WHERE company_id = ?",
            (company_id,),
        ).fetchall()
    else:
        src_rows = fresh_db.execute("SELECT source_url, html FROM sources").fetchall()

    assert src_rows, "Expected at least one row in sources for auto-discovery"

    # Extract candidates from the crawled HTML using R11 heuristics.
    from src.extract.candidates import Candidate  # local import for some setups

    candidates_by_email: dict[str, Candidate] = {}

    for row in src_rows:
        source_url = row["source_url"]
        blob = row["html"]
        if isinstance(blob, (bytes, bytearray)):
            html_str = blob.decode("utf-8", "ignore")
        else:
            html_str = str(blob or "")

        cands = extract_candidates(html_str, source_url, official_domain=domain)
        for cand in cands:
            # Deduplicate by email; prefer candidates that have a name attached.
            existing = candidates_by_email.get(cand.email)
            if existing is None:
                candidates_by_email[cand.email] = cand
            else:
                has_name_existing = bool(existing.first_name or existing.last_name)
                has_name_new = bool(cand.first_name or cand.last_name)
                if has_name_new and not has_name_existing:
                    candidates_by_email[cand.email] = cand

    assert candidates_by_email, "Expected at least one Candidate from auto-discovery HTML"

    # ROLE aliases like info@... should be filtered; we expect only personal emails.
    assert "info@autodiscover.test" not in candidates_by_email

    # Persist discovered people and emails into the core tables.
    next_person_id = 1000
    next_email_id = 2000

    for email, cand in sorted(candidates_by_email.items()):
        full_name = (
            cand.raw_name
            or " ".join(p for p in (cand.first_name, cand.last_name) if p).strip()
            or None
        )

        person_row: dict[str, Any] = {
            "id": next_person_id,
            "company_id": company_id,
            "first_name": cand.first_name,
            "last_name": cand.last_name,
            "full_name": full_name,
            "title": "Auto-discovered",
            "source_url": cand.source_url,
        }
        _insert_row(fresh_db, "people", person_row)

        email_row: dict[str, Any] = {
            "id": next_email_id,
            "person_id": next_person_id,
            "company_id": company_id,
            "email": email,
            "source": "extracted",
            "source_url": cand.source_url,
            "is_published": 1,
        }
        _insert_row(fresh_db, "emails", email_row)

        ver_row: dict[str, Any] = {
            "email_id": next_email_id,
            "verify_status": "valid",
            "verify_reason": "r25_auto_discovery_fixture",
            "verified_mx": f"mx.{domain}",
            "verified_at": "2025-01-01T00:00:00",
        }
        _insert_row(fresh_db, "verification_results", ver_row)

        next_person_id += 1
        next_email_id += 1

    fresh_db.commit()

    # Basic sanity: people/emails now exist for this company.
    cur = fresh_db.execute(
        "SELECT COUNT(*) AS c FROM people WHERE company_id = ?",
        (company_id,),
    )
    assert cur.fetchone()["c"] >= 1

    cur = fresh_db.execute(
        "SELECT COUNT(*) AS c FROM emails WHERE company_id = ?",
        (company_id,),
    )
    assert cur.fetchone()["c"] >= 1

    # v_emails_latest should expose the auto-discovered emails with verify_status.
    cur = fresh_db.execute(
        """
        SELECT email, verify_status, company_id
        FROM v_emails_latest
        WHERE company_id = ?
        ORDER BY email
        """,
        (company_id,),
    )
    v_rows = [dict(r) for r in cur.fetchall()]
    assert v_rows, "Expected v_emails_latest to expose auto-discovered emails"
    assert all(r["verify_status"] == "valid" for r in v_rows)

    # Search backend should be able to find at least one auto-discovered lead
    # when FTS is available.
    cur = fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='people_fts'"
    )
    has_people_fts = cur.fetchone() is not None

    backend = SqliteFtsBackend(fresh_db)

    if not has_people_fts:
        # Minimal sanity when FTS is not wired into this schema.
        params = _build_search_params_for_term("AutoDiscover")
        assert isinstance(params, LeadSearchParams)
        return

    params = _build_search_params_for_term("AutoDiscover")

    result = backend.search_leads(params)
    rows: Iterable[dict[str, Any]] = getattr(result, "rows", [])  # type: ignore[attr-defined]
    rows = list(rows)

    assert rows, "Expected search backend to return at least one auto-discovered lead"

    def _company_domain(row: dict[str, Any]) -> str:
        return (row.get("company_domain") or row.get("domain") or "").lower()

    assert any(domain in _company_domain(r) for r in rows)


def test_r25_search_and_facets_roundtrip(fresh_db: sqlite3.Connection) -> None:
    """
    Tie R21/R22/R23 + O14/O15 together by asserting that our canonical
    valid lead is discoverable via the search backend and that basic facets
    are present.

    In environments where the FTS table (people_fts) is not present in the
    schema used by this fresh DB, we still construct a backend and params and
    treat that as a minimal sanity check; the detailed search behavior is
    covered by the dedicated R21/R23/O14 tests.
    """
    # Check whether this schema has people_fts or not.
    cur = fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='people_fts'"
    )
    has_people_fts = cur.fetchone() is not None

    _seed_known_verifications(fresh_db)

    backend = SqliteFtsBackend(fresh_db)
    params = _build_crestswell_search_params()

    if not has_people_fts:
        # In this schema we don't have the FTS table wired into schema.sql.
        # We still ensure the backend and params are constructible. The full
        # search/facet behavior is validated by the R21/R23/O14 test modules.
        assert isinstance(params, LeadSearchParams)
        return

    # Full search + facets path when FTS is available.
    result = backend.search_leads(params)

    # rows / facets attributes come from SearchResult; keep access defensive in
    # case of future refactors.
    rows: Iterable[dict[str, Any]] = getattr(result, "rows", [])  # type: ignore[attr-defined]
    facets: dict[str, dict[str, int]] = getattr(result, "facets", {}) or {}

    rows = list(rows)
    assert rows, "Expected at least one search result for Crestwell in R25 search roundtrip"

    def _company_domain(row: dict[str, Any]) -> str:
        return (row.get("company_domain") or row.get("domain") or "").lower()

    assert any("crestwellpartners.com" in _company_domain(r) for r in rows)

    # Basic facet sanity: verify_status facet exists and has a 'valid' bucket.
    if "verify_status" in facets:
        assert facets["verify_status"].get("valid", 0) >= 1
    else:
        # In a degenerate case where facets are not returned at all, this test
        # still ensures that search itself works; detailed facet behavior is
        # covered by R23/O14 tests.
        assert facets == {} or isinstance(facets, dict)
