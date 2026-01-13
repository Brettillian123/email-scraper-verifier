# scripts/extract_candidates.py
from __future__ import annotations

r"""
R11 CLI: Extract person/email candidates from HTML saved by R10 and persist
them to the database with provenance.

Usage examples (PowerShell-friendly):
  python .\scripts\extract_candidates.py --domain crestwellpartners.com --db dev.db
  python .\scripts\extract_candidates.py --source-url https://www.example.com/team --db dev.db
  # Optional override if needed:
  python .\scripts\extract_candidates.py --source-url https://sub.example.com/contact --official-domain example.com

O26 adjustments:
  - We still extract role/placeholder emails (info@, support@, example@, etc.),
    but we do NOT attach them to specific people. They are stored as emails with
    person_id = NULL (or left unattached, depending on schema).

People-page enhancement:
  - On obvious "team" / "our-team" / "leadership" style URLs, we also scan
    headings (h1–h6) for person-like names even when there is no personal
    email on the page, and insert/update people rows for those names.
"""

import argparse
import datetime as _dt
import sqlite3
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

# Ensure "src/" is importable when running as a script
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import src.extract.candidates as _extract_mod  # noqa: E402
from src.emails.classify import is_role_or_placeholder_email  # noqa: E402
from src.extract import Candidate, extract_candidates  # noqa: E402
from src.extract.url_filters import is_people_page_url


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="R11: extract people/emails from saved sources")
    p.add_argument("--db", default="dev.db", help="Path to SQLite database (default: dev.db)")

    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--domain", help="Restrict to sources under this domain (e.g., example.com)"
    )
    target.add_argument("--source-url", help="Extract from a single source URL")

    p.add_argument(
        "--official-domain",
        help=(
            "Official domain used to filter emails "
            "(defaults to --domain or derived from --source-url)"
        ),
    )
    return p.parse_args(argv)


# ----------------------------- DB helpers ------------------------------------


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    # Reasonable safety defaults
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    cols: set[str] = set()
    for row in cur.fetchall():
        # Row may be sqlite3.Row (dict-like) or a plain tuple
        name = None
        try:
            name = row["name"]  # type: ignore[index]
        except Exception:
            # PRAGMA table_info columns: (cid, name, type, notnull, dflt_value, pk)
            if isinstance(row, (tuple, list)) and len(row) > 1:
                name = row[1]
        if name:
            cols.add(str(name).lower())
    return cols


def _utc_now_iso() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fetch_row_id(row: sqlite3.Row | tuple[object, ...] | None) -> int | None:
    if not row:
        return None
    try:
        return int(row["id"])  # type: ignore[index]
    except Exception:
        return int(row[0])  # type: ignore[index]


def _select_sources_by_domain(con: sqlite3.Connection, domain: str) -> list[sqlite3.Row]:
    """
    Select candidate source rows whose URLs appear to live under the given domain.
    We match common forms:
      http(s)://example.com/...
      http(s)://www.example.com/...
      http(s)://<sub>.example.com/...

    If the sources table has a company_id column, include it. Otherwise we
    project a NULL company_id so downstream code can always rely on the key.
    """
    domain = domain.strip().lstrip(".").lower()
    patterns = [
        f"http://{domain}/%",
        f"https://{domain}/%",
        f"http://www.{domain}/%",
        f"https://www.{domain}/%",
        f"http://%.{domain}/%",
        f"https://%.{domain}/%",
    ]
    # Deduplicate patterns (in case domain already includes 'www.')
    patterns = list(dict.fromkeys(patterns))

    source_cols = _table_columns(con, "sources")
    has_company_id = "company_id" in source_cols

    if has_company_id:
        select_cols = "id, company_id, source_url, html"
    else:
        # project a NULL company_id column so row['company_id'] works
        select_cols = "id, NULL AS company_id, source_url, html"

    # Build OR chain
    ors = " OR ".join(["source_url LIKE ?"] * len(patterns))
    sql = f"SELECT {select_cols} FROM sources WHERE {ors}"
    cur = con.execute(sql, patterns)
    return cur.fetchall()


def _select_source_by_url(con: sqlite3.Connection, url: str) -> list[sqlite3.Row]:
    """
    Select a single source row by its exact URL. As in _select_sources_by_domain,
    we include company_id when present on the sources table, and otherwise
    project a NULL company_id.
    """
    source_cols = _table_columns(con, "sources")
    has_company_id = "company_id" in source_cols

    if has_company_id:
        select_cols = "id, company_id, source_url, html"
    else:
        select_cols = "id, NULL AS company_id, source_url, html"

    cur = con.execute(
        f"SELECT {select_cols} FROM sources WHERE source_url = ? LIMIT 1",
        (url,),
    )
    return cur.fetchall()


def _normalized_domain_from_url(url: str) -> str:
    try:
        host = (urlparse(url).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _full_name_column(people_cols: set[str]) -> str | None:
    if "full_name" in people_cols:
        return "full_name"
    if "name" in people_cols:
        return "name"
    return None


def _lookup_person_id_first_last(
    con: sqlite3.Connection,
    *,
    first_name: str | None,
    last_name: str | None,
    company_id: int | None,
    has_company_id: bool,
) -> int | None:
    where = "first_name IS ? AND last_name IS ?"
    params: list[object] = [first_name, last_name]
    if has_company_id:
        where += " AND company_id IS ?"
        params.append(company_id)
    cur = con.execute(f"SELECT id FROM people WHERE {where}", params)
    return _fetch_row_id(cur.fetchone())


def _insert_person_first_last(
    con: sqlite3.Connection,
    people_cols: set[str],
    *,
    first_name: str | None,
    last_name: str | None,
    company_id: int | None,
    has_company_id: bool,
    now_iso: str,
) -> int:
    cols = ["first_name", "last_name"]
    vals: list[object] = [first_name, last_name]

    if has_company_id:
        cols.append("company_id")
        vals.append(company_id)

    if "created_at" in people_cols:
        cols.append("created_at")
        vals.append(now_iso)
    if "updated_at" in people_cols:
        cols.append("updated_at")
        vals.append(now_iso)

    sql = f"INSERT INTO people ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(vals))})"
    cur = con.execute(sql, vals)
    return int(cur.lastrowid)


def _find_or_create_by_first_last(
    con: sqlite3.Connection,
    people_cols: set[str],
    *,
    first_name: str | None,
    last_name: str | None,
    company_id: int | None,
    now_iso: str,
) -> int | None:
    has_company_id = "company_id" in people_cols

    person_id = _lookup_person_id_first_last(
        con,
        first_name=first_name,
        last_name=last_name,
        company_id=company_id,
        has_company_id=has_company_id,
    )
    if person_id is not None:
        return person_id

    # If company_id exists but caller doesn't have one, avoid violating NOT NULL.
    if has_company_id and company_id is None:
        return None

    return _insert_person_first_last(
        con,
        people_cols,
        first_name=first_name,
        last_name=last_name,
        company_id=company_id,
        has_company_id=has_company_id,
        now_iso=now_iso,
    )


def _lookup_person_id_full_name(
    con: sqlite3.Connection,
    *,
    full_name_col: str,
    full_name: str,
    company_id: int | None,
    has_company_id: bool,
) -> int | None:
    where = f"{full_name_col} = ?"
    params: list[object] = [full_name]
    if has_company_id:
        where += " AND company_id IS ?"
        params.append(company_id)
    cur = con.execute(f"SELECT id FROM people WHERE {where}", params)
    return _fetch_row_id(cur.fetchone())


def _insert_person_full_name(
    con: sqlite3.Connection,
    people_cols: set[str],
    *,
    full_name_col: str,
    full_name: str,
    company_id: int | None,
    has_company_id: bool,
    now_iso: str,
) -> int:
    cols = [full_name_col]
    vals: list[object] = [full_name]

    if has_company_id:
        cols.append("company_id")
        vals.append(company_id)

    if "created_at" in people_cols:
        cols.append("created_at")
        vals.append(now_iso)
    if "updated_at" in people_cols:
        cols.append("updated_at")
        vals.append(now_iso)

    sql = f"INSERT INTO people ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(vals))})"
    cur = con.execute(sql, vals)
    return int(cur.lastrowid)


def _find_or_create_by_full_name(
    con: sqlite3.Connection,
    people_cols: set[str],
    *,
    full_name_col: str,
    first_name: str | None,
    last_name: str | None,
    company_id: int | None,
    now_iso: str,
) -> int | None:
    has_company_id = "company_id" in people_cols
    full_name = " ".join([p for p in (first_name or "", last_name or "") if p]).strip()
    if not full_name:
        return None

    person_id = _lookup_person_id_full_name(
        con,
        full_name_col=full_name_col,
        full_name=full_name,
        company_id=company_id,
        has_company_id=has_company_id,
    )
    if person_id is not None:
        return person_id

    if has_company_id and company_id is None:
        return None

    return _insert_person_full_name(
        con,
        people_cols,
        full_name_col=full_name_col,
        full_name=full_name,
        company_id=company_id,
        has_company_id=has_company_id,
        now_iso=now_iso,
    )


def _find_or_create_person_id(
    con: sqlite3.Connection,
    first_name: str | None,
    last_name: str | None,
    *,
    company_id: int | None = None,
) -> int | None:
    """
    Conservative: only create a person when we have at least a first or last name.

    If the people table has a company_id column, we scope lookups and inserts
    by (company_id, first_name, last_name) so that the same name at different
    companies is treated as a distinct person.

    If company_id is required by the schema (NOT NULL) but we are invoked
    without one (company_id=None), we will not insert, and simply return None.
    """
    if not first_name and not last_name:
        return None

    people_cols = _table_columns(con, "people")
    now_iso = _utc_now_iso()

    if {"first_name", "last_name"}.issubset(people_cols):
        return _find_or_create_by_first_last(
            con,
            people_cols,
            first_name=first_name,
            last_name=last_name,
            company_id=company_id,
            now_iso=now_iso,
        )

    full_name_col = _full_name_column(people_cols)
    if full_name_col:
        return _find_or_create_by_full_name(
            con,
            people_cols,
            full_name_col=full_name_col,
            first_name=first_name,
            last_name=last_name,
            company_id=company_id,
            now_iso=now_iso,
        )

    # If schema is unknown, skip creating people; email will still be recorded.
    return None


def _upsert_email(
    con: sqlite3.Connection,
    email: str,
    source_url: str,
    company_id: int | None,
    person_id: int | None,
    extracted_at_iso: str,
) -> None:
    """
    Upsert into emails, preserving existing richer data:
      - Do not overwrite person_id or company_id if they are already set.
      - Only set source_url/extracted_at if they are NULL.

    Requires a UNIQUE constraint on emails.email.
    """
    email_cols = _table_columns(con, "emails")

    # Must have 'email' column to proceed
    if "email" not in email_cols:
        raise RuntimeError("emails table must have an 'email' column")

    insert_cols = ["email"]
    insert_vals: list[object] = [email]

    if "source_url" in email_cols:
        insert_cols.append("source_url")
        insert_vals.append(source_url)
    if "company_id" in email_cols:
        insert_cols.append("company_id")
        insert_vals.append(company_id)
    if "person_id" in email_cols:
        insert_cols.append("person_id")
        insert_vals.append(person_id)
    if "extracted_at" in email_cols:
        insert_cols.append("extracted_at")
        insert_vals.append(extracted_at_iso)

    placeholders = ", ".join(["?"] * len(insert_vals))
    insert_list = ", ".join(insert_cols)

    # Build ON CONFLICT update set preserving existing values (COALESCE)
    updates: list[str] = []
    if "person_id" in email_cols:
        updates.append("person_id = COALESCE(emails.person_id, excluded.person_id)")
    if "company_id" in email_cols:
        updates.append("company_id = COALESCE(emails.company_id, excluded.company_id)")
    if "source_url" in email_cols:
        updates.append("source_url = COALESCE(emails.source_url, excluded.source_url)")
    if "extracted_at" in email_cols:
        updates.append("extracted_at = COALESCE(emails.extracted_at, excluded.extracted_at)")

    if updates:
        sql = (
            f"INSERT INTO emails ({insert_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(email) DO UPDATE SET {', '.join(updates)}"
        )
    else:
        # Fallback if there is no column to update beyond email
        sql = f"INSERT OR IGNORE INTO emails ({insert_list}) VALUES ({placeholders})"

    con.execute(sql, insert_vals)


def _get_email_id(con: sqlite3.Connection, email: str) -> int | None:
    cur = con.execute("SELECT id FROM emails WHERE email = ?", (email,))
    return _fetch_row_id(cur.fetchone())


def _record_provenance(con: sqlite3.Connection, email_id: int, source_url: str) -> bool:
    """
    Record a provenance entry if the email_provenance table exists with the expected columns.
    Returns True if an insert (or conflict-no-op) statement executed successfully, False otherwise.
    """
    prov_cols = _table_columns(con, "email_provenance")
    if not {"email_id", "source_url"}.issubset(prov_cols):
        return False

    con.execute(
        """
        INSERT INTO email_provenance(email_id, source_url)
        VALUES (?, ?)
        ON CONFLICT(email_id, source_url) DO NOTHING
        """,
        (email_id, source_url),
    )
    return True


# ----------------------- People from headings (no email) ---------------------


def _iter_name_only_people_from_html(
    source_url: str,
    html: str,
) -> list[tuple[str, str, str]]:
    """
    Extract plausible people names from headings on team/about pages.

    Returns a list of (first_name, last_name, raw_label) tuples.
    """
    if not is_people_page_url(source_url):
        return []

    soup = BeautifulSoup(html or "", "html.parser")
    seen: set[tuple[str, str]] = set()
    results: list[tuple[str, str, str]] = []

    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        text = heading.get_text(" ", strip=True)
        if not text:
            continue

        # Reuse the same conservative name heuristics as the HTML extractor.
        piece = _extract_mod._choose_name_piece(text)
        if not _extract_mod._looks_human_name(piece):
            continue

        first, last = _extract_mod._split_first_last(piece)
        if not first or not last:
            continue

        key = (first, last)
        if key in seen:
            continue
        seen.add(key)
        results.append((first, last, piece))

    return results


def _persist_name_only_people(
    con: sqlite3.Connection,
    rows: Iterable[tuple[int | None, str, str, str, str]],
) -> int:
    """
    Persist name-only people (no email yet).

    Each row is: (company_id, first_name, last_name, raw_label, source_url).

    Returns the count of people rows found or created (logical count; we don't
    try to deduplicate across multiple source URLs for the same person beyond
    the _find_or_create_person_id lookup).
    """
    n_people = 0

    with con:
        for company_id, first_name, last_name, _raw_label, _source_url in rows:
            pid = _find_or_create_person_id(
                con,
                first_name=first_name,
                last_name=last_name,
                company_id=company_id,
            )
            if pid:
                n_people += 1

    return n_people


def _persist_candidates(
    con: sqlite3.Connection,
    items: Iterable[tuple[Candidate, int | None]],
) -> tuple[int, int, int]:
    """
    Persist email-based candidates to DB.

    `items` is an iterable of (Candidate, company_id) tuples.

    Returns (n_people_inserted_or_found, n_emails_upserted, n_provenance_written).

    O26 behavior:
      - Role/placeholder emails (info@, support@, example@, noreply@, etc.)
        are never attached to a specific person via this script, even if a
        name was inferred from context. They remain person_id = NULL (or
        whatever default your schema implies).
    """
    n_people = 0
    n_emails = 0
    n_prov = 0
    now_iso = _utc_now_iso()

    with con:  # transaction
        for cand, company_id in items:
            email = cand.email

            # Role/placeholder emails are stored, but never auto-attached
            # to a specific person via this script.
            if is_role_or_placeholder_email(email):
                person_id: int | None = None
            else:
                person_id = None
                if getattr(cand, "first_name", None) or getattr(cand, "last_name", None):
                    person_id = _find_or_create_person_id(
                        con,
                        first_name=cand.first_name,
                        last_name=cand.last_name,
                        company_id=company_id,
                    )
                    if person_id:
                        n_people += 1  # counts "found or created" logically

            _upsert_email(
                con,
                email=email,
                source_url=cand.source_url,
                company_id=company_id,
                person_id=person_id,
                extracted_at_iso=now_iso,
            )
            n_emails += 1

            # provenance (email_id + source_url)
            eid = _get_email_id(con, email)
            if eid is not None and _record_provenance(con, eid, cand.source_url):
                n_prov += 1

    return n_people, n_emails, n_prov


# ------------------------------ Main flow ------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    db_path = args.db

    if not Path(db_path).exists():
        print(
            f"[warn] Database not found at {db_path!r}. Proceeding to create/connect anyway.",
            file=sys.stderr,
        )

    con = _connect(db_path)

    if args.domain:
        official_domain = (args.official_domain or args.domain).lower()
        src_rows = _select_sources_by_domain(con, args.domain)
        target_desc = f"domain={args.domain}"
    else:
        # args.source_url path
        official_domain = (
            args.official_domain or _normalized_domain_from_url(args.source_url)
        ).lower()
        src_rows = _select_source_by_url(con, args.source_url)
        target_desc = f"source_url={args.source_url}"

    if not src_rows:
        print(
            f"[info] No matching sources found for {target_desc}. Nothing to do.",
            file=sys.stderr,
        )
        return 0

    total_pages = len(src_rows)

    email_items: list[tuple[Candidate, int | None]] = []
    name_only_rows: list[tuple[int | None, str, str, str, str]] = []

    for row in src_rows:
        source_url = row["source_url"]
        html = row["html"] or ""
        company_id = row["company_id"]

        cands = extract_candidates(
            html=html,
            source_url=source_url,
            official_domain=official_domain,
        )
        for cand in cands:
            email_items.append((cand, company_id))

        # Secondary pass: infer people from headings on team/leadership pages.
        for first_name, last_name, raw_label in _iter_name_only_people_from_html(source_url, html):
            name_only_rows.append((company_id, first_name, last_name, raw_label, source_url))

    if not email_items and not name_only_rows:
        print(f"[info] No candidates extracted from {total_pages} page(s) under {target_desc}.")
        return 0

    n_people_emails, n_emails, n_prov = _persist_candidates(con, email_items)
    n_people_headings = _persist_name_only_people(con, name_only_rows)
    n_people_total = n_people_emails + n_people_headings

    # Small summary
    unique_emails = len({c.email for (c, _cid) in email_items})
    print(
        f"[ok] Processed {total_pages} page(s) for {target_desc}. "
        f"Extracted {len(email_items)} email candidates ({unique_emails} unique emails) "
        f"and {len(name_only_rows)} heading-based person(s). "
        f"Upserted {n_emails} email rows; matched/created ~{n_people_total} people; "
        f"recorded {n_prov} provenance entries."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
