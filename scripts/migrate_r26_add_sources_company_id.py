#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sqlite3
from urllib.parse import urlparse


def _infer_sqlite_path(db_path: str | None, db_url: str | None) -> str:
    """
    Resolve the SQLite file path from either --db-path or DB_URL.

    Supports DB_URL values like: sqlite:///C:/path/to/dev.db
    """
    if db_path:
        return db_path

    if not db_url:
        raise SystemExit(
            "No database provided. Either set DB_URL=sqlite:///path/to/dev.db or pass --db-path."
        )

    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise SystemExit(
            f"Unsupported DB_URL {db_url!r}. This migration only supports sqlite:/// URLs."
        )

    # On Windows, the path portion may include drive letters and slashes.
    return db_url[len(prefix) :]


def _ensure_sources_table(conn: sqlite3.Connection) -> None:
    """
    Fail fast with a clear message if the sources table is missing.
    """
    try:
        conn.execute("SELECT 1 FROM sources LIMIT 1")
    except sqlite3.OperationalError as exc:  # pragma: no cover - safety guard
        raise SystemExit(
            "Table 'sources' not found. Run the R10 migration (migrate_r10_add_sources.py) first."
        ) from exc


def _ensure_company_id_column(conn: sqlite3.Connection) -> None:
    """
    Add sources.company_id if it does not already exist.

    This function is idempotent: re-running the migration will skip the ALTER
    TABLE once the column is present.
    """
    cur = conn.execute("PRAGMA table_info(sources)")
    cols = [row[1] for row in cur.fetchall()]

    if "company_id" in cols:
        print("sources.company_id already exists; skipping ALTER TABLE.")
        return

    print("Adding sources.company_id column ...")
    conn.execute(
        """
        ALTER TABLE sources
        ADD COLUMN company_id INTEGER REFERENCES companies(id)
        """
    )
    print("sources.company_id added.")


def _load_company_domains(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Build a mapping from normalized domain -> company_id.

    Uses both companies.domain and companies.official_domain, and also maps
    'www.foo.com' -> 'foo.com' to handle common host patterns.
    """
    cur = conn.execute(
        """
        SELECT id,
               lower(domain) AS domain,
               lower(official_domain) AS official_domain
        FROM companies
        """
    )

    mapping: dict[str, int] = {}
    rows = cur.fetchall()
    if not rows:
        print("No companies found in companies table.")
        return mapping

    for company_id, domain, official_domain in rows:
        for value in (domain, official_domain):
            if not value:
                continue
            host = value.strip().lower()
            if not host:
                continue

            # Exact domain
            mapping.setdefault(host, company_id)

            # Common 'www.' variant (in both directions)
            if host.startswith("www."):
                bare = host[4:]
                if bare:
                    mapping.setdefault(bare, company_id)
            else:
                mapping.setdefault(f"www.{host}", company_id)

    print(f"Loaded {len(mapping)} domain → company_id mappings.")
    return mapping


def _extract_host_from_url(url: str | None) -> str | None:
    """
    Normalize a URL to its host, lowercased and with 'www.' stripped.

    Returns None if the URL is malformed or host cannot be determined.
    """
    if not url:
        return None

    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return None

    host = host.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _backfill_sources_company_id(conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Populate sources.company_id for rows where it is NULL by joining on domain.

    Strategy:
      - Load a domain → company_id mapping from companies.domain/official_domain.
      - For each source with company_id IS NULL, parse the host from source_url.
      - If the host (or its www./bare variant) is in the mapping, update.

    Returns (updated_count, skipped_count).
    """
    domain_map = _load_company_domains(conn)
    if not domain_map:
        print("No domain mappings available; skipping backfill.")
        return (0, 0)

    cur = conn.execute(
        """
        SELECT id, source_url
        FROM sources
        WHERE company_id IS NULL
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("No sources rows with NULL company_id; nothing to backfill.")
        return (0, 0)

    updated = 0
    skipped = 0

    print(f"Found {len(rows)} sources rows with NULL company_id to inspect.")

    for source_id, source_url in rows:
        host = _extract_host_from_url(source_url)
        if not host:
            skipped += 1
            continue

        # Try both bare host and 'www.' variant, since _load_company_domains
        # populated both.
        company_id = domain_map.get(host) or domain_map.get(f"www.{host}")
        if company_id is None:
            skipped += 1
            continue

        conn.execute(
            "UPDATE sources SET company_id = ? WHERE id = ?",
            (company_id, source_id),
        )
        updated += 1

    print(f"Backfill complete: {updated} sources rows updated, {skipped} rows skipped.")
    return (updated, skipped)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Migration: add sources.company_id and backfill it from companies "
            "based on source_url host."
        )
    )
    parser.add_argument(
        "--db-path",
        dest="db_path",
        help=(
            "Path to SQLite database file. If omitted, DB_URL is used and must "
            "be of the form sqlite:///path/to/dev.db."
        ),
    )
    args = parser.parse_args()

    db_url = os.environ.get("DB_URL")
    db_path = _infer_sqlite_path(args.db_path, db_url)

    print(f"→ Using SQLite at: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        _ensure_sources_table(conn)
        _ensure_company_id_column(conn)
        updated, skipped = _backfill_sources_company_id(conn)

        conn.commit()
        print(f"Migration completed successfully. Updated={updated}, skipped={skipped}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
