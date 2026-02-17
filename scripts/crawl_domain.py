# scripts/crawl_domain.py
from __future__ import annotations

import argparse
import logging
import os
import sqlite3

from src.crawl.runner import crawl_domain
from src.db_pages import save_pages

log = logging.getLogger(__name__)


def _is_postgres_configured() -> bool:
    """Check if DATABASE_URL points to PostgreSQL."""
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


def _get_conn(db_path: str):
    """
    Get a database connection, supporting both PostgreSQL and SQLite.

    - If DATABASE_URL points to PostgreSQL, uses src.db.get_conn() (ignores db_path).
    - Otherwise, falls back to sqlite3.connect(db_path) for legacy dev mode.

    Note: src.db.get_conn() does NOT accept a db_path argument — it reads
    DATABASE_URL from the environment.
    """
    if _is_postgres_configured():
        try:
            from src.db import get_conn  # type: ignore

            log.info("Using PostgreSQL connection via get_conn()")
            return get_conn()
        except ImportError:
            log.warning(
                "DATABASE_URL points to PostgreSQL but src.db is not importable; "
                "falling back to SQLite at %s",
                db_path,
            )

    log.info("Using SQLite connection: %s", db_path)
    return sqlite3.connect(db_path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Crawl a single domain and persist fetched pages.")
    ap.add_argument("domain", help="Domain to crawl (e.g., example.com)")
    ap.add_argument(
        "--db",
        default="dev.db",
        help="SQLite database path (default: dev.db). "
        "Ignored when DATABASE_URL points to PostgreSQL.",
    )
    args = ap.parse_args()

    pages = crawl_domain(args.domain)
    conn = _get_conn(args.db)
    try:
        save_pages(conn, pages)
    finally:
        try:
            conn.close()
        except Exception:
            log.debug("Error closing DB connection", exc_info=True)

    db_label = "PostgreSQL" if _is_postgres_configured() else args.db
    print(f"Saved {len(pages)} pages to {db_label}")


if __name__ == "__main__":
    main()
