# scripts/crawl_domain.py
from __future__ import annotations

import argparse
import sqlite3

from src.crawl.runner import crawl_domain
from src.db_pages import save_pages


def _get_conn(db_path: str) -> sqlite3.Connection:
    """
    Prefer a project-provided get_conn() (if available), otherwise fall back to sqlite3.connect().
    This keeps the script compatible whether or not you already have src/db.py.
    """
    try:
        # Local import to avoid hard dependency when src.db doesn't exist.
        from src.db import get_conn  # type: ignore
    except Exception:
        get_conn = None  # type: ignore

    if callable(get_conn):  # type: ignore[truthy-bool]
        return get_conn(db_path)  # type: ignore[misc]
    return sqlite3.connect(db_path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Crawl a single domain and persist fetched pages.")
    ap.add_argument("domain", help="Domain to crawl (e.g., example.com)")
    ap.add_argument("--db", default="dev.db", help="SQLite database path (default: dev.db)")
    args = ap.parse_args()

    pages = crawl_domain(args.domain)
    conn = _get_conn(args.db)
    try:
        save_pages(conn, pages)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(f"Saved {len(pages)} pages to {args.db}")


if __name__ == "__main__":
    main()
