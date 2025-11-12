# scripts/migrate_r10_add_sources.py
from __future__ import annotations

import argparse
import sqlite3

DDL = """
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT UNIQUE NOT NULL,
    html BLOB NOT NULL,
    fetched_at INTEGER NOT NULL
);
"""


def ensure_sources_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(DDL)
    # Optional: lightweight index to speed up lookups by URL (unique already).
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_url ON sources(source_url)")
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description="R10 migration: create 'sources' table if missing.")
    ap.add_argument(
        "--db",
        default="dev.db",
        help="SQLite database path (default: dev.db)",
    )
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        ensure_sources_table(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(f"Migration complete. Ensured 'sources' table exists in {args.db}.")


if __name__ == "__main__":
    main()
