# scripts/migrate_r13_add_normalization.py
"""
R13 migration: add raw/normalized title fields to people and normalized
name/key fields to companies, with a safety backfill to preserve provenance.

- Adds:
    people.title_raw  (TEXT, NULLABLE)
    people.title_norm (TEXT, NULLABLE)

    companies.name_norm (TEXT, NULLABLE)
    companies.norm_key  (TEXT, NULLABLE, indexed)

- Backfills:
    people.title_raw <- people.title   (ONLY where title_raw IS NULL)

Usage (SQLite):
    python scripts/migrate_r13_add_normalization.py --db dev.db

Notes:
- Designed for SQLite (local/dev). If you’re using a different driver,
  adapt as needed or pass a compatible DATABASE_URL in your own wrapper.
- The script is idempotent: it checks for columns/index before adding.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    for _, name, *_ in cur.fetchall():
        if name == column:
            return True
    return False


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        (index_name,),
    )
    return cur.fetchone() is not None


def add_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> bool:
    if not table_exists(conn, table):
        raise RuntimeError(f'Table "{table}" does not exist.')
    if column_exists(conn, table, column):
        return False
    conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {ddl_type}')
    return True


def run_migration(conn: sqlite3.Connection) -> list[str]:
    actions: list[str] = []

    # --- people: title_raw / title_norm ---
    if add_column(conn, "people", "title_raw", "TEXT"):
        actions.append("Added column people.title_raw (TEXT)")
    else:
        actions.append("people.title_raw already present; skipping")

    if add_column(conn, "people", "title_norm", "TEXT"):
        actions.append("Added column people.title_norm (TEXT)")
    else:
        actions.append("people.title_norm already present; skipping")

    # Backfill title_raw from title where missing (provenance safety)
    # Only execute when people table has both columns present.
    if column_exists(conn, "people", "title_raw") and column_exists(conn, "people", "title"):
        cur = conn.execute(
            "UPDATE people SET title_raw = title WHERE title_raw IS NULL AND title IS NOT NULL"
        )
        actions.append(f"Backfilled people.title_raw from people.title ({cur.rowcount} rows)")
    else:
        actions.append("Skipped backfill: people.title or people.title_raw missing")

    # --- companies: name_norm / norm_key (+ index) ---
    if add_column(conn, "companies", "name_norm", "TEXT"):
        actions.append("Added column companies.name_norm (TEXT)")
    else:
        actions.append("companies.name_norm already present; skipping")

    if add_column(conn, "companies", "norm_key", "TEXT"):
        actions.append("Added column companies.norm_key (TEXT)")
    else:
        actions.append("companies.norm_key already present; skipping")

    # Index on companies.norm_key for clustering/lookup (no auto-merge logic)
    idx_name = "idx_companies_norm_key"
    if not index_exists(conn, idx_name):
        # Use IF NOT EXISTS for safety on newer SQLite, but we already checked.
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON companies(norm_key)")
        actions.append(f"Created index {idx_name} on companies(norm_key)")
    else:
        actions.append(f"Index {idx_name} already present; skipping")

    return actions


def main() -> None:
    p = argparse.ArgumentParser(description="R13 DB migration (normalization fields)")
    p.add_argument(
        "--db",
        default="dev.db",
        help="Path to SQLite database file (default: dev.db)",
    )
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(
            f'Error: database file "{db_path}" not found. Pass --db <path> to your SQLite database.'
        )

    with sqlite3.connect(str(db_path)) as conn:
        conn.isolation_level = None  # explicit transaction control
        try:
            conn.execute("BEGIN")
            actions = run_migration(conn)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    print("✔ R13 migration completed.")
    for a in actions:
        print(" -", a)


if __name__ == "__main__":
    main()
