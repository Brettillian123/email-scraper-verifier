# scripts/migrate_o03_company_attrs.py
"""
O03 migration: add lightweight enrichment field to companies.

Adds (if absent):
  - companies.attrs  (TEXT, NULLABLE) — JSON blob of heuristic enrichment
    e.g., {"size_bucket":"51-200","industry":["B2B SaaS"],"tech":["Salesforce","AWS"]}

Notes
-----
- Stored as TEXT for broad SQLite compatibility (json1 not required).
- Writers should json.dumps(...) with ensure_ascii=False.
- Readers should treat missing/empty as {}.
- Idempotent: checks for column before adding.

Usage:
  python scripts/migrate_o03_company_attrs.py --db dev.db
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    for _, name, *_ in cur.fetchall():
        if name == column:
            return True
    return False


def add_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> bool:
    if not table_exists(conn, table):
        raise RuntimeError(f'Table "{table}" does not exist.')
    if column_exists(conn, table, column):
        return False
    conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {ddl_type}')
    return True


def run(conn: sqlite3.Connection) -> list[str]:
    actions: list[str] = []

    # -- companies.attrs (TEXT for JSON) --
    if add_column(conn, "companies", "attrs", "TEXT"):
        actions.append("Added column companies.attrs (TEXT for JSON)")
    else:
        actions.append("companies.attrs already present; skipping")

    return actions


def main() -> None:
    p = argparse.ArgumentParser(description="O03 DB migration (companies.attrs JSON field)")
    p.add_argument("--db", default="dev.db", help="Path to SQLite database file (default: dev.db)")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f'Error: database file "{db_path}" not found.')

    with sqlite3.connect(str(db_path)) as conn:
        conn.isolation_level = None
        try:
            conn.execute("BEGIN")
            actions = run(conn)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    print("✔ O03 migration completed.")
    for a in actions:
        print(" -", a)


if __name__ == "__main__":
    main()
