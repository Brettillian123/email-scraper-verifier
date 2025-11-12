# scripts/migrate_o02_title_fields.py
"""
O02 migration: add canonicalized title fields to people.

Adds (if absent):
  - people.role_family  (TEXT, NULLABLE)
  - people.seniority    (TEXT, NULLABLE)

Notes
-----
- These are populated by src/ingest/title_norm.py (O02) after R13 title normalization.
- We keep them nullable to avoid destructive updates during phased rollout.
- Designed for SQLite dev DBs; adapt as needed for other engines.

Usage:
  python scripts/migrate_o02_title_fields.py --db dev.db
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


def index_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        (name,),
    )
    return cur.fetchone() is not None


def add_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> bool:
    if not table_exists(conn, table):
        raise RuntimeError(f'Table "{table}" does not exist.')
    if column_exists(conn, table, column):
        return False
    conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {ddl_type}')
    return True


def run(conn: sqlite3.Connection) -> list[str]:
    actions: list[str] = []

    # -- people.role_family --
    if add_column(conn, "people", "role_family", "TEXT"):
        actions.append("Added column people.role_family (TEXT)")
    else:
        actions.append("people.role_family already present; skipping")

    # -- people.seniority --
    if add_column(conn, "people", "seniority", "TEXT"):
        actions.append("Added column people.seniority (TEXT)")
    else:
        actions.append("people.seniority already present; skipping")

    # Light indexes (optional; safe to create if not exists)
    if not index_exists(conn, "idx_people_role_family"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_people_role_family ON people(role_family)")
        actions.append("Created index idx_people_role_family on people(role_family)")
    else:
        actions.append("Index idx_people_role_family already present; skipping")

    if not index_exists(conn, "idx_people_seniority"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_people_seniority ON people(seniority)")
        actions.append("Created index idx_people_seniority on people(seniority)")
    else:
        actions.append("Index idx_people_seniority already present; skipping")

    return actions


def main() -> None:
    p = argparse.ArgumentParser(description="O02 DB migration (role_family/seniority fields)")
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

    print("✔ O02 migration completed.")
    for a in actions:
        print(" -", a)


if __name__ == "__main__":
    main()
