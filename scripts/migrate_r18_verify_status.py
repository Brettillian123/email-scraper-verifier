from __future__ import annotations

"""
R18 migration — add verify_status / reason / MX / timestamp columns.

Usage (PowerShell):

  $PyExe = ".\.venv\Scripts\python.exe"; if (!(Test-Path $PyExe)) { $PyExe = "python" }
  & $PyExe scripts\migrate_r18_verify_status.py --db data\dev.db

Behavior:
  - Connects to the SQLite database specified by --db.
  - Ensures verification_results has the following columns:
        verify_status  TEXT
        verify_reason  TEXT
        verified_mx    TEXT
        verified_at    TEXT
  - Idempotent: skips columns that already exist.
"""

import argparse
import sqlite3
from pathlib import Path


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in cur.fetchall()}


def ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    ddl_fragment: str,
) -> None:
    """
    Ensure `table.column` exists; if not, ALTER TABLE to add it.

    ddl_fragment should be the 'col_name TYPE ...' part, e.g.
      'verify_status TEXT'
    """
    existing = _table_columns(conn, table)
    if column in existing:
        print(f"· Skipping {table}.{column} (already exists)")
        return

    sql = f"ALTER TABLE {table} ADD COLUMN {ddl_fragment};"
    conn.execute(sql)
    conn.commit()
    print(f"· Added {table}.{column}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="R18: ensure verification_results has verify_status/verify_reason/verified_mx/verified_at columns.",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to the SQLite database file (default: data/dev.db)",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    print(f"→ Using SQLite at: {db_path.resolve()}")

    conn = get_connection(str(db_path))

    try:
        table = "verification_results"
        print("Ensuring R18 verify-status columns on verification_results...")

        ensure_column(conn, table, "verify_status", "verify_status TEXT")
        ensure_column(conn, table, "verify_reason", "verify_reason TEXT")
        ensure_column(conn, table, "verified_mx", "verified_mx TEXT")
        ensure_column(conn, table, "verified_at", "verified_at TEXT")

        print("✔ R18 migration complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
