# scripts/migrate_o07_fallback.py
from __future__ import annotations

"""
O07 migration — add third-party fallback columns to verification_results.

New nullable columns on verification_results:

  fallback_status      TEXT   -- mapped FallbackStatus ("valid" | "invalid" | "catch_all" | "unknown")
  fallback_raw         TEXT   -- JSON blob / provider payload or short reason
  fallback_checked_at  TEXT   -- ISO8601 UTC timestamp when fallback was called

Usage:

    python scripts/migrate_o07_fallback.py --db data\dev.db
"""

import argparse
import contextlib
import pathlib
import sqlite3


def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def ensure_columns(cur: sqlite3.Cursor) -> None:
    """
    Idempotently add fallback_* columns to verification_results if missing.
    """
    table = "verification_results"

    if not col_exists(cur, table, "fallback_status"):
        print(f"· Adding {table}.fallback_status (TEXT)")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN fallback_status TEXT")
    else:
        print(f"· {table}.fallback_status already present; skipping")

    if not col_exists(cur, table, "fallback_raw"):
        print(f"· Adding {table}.fallback_raw (TEXT)")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN fallback_raw TEXT")
    else:
        print(f"· {table}.fallback_raw already present; skipping")

    if not col_exists(cur, table, "fallback_checked_at"):
        print(f"· Adding {table}.fallback_checked_at (TEXT)")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN fallback_checked_at TEXT")
    else:
        print(f"· {table}.fallback_checked_at already present; skipping")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="O07 migration: add fallback_* columns to verification_results"
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to SQLite DB (default: data/dev.db)",
    )
    args = parser.parse_args()

    db_path = pathlib.Path(args.db_path).resolve()
    print(f"→ Using SQLite at: {db_path}")

    if not db_path.exists():
        print(f"! WARNING: DB file does not exist yet at {db_path} (will be created on write)")

    con = sqlite3.connect(str(db_path))
    with contextlib.closing(con):
        cur = con.cursor()
        # Basic sanity: ensure verification_results exists
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='verification_results'"
        )
        row = cur.fetchone()
        if not row:
            print("! Table verification_results does not exist; nothing to migrate.")
            return

        ensure_columns(cur)
        con.commit()

    print("✔ O07 migration completed (verification_results fallback_* columns ensured).")


if __name__ == "__main__":
    main()
