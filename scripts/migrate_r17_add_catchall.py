# scripts/migrate_r17_add_catchall.py
from __future__ import annotations

import argparse
import pathlib
import sqlite3


def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    """
    Return True if the given column exists on the table.
    """
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col for row in cur.fetchall())


def ensure_catchall_columns(cur: sqlite3.Cursor) -> None:
    """
    Ensure R17 catch-all columns exist on domain_resolutions.
    Idempotent: only ALTER TABLE when a column is missing.
    """

    def add_col(name: str, ddl: str) -> None:
        if not col_exists(cur, "domain_resolutions", name):
            print(f"· Adding domain_resolutions.{name} ...")
            cur.execute(f"ALTER TABLE domain_resolutions ADD COLUMN {ddl}")
        else:
            print(f"· Skipping domain_resolutions.{name} (already exists)")

    print("Ensuring R17 catch-all columns on domain_resolutions...")

    add_col("catch_all_status", "catch_all_status TEXT")
    add_col("catch_all_checked_at", "catch_all_checked_at TEXT")
    add_col("catch_all_localpart", "catch_all_localpart TEXT")
    add_col("catch_all_smtp_code", "catch_all_smtp_code INTEGER")
    add_col("catch_all_smtp_msg", "catch_all_smtp_msg TEXT")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="R17 migration: add catch-all columns to domain_resolutions."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite database file (e.g. data/dev.db)",
    )
    args = parser.parse_args()

    db_path = pathlib.Path(args.db)
    print(f"→ Using SQLite at: {db_path.resolve()}")

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        ensure_catchall_columns(cur)
        conn.commit()
    finally:
        conn.close()

    print("✔ R17 migration completed (catch-all columns ensured on domain_resolutions).")


if __name__ == "__main__":
    main()
