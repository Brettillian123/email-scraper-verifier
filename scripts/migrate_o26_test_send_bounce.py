# scripts/migrate_o26_test_send_bounce.py
from __future__ import annotations

"""
O26 — Add test-send / bounce fields to verification_results and
      domain-level delivery flags to domain_resolutions.

This migration adds the columns needed for bounce-based verification:

  On verification_results:
    verification_results.test_send_status   TEXT NOT NULL DEFAULT 'not_requested'
    verification_results.test_send_token    TEXT
    verification_results.test_send_at       TEXT  (ISO8601)
    verification_results.bounce_code        TEXT
    verification_results.bounce_reason      TEXT

  On domain_resolutions (O26 domain-level delivery flags):
    domain_resolutions.delivery_catchall_status       TEXT
    domain_resolutions.delivery_catchall_checked_at   TEXT
    domain_resolutions.domain                         TEXT

and an index on verification_results.test_send_token to support fast
lookup when processing bounce messages.

Usage (example):

    python scripts/migrate_o26_test_send_bounce.py data/dev.db

The migration is idempotent:
  - It checks for existing columns before ALTER TABLE.
  - It uses CREATE INDEX IF NOT EXISTS for the new index.
  - It is safe to run multiple times.
"""

import argparse
import sqlite3
from collections.abc import Iterable
from pathlib import Path


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {db_path}")


def _get_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _execute_many(conn: sqlite3.Connection, statements: Iterable[str]) -> None:
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()


# ---------- O26 schema helpers ----------


def _migrate_verification_results(conn: sqlite3.Connection) -> None:
    """
    Ensure O26 test-send / bounce columns on verification_results.
    """
    table = "verification_results"
    columns = _get_columns(conn, table)

    statements: list[str] = []

    if "test_send_status" not in columns:
        # Default to "not_requested" so existing rows have a sensible state.
        statements.append(
            f"ALTER TABLE {table} ADD COLUMN test_send_status TEXT NOT NULL DEFAULT 'not_requested'"
        )

    if "test_send_token" not in columns:
        statements.append(f"ALTER TABLE {table} ADD COLUMN test_send_token TEXT")

    if "test_send_at" not in columns:
        # Store timestamps as ISO8601 text, consistent with other *_at fields.
        statements.append(f"ALTER TABLE {table} ADD COLUMN test_send_at TEXT")

    if "bounce_code" not in columns:
        statements.append(f"ALTER TABLE {table} ADD COLUMN bounce_code TEXT")

    if "bounce_reason" not in columns:
        statements.append(f"ALTER TABLE {table} ADD COLUMN bounce_reason TEXT")

    if statements:
        print(f"Applying {len(statements)} schema changes to {table}...")
        _execute_many(conn, statements)
    else:
        print(f"No schema changes needed for {table} (columns already present).")

    # Index for fast bounce lookups by token.
    # This is safe and idempotent due to IF NOT EXISTS.
    print("Ensuring index idx_verif_test_send_token exists...")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_verif_test_send_token "
        "ON verification_results(test_send_token)"
    )
    conn.commit()


def _migrate_domain_resolutions(conn: sqlite3.Connection) -> None:
    """
    Ensure O26 domain-level delivery columns on domain_resolutions.

    These are used by:
      - backfill_o26_delivery_catchall.py
      - backfill_o26_upgrade_risky_to_valid.py
    """
    table = "domain_resolutions"
    columns = _get_columns(conn, table)

    statements: list[str] = []

    if "delivery_catchall_status" not in columns:
        statements.append(f"ALTER TABLE {table} ADD COLUMN delivery_catchall_status TEXT")

    if "delivery_catchall_checked_at" not in columns:
        statements.append(f"ALTER TABLE {table} ADD COLUMN delivery_catchall_checked_at TEXT")

    if "domain" not in columns:
        statements.append(f"ALTER TABLE {table} ADD COLUMN domain TEXT")

    if statements:
        print(f"Applying {len(statements)} schema changes to {table}...")
        _execute_many(conn, statements)
    else:
        print(f"No schema changes needed for {table} (columns already present).")

    # Backfill domain from chosen_domain where missing/empty.
    conn.execute(
        """
        UPDATE domain_resolutions
        SET domain = chosen_domain
        WHERE (domain IS NULL OR domain = '')
          AND chosen_domain IS NOT NULL
        """
    )
    conn.commit()


def migrate(conn: sqlite3.Connection) -> None:
    """
    Apply the O26 migration to the given SQLite connection.

    Idempotent and safe to run multiple times.
    """
    _migrate_domain_resolutions(conn)
    _migrate_verification_results(conn)
    print("O26 migration complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="O26: Ensure test-send/bounce fields and domain delivery flags."
    )
    parser.add_argument(
        "db_path",
        help="Path to the SQLite database (e.g. data/dev.db)",
    )
    args = parser.parse_args()

    _ensure_db_exists(args.db_path)

    conn = sqlite3.connect(args.db_path)
    try:
        migrate(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
