from __future__ import annotations

"""
O26 — Domain-level delivery catch-all status.

This migration adds two columns to domain_resolutions so that we can persist
evidence from real test-sends (O26 test_send helpers):

  - delivery_catchall_status: TEXT
        'unknown' | 'not_catchall_proven' | 'catchall_consistent' (optional)
  - delivery_catchall_checked_at: TEXT (ISO-8601 UTC timestamp)

These fields are intentionally separate from the RCPT-level catch_all_status
added in R17. They capture *delivery-time* behavior:

  - If at least one real address on the domain has a test-send that did NOT
    hard-bounce, AND at least one obviously fake address on the same domain
    DID hard-bounce with a 5.1.x user-unknown style code, we can safely set:

        delivery_catchall_status = 'not_catchall_proven'

Downstream helpers (O26 policy module / R18+ reclassification) will use these
columns to upgrade risky_catch_all addresses with successful test-sends to
valid/no_bounce_after_test_send, while keeping everything idempotent.
"""

import argparse
import sqlite3
from pathlib import Path


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {db_path}")


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """
    Return True if the given table has a column named `column`.

    Uses PRAGMA table_info and is safe here because `table` is a constant
    string defined in code (not user input).
    """
    cur = conn.execute(f"PRAGMA table_info({table})")
    for row in cur.fetchall():
        # PRAGMA table_info columns:
        # (cid, name, type, notnull, dflt_value, pk)
        if row[1] == column:
            return True
    return False


def migrate(conn: sqlite3.Connection) -> None:
    """
    Apply the O26 delivery catch-all migration in an idempotent way.

    - If delivery_catchall_status does not exist, add it as TEXT.
    - If delivery_catchall_checked_at does not exist, add it as TEXT.

    We do not backfill any values here; the O26 helper code will populate
    these fields based on observed test-send outcomes per domain.
    """
    # Ensure the base table exists; if not, this will raise a clear error.
    conn.execute("SELECT 1 FROM domain_resolutions LIMIT 1")

    if not _has_column(conn, "domain_resolutions", "delivery_catchall_status"):
        conn.execute("ALTER TABLE domain_resolutions ADD COLUMN delivery_catchall_status TEXT")

    if not _has_column(conn, "domain_resolutions", "delivery_catchall_checked_at"):
        conn.execute("ALTER TABLE domain_resolutions ADD COLUMN delivery_catchall_checked_at TEXT")

    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 migration: add delivery_catchall_status and "
            "delivery_catchall_checked_at to domain_resolutions."
        )
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to the SQLite database (default: data/dev.db).",
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
