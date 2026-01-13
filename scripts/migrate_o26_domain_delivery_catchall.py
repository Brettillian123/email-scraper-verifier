# scripts/migrate_o26_domain_delivery_catchall.py
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
import os
import sys
from typing import Any

from src.db import get_conn


def _apply_dsn_override(dsn: str | None) -> None:
    """
    Allow overriding the Postgres DSN for this run while still routing all DB
    access through src.db.get_conn().

    We set both DATABASE_URL and PG_DSN to maximize compatibility with different
    existing get_conn() implementations.
    """
    if not dsn:
        return
    os.environ["DATABASE_URL"] = dsn
    os.environ["PG_DSN"] = dsn


def _table_exists(cur: Any, *, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def _has_column(cur: Any, *, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
           AND column_name = %s
         LIMIT 1
        """,
        (schema, table, column),
    )
    return cur.fetchone() is not None


def migrate(cur: Any, *, schema: str) -> None:
    """
    Apply the O26 delivery catch-all migration in an idempotent way.

    - If delivery_catchall_status does not exist, add it as TEXT.
    - If delivery_catchall_checked_at does not exist, add it as TEXT.

    We do not backfill any values here; the O26 helper code will populate
    these fields based on observed test-send outcomes per domain.
    """
    table = "domain_resolutions"

    if not _table_exists(cur, schema=schema, table=table):
        raise RuntimeError(f'Table "{schema}.{table}" does not exist.')

    fq = f'{schema}."{table}"'

    if not _has_column(cur, schema=schema, table=table, column="delivery_catchall_status"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN delivery_catchall_status TEXT")

    if not _has_column(cur, schema=schema, table=table, column="delivery_catchall_checked_at"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN delivery_catchall_checked_at TEXT")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 migration: add delivery_catchall_status and "
            "delivery_catchall_checked_at to domain_resolutions (PostgreSQL)."
        )
    )
    parser.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL (optional; overrides DATABASE_URL/PG_DSN for this run).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("PGSCHEMA", "public"),
        help="Target schema (default: public, or PGSCHEMA env var).",
    )
    args = parser.parse_args()
    _apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            migrate(cur, schema=args.schema)
        finally:
            try:
                cur.close()
            except Exception:
                pass
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("O26 delivery catch-all migration complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
