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

The migration is idempotent:
  - It checks for existing columns before ALTER TABLE.
  - It uses CREATE INDEX IF NOT EXISTS for the new index.
  - It is safe to run multiple times.
"""

import argparse
import os
import sys
from typing import Any

from src.db import get_conn


def _apply_dsn_override(dsn: str | None) -> None:
    if not dsn:
        return
    os.environ["DATABASE_URL"] = dsn
    os.environ["PG_DSN"] = dsn


def _table_exists(cur: Any, *, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def _col_exists(cur: Any, *, schema: str, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
           AND column_name = %s
         LIMIT 1
        """,
        (schema, table, col),
    )
    return cur.fetchone() is not None


def _index_exists(cur: Any, *, schema: str, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{name}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def _migrate_verification_results(cur: Any, *, schema: str) -> None:
    """
    Ensure O26 test-send / bounce columns on verification_results.
    """
    table = "verification_results"
    if not _table_exists(cur, schema=schema, table=table):
        raise RuntimeError(f'Table "{schema}.{table}" does not exist.')

    fq = f'{schema}."{table}"'

    if not _col_exists(cur, schema=schema, table=table, col="test_send_status"):
        # Default to "not_requested" so existing rows have a sensible state.
        cur.execute(
            f"ALTER TABLE {fq} ADD COLUMN test_send_status TEXT NOT NULL DEFAULT 'not_requested'"
        )

    if not _col_exists(cur, schema=schema, table=table, col="test_send_token"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN test_send_token TEXT")

    if not _col_exists(cur, schema=schema, table=table, col="test_send_at"):
        # Store timestamps as ISO8601 text, consistent with other *_at fields.
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN test_send_at TEXT")

    if not _col_exists(cur, schema=schema, table=table, col="bounce_code"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN bounce_code TEXT")

    if not _col_exists(cur, schema=schema, table=table, col="bounce_reason"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN bounce_reason TEXT")

    # Index for fast bounce lookups by token (safe + idempotent).
    idx_name = "idx_verif_test_send_token"
    if not _index_exists(cur, schema=schema, name=idx_name):
        cur.execute(f'CREATE INDEX {idx_name} ON {schema}."verification_results"(test_send_token)')


def _migrate_domain_resolutions(cur: Any, *, schema: str) -> None:
    """
    Ensure O26 domain-level delivery columns on domain_resolutions.

    These are used by:
      - backfill_o26_delivery_catchall.py
      - backfill_o26_upgrade_risky_to_valid.py
    """
    table = "domain_resolutions"
    if not _table_exists(cur, schema=schema, table=table):
        raise RuntimeError(f'Table "{schema}.{table}" does not exist.')

    fq = f'{schema}."{table}"'

    if not _col_exists(cur, schema=schema, table=table, col="delivery_catchall_status"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN delivery_catchall_status TEXT")

    if not _col_exists(cur, schema=schema, table=table, col="delivery_catchall_checked_at"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN delivery_catchall_checked_at TEXT")

    if not _col_exists(cur, schema=schema, table=table, col="domain"):
        cur.execute(f"ALTER TABLE {fq} ADD COLUMN domain TEXT")

    # Backfill domain from chosen_domain where missing/empty.
    cur.execute(
        f"""
        UPDATE {fq}
           SET domain = chosen_domain
         WHERE (domain IS NULL OR domain = '')
           AND chosen_domain IS NOT NULL
        """
    )


def migrate(cur: Any, *, schema: str) -> None:
    """
    Apply the O26 migration.

    Idempotent and safe to run multiple times.
    """
    _migrate_domain_resolutions(cur, schema=schema)
    _migrate_verification_results(cur, schema=schema)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="O26: Ensure test-send/bounce fields and domain delivery flags (PostgreSQL)."
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

    print("O26 migration complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
