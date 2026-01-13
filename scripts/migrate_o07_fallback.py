# scripts/migrate_o07_fallback.py
from __future__ import annotations

"""
O07 migration — add third-party fallback columns to verification_results.

New nullable columns on verification_results:

  fallback_status      TEXT   -- mapped FallbackStatus ("valid" | "invalid" | "catch_all" | "unknown")
  fallback_raw         TEXT   -- JSON blob / provider payload or short reason
  fallback_checked_at  TEXT   -- ISO8601 UTC timestamp when fallback was called

Usage:

    python scripts/migrate_o07_fallback.py
    python scripts/migrate_o07_fallback.py --dsn "postgresql://..."
"""

import argparse
import os
import sys
from typing import Any

from src.db import get_conn


def apply_dsn_override(dsn: str | None) -> None:
    if not dsn:
        return
    os.environ["DATABASE_URL"] = dsn


def table_exists(cur: Any, *, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def col_exists(cur: Any, *, schema: str, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        LIMIT 1
        """,
        (schema, table, col),
    )
    return cur.fetchone() is not None


def ensure_columns(cur: Any, *, schema: str) -> None:
    """
    Idempotently add fallback_* columns to verification_results if missing.
    """
    table = "verification_results"
    fq_table = f'{schema}."{table}"'

    if not col_exists(cur, schema=schema, table=table, col="fallback_status"):
        print(f"· Adding {table}.fallback_status (TEXT)")
        cur.execute(f"ALTER TABLE {fq_table} ADD COLUMN fallback_status TEXT")
    else:
        print(f"· {table}.fallback_status already present; skipping")

    if not col_exists(cur, schema=schema, table=table, col="fallback_raw"):
        print(f"· Adding {table}.fallback_raw (TEXT)")
        cur.execute(f"ALTER TABLE {fq_table} ADD COLUMN fallback_raw TEXT")
    else:
        print(f"· {table}.fallback_raw already present; skipping")

    if not col_exists(cur, schema=schema, table=table, col="fallback_checked_at"):
        print(f"· Adding {table}.fallback_checked_at (TEXT)")
        cur.execute(f"ALTER TABLE {fq_table} ADD COLUMN fallback_checked_at TEXT")
    else:
        print(f"· {table}.fallback_checked_at already present; skipping")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="O07 migration: add fallback_* columns to verification_results (PostgreSQL)"
    )
    parser.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL (optional; overrides DATABASE_URL for this run).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("PGSCHEMA", "public"),
        help="Target schema (default: public, or PGSCHEMA env var).",
    )
    args = parser.parse_args()
    apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            if not table_exists(cur, schema=args.schema, table="verification_results"):
                print("! Table verification_results does not exist; nothing to migrate.")
                return

            ensure_columns(cur, schema=args.schema)
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

    print("✔ O07 migration completed (verification_results fallback_* columns ensured).")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
