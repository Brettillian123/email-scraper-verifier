# scripts/migrate_r17_add_catchall.py
from __future__ import annotations

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


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def col_exists(cur: Any, *, schema: str, table: str, col: str) -> bool:
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


def ensure_catchall_columns(cur: Any, *, schema: str) -> None:
    """
    Ensure R17 catch-all columns exist on domain_resolutions.
    Idempotent: only ALTER TABLE when a column is missing.
    """
    table = "domain_resolutions"
    fq = f"{_qi(schema)}.{_qi(table)}"

    def add_col(name: str, ddl: str) -> None:
        if not col_exists(cur, schema=schema, table=table, col=name):
            print(f"· Adding {table}.{name} ...")
            cur.execute(f"ALTER TABLE {fq} ADD COLUMN {ddl}")
        else:
            print(f"· Skipping {table}.{name} (already exists)")

    print("Ensuring R17 catch-all columns on domain_resolutions...")

    add_col("catch_all_status", "catch_all_status TEXT")
    add_col("catch_all_checked_at", "catch_all_checked_at TEXT")
    add_col("catch_all_localpart", "catch_all_localpart TEXT")
    add_col("catch_all_smtp_code", "catch_all_smtp_code INTEGER")
    add_col("catch_all_smtp_msg", "catch_all_smtp_msg TEXT")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="R17 migration: add catch-all columns to domain_resolutions (PostgreSQL)."
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
        help="Target Postgres schema (default: public, or PGSCHEMA env var).",
    )
    args = parser.parse_args()
    _apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            ensure_catchall_columns(cur, schema=args.schema)
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

    print("✔ R17 migration completed (catch-all columns ensured on domain_resolutions).")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
