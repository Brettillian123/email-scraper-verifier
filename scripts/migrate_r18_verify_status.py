from __future__ import annotations

"""
R18 migration — add verify_status / reason / MX / timestamp columns.

PostgreSQL version.

Usage (PowerShell):

  $PyExe = ".\.venv\Scripts\python.exe"; if (!(Test-Path $PyExe)) { $PyExe = "python" }
  & $PyExe scripts\migrate_r18_verify_status.py --dsn $env:DATABASE_URL

Behavior:
  - Connects via src.db.get_conn() (expects DATABASE_URL / PG_DSN).
  - Ensures verification_results has the following columns:
        verify_status  TEXT
        verify_reason  TEXT
        verified_mx    TEXT
        verified_at    TEXT
  - Idempotent: re-running skips columns that already exist.
"""

import argparse
import os
import sys
from typing import Any

from src.db import get_conn


def _apply_dsn_override(dsn: str | None) -> None:
    if not dsn:
        return
    # Support both names because different environments use different keys.
    os.environ["DATABASE_URL"] = dsn
    os.environ["PG_DSN"] = dsn


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _table_exists(cur: Any, *, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def _table_columns(cur: Any, *, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def ensure_column(
    cur: Any,
    *,
    schema: str,
    table: str,
    column: str,
    ddl_fragment: str,
) -> None:
    """
    Ensure `schema.table.column` exists; if not, ALTER TABLE to add it.

    ddl_fragment should be the 'col_name TYPE ...' part, e.g.
      'verify_status TEXT'
    """
    existing = _table_columns(cur, schema=schema, table=table)
    if column in existing:
        print(f"· Skipping {table}.{column} (already exists)")
        return

    fq = f"{_qi(schema)}.{_qi(table)}"
    sql = f"ALTER TABLE {fq} ADD COLUMN {ddl_fragment}"
    cur.execute(sql)
    print(f"· Added {table}.{column}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R18: ensure verification_results has verify_status/verify_reason/verified_mx/verified_at columns (PostgreSQL).",
    )
    parser.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL override (otherwise uses env DATABASE_URL/PG_DSN).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("PGSCHEMA", "public"),
        help="Target Postgres schema (default: public, or PGSCHEMA env var).",
    )
    args = parser.parse_args(argv)
    _apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            table = "verification_results"

            if not _table_exists(cur, schema=args.schema, table=table):
                raise SystemExit(f'Error: table "{args.schema}.{table}" does not exist.')

            print("Ensuring R18 verify-status columns on verification_results...")

            ensure_column(
                cur,
                schema=args.schema,
                table=table,
                column="verify_status",
                ddl_fragment="verify_status TEXT",
            )
            ensure_column(
                cur,
                schema=args.schema,
                table=table,
                column="verify_reason",
                ddl_fragment="verify_reason TEXT",
            )
            ensure_column(
                cur,
                schema=args.schema,
                table=table,
                column="verified_mx",
                ddl_fragment="verified_mx TEXT",
            )
            ensure_column(
                cur,
                schema=args.schema,
                table=table,
                column="verified_at",
                ddl_fragment="verified_at TEXT",
            )

        finally:
            try:
                cur.close()
            except Exception:
                pass
        conn.commit()
        print("✔ R18 migration complete.")
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
