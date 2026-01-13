# scripts/migrate_r08_add_domains.py
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


def _column_exists(cur: Any, *, schema: str, table: str, col: str) -> bool:
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


def _add_column_if_missing(
    cur: Any,
    *,
    schema: str,
    table: str,
    col: str,
    type_sql: str,
    dry: bool,
) -> None:
    if _column_exists(cur, schema=schema, table=table, col=col):
        print(f"[SKIP]  {table}.{col} already exists")
        return

    sql = f'ALTER TABLE {schema}."{table}" ADD COLUMN "{col}" {type_sql}'
    print(f"[APPLY] {sql}")
    if not dry:
        cur.execute(sql)


def main() -> None:
    p = argparse.ArgumentParser(
        description="R08 migration: add domain-resolution columns (PostgreSQL)"
    )
    p.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL (optional; overrides DATABASE_URL/PG_DSN for this run).",
    )
    p.add_argument("--dry-run", action="store_true", help="Show actions without writing changes")
    p.add_argument(
        "--schema",
        default=os.getenv("PGSCHEMA", "public"),
        help="Target schema (default: public, or PGSCHEMA env var).",
    )
    args = p.parse_args()
    _apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            # --- companies: where the official, resolved domain lives ---
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="companies",
                col="official_domain",
                type_sql="TEXT",
                dry=args.dry_run,
            )
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="companies",
                col="official_domain_source",
                type_sql="TEXT",
                dry=args.dry_run,
            )  # e.g., 'home_page', 'whois', 'search'
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="companies",
                col="official_domain_confidence",
                type_sql="DOUBLE PRECISION",
                dry=args.dry_run,
            )  # 0.0–1.0
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="companies",
                col="official_domain_checked_at",
                type_sql="TEXT",
                dry=args.dry_run,
            )  # ISO8601

            # Helpful lookup speed-up (non-unique, partial uniqueness can come later if needed)
            idx_name = "ix_companies_official_domain"
            idx_sql = f'CREATE INDEX {idx_name} ON {args.schema}."companies"(official_domain)'
            if _index_exists(cur, schema=args.schema, name=idx_name):
                print(f"[SKIP]  {idx_name} already exists")
            else:
                print(f"[APPLY] {idx_sql}")
                if not args.dry_run:
                    cur.execute(idx_sql)

            # --- ingest_items: keep what we attempted/resolved during intake ---
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="ingest_items",
                col="resolved_domain",
                type_sql="TEXT",
                dry=args.dry_run,
            )
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="ingest_items",
                col="resolved_domain_source",
                type_sql="TEXT",
                dry=args.dry_run,
            )
            _add_column_if_missing(
                cur,
                schema=args.schema,
                table="ingest_items",
                col="resolved_domain_confidence",
                type_sql="DOUBLE PRECISION",
                dry=args.dry_run,
            )

        finally:
            try:
                cur.close()
            except Exception:
                pass

        if args.dry_run:
            conn.rollback()
        else:
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

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
