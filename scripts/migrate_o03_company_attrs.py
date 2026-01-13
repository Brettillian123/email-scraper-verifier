# scripts/migrate_o03_company_attrs.py
"""
O03 migration: add lightweight enrichment field to companies.

Adds (if absent):
  - companies.attrs  (TEXT, NULLABLE) — JSON blob of heuristic enrichment
    e.g., {"size_bucket":"51-200","industry":["B2B SaaS"],"tech":["Salesforce","AWS"]}

Notes
-----
- Stored as TEXT to preserve existing application logic and broad compatibility.
- Writers should json.dumps(...) with ensure_ascii=False.
- Readers should treat missing/empty as {}.
- Postgres-only migration; all DB access routed through src.db.get_conn().

Usage:
  python scripts/migrate_o03_company_attrs.py
  python scripts/migrate_o03_company_attrs.py --dsn "postgresql://..."
"""

from __future__ import annotations

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


def column_exists(cur: Any, *, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        LIMIT 1
        """,
        (schema, table, column),
    )
    return cur.fetchone() is not None


def add_column(cur: Any, *, schema: str, table: str, column: str, ddl_type: str) -> bool:
    if not table_exists(cur, schema=schema, table=table):
        raise RuntimeError(f'Table "{schema}.{table}" does not exist.')
    if column_exists(cur, schema=schema, table=table, column=column):
        return False
    cur.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN "{column}" {ddl_type}')
    return True


def run(cur: Any, *, schema: str) -> list[str]:
    actions: list[str] = []

    # -- companies.attrs (TEXT for JSON) --
    if add_column(cur, schema=schema, table="companies", column="attrs", ddl_type="TEXT"):
        actions.append("Added column companies.attrs (TEXT for JSON)")
    else:
        actions.append("companies.attrs already present; skipping")

    return actions


def main() -> None:
    p = argparse.ArgumentParser(description="O03 DB migration (companies.attrs JSON field)")
    p.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL (optional; overrides DATABASE_URL for this run).",
    )
    p.add_argument(
        "--schema",
        default=os.getenv("PGSCHEMA", "public"),
        help="Target schema (default: public, or PGSCHEMA env var).",
    )
    args = p.parse_args()
    apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            actions = run(cur, schema=args.schema)
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

    print("✔ O03 migration completed.")
    for a in actions:
        print(" -", a)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
