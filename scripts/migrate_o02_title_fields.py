# scripts/migrate_o02_title_fields.py
"""
O02 migration: add canonicalized title fields to people.

Adds (if absent):
  - people.role_family  (TEXT, NULLABLE)
  - people.seniority    (TEXT, NULLABLE)

Notes
-----
- These are populated by src/ingest/title_norm.py (O02) after R13 title normalization.
- We keep them nullable to avoid destructive updates during phased rollout.
- Postgres-only migration; all DB access routed through src.db.get_conn().

Usage:
  python scripts/migrate_o02_title_fields.py
  python scripts/migrate_o02_title_fields.py --dsn "postgresql://..."
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


def index_exists(cur: Any, *, schema: str, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{name}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def add_column(cur: Any, *, schema: str, table: str, column: str, ddl_type: str) -> bool:
    if not table_exists(cur, schema=schema, table=table):
        raise RuntimeError(f'Table "{schema}.{table}" does not exist.')

    if column_exists(cur, schema=schema, table=table, column=column):
        return False

    cur.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN "{column}" {ddl_type}')
    return True


def run(cur: Any, *, schema: str) -> list[str]:
    actions: list[str] = []

    # -- people.role_family --
    if add_column(cur, schema=schema, table="people", column="role_family", ddl_type="TEXT"):
        actions.append("Added column people.role_family (TEXT)")
    else:
        actions.append("people.role_family already present; skipping")

    # -- people.seniority --
    if add_column(cur, schema=schema, table="people", column="seniority", ddl_type="TEXT"):
        actions.append("Added column people.seniority (TEXT)")
    else:
        actions.append("people.seniority already present; skipping")

    # Optional light indexes (idempotent)
    if not index_exists(cur, schema=schema, name="idx_people_role_family"):
        cur.execute(f'CREATE INDEX idx_people_role_family ON {schema}."people" ("role_family")')
        actions.append("Created index idx_people_role_family on people(role_family)")
    else:
        actions.append("Index idx_people_role_family already present; skipping")

    if not index_exists(cur, schema=schema, name="idx_people_seniority"):
        cur.execute(f'CREATE INDEX idx_people_seniority ON {schema}."people" ("seniority")')
        actions.append("Created index idx_people_seniority on people(seniority)")
    else:
        actions.append("Index idx_people_seniority already present; skipping")

    return actions


def main() -> None:
    p = argparse.ArgumentParser(description="O02 DB migration (role_family/seniority fields)")
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

    print("✔ O02 migration completed.")
    for a in actions:
        print(" -", a)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
