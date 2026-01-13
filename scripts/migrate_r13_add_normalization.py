# scripts/migrate_r13_add_normalization.py
"""
R13 migration (PostgreSQL): add raw/normalized title fields to people and
normalized name/key fields to companies, with a safety backfill to preserve provenance.

- Adds:
    people.title_raw   (TEXT, NULLABLE)
    people.title_norm  (TEXT, NULLABLE)

    companies.name_norm (TEXT, NULLABLE)
    companies.norm_key  (TEXT, NULLABLE, indexed)

- Backfills:
    people.title_raw <- people.title   (ONLY where title_raw IS NULL)

This script is idempotent (safe to run repeatedly).
"""

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


def _to_regclass(cur: Any, reg: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (reg,))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def table_exists(cur: Any, *, schema: str, table: str) -> bool:
    return _to_regclass(cur, f'"{schema}"."{table}"')


def index_exists(cur: Any, *, schema: str, index_name: str) -> bool:
    return _to_regclass(cur, f'"{schema}"."{index_name}"')


def column_exists(cur: Any, *, schema: str, table: str, column: str) -> bool:
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


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def add_column(cur: Any, *, schema: str, table: str, column: str, ddl_type: str) -> bool:
    if not table_exists(cur, schema=schema, table=table):
        raise RuntimeError(f'Table "{schema}.{table}" does not exist.')
    if column_exists(cur, schema=schema, table=table, column=column):
        return False
    cur.execute(f"ALTER TABLE {_qi(schema)}.{_qi(table)} ADD COLUMN {_qi(column)} {ddl_type}")
    return True


def run_migration(cur: Any, *, schema: str) -> list[str]:
    actions: list[str] = []

    # --- people: title_raw / title_norm ---
    if add_column(cur, schema=schema, table="people", column="title_raw", ddl_type="TEXT"):
        actions.append("Added column people.title_raw (TEXT)")
    else:
        actions.append("people.title_raw already present; skipping")

    if add_column(cur, schema=schema, table="people", column="title_norm", ddl_type="TEXT"):
        actions.append("Added column people.title_norm (TEXT)")
    else:
        actions.append("people.title_norm already present; skipping")

    # Backfill title_raw from title where missing (provenance safety)
    if column_exists(cur, schema=schema, table="people", column="title_raw") and column_exists(
        cur, schema=schema, table="people", column="title"
    ):
        cur.execute(
            f"""
            UPDATE {_qi(schema)}.{_qi("people")}
               SET title_raw = title
             WHERE title_raw IS NULL
               AND title IS NOT NULL
            """
        )
        actions.append(f"Backfilled people.title_raw from people.title ({cur.rowcount} rows)")
    else:
        actions.append("Skipped backfill: people.title or people.title_raw missing")

    # --- companies: name_norm / norm_key (+ index) ---
    if add_column(cur, schema=schema, table="companies", column="name_norm", ddl_type="TEXT"):
        actions.append("Added column companies.name_norm (TEXT)")
    else:
        actions.append("companies.name_norm already present; skipping")

    if add_column(cur, schema=schema, table="companies", column="norm_key", ddl_type="TEXT"):
        actions.append("Added column companies.norm_key (TEXT)")
    else:
        actions.append("companies.norm_key already present; skipping")

    # Index on companies.norm_key for clustering/lookup (no auto-merge logic)
    idx_name = "idx_companies_norm_key"
    if not index_exists(cur, schema=schema, index_name=idx_name):
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {_qi(schema)}.{_qi('companies')}(norm_key)"
        )
        actions.append(f"Created index {idx_name} on companies(norm_key)")
    else:
        actions.append(f"Index {idx_name} already present; skipping")

    return actions


def main() -> None:
    p = argparse.ArgumentParser(description="R13 DB migration (normalization fields) — PostgreSQL")
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
        help="Target Postgres schema (default: public, or PGSCHEMA env var).",
    )
    args = p.parse_args()
    _apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            actions = run_migration(cur, schema=args.schema)
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

    print("✔ R13 migration completed.")
    for a in actions:
        print(" -", a)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
