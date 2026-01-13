# scripts/migrate_r21_search_indexing.py
from __future__ import annotations

"""
R21 migration: search indexing for people/companies (PostgreSQL).

SQLite used FTS5 virtual tables (people_fts/companies_fts) plus triggers. In Postgres we
preserve the same logical shape:

  - Keep tables named people_fts / companies_fts
  - Keep `rowid` aligned to the base table primary keys (people.id / companies.id)
  - Maintain these tables via triggers on people/companies
  - Provide a `tsv` tsvector column with a GIN index for full-text search

Notes:
  - attrs_text remains a placeholder (empty string), matching the original migration intent.
  - If tenant_id exists (expected), it is propagated into *_fts and used to scope updates.
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


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _exec_multi(cur: Any, sql: str) -> None:
    """
    Execute a (small) multi-statement SQL string safely by splitting on ';'.
    """
    for stmt in sql.split(";"):
        s = stmt.strip()
        if s:
            cur.execute(s)


def _trigger_exists(cur: Any, *, schema: str, table: str, trigger_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = %s
           AND c.relname = %s
           AND t.tgname = %s
           AND NOT t.tgisinternal
         LIMIT 1
        """,
        (schema, table, trigger_name),
    )
    return cur.fetchone() is not None


def _table_exists(cur: Any, *, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def ensure_search_tables(cur: Any, *, schema: str) -> None:
    """
    Create people_fts / companies_fts tables and their indexes if missing.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {_qi(schema)}.{_qi("people_fts")} (
      rowid        BIGINT PRIMARY KEY,
      tenant_id    TEXT NOT NULL DEFAULT 'dev',
      company_id   BIGINT,
      full_name    TEXT,
      first_name   TEXT,
      last_name    TEXT,
      title_norm   TEXT,
      role_family  TEXT,
      seniority    TEXT,
      company_name TEXT,
      company_domain TEXT,
      attrs_text   TEXT,
      tsv          tsvector
    );

    CREATE TABLE IF NOT EXISTS {_qi(schema)}.{_qi("companies_fts")} (
      rowid      BIGINT PRIMARY KEY,
      tenant_id  TEXT NOT NULL DEFAULT 'dev',
      name_norm  TEXT,
      domain     TEXT,
      attrs_text TEXT,
      tsv        tsvector
    );

    CREATE INDEX IF NOT EXISTS idx_people_fts_tenant_company ON {_qi(schema)}.{_qi("people_fts")}(tenant_id, company_id);
    CREATE INDEX IF NOT EXISTS idx_people_fts_company_id ON {_qi(schema)}.{_qi("people_fts")}(company_id);
    CREATE INDEX IF NOT EXISTS idx_people_fts_tsv ON {_qi(schema)}.{_qi("people_fts")} USING GIN(tsv);

    CREATE INDEX IF NOT EXISTS idx_companies_fts_tenant ON {_qi(schema)}.{_qi("companies_fts")}(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_companies_fts_tsv ON {_qi(schema)}.{_qi("companies_fts")} USING GIN(tsv);
    """.strip()

    _exec_multi(cur, ddl)


def ensure_trigger_functions(cur: Any, *, schema: str) -> None:
    """
    Create/replace trigger functions used to keep *_fts tables in sync.
    """
    sql = f"""
    CREATE OR REPLACE FUNCTION {_qi(schema)}.{_qi("people_fts_upsert")}()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
      c_name   TEXT;
      c_domain TEXT;
      tid      TEXT;
      doc      TEXT;
    BEGIN
      tid := COALESCE(NEW.tenant_id, 'dev');

      SELECT
        COALESCE(c.name_norm, c.name),
        COALESCE(c.official_domain, c.domain)
      INTO c_name, c_domain
      FROM {_qi(schema)}.{_qi("companies")} AS c
      WHERE c.id = NEW.company_id;

      doc :=
        COALESCE(NEW.full_name, '') || ' ' ||
        COALESCE(NEW.first_name, '') || ' ' ||
        COALESCE(NEW.last_name, '') || ' ' ||
        COALESCE(NEW.title_norm, '') || ' ' ||
        COALESCE(NEW.role_family, '') || ' ' ||
        COALESCE(NEW.seniority, '') || ' ' ||
        COALESCE(c_name, '') || ' ' ||
        COALESCE(c_domain, '') || ' ';

      INSERT INTO {_qi(schema)}.{_qi("people_fts")} (
        rowid,
        tenant_id,
        company_id,
        full_name,
        first_name,
        last_name,
        title_norm,
        role_family,
        seniority,
        company_name,
        company_domain,
        attrs_text,
        tsv
      )
      VALUES (
        NEW.id,
        tid,
        NEW.company_id,
        NEW.full_name,
        NEW.first_name,
        NEW.last_name,
        NEW.title_norm,
        NEW.role_family,
        NEW.seniority,
        c_name,
        c_domain,
        '',
        to_tsvector('simple', doc)
      )
      ON CONFLICT (rowid) DO UPDATE SET
        tenant_id = EXCLUDED.tenant_id,
        company_id = EXCLUDED.company_id,
        full_name = EXCLUDED.full_name,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        title_norm = EXCLUDED.title_norm,
        role_family = EXCLUDED.role_family,
        seniority = EXCLUDED.seniority,
        company_name = EXCLUDED.company_name,
        company_domain = EXCLUDED.company_domain,
        attrs_text = EXCLUDED.attrs_text,
        tsv = EXCLUDED.tsv;

      RETURN NEW;
    END;
    $$;

    CREATE OR REPLACE FUNCTION {_qi(schema)}.{_qi("people_fts_delete")}()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      DELETE FROM {_qi(schema)}.{_qi("people_fts")} WHERE rowid = OLD.id;
      RETURN OLD;
    END;
    $$;

    CREATE OR REPLACE FUNCTION {_qi(schema)}.{_qi("companies_fts_upsert")}()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
      tid TEXT;
      nm  TEXT;
      dm  TEXT;
      doc TEXT;
    BEGIN
      tid := COALESCE(NEW.tenant_id, 'dev');
      nm := COALESCE(NEW.name_norm, NEW.name);
      dm := COALESCE(NEW.official_domain, NEW.domain);

      doc := COALESCE(nm, '') || ' ' || COALESCE(dm, '') || ' ';

      INSERT INTO {_qi(schema)}.{_qi("companies_fts")} (
        rowid,
        tenant_id,
        name_norm,
        domain,
        attrs_text,
        tsv
      )
      VALUES (
        NEW.id,
        tid,
        nm,
        dm,
        '',
        to_tsvector('simple', doc)
      )
      ON CONFLICT (rowid) DO UPDATE SET
        tenant_id = EXCLUDED.tenant_id,
        name_norm = EXCLUDED.name_norm,
        domain = EXCLUDED.domain,
        attrs_text = EXCLUDED.attrs_text,
        tsv = EXCLUDED.tsv;

      -- Keep people_fts company_name/company_domain in sync when companies change.
      UPDATE {_qi(schema)}.{_qi("people_fts")} pf
         SET company_name = nm,
             company_domain = dm,
             tsv = to_tsvector(
                     'simple',
                     COALESCE(pf.full_name, '') || ' ' ||
                     COALESCE(pf.first_name, '') || ' ' ||
                     COALESCE(pf.last_name, '') || ' ' ||
                     COALESCE(pf.title_norm, '') || ' ' ||
                     COALESCE(pf.role_family, '') || ' ' ||
                     COALESCE(pf.seniority, '') || ' ' ||
                     COALESCE(nm, '') || ' ' ||
                     COALESCE(dm, '') || ' '
                   )
       WHERE pf.company_id = NEW.id
         AND pf.tenant_id = tid;

      RETURN NEW;
    END;
    $$;

    CREATE OR REPLACE FUNCTION {_qi(schema)}.{_qi("companies_fts_delete")}()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      DELETE FROM {_qi(schema)}.{_qi("companies_fts")} WHERE rowid = OLD.id;
      RETURN OLD;
    END;
    $$;
    """.strip()

    _exec_multi(cur, sql)


def ensure_triggers(cur: Any, *, schema: str) -> None:
    """
    Create triggers if they don't already exist.
    """
    # people triggers
    if not _trigger_exists(cur, schema=schema, table="people", trigger_name="people_fts_aiu"):
        cur.execute(
            f"""
            CREATE TRIGGER people_fts_aiu
            AFTER INSERT OR UPDATE ON {_qi(schema)}.{_qi("people")}
            FOR EACH ROW
            EXECUTE FUNCTION {_qi(schema)}.{_qi("people_fts_upsert")}()
            """.strip()
        )

    if not _trigger_exists(cur, schema=schema, table="people", trigger_name="people_fts_ad"):
        cur.execute(
            f"""
            CREATE TRIGGER people_fts_ad
            AFTER DELETE ON {_qi(schema)}.{_qi("people")}
            FOR EACH ROW
            EXECUTE FUNCTION {_qi(schema)}.{_qi("people_fts_delete")}()
            """.strip()
        )

    # companies triggers
    if not _trigger_exists(cur, schema=schema, table="companies", trigger_name="companies_fts_aiu"):
        cur.execute(
            f"""
            CREATE TRIGGER companies_fts_aiu
            AFTER INSERT OR UPDATE ON {_qi(schema)}.{_qi("companies")}
            FOR EACH ROW
            EXECUTE FUNCTION {_qi(schema)}.{_qi("companies_fts_upsert")}()
            """.strip()
        )

    if not _trigger_exists(cur, schema=schema, table="companies", trigger_name="companies_fts_ad"):
        cur.execute(
            f"""
            CREATE TRIGGER companies_fts_ad
            AFTER DELETE ON {_qi(schema)}.{_qi("companies")}
            FOR EACH ROW
            EXECUTE FUNCTION {_qi(schema)}.{_qi("companies_fts_delete")}()
            """.strip()
        )


def backfill_people_fts(cur: Any, *, schema: str) -> None:
    """
    Backfill people_fts from existing people + companies rows.

    Mirrors the original intent: insert only missing rows (by rowid).
    """
    sql = f"""
    INSERT INTO {_qi(schema)}.{_qi("people_fts")} (
      rowid,
      tenant_id,
      company_id,
      full_name,
      first_name,
      last_name,
      title_norm,
      role_family,
      seniority,
      company_name,
      company_domain,
      attrs_text,
      tsv
    )
    SELECT
      p.id AS rowid,
      COALESCE(p.tenant_id, 'dev') AS tenant_id,
      p.company_id,
      p.full_name,
      p.first_name,
      p.last_name,
      p.title_norm,
      p.role_family,
      p.seniority,
      COALESCE(c.name_norm, c.name) AS company_name,
      COALESCE(c.official_domain, c.domain) AS company_domain,
      '' AS attrs_text,
      to_tsvector(
        'simple',
        COALESCE(p.full_name, '') || ' ' ||
        COALESCE(p.first_name, '') || ' ' ||
        COALESCE(p.last_name, '') || ' ' ||
        COALESCE(p.title_norm, '') || ' ' ||
        COALESCE(p.role_family, '') || ' ' ||
        COALESCE(p.seniority, '') || ' ' ||
        COALESCE(COALESCE(c.name_norm, c.name), '') || ' ' ||
        COALESCE(COALESCE(c.official_domain, c.domain), '') || ' '
      ) AS tsv
    FROM {_qi(schema)}.{_qi("people")} p
    JOIN {_qi(schema)}.{_qi("companies")} c
      ON c.id = p.company_id
     AND COALESCE(c.tenant_id, 'dev') = COALESCE(p.tenant_id, 'dev')
    ON CONFLICT (rowid) DO NOTHING
    """.strip()
    cur.execute(sql)


def backfill_companies_fts(cur: Any, *, schema: str) -> None:
    """
    Backfill companies_fts from existing companies rows.

    Mirrors the original intent: insert only missing rows (by rowid).
    """
    sql = f"""
    INSERT INTO {_qi(schema)}.{_qi("companies_fts")} (
      rowid,
      tenant_id,
      name_norm,
      domain,
      attrs_text,
      tsv
    )
    SELECT
      c.id AS rowid,
      COALESCE(c.tenant_id, 'dev') AS tenant_id,
      COALESCE(c.name_norm, c.name) AS name_norm,
      COALESCE(c.official_domain, c.domain) AS domain,
      '' AS attrs_text,
      to_tsvector(
        'simple',
        COALESCE(COALESCE(c.name_norm, c.name), '') || ' ' ||
        COALESCE(COALESCE(c.official_domain, c.domain), '') || ' '
      ) AS tsv
    FROM {_qi(schema)}.{_qi("companies")} c
    ON CONFLICT (rowid) DO NOTHING
    """.strip()
    cur.execute(sql)


def run_migration(dsn: str | None, *, schema: str) -> None:
    _apply_dsn_override(dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            # Base tables must exist for R21.
            if not _table_exists(cur, schema=schema, table="people"):
                raise SystemExit(f'Error: table "{schema}.people" does not exist.')
            if not _table_exists(cur, schema=schema, table="companies"):
                raise SystemExit(f'Error: table "{schema}.companies" does not exist.')

            print("[R21] Ensuring search index tables ...")
            ensure_search_tables(cur, schema=schema)

            print("[R21] Ensuring trigger functions ...")
            ensure_trigger_functions(cur, schema=schema)

            print("[R21] Ensuring triggers ...")
            ensure_triggers(cur, schema=schema)

            print("[R21] Backfilling existing people into people_fts ...")
            backfill_people_fts(cur, schema=schema)

            print("[R21] Backfilling existing companies into companies_fts ...")
            backfill_companies_fts(cur, schema=schema)

        finally:
            try:
                cur.close()
            except Exception:
                pass

        conn.commit()
        print("✔ R21 search indexing migration applied successfully.")
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="R21 migration (PostgreSQL): create search index tables + triggers for people/companies."
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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_migration(args.dsn, schema=args.schema)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
