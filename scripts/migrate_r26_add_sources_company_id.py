#!/usr/bin/env python
from __future__ import annotations

"""
R26 migration — add sources.company_id and backfill it from companies based on source_url host.

PostgreSQL version.

Behavior:
  - Connects via src.db.get_conn() (expects DATABASE_URL / PG_DSN).
  - Ensures sources.company_id exists (BIGINT REFERENCES companies(id)).
  - Backfills sources.company_id for rows where it is NULL by matching
    the normalized host from sources.source_url to companies.domain / companies.official_domain.
  - Tenant-safe when tenant_id exists on both tables (updates are scoped per tenant_id).
"""

import argparse
import os
import sys
from typing import Any
from urllib.parse import urlparse

from src.db import get_conn


def _apply_dsn_override(dsn: str | None) -> None:
    if not dsn:
        return
    os.environ["DATABASE_URL"] = dsn
    os.environ["PG_DSN"] = dsn


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _table_exists(cur: Any, *, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def _column_exists(cur: Any, *, schema: str, table: str, column: str) -> bool:
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


def _ensure_sources_table(cur: Any, *, schema: str) -> None:
    if not _table_exists(cur, schema=schema, table="sources"):
        raise SystemExit(
            f'Table "{schema}.sources" not found. Run the R10 migration (migrate_r10_add_sources.py) first.'
        )


def _ensure_company_id_column(cur: Any, *, schema: str) -> None:
    """
    Add sources.company_id if it does not already exist.

    Idempotent: re-running skips the ALTER TABLE once the column is present.
    """
    if _column_exists(cur, schema=schema, table="sources", column="company_id"):
        print("sources.company_id already exists; skipping ALTER TABLE.")
        return

    print("Adding sources.company_id column ...")
    fq = f"{_qi(schema)}.{_qi('sources')}"
    cur.execute(
        f"""
        ALTER TABLE {fq}
        ADD COLUMN company_id BIGINT REFERENCES {_qi(schema)}.{_qi("companies")}(id) ON DELETE SET NULL
        """.strip()
    )
    print("sources.company_id added.")


def _extract_host_from_url(url: str | None) -> str | None:
    """
    Normalize a URL to its host, lowercased and with 'www.' stripped.

    Returns None if the URL is malformed or host cannot be determined.
    """
    if not url:
        return None

    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return None

    host = host.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _load_company_domains(
    cur: Any, *, schema: str, tenant_aware: bool
) -> dict[tuple[str, str], int] | dict[str, int]:
    """
    Build a mapping from normalized domain -> company_id.

    Uses both companies.domain and companies.official_domain, and also maps
    'www.foo.com' <-> 'foo.com' to handle common host patterns.

    If tenant_aware is True, returns mapping keyed by (tenant_id, host).
    """
    if tenant_aware:
        cur.execute(
            f"""
            SELECT id,
                   COALESCE(tenant_id, 'dev') AS tenant_id,
                   lower(domain) AS domain,
                   lower(official_domain) AS official_domain
              FROM {_qi(schema)}.{_qi("companies")}
            """
        )
    else:
        cur.execute(
            f"""
            SELECT id,
                   lower(domain) AS domain,
                   lower(official_domain) AS official_domain
              FROM {_qi(schema)}.{_qi("companies")}
            """
        )

    rows = cur.fetchall()
    if not rows:
        print("No companies found in companies table.")
        return {}  # type: ignore[return-value]

    if tenant_aware:
        mapping_t: dict[tuple[str, str], int] = {}
        for company_id, tenant_id, domain, official_domain in rows:
            tid = (tenant_id or "dev").strip() or "dev"
            for value in (domain, official_domain):
                if not value:
                    continue
                host = value.strip().lower()
                if not host:
                    continue

                mapping_t.setdefault((tid, host), company_id)
                if host.startswith("www."):
                    bare = host[4:]
                    if bare:
                        mapping_t.setdefault((tid, bare), company_id)
                else:
                    mapping_t.setdefault((tid, f"www.{host}"), company_id)

        print(f"Loaded {len(mapping_t)} (tenant,domain) → company_id mappings.")
        return mapping_t

    mapping: dict[str, int] = {}
    for company_id, domain, official_domain in rows:
        for value in (domain, official_domain):
            if not value:
                continue
            host = value.strip().lower()
            if not host:
                continue

            mapping.setdefault(host, company_id)
            if host.startswith("www."):
                bare = host[4:]
                if bare:
                    mapping.setdefault(bare, company_id)
            else:
                mapping.setdefault(f"www.{host}", company_id)

    print(f"Loaded {len(mapping)} domain → company_id mappings.")
    return mapping


def _backfill_sources_company_id(cur: Any, *, schema: str) -> tuple[int, int]:
    """
    Populate sources.company_id for rows where it is NULL by joining on domain.

    Strategy:
      - Load a domain → company_id mapping from companies.domain/official_domain.
      - For each source with company_id IS NULL, parse the host from source_url.
      - If the host (or its www./bare variant) is in the mapping, update.

    Returns (updated_count, skipped_count).
    """
    tenant_aware = _column_exists(
        cur, schema=schema, table="sources", column="tenant_id"
    ) and _column_exists(cur, schema=schema, table="companies", column="tenant_id")

    domain_map = _load_company_domains(cur, schema=schema, tenant_aware=tenant_aware)
    if not domain_map:
        print("No domain mappings available; skipping backfill.")
        return (0, 0)

    sources_fq = f"{_qi(schema)}.{_qi('sources')}"

    if tenant_aware:
        cur.execute(
            f"""
            SELECT id, COALESCE(tenant_id, 'dev') AS tenant_id, source_url
              FROM {sources_fq}
             WHERE company_id IS NULL
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("No sources rows with NULL company_id; nothing to backfill.")
            return (0, 0)

        updated = 0
        skipped = 0

        print(f"Found {len(rows)} sources rows with NULL company_id to inspect.")
        for source_id, tenant_id, source_url in rows:
            host = _extract_host_from_url(source_url)
            if not host:
                skipped += 1
                continue

            tid = (tenant_id or "dev").strip() or "dev"
            company_id = domain_map.get((tid, host)) or domain_map.get((tid, f"www.{host}"))  # type: ignore[arg-type]
            if company_id is None:
                skipped += 1
                continue

            cur.execute(
                f"UPDATE {sources_fq} SET company_id = %s WHERE id = %s AND COALESCE(tenant_id,'dev') = %s",
                (company_id, source_id, tid),
            )
            updated += int(cur.rowcount or 0)

        print(f"Backfill complete: {updated} sources rows updated, {skipped} rows skipped.")
        return (updated, skipped)

    # Not tenant-aware
    cur.execute(
        f"""
        SELECT id, source_url
          FROM {sources_fq}
         WHERE company_id IS NULL
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("No sources rows with NULL company_id; nothing to backfill.")
        return (0, 0)

    updated = 0
    skipped = 0
    print(f"Found {len(rows)} sources rows with NULL company_id to inspect.")

    for source_id, source_url in rows:
        host = _extract_host_from_url(source_url)
        if not host:
            skipped += 1
            continue

        company_id = domain_map.get(host) or domain_map.get(f"www.{host}")  # type: ignore[arg-type]
        if company_id is None:
            skipped += 1
            continue

        cur.execute(
            f"UPDATE {sources_fq} SET company_id = %s WHERE id = %s",
            (company_id, source_id),
        )
        updated += int(cur.rowcount or 0)

    print(f"Backfill complete: {updated} sources rows updated, {skipped} rows skipped.")
    return (updated, skipped)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "R26 migration (Postgres): add sources.company_id and backfill it from companies based on source_url host."
        )
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
            _ensure_sources_table(cur, schema=args.schema)
            _ensure_company_id_column(cur, schema=args.schema)
            updated, skipped = _backfill_sources_company_id(cur, schema=args.schema)

        finally:
            try:
                cur.close()
            except Exception:
                pass

        conn.commit()
        print(f"Migration completed successfully. Updated={updated}, skipped={skipped}.")
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
