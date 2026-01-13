# scripts/backfill_o14_lead_search_docs.py
from __future__ import annotations

import argparse
import json
import os
from contextlib import closing
from datetime import datetime
from typing import Any

from src.db import get_conn  # type: ignore[import]


def _relation_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        row = cur.fetchone()
    return bool(row and row[0] is not None)


def _has_company_attrs(conn) -> bool:
    """
    Detect whether companies.attrs exists.

    O14 can still work without attrs (industry/size_bucket will just be NULL),
    but we prefer to populate those fields when available.
    """
    q = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'companies'
          AND column_name = 'attrs'
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchone() is not None


def _parse_company_attrs(raw: Any) -> tuple[str | None, str | None]:
    """
    Parse companies.attrs JSON into (industry, size_bucket).

    Expects either:
      - a JSON string, or
      - a dict with "industry" / "size_bucket" keys.

    Returns (industry, size_bucket), each possibly None.
    """
    if not raw:
        return None, None

    data: Any
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None, None
    elif isinstance(raw, dict):
        data = raw
    else:
        return None, None

    industry = data.get("industry")
    size_bucket = data.get("size_bucket")
    return (str(industry) if industry else None, str(size_bucket) if size_bucket else None)


def _icp_bucket(score: Any) -> str | None:
    """
    Bucket an ICP score into one of the coarse ranges used for facets.

    Buckets:
      - 80-100
      - 60-79
      - 40-59
      - 0-39

    Returns None if score is None or not an integer.
    """
    if score is None:
        return None
    try:
        value = int(score)
    except (TypeError, ValueError):
        return None

    if value >= 80:
        return "80-100"
    if value >= 60:
        return "60-79"
    if value >= 40:
        return "40-59"
    return "0-39"


def backfill_lead_search_docs(conn, tenant_id: str | None) -> None:
    """
    Full refresh of the O14 lead_search_docs materialized table.

    Strategy:
      - Delete all existing rows (or tenant-scoped rows if tenant_id provided).
      - Rebuild from people + companies + v_emails_latest join.
      - One row per person_id (primary "lead doc").
    """
    if not _relation_exists(conn, "lead_search_docs"):
        print("[O14] Table lead_search_docs does not exist; nothing to backfill.")
        return

    if not _relation_exists(conn, "v_emails_latest"):
        raise RuntimeError(
            "[O14] v_emails_latest view is missing; run schema/migrations before backfill."
        )

    has_attrs = _has_company_attrs(conn)
    print(f"[O14] companies.attrs present: {has_attrs}")

    # Start fresh for a full, deterministic refresh.
    print("[O14] Clearing existing rows from lead_search_docs ...")
    with conn:
        with conn.cursor() as cur:
            if tenant_id:
                cur.execute("DELETE FROM lead_search_docs WHERE tenant_id = %s", (tenant_id,))
            else:
                cur.execute("DELETE FROM lead_search_docs")

    # Build base query; we always join people + v_emails_latest + companies.
    # If companies.attrs exists we select it for Python-side parsing; otherwise
    # we select NULL and industry/size_bucket will remain None.
    select_attrs = "c.attrs AS company_attrs" if has_attrs else "NULL AS company_attrs"

    query = f"""
        SELECT
          p.id AS person_id,
          p.tenant_id AS tenant_id,
          vel.email AS email,
          vel.verify_status AS verify_status,
          p.icp_score AS icp_score,
          p.role_family AS role_family,
          p.seniority AS seniority,
          {select_attrs}
        FROM people AS p
        JOIN v_emails_latest AS vel
          ON vel.person_id = p.id
         AND vel.tenant_id = p.tenant_id
        JOIN companies AS c
          ON c.id = p.company_id
         AND c.tenant_id = p.tenant_id
    """

    params: list[Any] = []
    if tenant_id:
        query += " WHERE p.tenant_id = %s"
        params.append(tenant_id)

    print("[O14] Selecting source rows for materialization ...")
    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    total = len(rows)
    print(f"[O14] Found {total} source rows for lead_search_docs.")

    now_str = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    insert_sql = """
        INSERT INTO lead_search_docs (
          person_id,
          tenant_id,
          email,
          verify_status,
          icp_score,
          role_family,
          seniority,
          company_size_bucket,
          company_industry,
          icp_bucket,
          created_at,
          updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (person_id) DO UPDATE SET
          tenant_id           = EXCLUDED.tenant_id,
          email               = EXCLUDED.email,
          verify_status       = EXCLUDED.verify_status,
          icp_score           = EXCLUDED.icp_score,
          role_family         = EXCLUDED.role_family,
          seniority           = EXCLUDED.seniority,
          company_size_bucket = EXCLUDED.company_size_bucket,
          company_industry    = EXCLUDED.company_industry,
          icp_bucket          = EXCLUDED.icp_bucket,
          created_at          = EXCLUDED.created_at,
          updated_at          = EXCLUDED.updated_at
    """

    inserted = 0
    with conn:
        with conn.cursor() as cur:
            for row in rows:
                # (person_id, tenant_id, email, verify_status, icp_score, role_family, seniority, company_attrs)
                person_id = row[0]
                row_tenant_id = row[1]
                email = row[2]
                verify_status = row[3]
                icp_score = row[4]
                role_family = row[5]
                seniority = row[6]
                company_attrs = row[7]

                industry, size_bucket = (
                    _parse_company_attrs(company_attrs) if has_attrs else (None, None)
                )
                bucket = _icp_bucket(icp_score)

                cur.execute(
                    insert_sql,
                    (
                        person_id,
                        row_tenant_id,
                        email,
                        verify_status,
                        int(icp_score) if icp_score is not None else None,
                        role_family,
                        seniority,
                        size_bucket,
                        industry,
                        bucket,
                        now_str,
                        now_str,
                    ),
                )
                inserted += 1

    print(f"[O14] Backfill complete. Inserted/updated {inserted} rows into lead_search_docs.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="O14: Backfill lead_search_docs materialized view table."
    )
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope the backfill to one tenant. Default: all tenants.",
    )
    parser.add_argument(
        "--dsn",
        dest="dsn",
        default=None,
        help="Optional Postgres DSN/URL override. If provided, sets DATABASE_URL for this run.",
    )
    args = parser.parse_args(argv)

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    with closing(get_conn()) as conn:
        backfill_lead_search_docs(conn, tenant_id=args.tenant_id)


if __name__ == "__main__":
    main()
