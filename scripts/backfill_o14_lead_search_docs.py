# scripts/backfill_o14_lead_search_docs.py
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from typing import Any

from src.db import get_connection  # type: ignore[import]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (name,),
    )
    row = cur.fetchone()
    return row is not None


def _has_company_attrs(conn: sqlite3.Connection) -> bool:
    """
    Detect whether companies.attrs exists.

    O14 can still work without attrs (industry/size_bucket will just be NULL),
    but we prefer to populate those fields when available.
    """
    try:
        cur = conn.execute("PRAGMA table_info(companies)")
    except sqlite3.Error:
        return False
    cols = [row[1] for row in cur.fetchall()]
    return "attrs" in cols


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


def backfill_lead_search_docs(conn: sqlite3.Connection) -> None:
    """
    Full refresh of the O14 lead_search_docs materialized table.

    Strategy:
      - Delete all existing rows.
      - Rebuild from people + companies + v_emails_latest join.
      - One row per person_id (primary "lead doc").
    """
    if not _table_exists(conn, "lead_search_docs"):
        print("[O14] Table lead_search_docs does not exist; nothing to backfill.")
        return

    if not _table_exists(conn, "v_emails_latest"):
        raise RuntimeError(
            "[O14] v_emails_latest view is missing; run schema/migrations before backfill."
        )

    has_attrs = _has_company_attrs(conn)
    print(f"[O14] companies.attrs present: {has_attrs}")

    conn.row_factory = sqlite3.Row

    # Start fresh for a full, deterministic refresh.
    print("[O14] Clearing existing rows from lead_search_docs ...")
    conn.execute("DELETE FROM lead_search_docs")
    conn.commit()

    # Build base query; we always join people + v_emails_latest + companies.
    # If companies.attrs exists we select it for Python-side parsing; otherwise
    # we select NULL and industry/size_bucket will remain None.
    select_attrs = "c.attrs AS company_attrs" if has_attrs else "NULL AS company_attrs"

    query = f"""
        SELECT
          p.id AS person_id,
          vel.email AS email,
          vel.verify_status AS verify_status,
          p.icp_score AS icp_score,
          p.role_family AS role_family,
          p.seniority AS seniority,
          {select_attrs}
        FROM people AS p
        JOIN v_emails_latest AS vel
          ON vel.person_id = p.id
        JOIN companies AS c
          ON c.id = p.company_id
    """

    print("[O14] Selecting source rows for materialization ...")
    cur = conn.execute(query)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[O14] Found {total} source rows for lead_search_docs.")

    now_str = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    insert_sql = """
        INSERT OR REPLACE INTO lead_search_docs (
          person_id,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    inserted = 0
    for row in rows:
        person_id = row["person_id"]
        email = row["email"]
        verify_status = row["verify_status"]
        icp_score = row["icp_score"]
        role_family = row["role_family"]
        seniority = row["seniority"]
        company_attrs = row["company_attrs"]

        industry, size_bucket = _parse_company_attrs(company_attrs) if has_attrs else (None, None)
        bucket = _icp_bucket(icp_score)

        conn.execute(
            insert_sql,
            (
                person_id,
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

    conn.commit()
    print(f"[O14] Backfill complete. Inserted/updated {inserted} rows into lead_search_docs.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="O14: Backfill lead_search_docs materialized view table."
    )
    parser.add_argument(
        "--db",
        "--db-path",
        dest="db_path",
        default="data/dev.db",
        help="Path to SQLite database file (default: data/dev.db)",
    )
    args = parser.parse_args()

    print(f"[O14] Using SQLite at: {args.db_path}")
    conn = get_connection(args.db_path)
    try:
        backfill_lead_search_docs(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
