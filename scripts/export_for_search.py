# scripts/export_for_search.py
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def iter_leads_for_search(conn: sqlite3.Connection) -> Iterator[dict[str, Any]]:
    """
    Yield JSON-serializable lead documents suitable for a search engine mirror.

    Document shape (keys are stable; values may be None/missing depending on schema):

        {
          "id": "email:<emails.id>",
          "email": "...",
          "first_name": "...",
          "last_name": "...",
          "full_name": "...",
          "title": "...",
          "role_family": "...",
          "seniority": "...",
          "company": "...",
          "domain": "...",
          "verify_status": "...",
          "icp_score": 92,
          "industry": "B2B SaaS",
          "company_size": "50-200",
          "tech_keywords": ["salesforce", "hubspot"],
          "verified_at": "2025-11-28T20:00:00Z",
          "source_url": "https://...",
        }

    This is intentionally close to what R22/R23's /leads/search will want.
    """
    # Select from v_emails_latest + people + companies. We keep the SQL as
    # simple and forgiving as possible: if some columns are missing in older
    # DBs, you can adjust the query to match your actual schema.
    sql = """
        SELECT
          ve.id            AS email_id,
          ve.email         AS email,
          ve.person_id     AS person_id,
          ve.verify_status AS verify_status,
          ve.verified_at   AS verified_at,
          ve.source_url    AS email_source_url,

          p.first_name     AS first_name,
          p.last_name      AS last_name,
          p.full_name      AS full_name,
          p.title          AS title,
          p.title_norm     AS title_norm,
          p.role_family    AS role_family,
          p.seniority      AS seniority,
          p.icp_score      AS icp_score,
          p.source_url     AS person_source_url,

          c.id             AS company_id,
          c.name           AS company_name_raw,
          c.name_norm      AS company_name_norm,
          c.domain         AS company_domain_raw,
          c.official_domain AS company_domain_official,
          c.website_url    AS company_website_url,
          c.attrs          AS company_attrs
        FROM v_emails_latest AS ve
        JOIN people AS p
          ON p.id = ve.person_id
        JOIN companies AS c
          ON c.id = p.company_id
    """

    cur = conn.execute(sql)
    for row in cur:
        # Base identity fields
        email_id = row["email_id"]
        email = row["email"]

        full_name = row["full_name"]
        if not full_name:
            fn = row["first_name"] or ""
            ln = row["last_name"] or ""
            full_name = (fn + " " + ln).strip() or None

        title = row["title_norm"] or row["title"]

        company_name = row["company_name_norm"] or row["company_name_raw"]
        domain = row["company_domain_official"] or row["company_domain_raw"]

        # Prefer the most specific source_url we have.
        source_url = (
            row["email_source_url"] or row["person_source_url"] or row["company_website_url"]
        )

        industry: str | None = None
        company_size: str | None = None
        tech_keywords: list[str] = []

        attrs_raw = row["company_attrs"]
        if attrs_raw:
            try:
                attrs = json.loads(attrs_raw)
                if isinstance(attrs, dict):
                    industry = attrs.get("industry") or attrs.get("industry_label")
                    company_size = attrs.get("size_bucket") or attrs.get("company_size")
                    tk = attrs.get("tech_keywords") or attrs.get("tech_stack")
                    if isinstance(tk, list):
                        tech_keywords = [str(x) for x in tk]
            except (TypeError, ValueError):
                # If attrs isn't valid JSON, we just leave the derived fields as None.
                pass

        doc: dict[str, Any] = {
            "id": f"email:{email_id}",
            "email": email,
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "full_name": full_name,
            "title": title,
            "role_family": row["role_family"],
            "seniority": row["seniority"],
            "company": company_name,
            "domain": domain,
            "verify_status": row["verify_status"],
            "icp_score": row["icp_score"],
            "industry": industry,
            "company_size": company_size,
            "tech_keywords": tech_keywords,
            "verified_at": row["verified_at"],
            "source_url": source_url,
        }

        yield doc


def write_jsonl(docs: Iterable[dict[str, Any]], out_file: Any) -> None:
    for doc in docs:
        out_file.write(json.dumps(doc, ensure_ascii=False) + "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export lead documents (from v_emails_latest + people + companies) "
            "as JSONL for a search engine mirror (O13)."
        )
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to SQLite database (default: data/dev.db)",
    )
    parser.add_argument(
        "--out",
        dest="out_path",
        default="-",
        help="Output path for JSONL (default: '-' for stdout)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    db_file = Path(args.db_path)
    print(f"[O13] Using SQLite database at: {db_file}", file=sys.stderr)

    conn = get_connection(str(db_file))
    try:
        docs = iter_leads_for_search(conn)

        if args.out_path == "-" or args.out_path.strip() == "":
            out_fh = sys.stdout
            write_jsonl(docs, out_fh)
        else:
            out_path = Path(args.out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", encoding="utf-8") as f:
                write_jsonl(docs, f)

        print("[O13] Export completed.", file=sys.stderr)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
