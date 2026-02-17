# scripts/export_for_search.py
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database connection (PostgreSQL + SQLite)
# ---------------------------------------------------------------------------


def _is_postgres_configured() -> bool:
    """Check if DATABASE_URL points to PostgreSQL."""
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


def _get_db_connection(db_path: str | None = None):
    """
    Get a database connection, supporting both PostgreSQL and SQLite.

    - If DATABASE_URL points to PostgreSQL, uses src.db.get_conn() (ignores db_path).
    - Otherwise, falls back to SQLite via src.db.get_connection() or sqlite3.connect().
    """
    if _is_postgres_configured():
        from src.db import get_conn

        log.info("Using PostgreSQL connection via get_conn()")
        return get_conn()

    # SQLite legacy path
    path = db_path or os.getenv("DB_PATH") or "data/dev.db"

    try:
        from src.db import get_connection

        log.info("Using SQLite connection via get_connection(): %s", path)
        return get_connection(path)
    except (ImportError, RuntimeError):
        # Fallback: direct sqlite3 if src.db isn't available or ALLOW_SQLITE_DEV is off
        import sqlite3

        log.info("Using direct sqlite3 connection: %s", path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn


# ---------------------------------------------------------------------------
# Lead document iteration
# ---------------------------------------------------------------------------


def iter_leads_for_search(conn: Any) -> Iterator[dict[str, Any]]:
    """
    Yield JSON-serializable lead documents suitable for a search engine mirror.

    Document shape (keys are stable; values may be None/missing depending on schema):

        {
          "id": "email:alice.anderson@crestwellpartners.com",
          "email": "alice.anderson@crestwellpartners.com",
          "first_name": "Alice",
          "last_name": "Anderson",
          "full_name": "Alice Anderson",
          "title": "VP of Sales",
          "role_family": "sales",
          "seniority": "vp",
          "company": "Crestwell Partners",
          "domain": "crestwellpartners.com",
          "verify_status": "valid",
          "icp_score": 92,
          "industry": "B2B SaaS",
          "company_size": "50-200",
          "tech_keywords": ["salesforce", "hubspot"],
          "verified_at": "2025-11-28T20:00:00Z",
          "source_url": "https://example.com/source"
        }

    This is intentionally close to what R22/R23's /leads/search will want.
    """
    sql = """
        SELECT
          ve.email          AS email,
          ve.person_id      AS person_id,
          ve.verify_status  AS verify_status,
          ve.verified_at    AS verified_at,
          ve.source_url     AS email_source_url,

          p.first_name      AS first_name,
          p.last_name       AS last_name,
          p.full_name       AS full_name,
          p.title           AS title,
          p.title_norm      AS title_norm,
          p.role_family     AS role_family,
          p.seniority       AS seniority,
          p.icp_score       AS icp_score,
          p.source_url      AS person_source_url,

          c.id              AS company_id,
          c.name            AS company_name_raw,
          c.name_norm       AS company_name_norm,
          c.domain          AS company_domain_raw,
          c.official_domain AS company_domain_official,
          c.website_url     AS company_website_url,
          c.attrs           AS company_attrs
        FROM v_emails_latest AS ve
        JOIN people AS p
          ON p.id = ve.person_id
        JOIN companies AS c
          ON c.id = p.company_id
    """

    cur = conn.execute(sql)
    for row in cur:
        email = row["email"]

        full_name = row["full_name"]
        if not full_name:
            fn = row["first_name"] or ""
            ln = row["last_name"] or ""
            full_name = (fn + " " + ln).strip() or None

        title = row["title_norm"] or row["title"]

        company_name = row["company_name_norm"] or row["company_name_raw"]
        domain = row["company_domain_official"] or row["company_domain_raw"]

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
                pass

        doc: dict[str, Any] = {
            "id": f"email:{email}",
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
        help="Path to SQLite database (default: data/dev.db). "
        "Ignored when DATABASE_URL points to PostgreSQL.",
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

    db_label = "PostgreSQL" if _is_postgres_configured() else f"SQLite at {args.db_path}"
    print(f"[O13] Using database: {db_label}", file=sys.stderr)

    conn = _get_db_connection(args.db_path)
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
        try:
            conn.close()
        except Exception:
            log.debug("Error closing DB connection", exc_info=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
