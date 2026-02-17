# scripts/export_leads.py
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from collections.abc import Iterable
from pathlib import Path

from src.export.exporter import ExportLead, iter_exportable_leads

log = logging.getLogger(__name__)


def _get_db_connection(db_path: str | None = None):
    """
    Get a database connection, supporting both PostgreSQL and SQLite.

    - If DATABASE_URL points to PostgreSQL, uses src.db.get_conn() (ignores db_path).
    - Otherwise, falls back to src.db.get_connection(db_path) for SQLite legacy mode.
    """
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    is_pg = url.startswith("postgres://") or url.startswith("postgresql://")

    if is_pg:
        from src.db import get_conn

        log.info("Using PostgreSQL connection via get_conn()")
        return get_conn()

    # SQLite legacy path
    from src.db import get_connection

    path = db_path or os.getenv("DB_PATH") or "data/dev.db"
    log.info("Using SQLite connection: %s", path)
    return get_connection(path)


# Public alias — tests monkeypatch this name
get_connection = _get_db_connection


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export verified leads.")
    parser.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite DB file (default: data/dev.db). "
        "Ignored when DATABASE_URL points to PostgreSQL.",
    )
    parser.add_argument(
        "--policy",
        default="default",
        help="Export policy name from config (default: default).",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl"],
        default="csv",
        help="Output format (csv or jsonl).",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output file path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    out_path = Path(args.output)

    conn = get_connection(args.db)
    try:
        leads = iter_exportable_leads(conn, policy_name=args.policy)
        if args.format == "csv":
            _write_csv(out_path, leads)
        else:
            _write_jsonl(out_path, leads)
    finally:
        # Be defensive so tests can monkeypatch to return
        # a simple dummy object without a close() method.
        close = getattr(conn, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                log.debug("Error closing DB connection", exc_info=True)


def _write_csv(path: Path, leads: Iterable[ExportLead]) -> None:
    fieldnames = [
        "email",
        "first_name",
        "last_name",
        "title",
        "company",
        "domain",
        "source_url",
        "icp_score",
        "verify_status",
        "verified_at",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            writer.writerow(
                {
                    "email": lead.email,
                    "first_name": lead.first_name,
                    "last_name": lead.last_name,
                    "title": lead.title,
                    "company": lead.company,
                    "domain": lead.domain,
                    "source_url": lead.source_url,
                    "icp_score": lead.icp_score,
                    "verify_status": lead.verify_status,
                    "verified_at": lead.verified_at,
                }
            )


def _write_jsonl(path: Path, leads: Iterable[ExportLead]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for lead in leads:
            f.write(
                json.dumps(
                    {
                        "email": lead.email,
                        "first_name": lead.first_name,
                        "last_name": lead.last_name,
                        "title": lead.title,
                        "company": lead.company,
                        "domain": lead.domain,
                        "source_url": lead.source_url,
                        "icp_score": lead.icp_score,
                        "verify_status": lead.verify_status,
                        "verified_at": lead.verified_at,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


if __name__ == "__main__":
    main()
