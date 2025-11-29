# scripts/export_leads.py
from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterable
from pathlib import Path

from src.db import get_connection
from src.export.exporter import ExportLead, iter_exportable_leads


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export verified leads.")
    parser.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite DB file (default: data/dev.db).",
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
        # Be defensive so tests can monkeypatch get_connection to return
        # a simple dummy object without a close() method.
        close = getattr(conn, "close", None)
        if callable(close):
            close()


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
