# scripts/cli.py
from __future__ import annotations

import argparse
import json
from typing import Any

from src.admin.metrics import get_admin_summary


def _print_section_title(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def _print_queues(queues: list[dict[str, Any]]) -> None:
    _print_section_title("Queues")

    if not queues:
        print("  (no queues reported)")
        return

    header = f"{'name':12} {'queued':>8} {'started':>8} {'failed':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    total_queued = 0
    total_failed = 0

    for q in queues:
        name = str(q.get("name", ""))
        queued = int(q.get("queued", 0) or 0)
        started = int(q.get("started", 0) or 0)
        failed = int(q.get("failed", 0) or 0)

        total_queued += queued
        total_failed += failed

        print(f"  {name:12} {queued:8d} {started:8d} {failed:8d}")

    print()
    print(f"  Total queued: {total_queued}")
    print(f"  Total failed: {total_failed}")


def _print_workers(workers: list[dict[str, Any]]) -> None:
    _print_section_title("Workers")

    if not workers:
        print("  (no workers reported)")
        return

    header = f"{'name':18} {'state':10} queues"
    print("  " + header)
    print("  " + "-" * len(header))

    for w in workers:
        name = str(w.get("name", ""))
        state = str(w.get("state", "unknown"))
        queues = ", ".join(w.get("queues", []) or [])
        print(f"  {name:18} {state:10} {queues}")


def _print_verification(verification: dict[str, Any]) -> None:
    _print_section_title("Verification")

    total = int(verification.get("total_emails", 0) or 0)
    valid_rate = float(verification.get("valid_rate", 0.0) or 0.0)
    by_status = verification.get("by_status") or {}

    print(f"  Total emails: {total}")
    print(f"  Valid rate : {valid_rate * 100:.1f}%")

    if not by_status:
        print("  (no by-status breakdown)")
        return

    print()
    header = f"{'status':20} {'count':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    for status, count in sorted(by_status.items()):
        print(f"  {status:20} {int(count or 0):8d}")


def _print_costs(costs: dict[str, Any]) -> None:
    _print_section_title("Cost proxies")

    smtp_probes = int(costs.get("smtp_probes", 0) or 0)
    catchall_checks = int(costs.get("catchall_checks", 0) or 0)
    domains_resolved = int(costs.get("domains_resolved", 0) or 0)
    pages_crawled = int(costs.get("pages_crawled", 0) or 0)

    print(f"  SMTP probes     : {smtp_probes}")
    print(f"  Catch-all checks: {catchall_checks}")
    print(f"  Domains resolved: {domains_resolved}")
    print(f"  Pages crawled   : {pages_crawled}")


def cmd_admin_status(format_: str) -> int:
    """
    O20: Print a human-friendly summary of the admin metrics.

    This reuses the same metrics service used by /admin/metrics so CLI and
    web UI stay in sync.
    """
    summary = get_admin_summary()

    if format_ == "json":
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    print("Email Scraper – Admin status")
    print("============================")

    queues = list(summary.get("queues", []))
    workers = list(summary.get("workers", []))
    verification = dict(summary.get("verification", {}) or {})
    costs = dict(summary.get("costs", {}) or {})

    _print_queues(queues)
    _print_workers(workers)
    _print_verification(verification)
    _print_costs(costs)

    print()
    print("Tip: use '--format json' for machine-readable output.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli",
        description="Email Scraper CLI for batch operations and admin status.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Admin/ops commands
    admin_parser = subparsers.add_parser(
        "admin",
        help="Admin and ops commands (status, etc.).",
    )
    admin_subparsers = admin_parser.add_subparsers(
        dest="admin_command",
        required=True,
    )

    status_parser = admin_subparsers.add_parser(
        "status",
        help="Show queue/worker/verification/cost summary (O20).",
    )
    status_parser.add_argument(
        "--format",
        dest="format_",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "admin" and args.admin_command == "status":
        return cmd_admin_status(format_=args.format_)

    parser.error("unknown command")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
