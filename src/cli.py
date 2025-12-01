# src/cli.py
from __future__ import annotations

import argparse
import json
from typing import Any

from src.admin.metrics import get_admin_summary, get_analytics_summary


def _section(title: str) -> None:
    """
    Print a simple section heading used by the human-readable admin status
    output. The exact format is asserted in tests.
    """
    print(f"=== {title} ===")


def _print_queues(queues: list[dict[str, Any]]) -> None:
    _section("Queues")

    if not queues:
        print("  (no queues reported)")
        print()
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
    print()


def _print_workers(workers: list[dict[str, Any]]) -> None:
    _section("Workers")

    if not workers:
        print("  (no workers reported)")
        print()
        return

    header = f"{'name':18} {'state':10} queues"
    print("  " + header)
    print("  " + "-" * len(header))

    for w in workers:
        name = str(w.get("name", ""))
        state = str(w.get("state", "unknown"))
        queues = ", ".join(w.get("queues", []) or [])
        print(f"  {name:18} {state:10} {queues}")

    print()


def _print_verification_summary(verification: dict[str, Any]) -> None:
    _section("Verification summary")

    total = int(verification.get("total_emails", 0) or 0)
    valid_rate = float(verification.get("valid_rate", 0.0) or 0.0)
    by_status = verification.get("by_status") or {}

    print(f"  Total emails : {total}")
    print(f"  Valid rate   : {valid_rate * 100:.1f}%")

    if not by_status:
        print("  (no by-status breakdown)")
        print()
        return

    print()
    header = f"{'status':20} {'count':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    for status, count in sorted(by_status.items()):
        print(f"  {status:20} {int(count or 0):8d}")

    print()


def _print_cost_proxies(costs: dict[str, Any]) -> None:
    _section("Cost proxies")

    smtp_probes = int(costs.get("smtp_probes", 0) or 0)
    catchall_checks = int(costs.get("catchall_checks", 0) or 0)
    domains_resolved = int(costs.get("domains_resolved", 0) or 0)
    pages_crawled = int(costs.get("pages_crawled", 0) or 0)

    print(f"  SMTP probes     : {smtp_probes}")
    print(f"  Catch-all checks: {catchall_checks}")
    print(f"  Domains resolved: {domains_resolved}")
    print(f"  Pages crawled   : {pages_crawled}")
    print()


def _print_verification_time_series(points: list[dict[str, Any]]) -> None:
    _section("Verification time series")

    if not points:
        print("  (no verification history yet)")
        print()
        return

    header = (
        f"{'date':12} {'total':>8} {'valid':>8} {'invalid':>8} {'catch_all':>10} {'valid_rate':>12}"
    )
    print("  " + header)
    print("  " + "-" * len(header))

    for p in points:
        date = str(p.get("date", ""))
        total = int(p.get("total", 0) or 0)
        valid = int(p.get("valid", 0) or 0)
        invalid = int(p.get("invalid", 0) or 0)
        risky = int(p.get("risky_catch_all", 0) or 0)
        valid_rate = float(p.get("valid_rate", 0.0) or 0.0)

        print(
            f"  {date:12} {total:8d} {valid:8d} {invalid:8d} {risky:10d} {valid_rate * 100:11.1f}%"
        )

    print()


def _print_top_domains(domains: list[dict[str, Any]]) -> None:
    _section("Top domains")

    if not domains:
        print("  (no domain breakdown available)")
        print()
        return

    header = f"{'domain':30} {'total':>8} {'valid_rate':>12}"
    print("  " + header)
    print("  " + "-" * len(header))

    for d in domains:
        domain = str(d.get("domain", ""))
        total = int(d.get("total", 0) or 0)
        valid_rate = float(d.get("valid_rate", 0.0) or 0.0)

        print(f"  {domain:30} {total:8d} {valid_rate * 100:11.1f}%")

    print()


def _print_top_errors(errors: dict[str, Any]) -> None:
    _section("Top errors")

    if not errors:
        print("  (no error breakdown available)")
        print()
        return

    header = f"{'error':30} {'count':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    for key, count in sorted(errors.items()):
        print(f"  {key:30} {int(count or 0):8d}")

    print()


def _admin_status_human(
    summary: dict[str, Any],
    analytics: dict[str, Any],
) -> int:
    """
    Render admin status (summary + analytics) in a human-readable format.
    """
    print("Email Scraper – Admin status")
    print("============================")

    queues = list(summary.get("queues", []))
    workers = list(summary.get("workers", []))
    verification = dict(summary.get("verification", {}) or {})
    costs = dict(summary.get("costs", {}) or {})

    timeseries = list(analytics.get("verification_time_series", []) or [])
    domains = list(analytics.get("domain_breakdown", []) or [])
    errors = dict(analytics.get("error_breakdown", {}) or {})

    _print_queues(queues)
    _print_workers(workers)
    _print_verification_summary(verification)
    _print_cost_proxies(costs)
    _print_verification_time_series(timeseries)
    _print_top_domains(domains)
    _print_top_errors(errors)

    print("Tip: use '--json' for machine-readable output.")
    return 0


def _admin_status_json(
    summary: dict[str, Any],
    analytics: dict[str, Any],
) -> int:
    payload = {
        "summary": summary,
        "analytics": analytics,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_admin_status(args: argparse.Namespace) -> int:
    """
    O20: Admin status command that reuses the same metrics + analytics
    helpers as the web admin UI, so both surfaces stay in sync.
    """
    summary = get_admin_summary()
    analytics = get_analytics_summary(
        window_days=args.window_days,
        top_domains=args.top_domains,
        top_errors=args.top_errors,
    )

    if args.json:
        return _admin_status_json(summary, analytics)
    return _admin_status_human(summary, analytics)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="email-scraper",
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
        help="Show queue/worker/verification/cost summary plus analytics.",
    )
    status_parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Number of days of verification history for analytics (default: 30).",
    )
    status_parser.add_argument(
        "--top-domains",
        type=int,
        default=20,
        help="Number of domains to include in the domain breakdown (default: 20).",
    )
    status_parser.add_argument(
        "--top-errors",
        type=int,
        default=20,
        help="Number of error keys to include in the error breakdown (default: 20).",
    )
    status_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON with summary + analytics instead of human-readable tables.",
    )
    status_parser.set_defaults(func=_cmd_admin_status)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    func = getattr(args, "func", None)
    if func is None:
        parser.error("no command specified")
        return 1

    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
