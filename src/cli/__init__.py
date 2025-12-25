from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections.abc import Sequence
from typing import Any, TextIO

from src.admin.metrics import get_admin_summary, get_analytics_summary


def _print_header(title: str, out: TextIO) -> None:
    out.write(f"\n=== {title} ===\n")


def _print_kv(label: str, value: Any, out: TextIO) -> None:
    out.write(f"{label}: {value}\n")


def _json_default(obj: Any) -> Any:
    """
    JSON serializer for objects not serializable by default json code.

    Currently:
      - datetime/date -> ISO8601 string
      - everything else -> str(obj)
    """
    if isinstance(obj, (dt.datetime, dt.date)):
        return obj.isoformat()
    return str(obj)


def _print_admin_status_human(
    summary: dict[str, Any], analytics: dict[str, Any], out: TextIO
) -> None:
    """
    Human-readable rendering for `admin status`.

    This intentionally mirrors the structure of the HTML dashboard so you can
    quickly sanity-check things over SSH or CI logs.
    """
    queues = summary.get("queues", []) or []
    workers = summary.get("workers", []) or []
    verification = summary.get("verification", {}) or {}
    costs = summary.get("costs", {}) or {}

    time_series = analytics.get("verification_time_series", []) or []
    domains = analytics.get("domain_breakdown", []) or []
    errors = analytics.get("error_breakdown", {}) or {}

    # Queues
    _print_header("Queues", out)
    total_queued = sum(int(q.get("queued", 0)) for q in queues)
    total_failed = sum(int(q.get("failed", 0)) for q in queues)
    _print_kv("Total queued", total_queued, out)
    _print_kv("Total failed", total_failed, out)
    if not queues:
        out.write("(no queues reported)\n")
    else:
        for q in queues:
            out.write(
                f"- {q.get('name', '(unknown)')}: "
                f"queued={q.get('queued', 0)}, "
                f"started={q.get('started', 0)}, "
                f"failed={q.get('failed', 0)}\n"
            )

    # Workers
    _print_header("Workers", out)
    if not workers:
        out.write("(no workers reported)\n")
    else:
        for w in workers:
            queues_str = ", ".join(w.get("queues", []) or [])
            out.write(
                f"- {w.get('name', '(unnamed)')}: "
                f"state={w.get('state', 'unknown')}, "
                f"queues=[{queues_str}]\n"
            )

    # Verification summary
    _print_header("Verification summary", out)
    total_emails = verification.get("total_emails", 0)
    valid_rate = verification.get("valid_rate", 0.0)
    _print_kv("Total emails", total_emails, out)
    _print_kv("Valid rate", f"{valid_rate:.3f}", out)
    by_status = verification.get("by_status", {}) or {}
    if not by_status:
        out.write("(no verification breakdown yet)\n")
    else:
        out.write("By status:\n")
        for status, count in sorted(by_status.items()):
            out.write(f"  - {status}: {count}\n")

    # Cost counters
    _print_header("Cost proxies", out)
    smtp_probes = costs.get("smtp_probes", 0)
    catchall_checks = costs.get("catchall_checks", 0)
    domains_resolved = costs.get("domains_resolved", 0)
    pages_crawled = costs.get("pages_crawled", 0)
    _print_kv("SMTP probes", smtp_probes, out)
    _print_kv("Catch-all checks", catchall_checks, out)
    _print_kv("Domains resolved", domains_resolved, out)
    _print_kv("Pages crawled", pages_crawled, out)

    # Analytics: verification time series
    _print_header("Verification time series", out)
    if not time_series:
        out.write("(no verification history in window)\n")
    else:
        out.write("date       total  valid  invalid  catch_all  valid_rate\n")
        out.write("---------  -----  -----  -------  ---------  ----------\n")
        for p in time_series:
            date = p.get("date", "")
            total = int(p.get("total", 0))
            valid = int(p.get("valid", 0))
            invalid = int(p.get("invalid", 0))
            risky = int(p.get("risky_catch_all", 0))
            vr = float(p.get("valid_rate", 0.0))
            out.write(f"{date:10} {total:5d} {valid:6d} {invalid:8d} {risky:9d} {vr:10.3f}\n")

    # Analytics: domains
    _print_header("Top domains", out)
    if not domains:
        out.write("(no domain breakdown)\n")
    else:
        out.write("domain               total  valid_rate\n")
        out.write("-------------------  -----  ----------\n")
        for d in domains:
            domain = d.get("domain", "")
            total = int(d.get("total", 0))
            vr = float(d.get("valid_rate", 0.0))
            out.write(f"{domain:19} {total:5d} {vr:10.3f}\n")

    # Analytics: errors
    _print_header("Top errors", out)
    if not errors:
        out.write("(no error breakdown)\n")
    else:
        for key, count in sorted(errors.items(), key=lambda kv: int(kv[1]), reverse=True):
            out.write(f"- {key}: {int(count)}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="email-scraper",
        description="CLI for email-scraper admin/batch operations.",
    )
    subparsers = parser.add_subparsers(dest="command")
    # Python 3.11+ supports required=..., but for compatibility we check manually.
    # Admin group
    admin_parser = subparsers.add_parser(
        "admin",
        help="Admin/ops commands (queues, workers, verification, etc.).",
    )
    admin_subparsers = admin_parser.add_subparsers(dest="admin_command")

    status_parser = admin_subparsers.add_parser(
        "status",
        help="Show queues, workers, verification, cost proxies, and analytics.",
    )
    status_parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Days of verification history for analytics (default: 30).",
    )
    status_parser.add_argument(
        "--top-domains",
        type=int,
        default=10,
        help="Number of domains in breakdown (default: 10).",
    )
    status_parser.add_argument(
        "--top-errors",
        type=int,
        default=10,
        help="Number of error keys in breakdown (default: 10).",
    )
    status_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit full JSON (admin + analytics) instead of human-readable text.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point for the email-scraper CLI.

    Intended usage from PowerShell or bash:

        python -m src.cli admin status
        python -m src.cli admin status --json
        python -m src.cli admin status --window-days 7 --top-domains 5
    """
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command != "admin":
        parser.print_help(file=sys.stderr)
        return 1

    if args.admin_command != "status":
        parser.print_help(file=sys.stderr)
        return 1

    # Admin status
    summary = get_admin_summary()
    analytics = get_analytics_summary(
        window_days=int(args.window_days),
        top_domains=int(args.top_domains),
        top_errors=int(args.top_errors),
    )

    if args.json:
        payload = {
            "summary": summary,
            "analytics": analytics,
        }
        json.dump(
            payload,
            sys.stdout,
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
        sys.stdout.write("\n")
    else:
        _print_admin_status_human(summary, analytics, sys.stdout)

    return 0


__all__ = ["main"]
