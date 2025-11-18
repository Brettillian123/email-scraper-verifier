# scripts/probe_smtp.py
from __future__ import annotations

r"""
R16 CLI — SMTP RCPT TO probe

Usage examples (PowerShell):
  # Minimal (resolve MX via R15 cache/resolver)
  #   $PyExe .\scripts\probe_smtp.py --email "someone@gmail.com"
  #
  # Force re-resolve MX even if cached:
  #   $PyExe .\scripts\probe_smtp.py --email "user@example.com" --force-resolve
  #
  # Specify an MX host explicitly (skips resolver):
  #   $PyExe .\scripts\probe_smtp.py --email "user@example.com" --mx-host "aspmx.l.google.com"

Behavior:
  - Uses src.resolve.mx.get_or_resolve_mx() when available, falling back to resolve_mx().
  - Reads identity/timeouts from src.config (SMTP_HELO_DOMAIN, SMTP_MAIL_FROM, etc.).
  - Returns a human-friendly printout with category/code and elapsed time.
"""

import argparse
import os
from types import SimpleNamespace
from typing import Any

from src.config import (
    SMTP_COMMAND_TIMEOUT,
    SMTP_CONNECT_TIMEOUT,
    SMTP_HELO_DOMAIN,
    SMTP_MAIL_FROM,
)
from src.verify.smtp import probe_rcpt


def _get_or_resolve_mx(domain: str, *, force: bool, db_path: str | None) -> Any:
    """
    Prefer a helper from src.resolve.mx; fall back to resolve_mx().
    Returns an object with attributes:
      - lowest_mx: str | None
      - behavior or mx_behavior: dict | None
    """
    try:  # pragma: no cover
        from src.resolve.mx import get_or_resolve_mx as _gomx  # type: ignore

        return _gomx(domain, force=force, db_path=db_path)
    except Exception:
        pass

    # Fallback: call resolve_mx(company_id=0, ...) and adapt to a simple namespace
    try:  # pragma: no cover
        from src.resolve.mx import resolve_mx as _resolve_mx  # type: ignore

        res = _resolve_mx(company_id=0, domain=domain, force=force, db_path=db_path)
        # resolve_mx returns MXResult without behavior; that's fine (we pass None)
        return res
    except Exception:
        # Last resort: no resolver available; just return a stub that points at the domain
        return SimpleNamespace(lowest_mx=domain, behavior=None, mx_behavior=None)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="probe_smtp.py",
        description="R16: Probe an email address via RCPT TO on the target domain's MX.",
    )
    p.add_argument(
        "--email",
        required=True,
        help="Target email address to probe (e.g., someone@example.com).",
    )
    p.add_argument(
        "--mx-host",
        default=None,
        help="Optional MX host (e.g., aspmx.l.google.com). If omitted, resolve via R15.",
    )
    p.add_argument(
        "--force-resolve",
        action="store_true",
        help="Force R15 to refresh the cached MX before probing.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    # Derive domain from the email (simple split; upstream validation happens in probe_rcpt)
    try:
        domain = args.email.split("@", 1)[1].strip().lower()
    except Exception as err:
        print("Error: --email must contain a single '@' with a domain part.")
        raise SystemExit(2) from err

    # Use DB path convention from other scripts (R15 resolver expects this)
    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"

    # Resolve MX host unless explicitly provided
    if args.mx_host:
        mx_host = args.mx_host.strip()
        behavior_hint = None
    else:
        mx_info = _get_or_resolve_mx(domain, force=bool(args.force_resolve), db_path=db_path)
        mx_host = getattr(mx_info, "lowest_mx", None) or domain
        behavior_hint = getattr(mx_info, "behavior", None) or getattr(mx_info, "mx_behavior", None)

    # Execute the probe using config-driven identity & timeouts
    result = probe_rcpt(
        args.email,
        mx_host,
        helo_domain=SMTP_HELO_DOMAIN,
        mail_from=SMTP_MAIL_FROM,
        connect_timeout=SMTP_CONNECT_TIMEOUT,
        command_timeout=SMTP_COMMAND_TIMEOUT,
        behavior_hint=behavior_hint,
    )

    # Pretty-print
    print(f"Target : {args.email}")
    print(f"MX     : {result.get('mx_host') or mx_host}")
    print(f"HELO   : {result.get('helo_domain') or SMTP_HELO_DOMAIN}")
    category = result.get("category")
    code = result.get("code")
    err = result.get("error")
    msg = result.get("message")
    print(f"Result : {category} (code={code}, error={err})")
    if msg:
        print(f"Message: {msg}")
    print(f"Elapsed: {int(result.get('elapsed_ms') or 0)} ms")


if __name__ == "__main__":
    main()
