# scripts/probe_smtp.py
from __future__ import annotations

r"""
R16/R18 CLI — SMTP RCPT TO probe + R18 status debug

Key behavior:
  - Resolve MX via src.resolve.mx.get_or_resolve_mx() / resolve_mx()
  - Probe RCPT TO via src.verify.smtp.probe_rcpt
  - Best-effort print latest verification_results row

Safety:
  - If MX resolution fails (lowest_mx is None), we refuse to probe the bare domain.
    Bare-domain A/AAAA often points to a CDN/Cloudflare and will always time out on :25.
"""

import argparse
import os
import sqlite3
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
      - failure: str | None (if present)
    """
    try:  # pragma: no cover
        from src.resolve.mx import get_or_resolve_mx as _gomx  # type: ignore

        return _gomx(domain, force=force, db_path=db_path)
    except Exception:
        pass

    try:  # pragma: no cover
        from src.resolve.mx import resolve_mx as _resolve_mx  # type: ignore

        return _resolve_mx(company_id=0, domain=domain, force=force, db_path=db_path)
    except Exception:
        return SimpleNamespace(lowest_mx=None, behavior=None, mx_behavior=None, failure="mx_resolver_unavailable")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="probe_smtp.py",
        description="R16/R18: Probe an email via RCPT TO and show R18 verify_status from the DB when available.",
    )
    p.add_argument("--email", required=True, help="Target email address to probe (e.g., someone@example.com).")
    p.add_argument("--mx-host", default=None, help="Optional MX host (e.g., aspmx.l.google.com). If omitted, resolve via R15.")
    p.add_argument("--force-resolve", action="store_true", help="Force R15 to refresh the cached MX before probing.")
    return p.parse_args()


def _load_latest_verification(db_path: str, email: str, domain: str) -> dict[str, Any] | None:
    email_norm = (email or "").strip().lower()
    dom = (domain or "").strip().lower()
    if not email_norm:
        return None

    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
    except Exception:
        return None

    try:
        row = con.execute("SELECT id FROM emails WHERE email = ?", (email_norm,)).fetchone()
        if not row:
            return None
        email_id = int(row["id"])

        vrow = con.execute(
            """
            SELECT
              verify_status,
              verify_reason,
              verified_mx,
              verified_at,
              fallback_status
            FROM verification_results
            WHERE email_id = ?
            ORDER BY COALESCE(verified_at, checked_at) DESC, id DESC
            LIMIT 1
            """,
            (email_id,),
        ).fetchone()
        if not vrow:
            return None

        drow = con.execute(
            """
            SELECT catch_all_status
            FROM domain_resolutions
            WHERE chosen_domain = ? OR user_hint = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (dom, dom),
        ).fetchone()
        catch_all_status = drow["catch_all_status"] if drow and "catch_all_status" in drow.keys() else None

        return {
            "verify_status": vrow["verify_status"],
            "verify_reason": vrow["verify_reason"],
            "verified_mx": vrow["verified_mx"],
            "verified_at": vrow["verified_at"],
            "fallback_status": vrow["fallback_status"],
            "catch_all_status": catch_all_status,
        }
    except Exception:
        return None
    finally:
        try:
            con.close()
        except Exception:
            pass


def main() -> None:
    args = _parse_args()

    try:
        domain = args.email.split("@", 1)[1].strip().lower()
    except Exception as err:
        print("Error: --email must contain a single '@' with a domain part.")
        raise SystemExit(2) from err

    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"

    if args.mx_host:
        mx_host = args.mx_host.strip()
        behavior_hint = None
    else:
        mx_info = _get_or_resolve_mx(domain, force=bool(args.force_resolve), db_path=db_path)
        mx_host = getattr(mx_info, "lowest_mx", None)
        behavior_hint = getattr(mx_info, "behavior", None) or getattr(mx_info, "mx_behavior", None)
        mx_failure = getattr(mx_info, "failure", None)

        if not mx_host:
            print(f"Email:          {args.email}")
            print(f"Domain:         {domain}")
            print("MX host:        (unresolved)")
            if mx_failure:
                print(f"MX failure:     {mx_failure}")
            print()
            print("Refusing to probe bare domain on port 25. Pass --mx-host explicitly or fix MX resolution.")
            raise SystemExit(2)

    result = probe_rcpt(
        args.email,
        mx_host,
        helo_domain=SMTP_HELO_DOMAIN,
        mail_from=SMTP_MAIL_FROM,
        connect_timeout=SMTP_CONNECT_TIMEOUT,
        command_timeout=SMTP_COMMAND_TIMEOUT,
        behavior_hint=behavior_hint,
    )

    category = result.get("category")
    code = result.get("code")
    err = result.get("error")
    msg = result.get("message")

    print(f"Email:          {args.email}")
    print(f"Domain:         {domain}")
    print(f"MX host:        {result.get('mx_host') or mx_host}")
    print(f"HELO:           {result.get('helo_domain') or SMTP_HELO_DOMAIN}")
    print(f"RCPT category:  {category}")
    if code is not None or msg:
        msg_part = (msg or "").strip()
        print(f"RCPT code/msg:  {code} {msg_part}".rstrip())
    if err:
        print(f"RCPT error:     {err}")
    print(f"Elapsed:        {int(result.get('elapsed_ms') or 0)} ms")

    vr = _load_latest_verification(db_path, args.email, domain)
    if vr is None:
        print()
        print("R18: no verification_results row found for this email in DB.")
    else:
        print()
        print("R18 classification (verification_results):")
        print(f"  Verify status : {vr.get('verify_status')}")
        print(f"  Reason        : {vr.get('verify_reason')}")
        print(f"  MX host       : {vr.get('verified_mx')}")
        print(f"  Verified at   : {vr.get('verified_at')}")
        print()
        print(f"  Catch-all     : {vr.get('catch_all_status') or '(unknown)'}")
        fb = vr.get("fallback_status") or "(none)"
        print(f"  Fallback      : {fb}")


if __name__ == "__main__":
    main()
