#!/usr/bin/env python
# scripts/diagnose_catchall.py
"""
Diagnostic script to check catch-all domain handling issues.

Helps identify why emails might be labeled "valid" on catch-all domains.

Usage:
  python scripts/diagnose_catchall.py
  python scripts/diagnose_catchall.py --domain openspace.ai
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] if Path(__file__).resolve().parents else Path.cwd()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _count_by_status(conn, domain: str, status: str) -> int:
    """Count emails matching a given verify_status for a domain."""
    result = conn.execute("""
        SELECT COUNT(*) FROM emails e
        JOIN verification_results vr ON vr.email_id = e.id
        WHERE vr.verify_status = %s
          AND e.email LIKE %s
    """, (status, f"%@{domain}"))
    return result.fetchone()[0]


def _get_valid_domains(conn, domain_filter: str | None) -> list[str]:
    """Return list of domains to check (either user-specified or from DB)."""
    if domain_filter:
        return [domain_filter]
    result = conn.execute("""
        SELECT DISTINCT SPLIT_PART(e.email, '@', 2) as domain
        FROM emails e
        JOIN verification_results vr ON vr.email_id = e.id
        WHERE vr.verify_status = 'valid'
        LIMIT 20
    """)
    return [row[0] for row in result.fetchall()]


# ---------------------------------------------------------------------------
# Diagnostic steps (each is its own function to keep complexity low)
# ---------------------------------------------------------------------------


def _step1_find_valid_emails(conn, domain_filter: str | None) -> None:
    """[1] Find emails labeled 'valid'."""
    print("\n[1] Finding emails labeled 'valid'...")
    try:
        if domain_filter:
            result = conn.execute("""
                SELECT e.id, e.email, vr.verify_status,
                       vr.verify_reason, vr.verified_at
                FROM emails e
                JOIN verification_results vr ON vr.email_id = e.id
                WHERE vr.verify_status = 'valid'
                  AND e.email LIKE %s
                ORDER BY e.id DESC LIMIT 20
            """, (f"%@{domain_filter}",))
        else:
            result = conn.execute("""
                SELECT e.id, e.email, vr.verify_status,
                       vr.verify_reason, vr.verified_at
                FROM emails e
                JOIN verification_results vr ON vr.email_id = e.id
                WHERE vr.verify_status = 'valid'
                ORDER BY e.id DESC LIMIT 20
            """)
        rows = result.fetchall()
        print(f"    Found {len(rows)} 'valid' emails")
        for row in rows[:10]:
            email_id, email, status, reason, verified_at = row
            domain = email.split("@")[1] if "@" in email else "unknown"
            print(f"    ID={email_id}: {email}")
            print(f"      verify_status={status}, reason={reason}")
            print(f"      verified_at={verified_at}, domain={domain}\n")
    except Exception as e:
        print(f"    Error: {e}")
        import traceback
        traceback.print_exc()


def _step2_check_domain_resolutions(conn, domains: list[str]) -> None:
    """[2] Check domain_resolutions for catch-all status."""
    print("\n[2] Checking domain_resolutions for catch-all status...")
    try:
        for domain in domains:
            result = conn.execute("""
                SELECT id, chosen_domain, catch_all_status,
                       catch_all_checked_at, catch_all_smtp_code
                FROM domain_resolutions
                WHERE chosen_domain = %s OR user_hint = %s
                ORDER BY id DESC LIMIT 1
            """, (domain, domain))
            row = result.fetchone()
            if not row:
                print(f"    {domain}: No domain_resolutions record found\n")
                continue

            _dr_id, _chosen, ca_status, ca_checked, ca_code = row
            print(f"    {domain}:")
            print(f"      catch_all_status = {ca_status!r}")
            print(f"      catch_all_smtp_code = {ca_code}")
            print(f"      catch_all_checked_at = {ca_checked}")

            if ca_status == "catch_all":
                valid_count = _count_by_status(conn, domain, "valid")
                if valid_count > 0:
                    print(
                        f"      ⚠️  WARNING: {valid_count} emails "
                        f"are 'valid' but domain is 'catch_all'!"
                    )
            elif ca_status == "not_catch_all":
                print("      (domain is NOT catch-all - 'valid' is correct)")
            else:
                print("      (catch-all status unknown/other)")
            print()
    except Exception as e:
        print(f"    Error: {e}")
        import traceback
        traceback.print_exc()


def _step3_compare_status_counts(conn, domains: list[str]) -> None:
    """[3] Show risky_catch_all emails for comparison."""
    print("\n[3] Finding 'risky_catch_all' emails for same domains...")
    try:
        for domain in domains[:5]:
            risky = _count_by_status(conn, domain, "risky_catch_all")
            valid = _count_by_status(conn, domain, "valid")
            invalid = _count_by_status(conn, domain, "invalid")
            print(f"    {domain}:")
            print(
                f"      valid={valid}, "
                f"risky_catch_all={risky}, "
                f"invalid={invalid}"
            )
    except Exception as e:
        print(f"    Error: {e}")


def _step4_find_misclassified(conn) -> None:
    """[4] Find emails marked 'valid' on catch-all domains."""
    print("\n[4] Finding emails marked 'valid' on catch-all domains...")
    try:
        result = conn.execute("""
            SELECT e.id, e.email, vr.id as vr_id,
                   vr.verify_status, vr.verify_reason,
                   dr.catch_all_status, dr.catch_all_checked_at
            FROM emails e
            JOIN verification_results vr ON vr.email_id = e.id
            JOIN domain_resolutions dr ON (
                dr.chosen_domain = SPLIT_PART(e.email, '@', 2)
                OR dr.user_hint = SPLIT_PART(e.email, '@', 2)
            )
            WHERE vr.verify_status = 'valid'
              AND dr.catch_all_status = 'catch_all'
            ORDER BY e.id DESC
        """)
        rows = result.fetchall()
        print(
            f"    Found {len(rows)} emails that should be "
            f"'risky_catch_all' instead of 'valid'"
        )
        if rows:
            print("\n    These emails are marked 'valid' but domain is catch-all:")
            for row in rows[:10]:
                eid, email, vr_id, vstatus, vreason, ca_st, ca_chk = row
                print(f"      email_id={eid}, vr_id={vr_id}: {email}")
                print(f"        verify_status={vstatus}, reason={vreason}")
                print(
                    f"        domain catch_all_status={ca_st} "
                    f"(checked: {ca_chk})"
                )
                print()
            print("\n    To fix these, run with --fix-catchall")
    except Exception as e:
        print(f"    Error: {e}")
        import traceback
        traceback.print_exc()


def _step5_reason_distribution(conn) -> None:
    """[5] Check verify_reason distribution and find stub-verified emails."""
    print("\n[5] Checking verify_reason distribution for 'valid' emails...")
    try:
        result = conn.execute("""
            SELECT vr.verify_reason, COUNT(*) as cnt
            FROM verification_results vr
            WHERE vr.verify_status = 'valid'
            GROUP BY vr.verify_reason
            ORDER BY cnt DESC
        """)
        for reason, cnt in result.fetchall():
            is_stub = reason in ("stub", "stub_not_verified")
            warning = " ⚠️  STUB - not actually verified!" if is_stub else ""
            print(f"    {reason}: {cnt}{warning}")
    except Exception as e:
        print(f"    Error: {e}")

    print("\n[5b] Finding emails with 'stub' verification (not actually verified)...")
    try:
        result = conn.execute("""
            SELECT e.id, e.email, vr.id as vr_id,
                   vr.verify_status, vr.verify_reason
            FROM emails e
            JOIN verification_results vr ON vr.email_id = e.id
            WHERE vr.verify_reason IN ('stub', 'stub_not_verified')
            ORDER BY e.id DESC LIMIT 20
        """)
        rows = result.fetchall()
        print(f"    Found {len(rows)} emails with stub verification")
        if rows:
            print("\n    These emails were never actually SMTP verified:")
            for eid, email, vr_id, vstatus, vreason in rows[:10]:
                print(f"      email_id={eid}, vr_id={vr_id}: {email}")
                print(f"        verify_status={vstatus}, reason={vreason}")
            print("\n    Run with --fix-stubs to delete these and re-verify")
    except Exception as e:
        print(f"    Error: {e}")


def _step6_fix_catchall(conn) -> None:
    """[6] Fix misclassified emails (valid -> risky_catch_all)."""
    print("\n[6] Fixing misclassified emails (valid -> risky_catch_all)...")
    try:
        result = conn.execute("""
            SELECT vr.id
            FROM verification_results vr
            JOIN emails e ON e.id = vr.email_id
            JOIN domain_resolutions dr ON (
                dr.chosen_domain = SPLIT_PART(e.email, '@', 2)
                OR dr.user_hint = SPLIT_PART(e.email, '@', 2)
            )
            WHERE vr.verify_status = 'valid'
              AND dr.catch_all_status = 'catch_all'
        """)
        vr_ids = [row[0] for row in result.fetchall()]
        if vr_ids:
            placeholders = ",".join(["%s"] * len(vr_ids))
            conn.execute(f"""
                UPDATE verification_results
                SET verify_status = 'risky_catch_all',
                    verify_reason = 'fixed_from_valid_on_catchall_domain'
                WHERE id IN ({placeholders})
            """, tuple(vr_ids))
            conn.commit()
            print(f"    Fixed {len(vr_ids)} verification results")
        else:
            print("    No misclassified emails found to fix")
    except Exception as e:
        print(f"    Error fixing: {e}")
        import traceback
        traceback.print_exc()


def _step7_fix_stubs(conn) -> None:
    """[7] Fix stub verifications (delete so they can be re-verified)."""
    print("\n[7] Fixing stub verifications (deleting so they can be re-verified)...")
    try:
        result = conn.execute("""
            SELECT vr.id, vr.email_id
            FROM verification_results vr
            WHERE vr.verify_reason IN ('stub', 'stub_not_verified')
        """)
        rows = result.fetchall()
        if not rows:
            print("    No stub verifications found to fix")
            return

        vr_ids = [row[0] for row in rows]
        email_ids = list(set(row[1] for row in rows if row[1]))

        placeholders = ",".join(["%s"] * len(vr_ids))
        conn.execute(f"""
            DELETE FROM verification_results
            WHERE id IN ({placeholders})
        """, tuple(vr_ids))
        conn.commit()
        print(f"    Deleted {len(vr_ids)} stub verification results")

        print(f"    Email IDs that need re-verification: {email_ids}")
        print("    You can re-verify these by running:")
        for eid in email_ids[:5]:
            print(
                f"      python -c \"from src.queueing.tasks "
                f"import task_probe_email; "
                f"task_probe_email(email_id={eid}, email='', domain='')\""
            )
        if len(email_ids) > 5:
            print(f"      ... and {len(email_ids) - 5} more")
    except Exception as e:
        print(f"    Error fixing: {e}")
        import traceback
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose catch-all domain issues",
    )
    parser.add_argument("--domain", help="Specific domain to check")
    parser.add_argument("--db", dest="db_url", help="Database URL")
    parser.add_argument(
        "--fix-catchall", action="store_true",
        help="Fix emails marked 'valid' that should be 'risky_catch_all'",
    )
    parser.add_argument(
        "--fix-stubs", action="store_true",
        help=(
            "Fix emails with 'stub' verification by marking them "
            "for re-verification"
        ),
    )
    args = parser.parse_args()

    if args.db_url:
        os.environ["DATABASE_URL"] = args.db_url

    try:
        from src.db import get_conn
    except ImportError:
        sys.path.insert(0, str(ROOT))
        from src.db import get_conn

    with get_conn() as conn:
        print("=" * 70)
        print("CATCH-ALL DOMAIN DIAGNOSTICS")
        print("=" * 70)

        _step1_find_valid_emails(conn, args.domain)

        domains = _get_valid_domains(conn, args.domain)
        _step2_check_domain_resolutions(conn, domains)
        _step3_compare_status_counts(conn, domains)
        _step4_find_misclassified(conn)
        _step5_reason_distribution(conn)

        if args.fix_catchall:
            _step6_fix_catchall(conn)
        if args.fix_stubs:
            _step7_fix_stubs(conn)

        print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
