#!/usr/bin/env python3
"""
Diagnostic script for catch-all detection issues.

Run this on your VPS to check:
1. What's in domain_resolutions for a domain
2. Whether catch-all detection is working
3. Why emails might be classified as "valid" instead of "risky_catch_all"

Usage:
    python diagnose_catchall.py crestwellpartners.com
    python diagnose_catchall.py crestwellpartners.com --probe  # Force fresh catch-all probe
    python diagnose_catchall.py crestwellpartners.com --fix    # Fix NULL catch_all_status
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime


def _get_conn():
    """Get DB connection using project helpers or fallback."""
    try:
        from src.db import get_conn

        return get_conn()
    except Exception:
        pass

    db_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH") or "data/dev.db"
    if db_url.startswith("sqlite"):
        import sqlite3

        path = db_url.replace("sqlite:///", "")
        return sqlite3.connect(path)

    raise RuntimeError("Could not connect to DB")


def diagnose_domain_resolutions(domain: str) -> dict:
    """Check all domain_resolutions rows for a domain."""
    dom = domain.strip().lower()
    con = _get_conn()

    print(f"\n{'=' * 60}")
    print(f"DOMAIN RESOLUTIONS for: {dom}")
    print(f"{'=' * 60}\n")

    try:
        # Get all potentially matching rows
        # NOTE: PostgreSQL schema uses chosen_domain and user_hint, NOT 'domain'
        rows = con.execute(
            """
            SELECT
                id,
                chosen_domain,
                user_hint,
                catch_all_status,
                catch_all_checked_at,
                catch_all_smtp_code,
                catch_all_localpart,
                created_at
            FROM domain_resolutions
            WHERE chosen_domain = ?
               OR user_hint = ?
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (dom, dom),
        ).fetchall()

        if not rows:
            print(f"❌ NO ROWS FOUND for domain '{dom}'")
            print("\n   This means catch-all detection has never run,")
            print("   or the domain was never resolved via R08/R15.")
            return {"found": False, "rows": []}

        print(f"Found {len(rows)} row(s):\n")

        results = []
        for i, row in enumerate(rows):
            r = {
                "id": row[0],
                "chosen_domain": row[1],
                "user_hint": row[2],
                "catch_all_status": row[3],
                "catch_all_checked_at": row[4],
                "catch_all_smtp_code": row[5],
                "catch_all_localpart": row[6],
                "created_at": row[7],
            }
            results.append(r)

            print(f"  Row #{i + 1} (id={r['id']}):")
            print(f"    chosen_domain:      {r['chosen_domain']!r}")
            print(f"    user_hint:          {r['user_hint']!r}")
            print(f"    catch_all_status:   {r['catch_all_status']!r}")
            print(f"    catch_all_checked:  {r['catch_all_checked_at']}")
            print(f"    catch_all_code:     {r['catch_all_smtp_code']}")
            print(f"    catch_all_local:    {r['catch_all_localpart']!r}")
            print(f"    created_at:         {r['created_at']}")
            print()

            # Diagnose issues
            if r["catch_all_status"] is None:
                print("    ⚠️  ISSUE: catch_all_status is NULL!")
                print("       → R18 classifier will treat as 'unknown' → emails marked 'valid'")
            elif r["catch_all_status"] == "catch_all":
                print("    ✅ catch_all_status = 'catch_all' (correct for catch-all domain)")
            elif r["catch_all_status"] == "not_catch_all":
                print("    ⚠️  catch_all_status = 'not_catch_all'")
                print("       → If domain IS catch-all, this is stale/wrong")
            else:
                print(f"    ⚠️  Unusual status: {r['catch_all_status']!r}")

            print()

        return {"found": True, "rows": results}

    finally:
        con.close()


def diagnose_verification_results(domain: str) -> dict:
    """Check recent verification_results for emails at this domain."""
    dom = domain.strip().lower()
    con = _get_conn()

    print(f"\n{'=' * 60}")
    print(f"RECENT VERIFICATION RESULTS for @{dom}")
    print(f"{'=' * 60}\n")

    try:
        # Join with emails table to get the actual email address
        # verification_results only has email_id, not email/domain directly
        rows = con.execute(
            """
            SELECT
                vr.id,
                e.email,
                vr.verify_status,
                vr.verify_reason,
                vr.verified_at,
                vr.status,
                vr.reason
            FROM verification_results vr
            JOIN emails e ON e.id = vr.email_id
            WHERE e.email LIKE ?
            ORDER BY vr.verified_at DESC
            LIMIT 10
            """,
            (f"%@{dom}",),
        ).fetchall()

        if not rows:
            print(f"No verification_results found for @{dom}")
            return {"found": False, "rows": []}

        print(f"Found {len(rows)} recent verification(s):\n")

        status_counts: dict[str, int] = {}
        for row in rows:
            r = {
                "id": row[0],
                "email": row[1],
                "verify_status": row[2],
                "verify_reason": row[3],
                "verified_at": row[4],
                "raw_status": row[5],
                "raw_reason": row[6],
            }

            vs = r["verify_status"] or "(null)"
            status_counts[vs] = status_counts.get(vs, 0) + 1

            # Flag the problem case
            flag = ""
            if (
                r["verify_status"] == "valid"
                and "non_catchall_or_unknown" in (r["verify_reason"] or "")
            ):
                flag = " ← ⚠️ SHOULD BE risky_catch_all!"

            print(f"  {r['email']}")
            print(f"    verify_status: {r['verify_status']}{flag}")
            print(f"    verify_reason: {r['verify_reason']}")
            print(f"    raw_status:    {r['raw_status']}")
            print(f"    verified_at:   {r['verified_at']}")
            print()

        print("\nStatus distribution:")
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count}")

        return {"found": True, "count": len(rows), "status_counts": status_counts}

    finally:
        con.close()


def probe_catchall_fresh(domain: str) -> dict:
    """Force a fresh catch-all probe and show results."""
    dom = domain.strip().lower()

    print(f"\n{'=' * 60}")
    print(f"FRESH CATCH-ALL PROBE for: {dom}")
    print(f"{'=' * 60}\n")

    try:
        from src.verify.catchall import check_catchall_for_domain
    except ImportError as e:
        print(f"❌ Could not import catchall module: {e}")
        return {"error": str(e)}

    try:
        print(f"Running check_catchall_for_domain('{dom}', force=True)...")
        result = check_catchall_for_domain(dom, force=True)

        print("\nResult:")
        print(f"  status:      {result.status}")
        print(f"  mx_host:     {result.mx_host}")
        print(f"  rcpt_code:   {result.rcpt_code}")
        print(f"  cached:      {result.cached}")
        print(f"  localpart:   {result.localpart}")
        print(f"  elapsed_ms:  {result.elapsed_ms}")
        print(f"  error:       {result.error}")

        if result.status == "catch_all":
            print("\n✅ Domain IS a catch-all (SMTP returned 2xx for random address)")
        elif result.status == "not_catch_all":
            print("\n✅ Domain is NOT a catch-all (SMTP returned 5xx for random address)")
        elif result.status == "tempfail":
            print("\n⚠️ Temporary failure - catch-all status unknown")
        else:
            print(f"\n⚠️ Unusual status: {result.status}")

        return {
            "status": result.status,
            "rcpt_code": result.rcpt_code,
            "mx_host": result.mx_host,
        }

    except Exception as e:
        print(f"❌ Probe failed: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e)}


def fix_null_catchall(domain: str) -> bool:
    """Update NULL catch_all_status to 'catch_all' for a domain."""
    dom = domain.strip().lower()
    con = _get_conn()

    print(f"\n{'=' * 60}")
    print(f"FIXING NULL catch_all_status for: {dom}")
    print(f"{'=' * 60}\n")

    try:
        # First check if a domain_resolutions row exists at all
        existing = con.execute(
            """
            SELECT id FROM domain_resolutions
            WHERE chosen_domain = ? OR user_hint = ?
            LIMIT 1
            """,
            (dom, dom),
        ).fetchone()

        if not existing:
            print(f"⚠️ No domain_resolutions row exists for {dom}")
            print("   Creating one now...")

            # We need a company_id - try to find or create one
            company_row = con.execute(
                """
                SELECT id, name FROM companies
                WHERE domain = ? OR user_supplied_domain = ? OR official_domain = ?
                LIMIT 1
                """,
                (dom, dom, dom),
            ).fetchone()

            if company_row:
                company_id = company_row[0]
                company_name = company_row[1] or dom
                print(f"   Found existing company: id={company_id}, name={company_name}")
            else:
                # Create a minimal company record
                cur = con.execute(
                    """
                    INSERT INTO companies (tenant_id, name, domain, user_supplied_domain)
                    VALUES ('dev', ?, ?, ?)
                    RETURNING id
                    """,
                    (dom, dom, dom),
                )
                company_id = cur.fetchone()[0]
                company_name = dom
                con.commit()
                print(f"   Created new company: id={company_id}")

            # Now create the domain_resolutions row
            con.execute(
                """
                INSERT INTO domain_resolutions
                    (tenant_id, company_id, company_name, user_hint, chosen_domain,
                     method, confidence, reason, resolver_version)
                VALUES
                    ('dev', ?, ?, ?, ?, 'manual', 100,
                     'created for catch-all fix', 'diagnose_catchall')
                """,
                (company_id, company_name, dom, dom),
            )
            con.commit()
            print(f"   Created domain_resolutions row for {dom}")

        # Now run a fresh probe to get the actual status
        try:
            from src.verify.catchall import check_catchall_for_domain

            print("\nRunning catch-all probe...")
            result = check_catchall_for_domain(dom, force=True)
            actual_status = result.status
            print(f"Fresh probe returned: {actual_status} (code={result.rcpt_code})")
        except Exception as e:
            print(f"Could not run fresh probe: {e}")
            import traceback

            traceback.print_exc()
            actual_status = "catch_all"  # Default assumption
            print(f"Defaulting to: {actual_status}")

        # Update rows with NULL catch_all_status
        cur = con.execute(
            """
            UPDATE domain_resolutions
            SET catch_all_status = ?,
                catch_all_checked_at = ?
            WHERE (chosen_domain = ? OR user_hint = ?)
              AND (catch_all_status IS NULL OR catch_all_status != ?)
            """,
            (
                actual_status,
                datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                dom,
                dom,
                actual_status,
            ),
        )

        updated = cur.rowcount
        con.commit()

        if updated > 0:
            print(f"\n✅ Updated {updated} row(s) with catch_all_status = '{actual_status}'")
        else:
            print("\nNo rows needed updating")

        return True

    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description="Diagnose catch-all detection issues")
    parser.add_argument("domain", help="Domain to diagnose (e.g., crestwellpartners.com)")
    parser.add_argument("--probe", action="store_true", help="Force fresh catch-all probe")
    parser.add_argument("--fix", action="store_true", help="Fix NULL catch_all_status")

    args = parser.parse_args()

    # Always show domain_resolutions state
    dr_result = diagnose_domain_resolutions(args.domain)

    # Always show verification_results
    vr_result = diagnose_verification_results(args.domain)

    # Optionally probe fresh
    if args.probe:
        probe_catchall_fresh(args.domain)

    # Optionally fix
    if args.fix:
        fix_null_catchall(args.domain)
        print("\n" + "=" * 60)
        print("Re-checking domain_resolutions after fix:")
        diagnose_domain_resolutions(args.domain)

    # Summary
    print(f"\n{'=' * 60}")
    print("DIAGNOSIS SUMMARY")
    print(f"{'=' * 60}\n")

    if not dr_result.get("found"):
        print("❌ No domain_resolutions row found")
        print("   → This is the ROOT CAUSE of the catch-all bug!")
        print("   → Without a row, catch-all status can't be cached")
        print("   → Every verification sees NULL catch-all → marks as 'valid'")
        print("")
        print("   FIX: Run with --fix to create the row and probe catch-all:")
        print(f"        python diagnose_catchall.py {args.domain} --fix")
    else:
        rows = dr_result.get("rows", [])
        has_catchall = any(r.get("catch_all_status") == "catch_all" for r in rows)
        has_null = any(r.get("catch_all_status") is None for r in rows)

        if has_null:
            print("⚠️ Some rows have NULL catch_all_status")
            print("   → R18 classifier treats NULL as 'unknown' → marks emails 'valid'")
            print("   → Run with --fix to update them")
        elif has_catchall:
            print("✅ catch_all_status is properly set to 'catch_all'")
        else:
            print("⚠️ catch_all_status is set but NOT 'catch_all'")
            print("   → Run with --probe to re-check if domain is catch-all")

    if vr_result.get("found"):
        counts = vr_result.get("status_counts", {})
        if counts.get("valid", 0) > 0 and counts.get("risky_catch_all", 0) == 0:
            print("\n⚠️ All recent verifications are 'valid' with no 'risky_catch_all'")
            print("   → This confirms the catch-all status wasn't being loaded")
            print("   → After fixing, re-run verifications or use reclassify script")


if __name__ == "__main__":
    main()
