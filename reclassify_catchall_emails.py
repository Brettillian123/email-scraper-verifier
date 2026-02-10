#!/usr/bin/env python3
"""
Reclassify emails that were incorrectly marked as 'valid' due to missing catch-all status.

This script:
1. Finds verification_results with verify_status='valid' and verify_reason containing
   'non_catchall_or_unknown' (indicating catch-all status was NULL/unknown)
2. Looks up the current catch_all_status for each domain
3. Reclassifies to 'risky_catch_all' if the domain is confirmed catch-all

Usage:
    python reclassify_catchall_emails.py                    # Dry run (show what would change)
    python reclassify_catchall_emails.py --commit           # Actually update the DB
    python reclassify_catchall_emails.py --domain example.com  # Only for one domain
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


def get_catchall_domains(con) -> dict[str, str]:
    """Get all domains with confirmed catch-all status."""
    # NOTE: PostgreSQL schema uses chosen_domain and user_hint, NOT 'domain'
    rows = con.execute(
        """
        SELECT DISTINCT 
            COALESCE(chosen_domain, user_hint) as dom,
            catch_all_status
        FROM domain_resolutions
        WHERE catch_all_status = 'catch_all'
          AND COALESCE(chosen_domain, user_hint) IS NOT NULL
        """
    ).fetchall()

    return {row[0].lower(): row[1] for row in rows if row[0]}


def find_misclassified_emails(con, domain_filter: str | None = None) -> list[dict]:
    """Find emails incorrectly marked valid due to unknown catch-all status."""

    # Join with emails table since verification_results only has email_id
    query = """
        SELECT 
            vr.id,
            e.email,
            vr.verify_status,
            vr.verify_reason,
            vr.verified_at
        FROM verification_results vr
        JOIN emails e ON e.id = vr.email_id
        WHERE vr.verify_status = 'valid'
          AND vr.verify_reason LIKE '%non_catchall_or_unknown%'
    """
    params = []

    if domain_filter:
        query += " AND e.email LIKE ?"
        params.append(f"%@{domain_filter.lower()}")

    query += " ORDER BY vr.verified_at DESC"

    rows = con.execute(query, params).fetchall()

    results = []
    for row in rows:
        email = row[1]
        domain = email.split("@")[1].lower() if email and "@" in email else None

        results.append(
            {
                "id": row[0],
                "email": email,
                "domain": domain,
                "verify_status": row[2],
                "verify_reason": row[3],
                "verified_at": row[4],
            }
        )

    return results


def reclassify_emails(
    con,
    misclassified: list[dict],
    catchall_domains: dict[str, str],
    commit: bool = False,
) -> tuple[int, int]:
    """
    Reclassify emails to risky_catch_all if their domain is confirmed catch-all.

    Returns (reclassified_count, skipped_count)
    """
    reclassified = 0
    skipped = 0

    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    for row in misclassified:
        domain = (row.get("domain") or "").lower()

        if domain not in catchall_domains:
            skipped += 1
            continue

        new_status = "risky_catch_all"
        new_reason = "rcpt_2xx_catchall"  # Retroactively corrected

        print(f"  {row['email']}")
        print(f"    OLD: {row['verify_status']} / {row['verify_reason']}")
        print(f"    NEW: {new_status} / {new_reason}")
        print()

        if commit:
            con.execute(
                """
                UPDATE verification_results
                SET verify_status = ?,
                    verify_reason = ?,
                    verified_at = ?
                WHERE id = ?
                """,
                (new_status, new_reason, now, row["id"]),
            )

        reclassified += 1

    if commit and reclassified > 0:
        con.commit()

    return reclassified, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Reclassify emails incorrectly marked valid due to missing catch-all status"
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually update the database (default is dry-run)",
    )
    parser.add_argument(
        "--domain",
        help="Only process emails for a specific domain",
    )

    args = parser.parse_args()

    con = _get_conn()

    try:
        print("=" * 60)
        print("RECLASSIFY CATCH-ALL EMAILS")
        print("=" * 60)
        print()

        if not args.commit:
            print("🔍 DRY RUN MODE - no changes will be made")
            print("   Run with --commit to apply changes")
            print()

        # Get catch-all domains
        catchall_domains = get_catchall_domains(con)
        print(f"Found {len(catchall_domains)} domain(s) with catch_all_status='catch_all':")
        for dom in sorted(catchall_domains.keys())[:10]:
            print(f"  - {dom}")
        if len(catchall_domains) > 10:
            print(f"  ... and {len(catchall_domains) - 10} more")
        print()

        # Find misclassified emails
        misclassified = find_misclassified_emails(con, args.domain)
        print(f"Found {len(misclassified)} email(s) with verify_reason='*non_catchall_or_unknown*'")
        print()

        if not misclassified:
            print("Nothing to reclassify!")
            return

        # Reclassify
        print("Emails to reclassify:")
        print("-" * 40)
        reclassified, skipped = reclassify_emails(
            con,
            misclassified,
            catchall_domains,
            commit=args.commit,
        )

        # Summary
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"  Reclassified: {reclassified}")
        print(f"  Skipped (domain not catch-all): {skipped}")

        if not args.commit and reclassified > 0:
            print()
            print("⚠️  This was a dry run. Run with --commit to apply changes.")
        elif args.commit and reclassified > 0:
            print()
            print("✅ Changes committed to database.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
