#!/usr/bin/env python3
"""
Database reset script for Email Scraper pipeline.

Usage:
    python reset_db.py --help
    python reset_db.py --mode full          # Delete everything (keeps tenants)
    python reset_db.py --mode soft          # Just reset AI flags and verification
    python reset_db.py --mode runs          # Delete runs + associated data only
    python reset_db.py --domains example.com,foo.com  # Reset specific domains only
"""

import argparse
import os
import sys

# Add src to path if running from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def get_connection():
    """Get database connection."""
    try:
        from src.db import get_conn

        return get_conn()
    except ImportError:
        # Fallback: direct psycopg2 connection
        import psycopg2

        database_url = os.getenv("DATABASE_URL", "postgresql://localhost/email_scraper")
        return psycopg2.connect(database_url)


def full_reset(con, dry_run: bool = False):
    """
    Delete ALL data except tenants table.
    Order matters due to foreign key constraints.
    """
    tables_in_order = [
        # Delete in FK-safe order (children first)
        "admin_audit",
        "lead_search_docs",
        "verification_results",
        "emails",
        "people",
        "sources",
        "domain_resolutions",
        "ingest_items",
        "companies",
        "runs",
        "run_metrics",
        "user_activity",
        # Keep: tenants, users
    ]

    print("=== FULL RESET ===")
    print("This will DELETE all data from:")
    for t in tables_in_order:
        print(f"  - {t}")
    print()

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    for table in tables_in_order:
        try:
            cur = con.execute(f"DELETE FROM {table}")
            count = cur.rowcount
            print(f"  Deleted {count} rows from {table}")
        except Exception as e:
            print(f"  Skipped {table}: {e}")

    con.commit()
    print("\n✓ Full reset complete.")


def soft_reset(con, dry_run: bool = False):
    """
    Soft reset: Clear AI flags and verification results so companies can be reprocessed.
    Keeps companies, people, emails - just resets their "processed" state.
    """
    print("=== SOFT RESET ===")
    print("This will:")
    print("  - Delete all verification_results")
    print("  - Delete all runs")
    print("  - Reset companies.attrs (AI flags)")
    print("  - Clear official_domain fields")
    print()

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    # Delete verification results
    try:
        cur = con.execute("DELETE FROM verification_results")
        print(f"  Deleted {cur.rowcount} verification_results")
    except Exception as e:
        print(f"  Skipped verification_results: {e}")

    # Delete runs
    try:
        cur = con.execute("DELETE FROM runs")
        print(f"  Deleted {cur.rowcount} runs")
    except Exception as e:
        print(f"  Skipped runs: {e}")

    # Reset AI flags on companies
    try:
        cur = con.execute(
            """
            UPDATE companies
            SET attrs = NULL,
                official_domain = NULL,
                official_domain_source = NULL,
                official_domain_confidence = NULL,
                official_domain_checked_at = NULL,
                run_id = NULL
        """
        )
        print(f"  Reset {cur.rowcount} companies (attrs, official_domain)")
    except Exception as e:
        print(f"  Skipped companies reset: {e}")

    # Clear people run_id
    try:
        cur = con.execute("UPDATE people SET run_id = NULL")
        print(f"  Cleared run_id on {cur.rowcount} people")
    except Exception as e:
        print(f"  Skipped people: {e}")

    # Clear emails run_id and reset verification fields
    try:
        cur = con.execute("UPDATE emails SET run_id = NULL")
        print(f"  Cleared run_id on {cur.rowcount} emails")
    except Exception as e:
        print(f"  Skipped emails: {e}")

    con.commit()
    print("\n✓ Soft reset complete. Companies can now be reprocessed.")


def reset_runs(con, dry_run: bool = False):
    """
    Delete all runs and cascade to associated data.
    Keeps companies but clears their run_id linkage.
    """
    print("=== RUNS RESET ===")
    print("This will delete all runs and their associated:")
    print("  - verification_results (by run_id)")
    print("  - And unlink run_id from companies/people/emails")
    print()

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    # Unlink first
    for table in ["companies", "people", "emails", "sources"]:
        try:
            cur = con.execute(
                f"UPDATE {table} SET run_id = NULL WHERE run_id IS NOT NULL"
            )
            print(f"  Unlinked {cur.rowcount} rows in {table}")
        except Exception as e:
            print(f"  Skipped {table}: {e}")

    # Delete verification results
    try:
        cur = con.execute("DELETE FROM verification_results")
        print(f"  Deleted {cur.rowcount} verification_results")
    except Exception as e:
        print(f"  Skipped verification_results: {e}")

    # Delete run_metrics
    try:
        cur = con.execute("DELETE FROM run_metrics")
        print(f"  Deleted {cur.rowcount} run_metrics")
    except Exception as e:
        print(f"  Skipped run_metrics: {e}")

    # Delete runs
    try:
        cur = con.execute("DELETE FROM runs")
        print(f"  Deleted {cur.rowcount} runs")
    except Exception as e:
        print(f"  Skipped runs: {e}")

    con.commit()
    print("\n✓ Runs reset complete.")


def reset_domains(con, domains: list[str], dry_run: bool = False):
    """
    Reset specific domains only.
    Deletes all data associated with those domains.
    """
    print(f"=== DOMAIN RESET: {', '.join(domains)} ===")

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    for domain in domains:
        domain = domain.strip().lower()
        print(f"\nResetting: {domain}")

        # Find company IDs for this domain
        try:
            cur = con.execute(
                """
                SELECT id FROM companies
                WHERE LOWER(domain) = %s
                   OR LOWER(official_domain) = %s
                   OR LOWER(user_supplied_domain) = %s
            """,
                (domain, domain, domain),
            )
            company_ids = [row[0] for row in cur.fetchall()]
        except Exception:
            # SQLite uses ? not %s
            cur = con.execute(
                """
                SELECT id FROM companies
                WHERE LOWER(domain) = ?
                   OR LOWER(official_domain) = ?
                   OR LOWER(user_supplied_domain) = ?
            """,
                (domain, domain, domain),
            )
            company_ids = [row[0] for row in cur.fetchall()]

        if not company_ids:
            print(f"  No companies found for {domain}")
            continue

        print(f"  Found {len(company_ids)} company IDs: {company_ids}")

        placeholders = ",".join(["?"] * len(company_ids))

        # Delete in FK-safe order
        for table, col in [
            ("verification_results", "email_id"),  # Need to join through emails
            ("emails", "company_id"),
            ("people", "company_id"),
            ("sources", "company_id"),
            ("domain_resolutions", "company_id"),
        ]:
            try:
                if table == "verification_results":
                    # Join through emails
                    cur = con.execute(
                        f"""
                        DELETE FROM verification_results
                        WHERE email_id IN (
                            SELECT id FROM emails WHERE company_id IN ({placeholders})
                        )
                    """,
                        tuple(company_ids),
                    )
                else:
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE {col} IN ({placeholders})",
                        tuple(company_ids),
                    )
                print(f"  Deleted {cur.rowcount} from {table}")
            except Exception as e:
                print(f"  Skipped {table}: {e}")

        # Reset company attrs instead of deleting
        try:
            cur = con.execute(
                f"""
                UPDATE companies
                SET attrs = NULL,
                    official_domain = NULL,
                    official_domain_source = NULL,
                    official_domain_confidence = NULL,
                    official_domain_checked_at = NULL,
                    run_id = NULL
                WHERE id IN ({placeholders})
            """,
                tuple(company_ids),
            )
            print(f"  Reset {cur.rowcount} companies")
        except Exception as e:
            print(f"  Skipped company reset: {e}")

    con.commit()
    print("\n✓ Domain reset complete.")


def main():
    parser = argparse.ArgumentParser(description="Reset Email Scraper database")
    parser.add_argument(
        "--mode",
        choices=["full", "soft", "runs"],
        help="Reset mode: full (delete all), soft (reset flags only), runs (delete runs)",
    )
    parser.add_argument(
        "--domains",
        help="Comma-separated list of domains to reset (alternative to --mode)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )

    args = parser.parse_args()

    if not args.mode and not args.domains:
        parser.print_help()
        print("\n❌ Error: Must specify --mode or --domains")
        sys.exit(1)

    # Confirmation
    if not args.yes and not args.dry_run:
        print("⚠️  WARNING: This will modify your database!")
        response = input("Continue? [y/N] ").strip().lower()
        if response != "y":
            print("Aborted.")
            sys.exit(0)

    con = get_connection()

    try:
        if args.domains:
            domains = [d.strip() for d in args.domains.split(",") if d.strip()]
            reset_domains(con, domains, dry_run=args.dry_run)
        elif args.mode == "full":
            full_reset(con, dry_run=args.dry_run)
        elif args.mode == "soft":
            soft_reset(con, dry_run=args.dry_run)
        elif args.mode == "runs":
            reset_runs(con, dry_run=args.dry_run)
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
