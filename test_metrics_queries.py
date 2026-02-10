#!/usr/bin/env python3
"""
Diagnostic script to test metrics queries directly.
Run this on your server to debug metrics issues.

Usage:
    python3 test_metrics_queries.py
"""

import os
import sys

# Add src to path
sys.path.insert(0, "/opt/email-scraper")


def _print_header() -> None:
    print("=" * 60)
    print("METRICS DIAGNOSTICS")
    print("=" * 60)


def _print_db_env() -> None:
    db_url = os.getenv("DATABASE_URL") or os.getenv("DB_URL") or ""
    print(f"\nDATABASE_URL set: {bool(db_url)}")
    if db_url:
        safe_url = db_url.split("@")[-1] if "@" in db_url else "***"
        print(f"  DB host/path: ...@{safe_url}")


def _connect():
    try:
        from src.db import get_conn

        conn = get_conn()
        print("\n✓ Database connection successful")
        print(f"  Is Postgres: {conn.is_postgres}")
        return conn
    except Exception as e:
        print(f"\n✗ Database connection failed: {e}")
        return None


def _run_scalar_query(conn, name: str, query: str) -> None:
    try:
        cur = conn.execute(query)
        row = cur.fetchone()
        if row is None:
            result = "NULL row"
        elif hasattr(row, "keys"):
            result = dict(row)
        elif hasattr(row, "__getitem__"):
            result = row[0]
        else:
            result = str(row)
        print(f"  {name}: {result}")
    except Exception as e:
        print(f"  {name}: ERROR - {e}")


def _print_query_results(conn) -> None:
    queries = [
        (
            "verification_results count",
            "SELECT COUNT(*) AS n FROM verification_results",
        ),
        (
            "domain_resolutions count",
            "SELECT COUNT(*) AS n FROM domain_resolutions",
        ),
        (
            "catch_all_status not null",
            "SELECT COUNT(*) AS n FROM domain_resolutions WHERE catch_all_status IS NOT NULL",
        ),
        (
            "catch_all_checked_at not null",
            "SELECT COUNT(*) AS n FROM domain_resolutions WHERE catch_all_checked_at IS NOT NULL",
        ),
        ("sources count", "SELECT COUNT(*) AS n FROM sources"),
        ("companies count", "SELECT COUNT(*) AS n FROM companies"),
        ("runs count", "SELECT COUNT(*) AS n FROM runs"),
        ("runs with user_id", "SELECT COUNT(*) AS n FROM runs WHERE user_id IS NOT NULL"),
        ("runs with label", "SELECT COUNT(*) AS n FROM runs WHERE label IS NOT NULL"),
    ]

    print("\n" + "-" * 60)
    print("QUERY RESULTS:")
    print("-" * 60)

    for name, query in queries:
        _run_scalar_query(conn, name, query)


def _print_catchall_breakdown(conn) -> None:
    print("\n" + "-" * 60)
    print("CATCH-ALL STATUS BREAKDOWN:")
    print("-" * 60)
    try:
        cur = conn.execute(
            """
            SELECT catch_all_status, COUNT(*) AS n
            FROM domain_resolutions
            WHERE catch_all_status IS NOT NULL
            GROUP BY catch_all_status
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("  No catch_all_status values found")
            return
        for row in rows:
            if hasattr(row, "keys"):
                print(f"  {row['catch_all_status']}: {row['n']}")
            else:
                print(f"  {row[0]}: {row[1]}")
    except Exception as e:
        print(f"  ERROR: {e}")


def _print_run_status_breakdown(conn) -> None:
    print("\n" + "-" * 60)
    print("RUN STATUS BREAKDOWN:")
    print("-" * 60)
    try:
        cur = conn.execute(
            """
            SELECT status, COUNT(*) AS n
            FROM runs
            GROUP BY status
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("  No runs found")
            return
        for row in rows:
            if hasattr(row, "keys"):
                print(f"  {row['status']}: {row['n']}")
            else:
                print(f"  {row[0]}: {row[1]}")
    except Exception as e:
        print(f"  ERROR: {e}")


def _print_users(conn) -> None:
    print("\n" + "-" * 60)
    print("USERS TABLE:")
    print("-" * 60)
    try:
        cur = conn.execute("SELECT id, email FROM users LIMIT 5")
        rows = cur.fetchall()
        if not rows:
            print("  No users found")
            return
        for row in rows:
            if hasattr(row, "keys"):
                print(f"  {row['id']}: {row['email']}")
            else:
                print(f"  {row[0]}: {row[1]}")
    except Exception as e:
        print(f"  ERROR: {e}")


def _print_recent_runs(conn) -> None:
    print("\n" + "-" * 60)
    print("RECENT RUNS (last 5):")
    print("-" * 60)
    try:
        cur = conn.execute(
            """
            SELECT id, user_id, label, status, created_at
            FROM runs
            ORDER BY created_at DESC
            LIMIT 5
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("  No runs found")
            return
        for row in rows:
            if hasattr(row, "keys"):
                print(
                    "  ID: "
                    f"{row['id'][:8]}... | User: {row['user_id']} | "
                    f"Label: {row['label']} | Status: {row['status']}"
                )
            else:
                print(
                    f"  ID: {row[0][:8]}... | User: {row[1]} | Label: {row[2]} | Status: {row[3]}"
                )
    except Exception as e:
        print(f"  ERROR: {e}")


def _test_metrics_module(conn) -> None:
    print("\n" + "-" * 60)
    print("TESTING METRICS MODULE:")
    print("-" * 60)
    try:
        from src.admin.metrics import get_admin_summary

        summary = get_admin_summary(conn)

        print(f"  costs.smtp_probes: {summary['costs']['smtp_probes']}")
        print(f"  costs.catchall_checks: {summary['costs']['catchall_checks']}")
        print(f"  costs.domains_resolved: {summary['costs']['domains_resolved']}")
        print(f"  costs.pages_crawled: {summary['costs']['pages_crawled']}")

        if "company_health" in summary:
            ch = summary["company_health"]
            print(f"  company_health.total_companies: {ch.get('total_companies', 'N/A')}")
            print(f"  company_health.companies_catch_all: {ch.get('companies_catch_all', 'N/A')}")

        if "run_status" in summary:
            rs = summary["run_status"]
            print(f"  run_status.total: {rs.get('total', 'N/A')}")

        if "user_stats" in summary:
            print(f"  user_stats count: {len(summary['user_stats'])}")

    except Exception as e:
        print(f"  ERROR importing/running metrics: {e}")
        import traceback

        traceback.print_exc()


def main() -> None:
    _print_header()
    _print_db_env()

    conn = _connect()
    if conn is None:
        return

    _print_query_results(conn)
    _print_catchall_breakdown(conn)
    _print_run_status_breakdown(conn)
    _print_users(conn)
    _print_recent_runs(conn)
    _test_metrics_module(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("DIAGNOSTICS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
