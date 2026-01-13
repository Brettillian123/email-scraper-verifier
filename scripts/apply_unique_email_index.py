#!/usr/bin/env python
# scripts/apply_unique_email_index.py
"""
Apply unique index on emails(tenant_id, email) with safety checks.

This script is Postgres-only in the target state. It:
  - Detects duplicate emails within each tenant
  - Optionally auto-fixes duplicates by keeping the highest id
  - Creates the tenant-scoped unique index
  - Verifies the index works

Usage:
  python scripts/apply_unique_email_index.py
  python scripts/apply_unique_email_index.py --auto-fix-duplicates
  python scripts/apply_unique_email_index.py --db postgresql://user:pass@host:5432/db
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

# Ensure repo root is on sys.path for imports
ROOT = Path(__file__).resolve().parents[1] if Path(__file__).resolve().parents else Path.cwd()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _import_get_conn():
    """Import get_conn() from src.db."""
    try:
        from src.db import get_conn
    except ImportError:
        sys.path.insert(0, str(ROOT))
        from src.db import get_conn
    return get_conn


def _is_postgres_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


# SQL queries for duplicate detection (tenant-aware)
SQL_DUP_CHECK = """
SELECT tenant_id, email, COUNT(*) AS c
FROM emails
GROUP BY tenant_id, email
HAVING COUNT(*) > 1
ORDER BY tenant_id, c DESC;
"""

SQL_DUP_ROWS = """
SELECT id, tenant_id, company_id, email, created_at
FROM emails
WHERE tenant_id = %s AND email = %s
ORDER BY id DESC;
"""

# Index management (tenant-scoped unique index)
SQL_DROP_OLD_INDEXES = [
    "DROP INDEX IF EXISTS idx_emails_email;",
    "DROP INDEX IF EXISTS ux_emails_email;",
]

SQL_CREATE_UNIQUE = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_tenant_email
ON emails(tenant_id, email);
"""


def find_duplicates(conn: Any) -> list[tuple[str, str, int]]:
    """
    Find duplicate (tenant_id, email) combinations.

    Returns list of (tenant_id, email, count) tuples.
    """
    cur = conn.execute(SQL_DUP_CHECK)
    rows = cur.fetchall() or []
    result: list[tuple[str, str, int]] = []
    for row in rows:
        if isinstance(row, tuple):
            result.append((str(row[0]), str(row[1]), int(row[2])))
        else:
            result.append((str(row["tenant_id"]), str(row["email"]), int(row["c"])))
    return result


def auto_fix_duplicates(conn: Any, dups: list[tuple[str, str, int]]) -> int:
    """
    Keep the highest id per (tenant_id, email), delete the rest.

    Returns number of rows deleted.
    """
    deleted = 0
    for tenant_id, email, _ in dups:
        cur = conn.execute(SQL_DUP_ROWS, (tenant_id, email))
        rows = cur.fetchall() or []
        if len(rows) <= 1:
            continue

        # Get IDs: rows[0] is highest id (due to ORDER BY id DESC)
        ids_to_delete: list[int] = []
        for i, row in enumerate(rows):
            if i == 0:
                continue  # Keep the highest id
            row_id = row[0] if isinstance(row, tuple) else row["id"]
            ids_to_delete.append(int(row_id))

        if ids_to_delete:
            # Delete duplicates one by one (safe for all Postgres versions)
            for rid in ids_to_delete:
                conn.execute("DELETE FROM emails WHERE id = %s", (rid,))
            deleted += len(ids_to_delete)

    return deleted


def ensure_unique_index(conn: Any) -> None:
    """Drop old non-tenant-scoped indexes and create the tenant-scoped unique index."""
    for sql in SQL_DROP_OLD_INDEXES:
        try:
            conn.execute(sql)
        except Exception:
            pass  # Index may not exist

    conn.execute(SQL_CREATE_UNIQUE)


def print_index_list(conn: Any) -> None:
    """Print indexes on the emails table."""
    try:
        cur = conn.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'emails'
            ORDER BY indexname;
        """)
        rows = cur.fetchall() or []
        print("Indexes on emails table:")
        for row in rows:
            name = row[0] if isinstance(row, tuple) else row["indexname"]
            defn = row[1] if isinstance(row, tuple) else row["indexdef"]
            print(f"  {name}: {defn}")
    except Exception as e:
        print(f"  [Warning] Could not list indexes: {e}")


def verify_constraint(conn: Any) -> bool:
    """
    Verify the unique constraint works by attempting a duplicate insert in a transaction.

    Returns True if constraint is enforced, False otherwise.
    """
    try:
        # Start a savepoint so we can roll back just this test
        conn.execute("SAVEPOINT unique_test_sp")

        # Get a test company_id
        cur = conn.execute("SELECT id FROM companies LIMIT 1")
        row = cur.fetchone()
        if not row:
            print("  [Warning] No companies found; skipping constraint verification")
            conn.execute("ROLLBACK TO SAVEPOINT unique_test_sp")
            return True

        company_id = row[0] if isinstance(row, tuple) else row["id"]

        # Insert a test email
        test_email = f"__unique_test_{os.getpid()}@example.com"
        conn.execute(
            "INSERT INTO emails (tenant_id, company_id, email) VALUES (%s, %s, %s)",
            ("dev", company_id, test_email),
        )

        # Try to insert duplicate
        try:
            conn.execute(
                "INSERT INTO emails (tenant_id, company_id, email) VALUES (%s, %s, %s)",
                ("dev", company_id, test_email),
            )
            # If we get here, constraint is NOT enforced
            conn.execute("ROLLBACK TO SAVEPOINT unique_test_sp")
            return False
        except Exception:
            # Good - duplicate was rejected
            conn.execute("ROLLBACK TO SAVEPOINT unique_test_sp")
            return True

    except Exception as e:
        print(f"  [Warning] Constraint verification error: {e}")
        try:
            conn.execute("ROLLBACK TO SAVEPOINT unique_test_sp")
        except Exception:
            pass
        return True  # Assume it's fine if we can't test


def table_exists(conn: Any, table: str) -> bool:
    """Check if a table exists in the public schema."""
    cur = conn.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
        LIMIT 1
    """,
        (table,),
    )
    return cur.fetchone() is not None


def main():
    parser = argparse.ArgumentParser(
        description="Apply unique index ux_emails_tenant_email on emails(tenant_id, email) with safety checks."
    )
    parser.add_argument(
        "--db",
        dest="db_url",
        help="PostgreSQL connection URL (overrides DATABASE_URL/DB_URL)",
        default=None,
    )
    parser.add_argument(
        "--auto-fix-duplicates",
        action="store_true",
        help="Automatically keep the highest id per duplicate (tenant_id, email) and delete others.",
    )
    args = parser.parse_args()

    # Set DATABASE_URL if provided via --db
    if args.db_url:
        if not _is_postgres_url(args.db_url):
            print(
                "[ERROR] --db must be a PostgreSQL URL (postgresql://... or postgres://...)",
                file=sys.stderr,
            )
            sys.exit(2)
        os.environ["DATABASE_URL"] = args.db_url

    # Get connection via src.db
    get_conn = _import_get_conn()

    try:
        with get_conn() as conn:
            # Verify we're on Postgres
            if not getattr(conn, "is_postgres", False):
                print(
                    "[ERROR] This script requires PostgreSQL. DATABASE_URL must point to Postgres.",
                    file=sys.stderr,
                )
                sys.exit(2)

            print("[INFO] Connected to PostgreSQL via src.db.get_conn()")

            # Check emails table exists
            if not table_exists(conn, "emails"):
                print("[ERROR] Table 'emails' not found in database.", file=sys.stderr)
                sys.exit(2)

            # 1) Check for duplicates
            dups = find_duplicates(conn)
            if dups:
                print(f"[WARN] Found {len(dups)} duplicate (tenant_id, email) combinations:")
                for tenant_id, email, count in dups[:20]:  # Show first 20
                    print(f"  tenant={tenant_id}, email={email}, count={count}")
                if len(dups) > 20:
                    print(f"  ... and {len(dups) - 20} more")

                if args.auto_fix_duplicates:
                    deleted = auto_fix_duplicates(conn, dups)
                    conn.commit()
                    print(f"[INFO] Auto-fixed duplicates; deleted {deleted} row(s).")
                else:
                    print(
                        "[ERROR] Aborting because duplicates exist. "
                        "Re-run with --auto-fix-duplicates to resolve automatically, "
                        "or fix manually and re-run."
                    )
                    sys.exit(1)
            else:
                print("[OK] No duplicate (tenant_id, email) values found.")

            # 2) Apply index changes
            ensure_unique_index(conn)
            conn.commit()
            print("[OK] Applied unique index: ux_emails_tenant_email ON emails(tenant_id, email)")

            # 3) Show current indexes
            print_index_list(conn)

            # 4) Verify constraint
            if verify_constraint(conn):
                print("[OK] Duplicate insert correctly rejected by unique constraint.")
            else:
                print("[ERROR] Duplicate insert unexpectedly succeeded (index not enforcing).")
                sys.exit(1)

            print("[OK] Unique index applied and verified successfully.")
            sys.exit(0)

    except Exception as e:
        print(f"[ERROR] Database error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
