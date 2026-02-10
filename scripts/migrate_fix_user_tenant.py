#!/usr/bin/env python3
"""
One-time migration: align user tenant_ids with the DEV_TENANT_ID used by existing data.

Problem: Users created via /auth/register got tenant_id="default" (from DEFAULT_TENANT_ID),
but all pipeline data (companies, people, emails, runs) was inserted with tenant_id="dev"
(from DEV_TENANT_ID). This causes the dashboard to show no data after session-based auth
correctly resolves the user's actual tenant_id.

Fix: Update all users whose tenant_id is "default" to use "dev" (or whatever DEV_TENANT_ID
is configured as).

Usage:
    python scripts/migrate_fix_user_tenant.py
    python scripts/migrate_fix_user_tenant.py --dry-run
"""

from __future__ import annotations

import os
import sys


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    dev_tenant = os.getenv("DEV_TENANT_ID", "dev").strip() or "dev"
    old_tenant = "default"

    if dev_tenant == old_tenant:
        print(f"DEV_TENANT_ID is already '{dev_tenant}' — nothing to fix.")
        return

    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from src.db import get_conn

    con = get_conn()

    try:
        # Check how many users are affected
        row = con.execute(
            "SELECT COUNT(*) FROM users WHERE tenant_id = %s",
            (old_tenant,),
        ).fetchone()
        count = row[0] if row else 0

        if count == 0:
            print(f"No users found with tenant_id='{old_tenant}'. Nothing to do.")
            return

        print(
            f"Found {count} user(s) with tenant_id='{old_tenant}' → will update to '{dev_tenant}'"
        )

        if dry_run:
            # Show which users would be affected
            rows = con.execute(
                "SELECT id, email, tenant_id FROM users WHERE tenant_id = %s",
                (old_tenant,),
            ).fetchall()
            for r in rows:
                print(f"  [DRY RUN] Would update user {r[0]} ({r[1]}): '{r[2]}' → '{dev_tenant}'")
            print("\nRe-run without --dry-run to apply.")
            return

        con.execute(
            "UPDATE users SET tenant_id = %s WHERE tenant_id = %s",
            (dev_tenant, old_tenant),
        )
        con.commit()
        print(f"Updated {count} user(s) to tenant_id='{dev_tenant}'.")

    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
