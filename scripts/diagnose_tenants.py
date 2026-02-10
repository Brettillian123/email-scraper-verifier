#!/usr/bin/env python3
"""
Diagnostic: show tenant_id distribution across all key tables
and the auth context that would be resolved for a given session.

Usage:
    python scripts/diagnose_tenants.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db import get_conn


def main() -> None:
    con = get_conn()

    print("=" * 60)
    print("TENANT ID DISTRIBUTION BY TABLE")
    print("=" * 60)

    tables = [
        "users",
        "companies",
        "people",
        "emails",
        "runs",
        "sources",
        "verification_results",
    ]

    for table in tables:
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = %s LIMIT 1",
            (table,),
        ).fetchone()
        if not exists:
            print(f"\n{table}: TABLE DOES NOT EXIST")
            continue

        has_col = con.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = current_schema() "
            "AND table_name = %s AND column_name = 'tenant_id' LIMIT 1",
            (table,),
        ).fetchone()

        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        if not has_col:
            print(f"\n{table}: NO tenant_id COLUMN ({total} rows total)")
            continue

        print(f"\n{table}: ({total} rows total)")
        rows = con.execute(
            f"SELECT tenant_id, COUNT(*) FROM {table} GROUP BY tenant_id ORDER BY COUNT(*) DESC"
        ).fetchall()
        if not rows:
            print("  (empty table)")
            continue

        for tid, cnt in rows:
            print(f"  tenant_id={tid!r:20s}  →  {cnt} rows")

    print("\n" + "=" * 60)
    print("ACTIVE SESSIONS")
    print("=" * 60)
    try:
        rows = con.execute(
            "SELECT s.id, s.user_id, u.email, u.tenant_id, s.expires_at "
            "FROM sessions s JOIN users u ON u.id = s.user_id "
            "WHERE s.expires_at > NOW() "
            "ORDER BY s.expires_at DESC"
        ).fetchall()
        if not rows:
            print("  No active sessions found.")
        for sid, _uid, email, tid, expires in rows:
            print(f"  session={sid[:12]}...  user={email}  tenant_id={tid!r}  expires={expires}")
    except Exception as e:
        print(f"  Could not query sessions: {e}")

    con.close()


if __name__ == "__main__":
    main()
