#!/usr/bin/env python
# scripts/diagnose_invalid_emails.py
"""
Diagnostic script to check why invalid generated emails aren't being deleted.

This script:
1. Shows invalid emails in the database
2. Checks their source/source_note/source_url values
3. Identifies which should be deleted but weren't
4. Can optionally force-delete them

Usage:
  python scripts/diagnose_invalid_emails.py              # Show diagnostics
  python scripts/diagnose_invalid_emails.py --delete     # Also delete orphaned invalids
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    return here.parents[1] if len(here.parents) > 1 else here.parent


ROOT = _repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose invalid email cleanup issues")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Force delete invalid generated emails",
    )
    parser.add_argument(
        "--db",
        dest="db_url",
        help="Database URL (overrides DATABASE_URL)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max rows to show (default: 50)",
    )
    return parser.parse_args()


def _get_conn():
    try:
        from src.db import get_conn

        return get_conn()
    except ImportError:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from src.db import get_conn

        return get_conn()


def _get_emails_columns(conn) -> list[str]:
    print("\n[1] Checking emails table columns...")
    try:
        cols_result = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'emails' AND table_schema = 'public'
            """
        )
        cols = [r[0] for r in cols_result.fetchall()]
        print(f"    Columns: {', '.join(sorted(cols))}")
        return cols
    except Exception as e:
        print(f"    Error: {e}")
        return []


def _build_invalid_select(
    has_source: bool,
    has_source_note: bool,
    has_source_url: bool,
) -> list[str]:
    select_parts = ["e.id AS email_id", "e.email"]
    if has_source:
        select_parts.append("e.source")
    if has_source_note:
        select_parts.append("e.source_note")
    if has_source_url:
        select_parts.append("e.source_url")
    select_parts.extend(["vr.verify_status", "vr.verify_reason"])
    return select_parts


def _fetch_invalid_rows(
    conn,
    select_parts: list[str],
    limit: int,
):
    print("\n[2] Finding invalid emails (from verification_results)...")
    try:
        result = conn.execute(
            f"""
            SELECT {", ".join(select_parts)}
            FROM emails e
            JOIN verification_results vr ON vr.email_id = e.id
            WHERE vr.verify_status = 'invalid'
            ORDER BY e.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = result.fetchall()
        print(f"    Found {len(rows)} invalid emails")
        return rows
    except Exception as e:
        print(f"    Error: {e}")
        traceback.print_exc()
        return []


def _unpack_row(
    row,
    has_source: bool,
    has_source_note: bool,
    has_source_url: bool,
) -> dict:
    idx = 0
    email_id = row[idx]
    idx += 1

    email = row[idx]
    idx += 1

    source = row[idx] if has_source else None
    idx += 1 if has_source else 0

    source_note = row[idx] if has_source_note else None
    idx += 1 if has_source_note else 0

    source_url = row[idx] if has_source_url else None
    idx += 1 if has_source_url else 0

    verify_status = row[idx]
    idx += 1

    verify_reason = row[idx]
    return {
        "email_id": email_id,
        "email": email,
        "source": source,
        "source_note": source_note,
        "source_url": source_url,
        "verify_status": verify_status,
        "verify_reason": verify_reason,
    }


def _is_generated(
    source: str | None,
    source_note: str | None,
    source_url: str | None,
    has_source: bool,
    has_source_note: bool,
) -> bool:
    if source == "generated":
        return True

    if source_note and any(
        source_note.startswith(p)
        for p in (
            "generated:",
            "sequential_",
            "permutation:",
            "unverified:",
            "invalid:",
        )
    ):
        return True

    if not has_source and not has_source_note:
        return not bool(source_url and source_url.strip())

    return False


def _should_delete(source_url: str | None) -> bool:
    has_url = bool(source_url and source_url.strip())
    return not has_url


def _print_sample_rows(
    rows,
    has_source: bool,
    has_source_note: bool,
    has_source_url: bool,
) -> None:
    if not rows:
        return

    print("\n    Sample invalid emails:")
    print("    " + "-" * 66)

    for row in rows[:10]:
        data = _unpack_row(
            row,
            has_source=has_source,
            has_source_note=has_source_note,
            has_source_url=has_source_url,
        )

        is_generated = _is_generated(
            source=data["source"],
            source_note=data["source_note"],
            source_url=data["source_url"],
            has_source=has_source,
            has_source_note=has_source_note,
        )
        has_url = bool(data["source_url"] and str(data["source_url"]).strip())
        should_delete = _should_delete(data["source_url"])

        print(f"    ID={data['email_id']}: {data['email']}")
        if has_source:
            print(f"      source={data['source']!r}")
        if has_source_note:
            print(f"      source_note={data['source_note']!r}")
        if has_source_url:
            print(f"      source_url={data['source_url']!r}")
        print(f"      verify_status={data['verify_status']!r}")
        print(f"      verify_reason={data['verify_reason']!r}")
        print(f"      is_generated={is_generated}, has_url={has_url}")
        print(f"      SHOULD DELETE: {should_delete}")
        print()


def _build_where_parts(
    has_source: bool,
    has_source_note: bool,
    has_source_url: bool,
) -> list[str]:
    where_parts = ["vr.verify_status = 'invalid'"]

    if has_source and has_source_note:
        where_parts.append(
            """(
                e.source = 'generated'
                OR e.source_note LIKE 'generated:%%'
                OR e.source_note LIKE 'sequential_%%'
                OR e.source_note LIKE 'permutation:%%'
                OR e.source_note LIKE 'unverified:%%'
                OR e.source_note LIKE 'invalid:%%'
            )"""
        )
    elif has_source:
        where_parts.append("e.source = 'generated'")
    elif has_source_note:
        where_parts.append(
            """(
                e.source_note LIKE 'generated:%%'
                OR e.source_note LIKE 'sequential_%%'
                OR e.source_note LIKE 'permutation:%%'
                OR e.source_note LIKE 'unverified:%%'
                OR e.source_note LIKE 'invalid:%%'
            )"""
        )

    if has_source_url:
        where_parts.append("(e.source_url IS NULL OR TRIM(e.source_url) = '')")

    return where_parts


def _count_deletable(conn, where_parts: list[str]) -> int:
    print("\n[3] Counting invalid generated emails that SHOULD be deleted...")
    try:
        result = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM emails e
            JOIN verification_results vr ON vr.email_id = e.id
            WHERE {" AND ".join(where_parts)}
            """
        )
        count = result.fetchone()[0]
        print(f"    Found {count} invalid generated emails that should have been deleted")
        return int(count)
    except Exception as e:
        print(f"    Error: {e}")
        return 0


def _print_cleanup_env() -> None:
    print("\n[4] Checking CLEANUP_INVALID_GENERATED env var...")
    cleanup_val = os.getenv("CLEANUP_INVALID_GENERATED", "1")
    cleanup_enabled = cleanup_val.strip().lower() in ("1", "true", "yes")
    print(f"    CLEANUP_INVALID_GENERATED = {cleanup_val!r}")
    print(f"    Cleanup enabled: {cleanup_enabled}")


def _delete_invalid_generated(conn, where_parts: list[str]) -> int:
    result = conn.execute(
        f"""
        SELECT e.id
        FROM emails e
        JOIN verification_results vr ON vr.email_id = e.id
        WHERE {" AND ".join(where_parts)}
        """
    )
    ids = [r[0] for r in result.fetchall()]
    if not ids:
        return 0

    placeholders = ",".join(["%s"] * len(ids))

    conn.execute(
        f"DELETE FROM verification_results WHERE email_id IN ({placeholders})",
        tuple(ids),
    )
    conn.execute(
        f"DELETE FROM emails WHERE id IN ({placeholders})",
        tuple(ids),
    )
    conn.commit()
    return len(ids)


def main() -> None:
    args = _parse_args()

    if args.db_url:
        os.environ["DATABASE_URL"] = args.db_url

    print("=" * 70)
    print("INVALID EMAIL DIAGNOSTICS")
    print("=" * 70)

    with _get_conn() as conn:
        cols = _get_emails_columns(conn)

        has_source = "source" in cols
        has_source_note = "source_note" in cols
        has_source_url = "source_url" in cols

        print(
            "    "
            f"has_source={has_source}, "
            f"has_source_note={has_source_note}, "
            f"has_source_url={has_source_url}"
        )

        select_parts = _build_invalid_select(
            has_source=has_source,
            has_source_note=has_source_note,
            has_source_url=has_source_url,
        )
        rows = _fetch_invalid_rows(conn, select_parts=select_parts, limit=args.limit)
        _print_sample_rows(
            rows,
            has_source=has_source,
            has_source_note=has_source_note,
            has_source_url=has_source_url,
        )

        where_parts = _build_where_parts(
            has_source=has_source,
            has_source_note=has_source_note,
            has_source_url=has_source_url,
        )
        count = _count_deletable(conn, where_parts=where_parts)

        _print_cleanup_env()

        if args.delete and count > 0:
            print(f"\n[5] Deleting {count} invalid generated emails...")
            try:
                deleted = _delete_invalid_generated(conn, where_parts=where_parts)
                print(f"    Deleted {deleted} emails and their verification results")
            except Exception as e:
                print(f"    Error deleting: {e}")
                traceback.print_exc()
        elif count > 0:
            print(f"\n    Run with --delete to remove these {count} emails")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
