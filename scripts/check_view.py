#!/usr/bin/env python
# scripts/check_view.py
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def resolve_sqlite_path() -> Path:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return ROOT / "dev.db"
    if not url.startswith("sqlite:"):
        raise SystemExit("ERROR: check_view.py only supports SQLite (sqlite:///...).")
    path = url[len("sqlite:") :]
    while path.startswith("/"):
        path = path[1:]
    p = Path(path)
    if p.drive or p.is_absolute():
        return p
    return (ROOT / p).resolve()


def main() -> None:
    db_path = resolve_sqlite_path()
    print(f"→ Checking view on: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    view_exists = conn.execute(
        "SELECT count(*) AS c FROM sqlite_master WHERE type='view' AND name='v_emails_latest';"
    ).fetchone()["c"]
    if not view_exists:
        raise SystemExit(
            "ERROR: v_emails_latest does not exist. Re-run scripts/apply_schema.py or inspect db/schema.sql."
        )

    # Ensure it returns rows and is 1-per-email
    total = conn.execute("SELECT count(*) AS c FROM v_emails_latest;").fetchone()["c"]
    distinct_emails = conn.execute(
        "SELECT count(DISTINCT email) AS c FROM v_emails_latest;"
    ).fetchone()["c"]

    print(f"· rows in v_emails_latest: {total}")
    print(f"· distinct emails        : {distinct_emails}")

    if total != distinct_emails:
        print(
            "WARNING: v_emails_latest returns duplicate rows per email (this breaks idempotency assumptions)."
        )

    # Show a small sample
    sample = conn.execute(
        "SELECT email, verify_status, reason, mx_host, verified_at FROM v_emails_latest ORDER BY verified_at DESC NULLS LAST LIMIT 10;"
    ).fetchall()
    print("Sample (up to 10):")
    for r in sample:
        print(
            f"· {r['email']:40}  {r['verify_status'] or '—':12}  {r['mx_host'] or '—':20}  {r['verified_at'] or '—'}"
        )

    print("✔ View check complete.")
    conn.close()


if __name__ == "__main__":
    main()
