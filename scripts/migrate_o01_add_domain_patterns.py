# scripts/migrate_o01_add_domain_patterns.py
from __future__ import annotations

import argparse
import os
import sys

from src.db import get_conn

UTC_NOW_TEXT_SQL = "to_char((now() at time zone 'utc'), 'YYYY-MM-DD HH24:MI:SS')"


DDL = f"""
CREATE TABLE IF NOT EXISTS domain_patterns (
  domain TEXT PRIMARY KEY,
  pattern TEXT,
  confidence DOUBLE PRECISION NOT NULL,
  samples INTEGER NOT NULL,
  inferred_at TEXT NOT NULL DEFAULT ({UTC_NOW_TEXT_SQL})
);
""".strip()


def apply_dsn_override(dsn: str | None) -> None:
    """
    Allow operators to pass a DSN on the command line while still routing all
    DB access through src.db.get_conn().

    If your project uses a different env var than DATABASE_URL, set it before
    calling this script, or adjust this helper accordingly.
    """
    if not dsn:
        return
    os.environ["DATABASE_URL"] = dsn


def run() -> None:
    parser = argparse.ArgumentParser(
        description="O01 migration: ensure domain_patterns table exists (PostgreSQL)",
    )
    parser.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL (optional; overrides DATABASE_URL for this run).",
    )
    args = parser.parse_args()
    apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(DDL)
        finally:
            try:
                cur.close()
            except Exception:
                pass
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("✔ O01 migration completed (domain_patterns ensured).")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
