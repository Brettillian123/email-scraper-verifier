#!/usr/bin/env python3
"""
Apply the email verification code migration (003).

Adds `code` and `attempts` columns to email_verification_tokens.

Usage:
    python -m scripts.apply_003_verification_code
    # or
    python scripts/apply_003_verification_code.py
"""

from __future__ import annotations

import os
import sys


def main() -> None:
    # Allow running from project root
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from src.db import get_conn

    migration_sql = """
    -- Add code column for 6-digit verification codes
    ALTER TABLE email_verification_tokens
      ADD COLUMN IF NOT EXISTS code TEXT;

    -- Track failed verification attempts (brute-force protection)
    ALTER TABLE email_verification_tokens
      ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;

    -- Index for fast code lookups during verification
    CREATE INDEX IF NOT EXISTS idx_email_verify_code
      ON email_verification_tokens(user_id, code);
    """

    conn = get_conn()
    try:
        conn.execute(migration_sql)
        conn.commit()
        print("✓ Migration 003_email_verification_code applied successfully.")
    except Exception as e:
        conn.rollback()
        print(f"✗ Migration failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
