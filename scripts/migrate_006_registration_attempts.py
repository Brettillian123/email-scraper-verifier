#!/usr/bin/env python3
"""
Migration 006: Create registration_attempts table.

Tracks per-email registration attempts for rate limiting.
Unverified emails can re-register up to 3 times per 7 days.

Usage:
    python scripts/migrate_006_registration_attempts.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MIGRATION_SQL = """
-- Create registration_attempts table for rate limiting
CREATE TABLE IF NOT EXISTS registration_attempts (
    id          SERIAL PRIMARY KEY,
    email       TEXT NOT NULL,
    ip_address  TEXT,
    created_at  TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_reg_attempts_email_created
    ON registration_attempts (email, created_at);

-- Index for quick unverified user lookups
CREATE INDEX IF NOT EXISTS idx_users_email_verified
    ON users (email, is_verified);
"""


def main() -> int:
    from src.db import get_conn

    conn = get_conn()
    try:
        for statement in MIGRATION_SQL.strip().split(";"):
            stmt = statement.strip()
            if not stmt:
                continue
            logger.info("Executing: %s...", stmt[:80])
            conn.execute(stmt + ";")

        conn.commit()
        logger.info("Migration 006 complete: registration_attempts table created")
        return 0
    except Exception as e:
        conn.rollback()
        logger.error("Migration failed: %s", e)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
