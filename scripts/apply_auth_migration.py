#!/usr/bin/env python3
"""
Apply auth tables migration to the database.

Usage:
    python scripts/apply_auth_migration.py

Or with a specific database URL:
    DATABASE_URL=postgresql://... python scripts/apply_auth_migration.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Apply the auth migration."""
    import psycopg2

    from src.config import settings

    migration_path = Path(__file__).parent.parent / "migrations" / "001_auth_tables.sql"

    if not migration_path.exists():
        logger.error(f"Migration file not found: {migration_path}")
        return 1

    logger.info(f"Reading migration from: {migration_path}")
    sql = migration_path.read_text()

    # Split into statements (basic split on semicolon, but be careful with strings)
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    # Connect directly with autocommit to handle each statement independently
    # This prevents PostgreSQL from aborting the entire transaction on a single failure
    db_url = settings.database_url
    conn = psycopg2.connect(db_url)
    conn.autocommit = True  # Each statement commits independently

    try:
        logger.info(f"Applying {len(statements)} statements...")

        success_count = 0
        skip_count = 0
        fail_count = 0

        for i, stmt in enumerate(statements, 1):
            # Skip empty statements and pure comments
            if not stmt or all(
                line.strip().startswith("--") or not line.strip() for line in stmt.split("\n")
            ):
                continue

            try:
                cur = conn.cursor()
                cur.execute(stmt)
                cur.close()
                success_count += 1
                logger.debug(f"  [{i}/{len(statements)}] OK")
            except psycopg2.errors.DuplicateColumn:
                skip_count += 1
                logger.debug(f"  [{i}/{len(statements)}] Skipped (column already exists)")
            except psycopg2.errors.DuplicateTable:
                skip_count += 1
                logger.debug(f"  [{i}/{len(statements)}] Skipped (table already exists)")
            except psycopg2.errors.DuplicateObject:
                skip_count += 1
                logger.debug(f"  [{i}/{len(statements)}] Skipped (already exists)")
            except Exception as e:
                err_str = str(e).lower()
                if "already exists" in err_str or "duplicate" in err_str:
                    skip_count += 1
                    logger.debug(f"  [{i}/{len(statements)}] Skipped (already exists)")
                else:
                    fail_count += 1
                    logger.warning(f"  [{i}/{len(statements)}] Failed: {e}")

        logger.info(
            f"Migration complete: {success_count} applied, {skip_count} skipped, {fail_count} failed"
        )

        if fail_count > 0:
            logger.warning("Some statements failed - review warnings above")

        return 0

    except Exception as e:
        logger.exception(f"Migration failed: {e}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
