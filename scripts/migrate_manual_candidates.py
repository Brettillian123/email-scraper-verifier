# scripts/migrate_manual_candidates.py
"""
Migration: Create manual_candidate_attempts audit table.

This table provides an audit trail for manually-added candidates, persisting
outcome data even after the person/email rows are deleted on verification
failure. Columns person_id and email_id are intentionally NOT foreign keys
so the audit survives cleanup.

Usage:
    python scripts/migrate_manual_candidates.py
"""

from __future__ import annotations

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

DDL = """\
CREATE TABLE IF NOT EXISTS manual_candidate_attempts (
  id              BIGSERIAL PRIMARY KEY,
  tenant_id       TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  company_id      BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  batch_id        TEXT NOT NULL,

  first_name      TEXT,
  last_name       TEXT,
  full_name       TEXT,
  title           TEXT,
  submitted_email TEXT,

  outcome         TEXT NOT NULL DEFAULT 'pending',
  verified_email  TEXT,
  verify_status   TEXT,
  verify_reason   TEXT,
  error_detail    TEXT,

  person_id       BIGINT,
  email_id        BIGINT,

  submitted_by    TEXT,
  submitted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed_at    TEXT,

  CHECK (outcome IN ('pending', 'valid', 'invalid', 'error', 'no_mx'))
);

CREATE INDEX IF NOT EXISTS ix_mca_tenant_company
  ON manual_candidate_attempts(tenant_id, company_id);

CREATE INDEX IF NOT EXISTS ix_mca_batch_id
  ON manual_candidate_attempts(batch_id);

CREATE INDEX IF NOT EXISTS ix_mca_tenant_submitted_at
  ON manual_candidate_attempts(tenant_id, submitted_at DESC);
"""


def run() -> None:
    if os.path.isdir("src") and "src" not in sys.path:
        sys.path.insert(0, ".")

    from src.db import get_conn

    con = get_conn()
    try:
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                con.execute(stmt)
        con.commit()
        log.info("manual_candidate_attempts table created (or already exists).")
    except Exception:
        log.exception("Migration failed")
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    run()
