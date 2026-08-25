-- db/migrations/2026_08_19_company_email_pattern.sql
--
-- Per-company email-format evidence (multi-pattern).
--
-- ONE ROW PER (company_id, pattern) -- NOT one pattern per company.
-- Of the 311 companies that currently have >=1 confirmed-valid address,
-- 192 use 1 pattern, 100 use 2, 17 use 3 and 2 use 4. A single
-- "the pattern" column would be wrong for 38% of them.
--
-- confirmed_count / failed_count are the durable memory of verification
-- outcomes. Invalid address rows are DELETED by design
-- (CLEANUP_INVALID_GENERATED=1), so failed_count is the only place a failed
-- guess is remembered. Never reset it from a backfill; see
-- src/generate/patterns.upsert_pattern_counts().
--
-- Additive and idempotent: safe to re-run.
-- Applied by scripts/apply_schema.py along with the rest of db/migrations/.

BEGIN;

CREATE TABLE IF NOT EXISTS company_email_pattern (
  id BIGSERIAL PRIMARY KEY,
  company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

  -- Mail domain the evidence was observed on (informational; company_id is
  -- the key). Companies that mail from more than one domain keep the most
  -- recently observed one.
  domain TEXT,

  -- One of src.generate.patterns.PATTERN_VOCABULARY:
  --   first.last, flast, first, firstlast, firstl, f.last,
  --   last.first, last, first_last, other
  -- Deliberately NOT a CHECK constraint: the vocabulary is owned by the
  -- application and may grow without a schema migration.
  pattern TEXT NOT NULL,

  -- Independent confirmed people whose verified-valid address matches.
  confirmed_count INTEGER NOT NULL DEFAULT 0,
  -- Independent people whose guess at this pattern came back invalid.
  failed_count INTEGER NOT NULL DEFAULT 0,

  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Upsert target for ON CONFLICT (company_id, pattern).
CREATE UNIQUE INDEX IF NOT EXISTS ux_company_email_pattern_company_pattern
  ON company_email_pattern(company_id, pattern);

CREATE INDEX IF NOT EXISTS idx_company_email_pattern_company_id
  ON company_email_pattern(company_id);

CREATE INDEX IF NOT EXISTS idx_company_email_pattern_domain
  ON company_email_pattern(domain);

COMMIT;
