-- db/migrations/2026_08_19_verification_pattern_confidence.sql
--
-- Confidence grading for addresses that SMTP cannot decide.
--
-- 48% of verifications (1,531) land on catch-all domains. A true catch-all
-- answers 250 to every local-part, so RCPT cannot tell a real mailbox from a
-- typo -- there is no verdict to record and we do not invent one:
-- verify_status stays 'risky_catch_all'.
--
-- What CAN be graded is the evidence behind the guess. These two columns carry
-- how well the company's own confirmed patterns support the address we stored,
-- so the UI and outreach can rank undecidable addresses instead of treating
-- 1,531 of them as one undifferentiated bucket.
--
--   pattern_confidence        Laplace-smoothed success rate of the pattern at
--                             this company, from
--                             src.generate.patterns.pattern_confidence().
--                             0.5 means "no evidence either way".
--   pattern_confidence_grade  the decision signal -- sort on this:
--                               high    pattern confirmed >=2x at this company
--                               medium  pattern confirmed exactly once
--                               low     no company evidence for this pattern
--
-- Written by src/queueing/tasks.py when a result is classified
-- 'risky_catch_all'. Both columns are NULL for every other verdict and for
-- every row written before this migration.
--
-- Additive and idempotent: safe to re-run. Existing rows are not modified.
-- Applied by scripts/apply_schema.py along with the rest of db/migrations/.

BEGIN;

ALTER TABLE verification_results
  ADD COLUMN IF NOT EXISTS pattern_confidence DOUBLE PRECISION;

ALTER TABLE verification_results
  ADD COLUMN IF NOT EXISTS pattern_confidence_grade TEXT;

-- Outreach reads "the high-confidence catch-alls for this tenant" often enough
-- to be worth an index; the column is NULL for every decided address, so a
-- partial index keeps it small.
CREATE INDEX IF NOT EXISTS idx_verif_pattern_confidence_grade
  ON verification_results(tenant_id, pattern_confidence_grade)
  WHERE pattern_confidence_grade IS NOT NULL;

COMMIT;
