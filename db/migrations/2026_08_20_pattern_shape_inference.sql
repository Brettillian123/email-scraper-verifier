-- db/migrations/2026_08_20_pattern_shape_inference.sql
--
-- Shape-inferred pattern evidence.
--
-- NEGATIVE EVIDENCE NEEDS NO SCHEMA CHANGE. company_email_pattern.failed_count
-- already exists (2026_08_19_company_email_pattern.sql) and is already the
-- counter a refuted format is recorded on -- one row per (company_id, pattern),
-- never an emails/verification_results row. There is nothing to backfill into
-- it either: the 36,320 hard failures measured in production were discarded at
-- the probe, not written anywhere, so no history survives to recompute from.
-- failed_count starts filling the moment the probe loop calls
-- src.generate.patterns.record_pattern_outcome(..., ok=False).
--
-- What IS new: a published local-part reveals its own format without a
-- matching people row (src.generate.patterns.infer_pattern_from_localpart).
-- That is a different kind of evidence from an SMTP confirmation and must not
-- be mixed into confirmed_count:
--
--   confirmed_count  SMTP said this address exists.
--   inferred_count   the company publishes an address SHAPED like this.
--                    Strong enough to reorder probes, NOT strong enough to
--                    call a pattern trusted -- pattern_confidence() ignores it
--                    on purpose. Ranked between "confirmed once" and the
--                    global base rate; see src/verify/candidates.rank_patterns.
--
-- Written by record_inferred_patterns() as an absolute SET recomputed from the
-- company's whole published-address set, so it is idempotent and safe to
-- re-derive after any crawl.
--
-- Additive and idempotent: safe to re-run. Existing rows keep inferred_count 0,
-- which is exactly "no shape evidence".
-- Applied by scripts/apply_schema.py along with the rest of db/migrations/.
--
-- NOT APPLIED. Run scripts/apply_schema.py to apply.

BEGIN;

ALTER TABLE company_email_pattern
  ADD COLUMN IF NOT EXISTS inferred_count INTEGER NOT NULL DEFAULT 0;

COMMIT;
