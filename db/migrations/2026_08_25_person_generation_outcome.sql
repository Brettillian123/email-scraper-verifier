-- Record WHY a person produced no address.
--
-- The generator already computes a precise reason -- no_mx, no_candidates,
-- all_invalid, exhausted, deferred, valid_found, risky_found -- and then throws
-- it away. Because CLEANUP_INVALID_GENERATED deletes the email row for every
-- hard failure, a person who was probed and missed leaves no trace at all, and
-- is indistinguishable from a person who was never probed.
--
-- That gap cost real time: the 2026-08-24 sweep looked like an extraction
-- failure (191 companies crawled, 87 retaining an address) when it was actually
-- a pattern-evidence bug. The funnel could not be attributed after the fact
-- because the losing rows had been deleted.
--
-- Three columns on `people` rather than a new table: this is one fact about one
-- person, overwritten each run, and it needs to be joinable in every query that
-- already reaches for people.

ALTER TABLE people ADD COLUMN IF NOT EXISTS generation_status  text;
ALTER TABLE people ADD COLUMN IF NOT EXISTS generation_at      text;
ALTER TABLE people ADD COLUMN IF NOT EXISTS generation_probes  integer;

-- The funnel query this exists to serve:
--   SELECT generation_status, count(*) FROM people
--    WHERE generation_at IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;
CREATE INDEX IF NOT EXISTS idx_people_generation_status
    ON people (generation_status)
    WHERE generation_status IS NOT NULL;
