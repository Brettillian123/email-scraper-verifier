-- Never let one 550 delete an address that was previously confirmed valid.
--
-- CLEANUP_INVALID_GENERATED deletes the email row, and its whole verification
-- history, the moment a probe returns `invalid`. That is right for a guess that
-- never worked. It is badly wrong for an address the system had already
-- confirmed, because a 550 is not always a verdict about the mailbox:
--
--   * a provider throttling us can reject rather than defer,
--   * a policy rejection under directory-harvest suspicion looks the same at
--     the SMTP layer as "no such user",
--   * mailboxes come back (leave of absence, alias re-pointed, MX migration).
--
-- Measured on the 2026-08-25 backfill: ~8,700 probes in one run, ~6,400 of them
-- at Google. That drew 921 x 450 throttling responses AND tripped the host's
-- outbound SMTP block (674 probes recorded port25_unreachable). In the same run
-- confirmed addresses fell 1,847 -> 1,564 and companies holding at least one
-- valid address fell 355 -> 286. Some of that is genuine churn. It is not
-- possible to say how much, because the rows were deleted.
--
-- This is the same principle as the person-scoped pattern fix
-- (2026_08_25_person_generation_outcome.sql): a single observation must not
-- erase accumulated evidence.
--
-- first_valid_at is set the first time an address verifies valid and is never
-- cleared. Deletion refuses when it is set; the address keeps its row and simply
-- records the new status, so a genuinely dead mailbox is still visible as
-- invalid rather than silently vanishing.

ALTER TABLE emails ADD COLUMN IF NOT EXISTS first_valid_at text;

-- Protect what is already in the table. Every address currently holding a valid
-- verification is stamped now, so the guard covers existing data rather than
-- only addresses confirmed from here on.
UPDATE emails e
   SET first_valid_at = COALESCE(
         e.first_valid_at,
         (SELECT min(vr.checked_at)
            FROM verification_results vr
           WHERE vr.email_id = e.id AND vr.verify_status = 'valid'))
 WHERE e.first_valid_at IS NULL
   AND EXISTS (SELECT 1 FROM verification_results vr
                WHERE vr.email_id = e.id AND vr.verify_status = 'valid');

CREATE INDEX IF NOT EXISTS idx_emails_first_valid_at
    ON emails (first_valid_at)
    WHERE first_valid_at IS NOT NULL;
