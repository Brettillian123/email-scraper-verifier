-- db/migrations/2026_08_20_run_reaper.sql
--
-- Run durability: bounded run-level retry for the reaper.
--
-- Context: one run has been stuck in 'running' since 2026-03-02 and a 59-domain
-- run failed 2026-03-03 and was never retried. Nothing ever timed a run out.
-- src/queueing/reaper.py now does, and needs somewhere to remember how many
-- times it has already re-enqueued a run so it cannot loop forever.
--
-- The reaper degrades gracefully if this migration has not been applied: with no
-- retry_count column it marks stale runs failed instead of re-enqueueing them.
--
-- Additive and idempotent: safe to re-run.
-- Applied by scripts/apply_schema.py along with the rest of db/migrations/.

BEGIN;

ALTER TABLE runs ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;

-- The reaper's only read: active runs ordered by heartbeat. runs is small, but
-- this keeps the scan off the table once run history grows.
CREATE INDEX IF NOT EXISTS idx_runs_status_updated_at
  ON runs(status, updated_at);

COMMIT;
