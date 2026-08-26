-- Where the daily operational audit puts its findings.
--
-- The audit runs in cw-iq-worker-1 and the admin API serves from cw-iq-api.
-- Those containers share no filesystem -- neither has a single mount -- so a
-- JSON file on disk is visible only to the container that wrote it. The database
-- is the one thing both can reach.
--
-- Rows accumulate rather than being overwritten, because "when did this start
-- failing?" is the question that actually gets asked. A check that has been
-- reporting ACTION for three weeks is a different problem from one that started
-- this morning, and a single overwritten row cannot tell them apart.

CREATE TABLE IF NOT EXISTS ops_audit_report (
    id            BIGSERIAL PRIMARY KEY,
    generated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    needs_action  BOOLEAN     NOT NULL DEFAULT FALSE,
    findings_json TEXT        NOT NULL
);

-- The API only ever wants the newest row.
CREATE INDEX IF NOT EXISTS idx_ops_audit_report_generated_at
    ON ops_audit_report (generated_at DESC);
