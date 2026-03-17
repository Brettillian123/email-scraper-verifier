-- migrations/004_google_discovery.sql
--
-- Google Custom Search lead discovery tables.
-- Run after existing migrations.
--
-- Adds:
--   - google_discovery_config: per-tenant settings for automated discovery
--   - google_discovery_runs: audit history of discovery runs

-- ---------------------------------------------------------------------------
-- Discovery configuration (one row per tenant)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_discovery_config (
    id                   BIGSERIAL PRIMARY KEY,
    tenant_id            TEXT NOT NULL DEFAULT 'dev',
    enabled              BOOLEAN NOT NULL DEFAULT FALSE,
    companies_per_day    INTEGER NOT NULL DEFAULT 20,
    min_people_threshold INTEGER NOT NULL DEFAULT 2,
    target_roles         TEXT NOT NULL DEFAULT 'CEO,CFO,COO,CTO,CIO,CHRO,CMO',
    daily_query_budget   INTEGER NOT NULL DEFAULT 140,
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at           TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id)
);

-- ---------------------------------------------------------------------------
-- Discovery run history
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS google_discovery_runs (
    id                  BIGSERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL DEFAULT 'dev',
    status              TEXT NOT NULL DEFAULT 'running',
    trigger_type        TEXT NOT NULL DEFAULT 'manual',
    companies_queried   INTEGER NOT NULL DEFAULT 0,
    queries_used        INTEGER NOT NULL DEFAULT 0,
    people_found        INTEGER NOT NULL DEFAULT 0,
    people_inserted     INTEGER NOT NULL DEFAULT 0,
    emails_generated    INTEGER NOT NULL DEFAULT 0,
    errors              TEXT,
    started_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at         TEXT,
    details_json        TEXT
);

CREATE INDEX IF NOT EXISTS ix_gdr_tenant_started
    ON google_discovery_runs(tenant_id, started_at DESC);
