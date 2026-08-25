-- db/migrations/2026_03_28_pipeline.sql
--
-- Sales Pipeline Tracker tables.
--
-- Tracks deals from lead discovery through placement, wired into the
-- existing companies/people/emails tables from CrestwellIQ.

BEGIN;

-- ---------------------------------------------------------------------------
-- pipeline_deals: core deal tracking
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_deals (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,

  -- Company info
  company_name TEXT NOT NULL,
  company_domain TEXT,

  -- Primary contact
  contact_name TEXT NOT NULL,
  contact_email TEXT,
  contact_title TEXT,
  contact_role TEXT,

  -- Deal state
  stage TEXT NOT NULL DEFAULT 'new_lead',
  deal_value REAL DEFAULT 0,

  -- Origin tracking
  source TEXT NOT NULL DEFAULT 'manual',
  source_detail TEXT,

  -- Next action scheduling
  next_action_date TEXT,
  next_action_desc TEXT,

  -- Close info (populated when stage = closed_won or closed_lost)
  close_reason TEXT,
  close_date TEXT,

  -- Links to CrestwellIQ entities (nullable)
  iq_company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
  iq_email_id BIGINT REFERENCES emails(id) ON DELETE SET NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_deals_tenant_stage
  ON pipeline_deals(tenant_id, stage);

CREATE INDEX IF NOT EXISTS idx_pipeline_deals_tenant_domain
  ON pipeline_deals(tenant_id, company_domain);

CREATE INDEX IF NOT EXISTS idx_pipeline_deals_tenant_next_action
  ON pipeline_deals(tenant_id, next_action_date);

CREATE INDEX IF NOT EXISTS idx_pipeline_deals_tenant_source
  ON pipeline_deals(tenant_id, source);

CREATE INDEX IF NOT EXISTS idx_pipeline_deals_tenant_created
  ON pipeline_deals(tenant_id, created_at);

-- ---------------------------------------------------------------------------
-- pipeline_activities: timeline of actions per deal
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_activities (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  deal_id BIGINT NOT NULL REFERENCES pipeline_deals(id) ON DELETE CASCADE,

  type TEXT NOT NULL,        -- email_sent, email_reply, call_scheduled, call_completed, stage_change, note, import
  description TEXT,
  touch_number INTEGER,      -- 1-4 for email touches
  metadata JSONB,            -- flexible extra data

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_activities_deal
  ON pipeline_activities(deal_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_activities_tenant_type
  ON pipeline_activities(tenant_id, type);

CREATE INDEX IF NOT EXISTS idx_pipeline_activities_created
  ON pipeline_activities(created_at);

-- ---------------------------------------------------------------------------
-- pipeline_stage_history: track when deals move between stages
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_stage_history (
  id BIGSERIAL PRIMARY KEY,
  deal_id BIGINT NOT NULL REFERENCES pipeline_deals(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL DEFAULT 'dev',

  from_stage TEXT,           -- NULL for initial stage
  to_stage TEXT NOT NULL,

  entered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_history_deal
  ON pipeline_stage_history(deal_id);

-- ---------------------------------------------------------------------------
-- pipeline_import_log: deduplication for imports
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_import_log (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev',

  source TEXT NOT NULL,            -- crestwell_iq, email_outreach, inbound_contact
  source_identifier TEXT,          -- email address or external ID for dedup
  deal_id BIGINT REFERENCES pipeline_deals(id) ON DELETE CASCADE,

  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pipeline_import_log_dedup
  ON pipeline_import_log(tenant_id, source, source_identifier);

CREATE INDEX IF NOT EXISTS idx_pipeline_import_log_deal
  ON pipeline_import_log(deal_id);

COMMIT;
