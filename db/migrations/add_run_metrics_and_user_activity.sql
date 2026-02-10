-- db/migrations/add_run_metrics_and_user_activity.sql
--
-- Migration to add run-level metrics tracking and user activity logging.
-- These tables support the web-app remote operation requirements.
--
-- Run with: psql $DATABASE_URL < add_run_metrics_and_user_activity.sql

BEGIN;

-- ---------------------------------------------------------------------------
-- Run Metrics: Aggregate metrics per run
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS run_metrics (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,

  -- Company-level metrics
  total_companies INTEGER DEFAULT 0,
  companies_with_candidates INTEGER DEFAULT 0,
  companies_zero_candidates INTEGER DEFAULT 0,
  companies_with_pages INTEGER DEFAULT 0,
  companies_zero_pages INTEGER DEFAULT 0,
  companies_403_blocked INTEGER DEFAULT 0,
  companies_robots_blocked INTEGER DEFAULT 0,
  companies_timeout INTEGER DEFAULT 0,

  -- Candidate/People metrics
  total_candidates_extracted INTEGER DEFAULT 0,
  candidates_with_email INTEGER DEFAULT 0,
  candidates_no_email INTEGER DEFAULT 0,
  people_upserted INTEGER DEFAULT 0,

  -- Email metrics
  emails_generated INTEGER DEFAULT 0,
  emails_verified INTEGER DEFAULT 0,
  emails_valid INTEGER DEFAULT 0,
  emails_invalid INTEGER DEFAULT 0,
  emails_risky_catch_all INTEGER DEFAULT 0,
  emails_unknown_timeout INTEGER DEFAULT 0,

  -- Domain metrics
  domains_catch_all INTEGER DEFAULT 0,
  domains_no_mx INTEGER DEFAULT 0,
  domains_smtp_blocked INTEGER DEFAULT 0,

  -- AI metrics (O27)
  ai_enabled BOOLEAN DEFAULT FALSE,
  ai_candidates_approved INTEGER DEFAULT 0,
  ai_candidates_rejected INTEGER DEFAULT 0,
  ai_total_tokens INTEGER DEFAULT 0,
  ai_total_time_s REAL DEFAULT 0,

  -- Performance metrics
  crawl_time_s REAL DEFAULT 0,
  extract_time_s REAL DEFAULT 0,
  generate_time_s REAL DEFAULT 0,
  verify_time_s REAL DEFAULT 0,
  total_time_s REAL DEFAULT 0,

  -- Error tracking
  total_errors INTEGER DEFAULT 0,
  error_summary TEXT,  -- JSON: {"error_type": count, ...}

  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraint: one metrics row per run
CREATE UNIQUE INDEX IF NOT EXISTS ux_run_metrics_run_id
  ON run_metrics(run_id);

CREATE INDEX IF NOT EXISTS idx_run_metrics_tenant_id
  ON run_metrics(tenant_id);

CREATE INDEX IF NOT EXISTS idx_run_metrics_created_at
  ON run_metrics(created_at);


-- ---------------------------------------------------------------------------
-- User Activity: Track user actions for usage monitoring
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS user_activity (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  user_id TEXT NOT NULL,

  -- Action details
  action TEXT NOT NULL,        -- 'run_created', 'run_completed', 'export', 'search', 'verify', etc.
  resource_type TEXT,          -- 'run', 'company', 'email', 'lead', etc.
  resource_id TEXT,            -- ID of the resource acted upon

  -- Request context
  ip_address TEXT,
  user_agent TEXT,

  -- Additional context
  metadata TEXT,               -- JSON blob with action-specific details

  -- Timestamps
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for querying user activity
CREATE INDEX IF NOT EXISTS idx_user_activity_tenant_user
  ON user_activity(tenant_id, user_id);

CREATE INDEX IF NOT EXISTS idx_user_activity_tenant_action
  ON user_activity(tenant_id, action);

CREATE INDEX IF NOT EXISTS idx_user_activity_created_at
  ON user_activity(created_at);

CREATE INDEX IF NOT EXISTS idx_user_activity_user_created
  ON user_activity(user_id, created_at);


-- ---------------------------------------------------------------------------
-- User Usage Summary View: Aggregate user activity
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS v_user_usage_summary;

CREATE VIEW v_user_usage_summary AS
SELECT
  tenant_id,
  user_id,
  COUNT(*) FILTER (WHERE action = 'run_created') AS runs_created,
  COUNT(*) FILTER (WHERE action = 'run_completed') AS runs_completed,
  COUNT(*) FILTER (WHERE action = 'export') AS exports,
  COUNT(*) FILTER (WHERE action = 'search') AS searches,
  COUNT(*) FILTER (WHERE action = 'verify') AS verifications,
  COUNT(*) AS total_actions,
  MIN(created_at) AS first_activity,
  MAX(created_at) AS last_activity
FROM user_activity
GROUP BY tenant_id, user_id;


-- ---------------------------------------------------------------------------
-- Run Summary View: Quick run overview with metrics
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS v_run_summary;

CREATE VIEW v_run_summary AS
SELECT
  r.id AS run_id,
  r.tenant_id,
  r.user_id,
  r.label,
  r.status,
  r.created_at,
  r.started_at,
  r.finished_at,
  r.error,

  -- Metrics (from run_metrics if available)
  COALESCE(m.total_companies, 0) AS total_companies,
  COALESCE(m.companies_with_candidates, 0) AS companies_with_candidates,
  COALESCE(m.companies_zero_candidates, 0) AS companies_zero_candidates,
  COALESCE(m.emails_valid, 0) AS emails_valid,
  COALESCE(m.emails_risky_catch_all, 0) AS emails_risky_catch_all,
  COALESCE(m.emails_invalid, 0) AS emails_invalid,
  COALESCE(m.total_time_s, 0) AS total_time_s,

  -- Calculated success rate
  CASE
    WHEN COALESCE(m.emails_verified, 0) > 0
    THEN ROUND(
      (COALESCE(m.emails_valid, 0)::NUMERIC / m.emails_verified) * 100,
      1
    )
    ELSE 0
  END AS valid_rate_pct

FROM runs r
LEFT JOIN run_metrics m ON m.run_id = r.id;


COMMIT;
