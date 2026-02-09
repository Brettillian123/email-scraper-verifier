-- migrations/001_auth_tables.sql
--
-- Authentication & Authorization schema extensions
-- Run this after the base schema.sql
--
-- Adds:
--   - Password auth columns to users table
--   - Sessions table for web UI auth
--   - User limits/quotas table
--   - Password reset tokens table
--   - Email verification tokens table

-- ---------------------------------------------------------------------------
-- Extend users table for password authentication
-- ---------------------------------------------------------------------------

-- Add auth columns to existing users table (idempotent)
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_verified BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_superuser BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until TEXT;

-- Index for email lookups (login flow)
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);

-- ---------------------------------------------------------------------------
-- Sessions table (for web UI cookie-based auth)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,                    -- UUID session token
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,               -- ISO-8601 UTC
    last_activity_at TEXT,                  -- Updated on each request
    
    ip_address TEXT,                        -- Client IP at session creation
    user_agent TEXT,                        -- Browser/client info
    
    -- Optional: for "remember me" vs short sessions
    is_persistent BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions(tenant_id);

-- ---------------------------------------------------------------------------
-- Password reset tokens
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id TEXT PRIMARY KEY,                    -- UUID token
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,               -- Short-lived (1 hour typical)
    used_at TEXT,                           -- NULL until token is consumed
    
    ip_address TEXT                         -- Client IP that requested reset
);

CREATE INDEX IF NOT EXISTS idx_password_reset_user_id ON password_reset_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_password_reset_expires ON password_reset_tokens(expires_at);

-- ---------------------------------------------------------------------------
-- Email verification tokens
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_verification_tokens (
    id TEXT PRIMARY KEY,                    -- UUID token
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,                    -- Email being verified
    
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,               -- 24-48 hours typical
    verified_at TEXT                        -- NULL until verified
);

CREATE INDEX IF NOT EXISTS idx_email_verify_user_id ON email_verification_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_email_verify_expires ON email_verification_tokens(expires_at);

-- ---------------------------------------------------------------------------
-- User limits / quotas (per-user rate limiting and feature gates)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS user_limits (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    
    -- Run limits
    max_runs_per_day INTEGER,               -- NULL = unlimited
    max_domains_per_run INTEGER,            -- NULL = unlimited
    max_concurrent_runs INTEGER DEFAULT 2,  -- NULL = unlimited
    
    -- Verification limits
    max_verifications_per_day INTEGER,      -- NULL = unlimited
    max_verifications_per_month INTEGER,    -- NULL = unlimited
    
    -- Export limits
    max_exports_per_day INTEGER,            -- NULL = unlimited
    max_export_rows INTEGER DEFAULT 10000,  -- Max rows per export
    
    -- Feature flags
    can_use_ai_extraction BOOLEAN NOT NULL DEFAULT TRUE,
    can_use_smtp_verify BOOLEAN NOT NULL DEFAULT TRUE,
    can_access_admin BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Timestamps
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_limits_tenant ON user_limits(tenant_id);

-- ---------------------------------------------------------------------------
-- Tenant limits (organization-wide limits, applied when user limit is NULL)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenant_limits (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    
    -- Same structure as user_limits (fallback values)
    max_runs_per_day INTEGER DEFAULT 50,
    max_domains_per_run INTEGER DEFAULT 100,
    max_concurrent_runs INTEGER DEFAULT 5,
    max_verifications_per_day INTEGER DEFAULT 5000,
    max_verifications_per_month INTEGER DEFAULT 100000,
    max_exports_per_day INTEGER DEFAULT 20,
    max_export_rows INTEGER DEFAULT 50000,
    max_users INTEGER DEFAULT 10,           -- Max users per tenant
    
    -- Feature flags (tenant-wide)
    can_use_ai_extraction BOOLEAN NOT NULL DEFAULT TRUE,
    can_use_smtp_verify BOOLEAN NOT NULL DEFAULT TRUE,
    
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(tenant_id)
);

-- ---------------------------------------------------------------------------
-- Usage tracking (for enforcing limits)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS usage_counters (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
    
    counter_type TEXT NOT NULL,             -- 'runs', 'verifications', 'exports'
    period_start TEXT NOT NULL,             -- Start of counting period (day/month)
    period_type TEXT NOT NULL,              -- 'daily', 'monthly'
    count INTEGER NOT NULL DEFAULT 0,
    
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(tenant_id, user_id, counter_type, period_start, period_type)
);

CREATE INDEX IF NOT EXISTS idx_usage_counters_lookup 
    ON usage_counters(tenant_id, user_id, counter_type, period_type);

-- ---------------------------------------------------------------------------
-- Extend tenants table with org-level settings
-- ---------------------------------------------------------------------------

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS settings_json TEXT;  -- JSON blob for tenant-specific config

-- ---------------------------------------------------------------------------
-- Create default tenant limits for existing tenants
-- ---------------------------------------------------------------------------

INSERT INTO tenant_limits (tenant_id)
SELECT id FROM tenants
WHERE NOT EXISTS (
    SELECT 1 FROM tenant_limits WHERE tenant_limits.tenant_id = tenants.id
)
ON CONFLICT (tenant_id) DO NOTHING;
