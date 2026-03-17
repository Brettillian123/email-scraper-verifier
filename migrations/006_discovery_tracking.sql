-- Track which companies have been searched by auto discovery.
-- Companies with a non-NULL last_discovery_at are permanently skipped
-- by future discovery runs (unless manually reset).

ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_discovery_at TIMESTAMPTZ DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_companies_last_discovery
    ON companies(tenant_id, last_discovery_at);
