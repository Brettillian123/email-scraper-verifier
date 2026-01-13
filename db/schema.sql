-- db/schema.sql
--
-- Multi-tenant schema (Postgres system of record)
--
-- Key properties:
--   - Adds tenants/users/runs primitives.
--   - Adds tenant_id to all user-owned tables with DEFAULT 'dev' to keep
--     existing single-tenant code paths working until tenant plumbing is
--     fully wired end-to-end.
--   - Aligns runs table shape to the Runs API contract (id is TEXT UUID).
--   - Adds run_id linkage to companies (and optional linkage columns to other
--     tables for propagation/auditability).
--   - Uses DEFAULT CURRENT_TIMESTAMP (portable SQL; Postgres-native).
--
-- IMPORTANT:
--   - Tenant scoping must still be enforced at the application layer.
--   - This schema intentionally stores JSON fields as TEXT for minimal code churn.

-- ---------------------------------------------------------------------------
-- Multi-tenant primitives
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Ensure default tenants exist (Postgres-safe/idempotent).
INSERT INTO tenants (id, name)
VALUES ('dev', 'Development')
ON CONFLICT (id) DO NOTHING;

-- Transitional: seed tenant_dev to avoid FK failures if legacy API still uses it.
INSERT INTO tenants (id, name)
VALUES ('tenant_dev', 'Development (legacy API default)')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_users_tenant_email
  ON users(tenant_id, email);

-- ---------------------------------------------------------------------------
-- Control Plane: runs (aligned to API contract)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, -- UUID string
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  user_id TEXT REFERENCES users(id) ON DELETE SET NULL,

  label TEXT,
  status TEXT NOT NULL DEFAULT 'queued', -- queued|running|succeeded|failed|cancelled

  -- Store run inputs/options as JSON text
  domains_json TEXT NOT NULL,
  options_json TEXT NOT NULL,

  -- Optional progress payload written by workers
  progress_json TEXT,
  error TEXT,

  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  started_at TEXT,
  finished_at TEXT,

  CHECK (status IN ('queued','running','succeeded','failed','cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_runs_tenant_created_at
  ON runs(tenant_id, created_at);

CREATE INDEX IF NOT EXISTS idx_runs_tenant_status
  ON runs(tenant_id, status);

-- ---------------------------------------------------------------------------
-- companies
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS companies (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,

  -- Run linkage (Phase 2 hard requirement for /runs/{run_id}/results)
  run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,

  name TEXT,
  domain TEXT,                               -- user/ingest field (nullable)
  website_url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,

  -- canonical resolver outputs written by src/db.py (official_*)
  official_domain TEXT,
  official_domain_source TEXT,
  official_domain_confidence DOUBLE PRECISION,
  official_domain_checked_at TEXT,

  -- raw hint from ingest
  user_supplied_domain TEXT
);

CREATE INDEX IF NOT EXISTS idx_companies_tenant_id
  ON companies(tenant_id);

CREATE INDEX IF NOT EXISTS idx_companies_tenant_run_id
  ON companies(tenant_id, run_id);

-- lookups used by ensure_company / ingest flows
CREATE INDEX IF NOT EXISTS idx_companies_domain
  ON companies(domain);

CREATE INDEX IF NOT EXISTS idx_companies_tenant_domain
  ON companies(tenant_id, domain);

-- handy for lookups by user hint
CREATE INDEX IF NOT EXISTS idx_companies_user_supplied_domain
  ON companies(user_supplied_domain);

CREATE INDEX IF NOT EXISTS idx_companies_tenant_user_supplied_domain
  ON companies(tenant_id, user_supplied_domain);

-- Optional uniqueness to prevent accidental duplicates per tenant (safe if your DB is clean).
-- If you already have duplicates, create a cleanup migration before enabling these.
CREATE UNIQUE INDEX IF NOT EXISTS ux_companies_tenant_domain_notnull
  ON companies(tenant_id, domain)
  WHERE domain IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_companies_tenant_user_supplied_domain_notnull
  ON companies(tenant_id, user_supplied_domain)
  WHERE user_supplied_domain IS NOT NULL;

-- ---------------------------------------------------------------------------
-- R10 + R26: sources (page-level cache for crawled HTML, tied to companies)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sources (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,

  -- optional run linkage (recommended for auditability)
  run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,

  company_id BIGINT REFERENCES companies(id) ON DELETE CASCADE,
  source_url TEXT NOT NULL,                   -- canonical URL of the page
  html TEXT,                                  -- raw HTML body
  fetched_at TEXT DEFAULT CURRENT_TIMESTAMP    -- when this page was fetched
);

CREATE INDEX IF NOT EXISTS idx_sources_company
  ON sources(company_id);

CREATE INDEX IF NOT EXISTS idx_sources_source_url
  ON sources(source_url);

CREATE INDEX IF NOT EXISTS idx_sources_tenant_company
  ON sources(tenant_id, company_id);

CREATE INDEX IF NOT EXISTS idx_sources_tenant_source_url
  ON sources(tenant_id, source_url);

CREATE INDEX IF NOT EXISTS idx_sources_tenant_run_id
  ON sources(tenant_id, run_id);

-- ---------------------------------------------------------------------------
-- people
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS people (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,

  -- optional run linkage (recommended for auditability)
  run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,

  company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  first_name TEXT,
  last_name TEXT,
  full_name TEXT,
  title TEXT,
  source_url TEXT,             -- where we found the person
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_people_company
  ON people(company_id);

CREATE INDEX IF NOT EXISTS idx_people_tenant_company
  ON people(tenant_id, company_id);

CREATE INDEX IF NOT EXISTS idx_people_tenant_run_id
  ON people(tenant_id, run_id);

CREATE INDEX IF NOT EXISTS idx_people_tenant_company_full_name
  ON people(tenant_id, company_id, full_name);

-- ---------------------------------------------------------------------------
-- emails (can be published or generated permutations)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS emails (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,

  -- optional run linkage (recommended for auditability)
  run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,

  person_id BIGINT REFERENCES people(id) ON DELETE SET NULL,
  company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  is_published INTEGER DEFAULT 0,  -- 1 if seen on-page
  source_url TEXT,                 -- page showing this exact email if published
  icp_score REAL,                  -- denormalized for convenience
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_emails_company
  ON emails(company_id);

CREATE INDEX IF NOT EXISTS idx_emails_tenant_company
  ON emails(tenant_id, company_id);

CREATE INDEX IF NOT EXISTS idx_emails_tenant_person
  ON emails(tenant_id, person_id);

CREATE INDEX IF NOT EXISTS idx_emails_tenant_run_id
  ON emails(tenant_id, run_id);

-- enforce tenant-scoped idempotency by email (single truth per tenant per email)
DROP INDEX IF EXISTS idx_emails_email;
DROP INDEX IF EXISTS ux_emails_email;

CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_tenant_email
  ON emails(tenant_id, email);

-- ---------------------------------------------------------------------------
-- verification results (many per email over time)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS verification_results (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,

  -- optional run linkage (recommended; can be derived via email -> emails.run_id)
  run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,

  email_id BIGINT NOT NULL REFERENCES emails(id) ON DELETE CASCADE,

  -- R16: primary SMTP probe outcome (low-level)
  mx_host TEXT,
  status TEXT,                      -- legacy/raw status (kept for backwards-compat)
  reason TEXT,                      -- legacy/raw reason
  checked_at TEXT DEFAULT CURRENT_TIMESTAMP,

  -- O07: vendor fallback status (low-level)
  fallback_status TEXT,             -- e.g. "deliverable" | "undeliverable" | "unknown"
  fallback_raw TEXT,                -- raw vendor payload / JSON blob
  fallback_checked_at TEXT,         -- ISO-8601 UTC timestamp

  -- R18: canonical classification
  verify_status TEXT,               -- "valid" | "risky_catch_all" | "invalid" | "unknown_timeout"
  verify_reason TEXT,               -- short machine-readable reason (rcpt_2xx_non_catchall, ...)
  verified_mx TEXT,                 -- MX host actually used for classification
  verified_at TEXT,                 -- ISO-8601 UTC timestamp of last classification

  -- O26: test-send / bounce tracking
  test_send_status TEXT NOT NULL DEFAULT 'not_requested',
  test_send_token TEXT,
  test_send_at TEXT,                -- ISO-8601 UTC (when test email was sent)
  bounce_code TEXT,                 -- parsed DSN status code, e.g. "5.1.1"
  bounce_reason TEXT,               -- normalized reason, e.g. "user_unknown"

  CHECK (
    test_send_status IN ('not_requested','requested','sent','delivered','bounced','complained','failed')
  )
);

CREATE INDEX IF NOT EXISTS idx_verif_email
  ON verification_results(email_id);

CREATE INDEX IF NOT EXISTS idx_verif_tenant_email
  ON verification_results(tenant_id, email_id);

CREATE INDEX IF NOT EXISTS idx_verif_tenant_run_id
  ON verification_results(tenant_id, run_id);

CREATE INDEX IF NOT EXISTS idx_verif_test_send_token
  ON verification_results(test_send_token);

CREATE INDEX IF NOT EXISTS idx_verif_tenant_test_send_token
  ON verification_results(tenant_id, test_send_token);

-- ---------------------------------------------------------------------------
-- suppression (global/tenant-wide do-not-verify/contact)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS suppression (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  email TEXT,
  domain TEXT,
  reason TEXT,
  source TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(tenant_id, email, domain)
);

-- ---------------------------------------------------------------------------
-- R07 ingestion staging table
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingest_items (
  id BIGSERIAL PRIMARY KEY,
  tenant_id     TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  company       TEXT,
  domain        TEXT,
  role          TEXT NOT NULL,
  first_name    TEXT,
  last_name     TEXT,
  full_name     TEXT,
  title         TEXT,
  source_url    TEXT,
  notes         TEXT,

  norm_domain   TEXT,
  norm_company  TEXT,
  norm_role     TEXT,

  errors        TEXT NOT NULL DEFAULT '[]',
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_ingest_items_created_at
  ON ingest_items(created_at);

CREATE INDEX IF NOT EXISTS ix_ingest_items_tenant_created_at
  ON ingest_items(tenant_id, created_at);

-- ---------------------------------------------------------------------------
-- R08: resolution audit log (many attempts per company over time)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS domain_resolutions (
  id               BIGSERIAL PRIMARY KEY,
  tenant_id        TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  company_id       BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  company_name     TEXT NOT NULL,
  user_hint        TEXT,                      -- from ingest row (may be NULL)
  chosen_domain    TEXT,                      -- punycode ascii
  method           TEXT NOT NULL,             -- 'http_ok' | 'dns_valid' | 'http_redirect' | 'candidate' | 'none'
  confidence       DOUBLE PRECISION,          -- 0..100 (allow floats)
  reason           TEXT,                      -- short human-readable decision note
  resolver_version TEXT NOT NULL,             -- e.g. 'r08.3'
  created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  -- R17: cached catch-all verdict (domain-level)
  catch_all_status      TEXT,                 -- "catch_all" | "not_catch_all" | "tempfail" | "no_mx" | "error"
  catch_all_checked_at  TEXT,                 -- ISO8601 UTC (when we last probed)
  catch_all_localpart   TEXT,                 -- random local-part used in the probe
  catch_all_smtp_code   INTEGER,              -- raw RCPT code (e.g. 250, 550)
  catch_all_smtp_msg    TEXT,                 -- shortened/decoded SMTP message (optional)

  CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 100))
);

CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id
  ON domain_resolutions(company_id);

CREATE INDEX IF NOT EXISTS idx_domain_resolutions_tenant_company_id
  ON domain_resolutions(tenant_id, company_id);

-- ---------------------------------------------------------------------------
-- View: v_emails_latest
--   Latest verification result per email, joined to people/companies.
--   Exposes R18 canonical verify_status/verify_reason/verified_at.
--   Uses DISTINCT ON to guarantee exactly one verification row per (tenant_id,email_id).
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS v_emails_latest;

CREATE VIEW v_emails_latest AS
WITH latest_verification AS (
  SELECT DISTINCT ON (vr.tenant_id, vr.email_id)
    vr.tenant_id,
    vr.email_id,
    vr.id AS verification_result_id
  FROM verification_results AS vr
  ORDER BY
    vr.tenant_id,
    vr.email_id,
    COALESCE(vr.verified_at, vr.checked_at) DESC,
    vr.id DESC
)
SELECT
  e.tenant_id   AS tenant_id,
  e.id          AS email_id,
  e.email,
  e.run_id      AS email_run_id,
  e.company_id,
  e.person_id,
  e.is_published,
  e.source_url  AS email_source_url,
  e.icp_score,
  e.created_at  AS email_created_at,
  e.updated_at  AS email_updated_at,

  p.first_name,
  p.last_name,
  p.full_name,
  p.title,
  p.title       AS title_raw,
  p.title       AS title_norm,
  p.source_url  AS person_source_url,

  c.name        AS company_name,
  c.run_id      AS company_run_id,
  LOWER(SPLIT_PART(e.email, '@', 2)) AS company_domain,
  c.domain      AS company_domain_raw,
  c.website_url,
  c.official_domain,
  c.official_domain_source,
  c.official_domain_confidence,
  c.official_domain_checked_at,

  -- canonical source URL for this lead
  COALESCE(e.source_url, p.source_url) AS source_url,

  -- low-level / legacy verification fields (pre-R18)
  vr.mx_host,
  vr.status     AS legacy_status,
  vr.reason     AS legacy_reason,
  vr.checked_at,

  -- R18 canonical fields
  vr.verify_status,
  vr.verify_reason,
  vr.verified_mx,
  vr.verified_at,

  -- Back-compat column expected by early tooling
  COALESCE(vr.verify_reason, vr.reason) AS reason

FROM emails AS e
LEFT JOIN people AS p
  ON p.id = e.person_id AND p.tenant_id = e.tenant_id
LEFT JOIN companies AS c
  ON c.id = e.company_id AND c.tenant_id = e.tenant_id
LEFT JOIN latest_verification AS lv
  ON lv.email_id = e.id AND lv.tenant_id = e.tenant_id
LEFT JOIN verification_results AS vr
  ON vr.id = lv.verification_result_id AND vr.tenant_id = e.tenant_id;

-- ---------------------------------------------------------------------------
-- O14: Materialized view table for facet-friendly lead docs (Postgres search ready)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS lead_search_docs (
  person_id BIGINT PRIMARY KEY,    -- 1:1 with people.id / primary lead row
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  email TEXT,                      -- canonical email for this lead
  verify_status TEXT,              -- latest verify_status
  icp_score INTEGER,               -- normalized ICP score (0-100)
  role_family TEXT,                -- canonical role_family
  seniority TEXT,                  -- canonical seniority
  company_size_bucket TEXT,        -- e.g. "1-10", "11-50", "51-200", ...
  company_industry TEXT,           -- e.g. "B2B SaaS", "Fintech"

  -- Optional: pre-bucketed columns for faster GROUP BY in facets
  icp_bucket TEXT,                 -- "0-39", "40-59", "60-79", "80-100"

  -- Postgres FTS: populated by backfill + trigger/upsert logic later
  doc_tsv tsvector,

  created_at TEXT,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS ix_lead_search_docs_tenant
  ON lead_search_docs(tenant_id);

-- Postgres-only (GIN) index for FTS.
CREATE INDEX IF NOT EXISTS ix_lead_search_docs_tsv
  ON lead_search_docs USING GIN (doc_tsv);

-- ---------------------------------------------------------------------------
-- O23: Admin audit log
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS admin_audit (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
  ts TEXT NOT NULL,          -- ISO-8601 UTC timestamp
  action TEXT NOT NULL,      -- short action key (view_metrics, view_analytics, ...)
  user_id TEXT,              -- optional logical user identifier
  remote_ip TEXT,            -- client IP as seen by FastAPI/uvicorn
  metadata TEXT              -- JSON blob with request context
);

CREATE INDEX IF NOT EXISTS ix_admin_audit_tenant_ts
  ON admin_audit(tenant_id, ts);
