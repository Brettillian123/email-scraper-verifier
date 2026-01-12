-- PostgreSQL-compatible schema for email-scraper

CREATE TABLE IF NOT EXISTS companies (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  domain TEXT UNIQUE,
  website_url TEXT,
  official_domain TEXT,
  official_method TEXT,
  official_confidence REAL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sources (
  id SERIAL PRIMARY KEY,
  company_id INTEGER REFERENCES companies(id),
  url TEXT NOT NULL,
  page_type TEXT,
  html TEXT,
  fetched_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sources_company ON sources(company_id);
CREATE INDEX IF NOT EXISTS idx_sources_url ON sources(url);

CREATE TABLE IF NOT EXISTS people (
  id SERIAL PRIMARY KEY,
  company_id INTEGER REFERENCES companies(id),
  full_name TEXT,
  first_name TEXT,
  last_name TEXT,
  title TEXT,
  role TEXT,
  source_url TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS emails (
  id SERIAL PRIMARY KEY,
  person_id INTEGER REFERENCES people(id),
  company_id INTEGER REFERENCES companies(id),
  email TEXT NOT NULL,
  domain TEXT,
  source TEXT,
  source_url TEXT,
  pattern TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_emails_email ON emails(email);

CREATE TABLE IF NOT EXISTS verification_results (
  id SERIAL PRIMARY KEY,
  email_id INTEGER REFERENCES emails(id),
  email TEXT,
  domain TEXT,
  verify_status TEXT,
  verify_reason TEXT,
  verified_mx TEXT,
  verified_at TIMESTAMP,
  checked_at TIMESTAMP DEFAULT NOW(),
  fallback_status TEXT,
  fallback_raw TEXT,
  catch_all_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_vr_email ON verification_results(email);
CREATE INDEX IF NOT EXISTS idx_vr_email_id ON verification_results(email_id);

CREATE TABLE IF NOT EXISTS domain_resolutions (
  id SERIAL PRIMARY KEY,
  company_id INTEGER REFERENCES companies(id),
  domain TEXT NOT NULL,
  lowest_mx TEXT,
  mx_hosts TEXT,
  preference_map TEXT,
  cached BOOLEAN DEFAULT FALSE,
  failure TEXT,
  resolved_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dr_domain ON domain_resolutions(domain);

CREATE TABLE IF NOT EXISTS ingest_items (
  id SERIAL PRIMARY KEY,
  batch_id TEXT,
  row_index INTEGER,
  status TEXT DEFAULT 'pending',
  raw_json TEXT,
  error TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catch_all_cache (
  domain TEXT PRIMARY KEY,
  is_catch_all BOOLEAN,
  checked_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mx_behavior (
  id SERIAL PRIMARY KEY,
  mx_host TEXT NOT NULL,
  domain TEXT,
  probe_count INTEGER DEFAULT 0,
  avg_latency_ms REAL,
  last_code INTEGER,
  last_category TEXT,
  last_error TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mx_behavior_host ON mx_behavior(mx_host);
