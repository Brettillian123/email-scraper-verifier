-- companies
CREATE TABLE companies (
  id INTEGER PRIMARY KEY,
  name TEXT,
  domain TEXT NOT NULL UNIQUE,
  website_url TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

-- people
CREATE TABLE people (
  id INTEGER PRIMARY KEY,
  company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  first_name TEXT,
  last_name TEXT,
  full_name TEXT,
  title TEXT,
  source_url TEXT,             -- where we found the person
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

-- emails (can be published or generated permutations)
CREATE TABLE emails (
  id INTEGER PRIMARY KEY,
  person_id INTEGER REFERENCES people(id) ON DELETE SET NULL,
  company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  is_published INTEGER DEFAULT 0,  -- 1 if seen on-page
  source_url TEXT,                 -- page showing this exact email if published
  icp_score REAL,                  -- denormalized for convenience
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  UNIQUE(company_id, email)
);

-- verification results (many per email over time)
CREATE TABLE verification_results (
  id INTEGER PRIMARY KEY,
  email_id INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
  mx_host TEXT,
  status TEXT NOT NULL,            -- valid | risky_catch_all | invalid | unknown_timeout
  reason TEXT,
  checked_at TEXT DEFAULT (datetime('now'))
);

-- suppression (global/tenant-wide do-not-verify/contact)
CREATE TABLE suppression (
  id INTEGER PRIMARY KEY,
  email TEXT,
  domain TEXT,
  reason TEXT,
  source TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(email, domain)
);

-- helpful indexes
CREATE INDEX idx_people_company ON people(company_id);
CREATE INDEX idx_emails_company ON emails(company_id);

-- enforce global idempotency by email
DROP INDEX IF EXISTS idx_emails_email;
CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email);

CREATE INDEX idx_verif_email ON verification_results(email_id);

-- R07 ingestion staging table
CREATE TABLE IF NOT EXISTS ingest_items (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
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
  created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
CREATE INDEX IF NOT EXISTS ix_ingest_items_created_at ON ingest_items(created_at);

-- R07 Guardrail: keep raw domain separate from official one
ALTER TABLE companies ADD COLUMN user_supplied_domain TEXT;
CREATE INDEX IF NOT EXISTS idx_companies_user_supplied_domain ON companies(user_supplied_domain);
