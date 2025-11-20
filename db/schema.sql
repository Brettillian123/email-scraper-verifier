-- companies (writer updates official_ domain is nullable)
CREATE TABLE IF NOT EXISTS companies (
  id INTEGER PRIMARY KEY,
  name TEXT,
  domain TEXT,                               -- user/ingest field (nullable)
  website_url TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),

  -- canonical resolver outputs written by src/db.py (official_*)
  official_domain TEXT,
  official_domain_source TEXT,
  official_domain_confidence INTEGER,
  official_domain_checked_at TEXT,

  -- raw hint from ingest
  user_supplied_domain TEXT
);

-- handy for lookups by user hint
CREATE INDEX IF NOT EXISTS idx_companies_user_supplied_domain
  ON companies(user_supplied_domain);

-- people
CREATE TABLE IF NOT EXISTS people (
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
CREATE TABLE IF NOT EXISTS emails (
  id INTEGER PRIMARY KEY,
  person_id INTEGER REFERENCES people(id) ON DELETE SET NULL,
  company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  is_published INTEGER DEFAULT 0,  -- 1 if seen on-page
  source_url TEXT,                  -- page showing this exact email if published
  icp_score REAL,                   -- denormalized for convenience
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

-- verification results (many per email over time)
CREATE TABLE IF NOT EXISTS verification_results (
  id INTEGER PRIMARY KEY,
  email_id INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
  mx_host TEXT,
  status TEXT NOT NULL,            -- valid | risky_catch_all | invalid | unknown_timeout
  reason TEXT,
  checked_at TEXT DEFAULT (datetime('now'))
);

-- suppression (global/tenant-wide do-not-verify/contact)
CREATE TABLE IF NOT EXISTS suppression (
  id INTEGER PRIMARY KEY,
  email TEXT,
  domain TEXT,
  reason TEXT,
  source TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(email, domain)
);

-- helpful indexes
CREATE INDEX IF NOT EXISTS idx_people_company ON people(company_id);
CREATE INDEX IF NOT EXISTS idx_emails_company ON emails(company_id);

-- enforce global idempotency by email (single truth for email rows)
DROP INDEX IF EXISTS idx_emails_email;
CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email);

CREATE INDEX IF NOT EXISTS idx_verif_email ON verification_results(email_id);

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

-- R08: resolution audit log (many attempts per company over time)
CREATE TABLE IF NOT EXISTS domain_resolutions (
  id               INTEGER PRIMARY KEY,
  company_id       INTEGER NOT NULL,
  company_name     TEXT NOT NULL,
  user_hint        TEXT,                      -- from ingest row (may be NULL)
  chosen_domain    TEXT,                      -- punycode ascii
  method           TEXT NOT NULL,             -- 'http_ok' | 'dns_valid' | 'http_redirect' | 'candidate' | 'none'
  confidence       INTEGER NOT NULL,          -- 0..100
  reason           TEXT,                      -- short human-readable decision note
  resolver_version TEXT NOT NULL,             -- e.g. 'r08.3'
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  -- R17: cached catch-all verdict (domain-level)
  catch_all_status      TEXT,                 -- "catch_all" | "not_catch_all" | "tempfail" | "no_mx" | "error"
  catch_all_checked_at  TEXT,                 -- ISO8601 UTC (when we last probed)
  catch_all_localpart   TEXT,                 -- random local-part used in the probe
  catch_all_smtp_code   INTEGER,              -- raw RCPT code (e.g. 250, 550)
  catch_all_smtp_msg    TEXT,                 -- shortened/decoded SMTP message (optional)

  FOREIGN KEY(company_id) REFERENCES companies(id)
);

CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id
  ON domain_resolutions(company_id);
