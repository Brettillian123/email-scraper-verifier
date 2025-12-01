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

  -- R16: primary SMTP probe outcome (low-level)
  mx_host TEXT,
  status TEXT,                      -- legacy/raw status (kept for backwards-compat)
  reason TEXT,                      -- legacy/raw reason
  checked_at TEXT DEFAULT (datetime('now')),

  -- O07: vendor fallback status (low-level)
  fallback_status TEXT,             -- e.g. "deliverable" | "undeliverable" | "unknown"
  fallback_raw TEXT,                -- raw vendor payload / JSON blob
  fallback_checked_at TEXT,         -- ISO-8601 UTC timestamp

  -- R18: canonical classification
  verify_status TEXT,               -- "valid" | "risky_catch_all" | "invalid" | "unknown_timeout"
  verify_reason TEXT,               -- short machine-readable reason (rcpt_2xx_non_catchall, ...)
  verified_mx TEXT,                 -- MX host actually used for classification
  verified_at TEXT                  -- ISO-8601 UTC timestamp of last classification
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

-- ---------------------------------------------------------------------------
-- View: v_emails_latest
--   Latest verification result per email, joined to people/companies.
--   Exposes R18 canonical verify_status/verify_reason/verified_at.
--   R20 additions:
--     - title_norm/title_raw aliases for export
--     - company_domain derived from email's domain
--     - canonical source_url via COALESCE(email_source_url, person_source_url)
--     - back-compat "reason" column used by older tools/checks
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_emails_latest;

CREATE VIEW v_emails_latest AS
WITH latest_verification AS (
  SELECT
    vr.email_id,
    vr.id AS verification_result_id,
    COALESCE(vr.verified_at, vr.checked_at) AS effective_verified_at
  FROM verification_results AS vr
  JOIN (
    SELECT
      email_id,
      MAX(COALESCE(verified_at, checked_at)) AS max_effective_verified_at
    FROM verification_results
    GROUP BY email_id
  ) AS x
    ON x.email_id = vr.email_id
   AND COALESCE(vr.verified_at, vr.checked_at) = x.max_effective_verified_at
)
SELECT
  e.id          AS email_id,
  e.email,
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
  LOWER(SUBSTR(e.email, INSTR(e.email, '@') + 1)) AS company_domain,
  c.domain      AS company_domain_raw,
  c.website_url,
  c.official_domain,
  c.official_domain_source,
  c.official_domain_confidence,
  c.official_domain_checked_at,

  -- R20 canonical source URL for this lead
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

  -- Back-compat column expected by scripts/check_view.py and early tooling
  COALESCE(vr.verify_reason, vr.reason) AS reason

FROM emails AS e
LEFT JOIN people AS p
  ON p.id = e.person_id
LEFT JOIN companies AS c
  ON c.id = e.company_id
LEFT JOIN latest_verification AS lv
  ON lv.email_id = e.id
LEFT JOIN verification_results AS vr
  ON vr.id = lv.verification_result_id;

-- ---------------------------------------------------------------------------
-- O14: Materialized view table for facet-friendly lead docs
--
-- This denormalized table pre-joins the fields needed for filtering &
-- faceting so R23 facet queries can avoid heavy joins under load.
--
-- It is populated/refreshed by scripts/backfill_o14_lead_search_docs.py
-- and consulted by the search indexing layer when FACET_USE_MV is enabled.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_search_docs (
  person_id INTEGER PRIMARY KEY,   -- 1:1 with people.id / primary lead row
  email TEXT,                      -- canonical email for this lead
  verify_status TEXT,              -- latest verify_status
  icp_score INTEGER,               -- normalized ICP score (0-100)
  role_family TEXT,                -- canonical role_family
  seniority TEXT,                  -- canonical seniority
  company_size_bucket TEXT,        -- e.g. "1-10", "11-50", "51-200", ...
  company_industry TEXT,           -- e.g. "B2B SaaS", "Fintech"

  -- Optional: pre-bucketed columns for faster GROUP BY in facets
  icp_bucket TEXT,                 -- "0-39", "40-59", "60-79", "80-100"

  created_at TEXT,                 -- when this doc was first materialized
  updated_at TEXT                  -- last refresh timestamp
);

-- ---------------------------------------------------------------------------
-- O23: Admin audit log
--
-- Best-effort audit trail for sensitive admin actions (e.g. viewing
-- metrics/analytics). The writer (src/admin/audit.py) swallows failures so
-- missing tables or migration gaps do not break admin endpoints.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS admin_audit (
  id INTEGER PRIMARY KEY,
  ts TEXT NOT NULL,          -- ISO-8601 UTC timestamp
  action TEXT NOT NULL,      -- short action key (view_metrics, view_analytics, ...)
  user_id TEXT,              -- optional logical user identifier
  remote_ip TEXT,            -- client IP as seen by FastAPI/uvicorn
  metadata TEXT              -- JSON blob with request context
);
