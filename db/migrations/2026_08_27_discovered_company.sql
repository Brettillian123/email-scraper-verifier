-- Companies discovery has found, scored, and decided about.
--
-- Separate from `companies` on purpose. `companies` means "in the pipeline";
-- this table means "seen, and here is what we thought". Most rows will never
-- become companies, and that record is the point: without it every nightly pass
-- re-crawls and re-scores the same rejects forever, which is the shape of bug
-- that has bitten this system twice already.
--
-- decision:
--   pending   scored, awaiting the auto-queue threshold check
--   queued    promoted into `companies` and enqueued for the normal pipeline
--   parked    scored below the threshold; kept so it is never re-profiled
--   rejected  disqualified outright (already known, no MX, not a company site)

CREATE TABLE IF NOT EXISTS discovered_company (
    id            BIGSERIAL PRIMARY KEY,
    domain        TEXT        NOT NULL UNIQUE,
    source        TEXT        NOT NULL,
    source_detail TEXT,
    icp_score     INTEGER,
    icp_reasons   TEXT,
    signals_json  TEXT,
    decision      TEXT        NOT NULL DEFAULT 'pending',
    decided_at    TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The two questions asked every run: "have we seen this domain?" and
-- "what is still worth acting on?"
CREATE INDEX IF NOT EXISTS idx_discovered_company_decision
    ON discovered_company (decision, icp_score DESC);

-- Sources are configured here rather than in code so a fund or a search phrase
-- can be added without a deploy. `kind` selects the adapter in
-- src/discover/sources.py; `target` is the URL or query it needs.
CREATE TABLE IF NOT EXISTS discovery_source (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT        NOT NULL,
    target      TEXT        NOT NULL,
    label       TEXT,
    enabled     BOOLEAN     NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMPTZ,
    last_found  INTEGER,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kind, target)
);

CREATE INDEX IF NOT EXISTS idx_discovery_source_enabled
    ON discovery_source (enabled, kind);
