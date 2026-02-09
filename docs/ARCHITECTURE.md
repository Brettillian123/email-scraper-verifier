# Architecture

## System Overview

The Email Scraper is a multi-service Python application that discovers B2B leads from company websites, generates and verifies email addresses, and exposes results through a web interface and REST API. It follows an event-driven architecture with Redis Queue (RQ) managing asynchronous work across dedicated queue channels.

## Service Components

### Web Server (FastAPI + Uvicorn)

The primary user-facing service serving the dashboard, API, and authentication flows.

**Entry point**: `src/api/app.py`

Responsibilities:
- Serve the web dashboard (Jinja2 templates) for run management, lead browsing, and admin analytics
- Expose the REST API under `/api/` for programmatic access
- Handle authentication flows (login, registration, email verification, password reset)
- Accept run creation requests and enqueue them to the orchestrator queue
- Serve export downloads (CSV/JSON) with policy enforcement

Key middleware:
- `RequireAuthMiddleware` — enforces session auth on dashboard routes (excludes `/auth/`, `/api/`, `/health`)
- `BodySizeLimitMiddleware` — caps request body size (default 5 MiB)

### RQ Workers

Background job processors that consume from one or more Redis queues. A single worker process can listen on multiple queues.

**Entry point**: `src/queueing/worker.py`

Queues (configurable via `RQ_QUEUE` env var):
- `orchestrator` — run-level coordination; fans out per-domain jobs
- `crawl` — website crawling and people extraction
- `generate` — email permutation generation
- `verify` — SMTP probing and verification

Workers include:
- Dead Letter Queue (DLQ) exception handling for permanent failures
- Windows compatibility (SimpleWorker fallback, no SIGALRM)
- Configurable worker class via `RQ_WORKER_CLASS`

### PostgreSQL

The system of record for all lead data, run state, and authentication.

**Schema**: `db/schema.sql`

Core tables:
- `tenants`, `users` — multi-tenant primitives
- `runs` — run lifecycle tracking (queued → running → succeeded/failed)
- `companies` — target companies with domain resolution results
- `sources` — crawled HTML pages linked to companies
- `people` — extracted individuals with name, title, source URL
- `emails` — generated/discovered email addresses
- `verification_results` — SMTP probe outcomes with canonical `verify_status`
- `domain_resolutions` — audit log of domain resolution attempts
- `suppression` — email/domain suppression lists
- `ingest_items` — staging table for CSV/JSONL ingestion
- `lead_search_docs` — materialized search documents for faceted search

Auth tables (applied separately):
- `auth_users`, `sessions`, `password_reset_tokens` — session-based authentication
- `email_verification_codes` — 6-digit email verification

### Redis

Serves dual purpose as the RQ job queue backend and an in-process cache for rate limiting, throttle state, and robots.txt results.

**Connection**: `src/queueing/redis_conn.py`

## Data Flow

### Full Run Pipeline

```
User creates run via Dashboard/API
    │
    ▼
Orchestrator (pipeline_v2.py)
    │  Validates domains, enforces company limits (1000/24h)
    │  Creates run record (status: "running")
    │  Fans out per-domain jobs:
    │
    ├──▶ task_autodiscovery(domain)  [crawl queue]
    │       │  Resolve domain (R08)
    │       │  Check robots.txt (R09)
    │       │  Crawl seed pages (R10) — depth/page limited
    │       │  Extract people from HTML (R11) — regex + optional AI
    │       │  Save sources + people to DB
    │       │  Score ICP (R14)
    │       ▼
    ├──▶ task_generate_emails(domain)  [generate queue, depends_on crawl]
    │       │  Detect email pattern for domain (first.last, flast, etc.)
    │       │  Generate permutations for each person (R12)
    │       │  Upsert emails to DB
    │       ▼
    └──▶ task_verify_domain(domain)  [verify queue, depends_on generate]
            │  For each unverified email:
            │    Resolve MX records (R15)
            │    Preflight port-25 check
            │    Detect catch-all (R17)
            │    SMTP RCPT-TO probe (R16)
            │    Classify: valid / risky_catch_all / invalid / unknown (R18)
            │    Optional: third-party fallback (O07)
            │  Update verification_results
            ▼
    Run finalization (run_finalize.py)
        │  Aggregate metrics (emails found, verified, valid rate)
        │  Update run status to "succeeded" or "failed"
        │  Log activity metrics
```

### Verification Classification (R18)

The `verify_status` field uses a four-value taxonomy:

| Status | Meaning |
|---|---|
| `valid` | RCPT accepted on a non-catch-all domain, or delivery confirmed |
| `risky_catch_all` | RCPT accepted but domain is catch-all (may accept anything) |
| `invalid` | RCPT rejected (5xx) or hard bounce confirmed |
| `unknown_timeout` | Inconclusive — timeout, temp failure, or no MX |

## Module Responsibilities

### Crawling (`src/crawl/`)

- **runner.py**: Orchestrates multi-page crawling with BFS over seed URLs. Enforces `CRAWL_MAX_PAGES_PER_DOMAIN` and `CRAWL_MAX_DEPTH`. Uses tiered seed paths (team, leadership, about pages) and stops early when enough people-pages are found.
- **targets.py**: Generates seed URLs for a given domain based on configured tiers.

### Fetching (`src/fetch/`)

- **client.py**: httpx-based HTTP client with configurable timeouts, retries (via tenacity), content-type filtering, and body size limits.
- **robots.py**: Parses and caches `robots.txt` with explainability (`RobotsBlockInfo`). Enforces crawl-delay directives. Supports the `EmailVerifierBot` user-agent and `Email-Scraper` alias.
- **cache.py**: HTTP response cache with configurable TTL.
- **throttle.py**: Per-domain request throttling to respect rate limits.

### Extraction (`src/extract/`)

- **candidates.py**: Regex and heuristic-based extraction of people from HTML. Detects names, titles, and published email addresses. Uses role aliases for title normalization.
- **ai_candidates.py**: OpenAI-powered extraction for pages where regex fails. Sends cleaned HTML to GPT and parses structured people data from the response.
- **ai_candidates_wrapper.py**: Coordination layer that tries regex extraction first, falls back to AI if enabled and results are sparse.

### Generation (`src/generate/`)

- **patterns.py**: Detects the email naming pattern for a domain by analyzing existing known emails (e.g., `first.last`, `flast`, `first_last`).
- **permutations.py**: Generates all plausible email permutations for a person given the detected pattern and domain.

### Verification (`src/verify/`)

- **smtp.py**: Core SMTP RCPT-TO probing logic with connection pooling, timeout handling, and error classification.
- **preflight.py**: Fast TCP port-25 reachability check before attempting full SMTP conversation.
- **catchall.py**: Probes a random address to detect catch-all domains.
- **status.py**: Canonical `verify_status` classification engine combining SMTP results, catch-all status, and fallback data.
- **labels.py**: Human-readable label generation for verification outcomes.
- **fallback.py**: Third-party verification API integration (when SMTP is inconclusive).
- **delivery_catchall.py**: Upgrades `risky_catch_all` to `valid` based on actual delivery confirmation.
- **test_send.py**: AWS SES test-send integration for delivery-based verification.

### Ingestion (`src/ingest/`)

- **cli.py**: Command-line tool for bulk CSV/JSONL ingestion with normalization.
- **normalize.py**: Field-level normalization (R13) — domain cleanup, name casing, title standardization.
- **http.py**: HTTP endpoint for programmatic lead ingestion.
- **persist.py**: Adapter for writing normalized rows to the database.
- **validators.py**: Input validation rules.
- **company_enrich.py**: Company metadata enrichment during ingestion.

### Search (`src/search/`)

- **backend.py**: Full-text search backend using PostgreSQL `tsvector`. Supports query parsing, ranking, and result pagination.
- **indexing.py**: Builds and maintains the `lead_search_docs` materialized table for fast faceted queries.
- **cache.py**: Search result caching layer.

### Export (`src/export/`)

- **exporter.py**: Generates CSV and JSON exports from search/query results.
- **policy.py**: Enforces export policies (field inclusion/exclusion, row limits, tenant scoping).
- **roles.py**: Role-based filtering for exports.

## Rate Limiting Strategy

Rate limits are enforced at multiple levels to protect target mail servers:

1. **Global**: `GLOBAL_MAX_CONCURRENCY` (default 12) concurrent SMTP connections, `GLOBAL_RPS` (default 6) requests per second across all workers.
2. **Per-MX Host**: `PER_MX_MAX_CONCURRENCY_DEFAULT` (default 2) concurrent connections per mail exchange, `PER_MX_RPS_DEFAULT` (default 1) RPS.
3. **Per-Domain Crawl**: `CRAWL_MAX_PAGES_PER_DOMAIN` (default 30) pages, `FETCH_DEFAULT_DELAY_SEC` (default 3s) between requests plus robots.txt `Crawl-delay`.
4. **Run-Level**: `HARD_COMPANY_LIMIT_24H` (default 1000) companies per 24-hour window.

## Multi-Tenancy

Every user-owned table includes a `tenant_id` column (defaulting to `'dev'` for backward compatibility). Tenant isolation is enforced at the application layer — all queries are scoped by `tenant_id` from the authenticated session context.

Authentication supports multiple modes via `AUTH_MODE`:
- `session` — production mode with bcrypt-hashed passwords, session cookies, and email verification
- `dev` — development mode with configurable default tenant/user
- `hs256` — JWT-based API authentication
- `none` — no authentication (testing only)
