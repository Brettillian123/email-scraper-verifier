# Email Scraper — B2B Lead Discovery & Verification Platform

A production-grade B2B lead generation system that discovers people from company websites, generates email permutations, verifies deliverability via SMTP probing, and exposes results through a searchable web dashboard and REST API.

## What It Does

Given a list of target company domains, the system:

1. **Crawls** company websites (respecting `robots.txt`) to find team/leadership pages
2. **Extracts** people — names, titles, and published emails — from HTML (with optional AI-assisted extraction via OpenAI)
3. **Generates** email permutations based on detected domain patterns (e.g., `first.last@`, `flast@`)
4. **Verifies** each email via SMTP RCPT-TO probing, catch-all detection, and optional third-party fallback
5. **Scores** leads against a configurable Ideal Customer Profile (ICP)
6. **Exports** verified leads as CSV/JSON through a web dashboard or API, subject to configurable export policies

## Key Features

- **Multi-tenant architecture** with session-based authentication, user registration, and email verification
- **Asynchronous job pipeline** powered by Redis Queue (RQ) with orchestrator, crawl, generate, and verify queues
- **Compliance-first design**: robots.txt enforcement, tiered rate limiting (global, per-MX, per-domain), and full audit trails
- **SMTP verification** with preflight port-25 checks, catch-all detection, and configurable retry/backoff
- **AI-powered extraction** (optional) using OpenAI GPT models for people extraction from unstructured HTML
- **Full-text search** with faceted filtering (role, seniority, verify status, ICP score) via PostgreSQL `tsvector`
- **Web dashboard** with run management, lead browsing, admin analytics, and user management
- **Suppression lists** for CRM deduplication and bounce import
- **Test-send integration** via AWS SES for delivery confirmation beyond SMTP probing

## Quick Start

### Prerequisites

| Dependency | Version | Purpose |
|---|---|---|
| Python | ≥ 3.12 | Runtime |
| PostgreSQL | ≥ 14 | Primary datastore |
| Redis | ≥ 7 | Job queue backend |
| Caddy or Nginx | Any | Reverse proxy (production) |

### 1. Clone and Install

```bash
git clone https://github.com/your-org/email-scraper.git
cd email-scraper

python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
pip install -e ".[dev]"          # editable install with dev/test deps
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your database, Redis, and SMTP settings
```

At minimum, set these values:

```ini
DATABASE_URL=postgresql://scraper_user:YOUR_PASSWORD@127.0.0.1:5432/email_scraper_db <!-- pragma: allowlist secret -->
REDIS_URL=redis://127.0.0.1:6379/0
SMTP_HELO_DOMAIN=verifier.yourdomain.com
SMTP_MAIL_FROM=bounce@verifier.yourdomain.com
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full environment variable reference.

### 3. Initialize the Database

```bash
# Create the PostgreSQL database first
createdb email_scraper_db

# Apply the schema
python scripts/apply_schema.py

# Apply auth tables (registration, sessions, password reset)
python scripts/apply_auth_migration.py

# Apply email verification support
python scripts/apply_003_verification_code.py
```

### 4. Start the Services

You need three processes running. Use separate terminal windows or a process manager:

```bash
# Terminal 1: Web server
uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2: RQ worker (processes all queue types)
python -m src.queueing.worker

# Terminal 3: Redis (if not already running as a system service)
redis-server
```

### 5. Access the Dashboard

Open `http://localhost:8000` in your browser. Register a new account, verify your email, and you're ready to run your first scrape.

## Project Structure

```
email-scraper/
├── src/
│   ├── api/              # FastAPI app, admin routes, middleware
│   │   ├── app.py        # Main FastAPI application
│   │   ├── admin.py      # Admin dashboard routes
│   │   ├── browser.py    # People cards / browser UI routes
│   │   ├── deps.py       # Dependency injection (auth context)
│   │   └── middleware/    # Body size limiting
│   ├── auth/             # Session auth, registration, password reset
│   │   ├── core.py       # Auth logic (sessions, users, tokens)
│   │   ├── routes.py     # Auth HTTP routes
│   │   ├── middleware.py  # RequireAuthMiddleware
│   │   ├── ses.py        # SES email sending (verification, reset)
│   │   └── templates/    # Login, register, reset HTML templates
│   ├── crawl/            # Website crawling
│   │   ├── runner.py     # Multi-page crawler with depth/page limits
│   │   └── targets.py    # Seed URL generation
│   ├── extract/          # People extraction from HTML
│   │   ├── candidates.py # Regex/heuristic extraction
│   │   ├── ai_candidates.py  # OpenAI-powered extraction
│   │   └── stopwords.py  # Name filtering
│   ├── fetch/            # HTTP client layer
│   │   ├── client.py     # httpx-based fetcher with retries
│   │   ├── robots.py     # robots.txt parsing and enforcement
│   │   ├── cache.py      # HTTP response caching
│   │   └── throttle.py   # Per-domain request throttling
│   ├── generate/         # Email permutation generation
│   │   ├── patterns.py   # Domain pattern detection (first.last, flast, etc.)
│   │   └── permutations.py  # Candidate email generation
│   ├── ingest/           # Lead data ingestion
│   │   ├── cli.py        # CSV/JSONL ingest CLI
│   │   ├── normalize.py  # Field normalization (R13)
│   │   ├── persist.py    # DB write adapter
│   │   ├── http.py       # HTTP ingest endpoint
│   │   └── validators.py # Input validation
│   ├── resolve/          # Domain resolution
│   │   ├── domain.py     # Company → domain resolution (R08)
│   │   ├── mx.py         # MX record lookup
│   │   └── behavior.py   # MX behavior classification
│   ├── verify/           # Email verification
│   │   ├── smtp.py       # SMTP RCPT-TO probing
│   │   ├── catchall.py   # Catch-all domain detection
│   │   ├── preflight.py  # Port-25 TCP preflight
│   │   ├── status.py     # Canonical verify_status classification
│   │   ├── labels.py     # Human-readable status labels
│   │   ├── fallback.py   # Third-party verification fallback
│   │   ├── delivery_catchall.py  # Delivery-based catch-all upgrade
│   │   └── test_send.py  # AWS SES test-send integration
│   ├── scoring/
│   │   └── icp.py        # ICP scoring engine
│   ├── search/           # Full-text search and facets
│   │   ├── backend.py    # Search backend (FTS)
│   │   ├── indexing.py   # Lead search document indexing
│   │   └── cache.py      # Search result caching
│   ├── export/           # Data export
│   │   ├── exporter.py   # CSV/JSON export logic
│   │   ├── policy.py     # Export policy enforcement
│   │   └── roles.py      # Role-based export filtering
│   ├── admin/            # Admin utilities
│   │   ├── audit.py      # Admin audit logging
│   │   └── metrics.py    # System metrics collection
│   ├── queueing/         # Job queue infrastructure
│   │   ├── tasks.py      # All RQ task definitions
│   │   ├── pipeline_v2.py # Run orchestration pipeline
│   │   ├── worker.py     # RQ worker entry point
│   │   ├── dlq.py        # Dead letter queue handling
│   │   ├── rate_limit.py # Redis-based rate limiting
│   │   └── redis_conn.py # Redis connection factory
│   ├── config.py         # Centralized configuration
│   ├── db.py             # Database operations (PostgreSQL)
│   ├── db_ingest.py      # Ingest-specific DB operations
│   ├── db_pages.py       # Page/source DB operations
│   └── db_suppression.py # Suppression list DB operations
├── db/
│   └── schema.sql        # PostgreSQL schema
├── scripts/              # Migration, backfill, and utility scripts
├── tests/                # pytest test suite
├── docs/                 # Extended documentation
├── samples/              # Sample input files
├── .env.example          # Environment variable template
├── pyproject.toml        # Build config and tool settings
├── requirements.txt      # Python dependencies
└── CaddyFile             # Production reverse proxy config
```

## Pipeline Architecture

Each "run" executes the following stages as RQ jobs:

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────┐
│  Orchestrate │────▶│   Crawl +    │────▶│  Generate   │────▶│  Verify  │
│  (per run)   │     │   Extract    │     │  Emails     │     │  (SMTP)  │
└─────────────┘     │  (per domain)│     │ (per domain)│     │(per email│
                    └──────────────┘     └────────────┘     └──────────┘
                           │                    │                  │
                           ▼                    ▼                  ▼
                      ┌─────────┐         ┌─────────┐       ┌──────────┐
                      │ sources │         │ emails  │       │ verif.   │
                      │ people  │         │         │       │ results  │
                      └─────────┘         └─────────┘       └──────────┘
```

**Run modes**: `autodiscovery` (crawl+extract only), `generate` (email generation only), `verify` (verification only), `full` (all stages).

## Documentation

| Document | Description |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, data flow, and module responsibilities |
| [CONFIGURATION.md](docs/CONFIGURATION.md) | Complete environment variable reference |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide |
| [DEVELOPMENT.md](docs/DEVELOPMENT.md) | Local development setup, testing, and contributing |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues, debugging techniques, and log analysis |
| [API.md](docs/API.md) | REST API reference |

## Running Tests

```bash
# Run full test suite
pytest

# Run a specific requirement's tests
pytest tests/test_r16_smtp.py -v

# Run with coverage
pytest --cov=src --cov-report=html
```

## License

Proprietary — Crestwell Partners. All rights reserved.
