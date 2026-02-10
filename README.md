# Email Scraper & Verifier

A production B2B lead discovery and verification system. Crawls company websites to find people, generates candidate email addresses from name patterns, and verifies them via SMTP probing. Multi-tenant, queue-driven, with a web dashboard and REST API.

**Stack**: Python 3.12 · FastAPI · PostgreSQL · Redis · RQ · httpx · BeautifulSoup · dnspython

## Pipeline Architecture

Each "run" executes up to four stages as RQ jobs:

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

**Run modes**: `full` (all stages) · `autodiscovery` (crawl + extract only) · `generate` (email generation only) · `verify` (SMTP verification only)

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 14+ (SQLite available for quick prototyping, but PostgreSQL required for full features)
- Redis 7+

### 1. Install

```bash
git clone https://github.com/your-org/email-scraper.git
cd email-scraper

python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

pip install -r requirements.txt
pip install -e ".[dev]"
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your database, Redis, and SMTP settings
```

At minimum, set:

```ini
DATABASE_URL=postgresql://scraper_user:YOUR_PASSWORD@127.0.0.1:5432/email_scraper_db  # pragma: allowlist secret
REDIS_URL=redis://127.0.0.1:6379/0
SMTP_HELO_DOMAIN=verifier.yourdomain.com
SMTP_MAIL_FROM=bounce@verifier.yourdomain.com
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full environment variable reference.

### 3. Initialize the Database

```bash
createdb email_scraper_db

python scripts/apply_schema.py
python scripts/apply_auth_migration.py
python scripts/apply_003_verification_code.py
```

### 4. Start Services

You need three processes running:

```bash
# Terminal 1: Redis (if not running as a system service)
redis-server

# Terminal 2: Web server
uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload

# Terminal 3: RQ worker (processes all queue types)
python -m src.queueing.worker
```

For development, set `AUTH_MODE=dev` in `.env` to bypass login.

### 5. Use

Open `http://localhost:8000` for the dashboard. Register an account, verify your email, and create your first run.

Or via API:

```bash
curl -X POST http://localhost:8000/api/browser/runs \
  -H "Content-Type: application/json" \
  -d '{"domains": ["example.com"], "ai_enabled": true}'
```

## Project Structure

```
email-scraper/
├── src/                  # Application source code
│   ├── api/              # FastAPI app, routes, middleware, templates
│   ├── auth/             # Session auth, registration, password reset, SES
│   ├── crawl/            # Website crawling engine
│   ├── extract/          # People extraction (regex + AI via OpenAI)
│   ├── fetch/            # HTTP client, robots.txt, caching, throttling
│   ├── generate/         # Email pattern detection and permutation
│   ├── ingest/           # CSV/JSONL/HTTP ingestion pipeline
│   ├── queueing/         # RQ tasks, pipeline orchestration, worker, DLQ
│   ├── resolve/          # Domain and MX resolution
│   ├── scoring/          # ICP scoring
│   ├── search/           # Full-text search and faceted filtering
│   ├── verify/           # SMTP probing, catch-all, verification status
│   ├── export/           # CSV/JSON export with policy enforcement
│   ├── admin/            # Admin audit and metrics
│   ├── config.py         # Centralized settings (env vars with defaults)
│   └── db.py             # Core database operations
├── db/                   # SQL schema and migrations
│   ├── schema.sql        # Canonical PostgreSQL schema
│   └── migrations/       # Incremental SQL migration files
├── scripts/              # Migration, utility, and acceptance scripts
│   ├── apply_*.py        # Schema bootstrap scripts
│   ├── migrate_*.py      # Idempotent migration scripts
│   ├── accept_*.ps1      # PowerShell acceptance tests per requirement
│   └── *.py              # CLI tools (crawl, probe, export, ingest, DLQ)
├── tests/                # pytest test suite
│   ├── test_*.py         # Tests organized by requirement (R##/O##)
│   └── fixtures/         # Sample CSV/JSONL data for tests
├── docs/                 # Extended documentation
├── samples/              # Sample input files (leads.csv)
├── .github/workflows/    # CI (lint + test on PostgreSQL + Redis)
├── .env.example          # Environment variable template
├── pyproject.toml        # Build config, Ruff, pytest settings
├── requirements.txt      # Pinned Python dependencies
└── CaddyFile             # Production reverse proxy config
```

## Key Modules

| Module | Purpose | Key Files |
|---|---|---|
| **Crawl** | Multi-page crawling with depth/page limits | `src/crawl/runner.py`, `src/crawl/targets.py` |
| **Extract** | Find people on HTML pages (regex + optional OpenAI) | `src/extract/candidates.py`, `src/extract/ai_candidates.py` |
| **Fetch** | HTTP client with robots.txt enforcement, caching, throttling | `src/fetch/client.py`, `src/fetch/robots.py` |
| **Generate** | Detect domain email patterns, generate permutations | `src/generate/patterns.py`, `src/generate/permutations.py` |
| **Verify** | SMTP RCPT TO probing, catch-all detection | `src/verify/smtp.py`, `src/verify/catchall.py` |
| **Resolve** | Domain resolution and MX lookup | `src/resolve/mx.py` |
| **Scoring** | ICP (Ideal Customer Profile) scoring | `src/scoring/icp.py` |
| **Export** | CSV/JSON export with policy enforcement | `src/export/policy.py`, `src/export/writer.py` |
| **Search** | Full-text search with faceted filtering | `src/search/indexer.py`, `src/search/query.py` |
| **Ingest** | CSV/JSONL ingestion with validation | `src/ingest/cli.py` |
| **Queue** | RQ orchestration, DLQ, Windows-compatible worker | `src/queueing/pipeline_v2.py`, `src/queueing/tasks.py` |
| **Auth** | Sessions, registration, email verification, password reset | `src/auth/core.py`, `src/auth/routes.py` |

## Compliance & Rate Limiting

- **robots.txt**: Always checked before crawling. Disallowed paths are never fetched.
- **Rate limiting**: Global, per-domain, and per-MX host throttling on all external requests.
- **SMTP probing**: Sequential per-MX with catch-all detection to skip unnecessary probes.
- **Audit trail**: Every person includes `source_url` provenance.
- **Company limit**: 1000 companies per 24-hour rolling window per tenant.
- **Suppression lists**: Email and domain suppression to respect opt-outs.

## Testing

```bash
pytest                                    # full suite
pytest tests/test_r16_smtp.py -v          # specific requirement
pytest -k "normalization or ingest"       # by keyword
pytest --cov=src --cov-report=html        # with coverage
```

Tests are organized by requirement code (R##/O##). PowerShell acceptance tests (`scripts/accept_*.ps1`) provide end-to-end verification per requirement.

## Useful Scripts

```bash
python scripts/crawl_domain.py example.com              # debug crawl
python scripts/probe_smtp.py --email test@example.com   # debug SMTP
python scripts/export_leads.py --format csv -o out.csv  # export leads
python scripts/ingest_csv.py samples/leads.csv          # ingest data
python scripts/print_settings.py                        # dump config
python scripts/peek_dlq.py                              # inspect DLQ
python scripts/seed_dev.py                              # seed dev data
```

## Windows Development

- Workers use `rq.SimpleWorker` automatically (no fork on Windows)
- Job timeouts disabled (no SIGALRM)
- Use `scripts/win_worker.py` as alternative worker entry point
- Acceptance tests (`scripts/accept_*.ps1`) are designed for Windows/PowerShell

## Production Deployment

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for systemd, Caddy, and PostgreSQL setup.
See [docs/service-management.md](docs/service-management.md) for the `esctl` operations CLI.

## Documentation

| Document | Description |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, data flow, module responsibilities |
| [CONFIGURATION.md](docs/CONFIGURATION.md) | Complete environment variable reference |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment (systemd, Caddy, TLS) |
| [DEVELOPMENT.md](docs/DEVELOPMENT.md) | Local setup, testing, code quality |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues, debugging, log analysis |
| [API.md](docs/API.md) | REST API reference |
| [service-management.md](docs/service-management.md) | VPS ops guide (`esctl`, monitoring) |
| [r25-qa-acceptance.md](docs/r25-qa-acceptance.md) | QA acceptance harness |

## License

Proprietary — Crestwell Partners. All rights reserved.
