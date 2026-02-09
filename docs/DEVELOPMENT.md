# Development Guide

## Local Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 14+ (or use SQLite for quick prototyping)
- Redis 7+
- Git

### Installation

```bash
git clone https://github.com/your-org/email-scraper.git
cd email-scraper

python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

pip install -r requirements.txt
pip install -e ".[dev]"
```

### Database Options

**Option A: PostgreSQL (recommended)**

```bash
createdb email_scraper_dev
cp .env.example .env
# Edit .env: DATABASE_URL=postgresql://localhost/email_scraper_dev
python scripts/apply_schema.py
python scripts/apply_auth_migration.py
python scripts/apply_003_verification_code.py
```

**Option B: SQLite (quick start)**

Leave `DATABASE_URL` unset or use the default. The app will create `dev.db` automatically.

> Note: Some features (full-text search, `DISTINCT ON`, materialized views) require PostgreSQL. SQLite is suitable for testing individual modules but not the full pipeline.

### Running Locally

```bash
# Terminal 1: Start Redis
redis-server

# Terminal 2: Start the web server with auto-reload
uvicorn src.api.app:app --reload --port 8000

# Terminal 3: Start a worker
python -m src.queueing.worker
```

For development auth, set `AUTH_MODE=dev` in `.env`. This bypasses login and uses the configured `DEV_TENANT_ID` / `DEV_USER_ID`.

### Windows Notes

The project supports Windows development with some caveats:

- Workers use `rq.SimpleWorker` automatically (no fork-based `Worker` on Windows)
- Job timeouts are disabled (SIGALRM not available)
- Use `scripts/win_worker.py` as an alternative worker entry point if needed
- PowerShell acceptance tests (`scripts/accept_*.ps1`) are designed for Windows

## Project Layout

The codebase follows a domain-oriented package structure under `src/`:

```
src/
├── api/         → FastAPI web server and middleware
├── auth/        → Authentication (sessions, registration, password reset)
├── crawl/       → Website crawling engine
├── extract/     → People extraction (regex + AI)
├── fetch/       → HTTP client, robots.txt, caching, throttling
├── generate/    → Email pattern detection and permutation
├── ingest/      → CSV/JSONL/HTTP data ingestion pipeline
├── queueing/    → RQ tasks, pipeline orchestration, worker, DLQ
├── resolve/     → Domain and MX resolution
├── scoring/     → ICP scoring
├── search/      → Full-text search and faceted filtering
├── verify/      → SMTP probing, catch-all, verification status
├── export/      → CSV/JSON export with policy enforcement
├── admin/       → Admin audit and metrics
├── config.py    → Centralized configuration
├── db.py        → Core database operations
└── ...          → Supporting DB modules (db_ingest, db_pages, db_suppression)
```

### Naming Conventions

- **Requirement codes**: `R01`–`R28` are required features, `O01`–`O27` are optional enhancements. These appear in docstrings, comments, and test filenames.
- **Migration scripts**: `scripts/migrate_<requirement>_<description>.py`
- **Acceptance tests**: `scripts/accept_<requirement>.ps1` (PowerShell)
- **Unit tests**: `tests/test_<requirement>_<feature>.py`

## Testing

### Running Tests

```bash
# Full suite
pytest

# Verbose output
pytest -v

# Specific test file
pytest tests/test_r16_smtp.py

# Specific test function
pytest tests/test_r16_smtp.py::test_smtp_probe_success -v

# With coverage report
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

### Test Organization

Tests are organized by requirement/feature:

| File | Covers |
|---|---|
| `test_r08_integration.py` | Domain resolution (R08) |
| `test_r10_crawler.py` | Web crawling (R10) |
| `test_r11_extraction.py` | People extraction (R11) |
| `test_r12_generator.py` | Email generation (R12) |
| `test_r14_scoring.py` | ICP scoring (R14) |
| `test_r16_smtp.py` | SMTP verification (R16) |
| `test_r17_catchall.py` | Catch-all detection (R17) |
| `test_r18_verify_status.py` | Verification status (R18) |
| `test_r21_search_indexing.py` | Search indexing (R21) |
| `test_r22_api.py` | API endpoints (R22) |
| `test_r23_facets_backend.py` | Faceted search (R23) |
| `test_robots_enforcement.py` | robots.txt compliance |
| `test_rate_limit_concurrency.py` | Rate limiting |

### Test Fixtures

Sample data files live in `tests/fixtures/`:
- `leads_small.csv` / `leads_small.jsonl` — small lead datasets for ingestion tests
- `r25_e2e_batch.csv` — end-to-end test batch
- `r25_known_domains.csv` — known-good domains for QA

### Writing Tests

Key patterns used in this project:

```python
# Use respx to mock HTTP requests
import respx

@respx.mock
def test_fetch_blocked_by_robots(self):
    respx.get("https://example.com/robots.txt").respond(
        text="User-agent: *\nDisallow: /private/"
    )
    # ... test that /private/ is blocked

# Use fakeredis for Redis-dependent tests
from fakeredis import FakeRedis

def test_rate_limit():
    r = FakeRedis()
    # ... test rate limiting logic

# Database tests should use a clean test database or transactions
def test_upsert_email(tmp_db):
    # tmp_db is a fixture providing a clean database connection
    ...
```

## Code Quality

### Linting

The project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Check for issues
ruff check .

# Auto-fix issues
ruff check --fix .

# Format code
ruff format .
```

Ruff configuration is in `pyproject.toml`:
- Target: Python 3.12
- Line length: 100
- Enabled rules: E (pycodestyle), F (pyflakes), I (isort), B (bugbear), UP (pyupgrade), C90 (mccabe complexity)
- Max complexity: 16

### Type Hints

All function signatures should include type hints. The codebase uses `from __future__ import annotations` for modern annotation syntax.

### Pre-commit

If `.pre-commit-config.yaml` is present, install hooks:

```bash
pip install pre-commit
pre-commit install
```

This runs Ruff and other checks automatically before each commit.

## Common Development Tasks

### Adding a New Migration

```bash
# 1. Create the migration script
touch scripts/migrate_<requirement>_<description>.py

# 2. Write idempotent SQL (use IF NOT EXISTS, ON CONFLICT DO NOTHING)
# 3. Update schema.sql to include the new columns/tables for fresh installs
# 4. Test: python scripts/migrate_<requirement>_<description>.py
```

### Adding a New Task

1. Define the task function in `src/queueing/tasks.py`
2. Wire it into the pipeline in `src/queueing/pipeline_v2.py`
3. Add a test in `tests/test_<feature>.py`

### Adding a New API Endpoint

1. Add the route in `src/api/app.py` or a dedicated route module
2. Use `AuthContext` dependency injection for tenant/user scoping
3. Add tests in `tests/test_r22_api.py`

### Running a Specific Script

```bash
# Crawl a single domain (debugging)
python scripts/crawl_domain.py example.com

# Probe SMTP for a specific email
python scripts/probe_smtp.py test@example.com

# Export leads from the database
python scripts/export_leads.py --format csv --output leads.csv

# Ingest a CSV file
python -m src.ingest.cli samples/leads.csv

# Check current settings
python scripts/print_settings.py

# Peek at the dead letter queue
python scripts/peek_dlq.py
```

### Seeding Development Data

```bash
python scripts/seed_dev.py
```

This populates the database with sample companies, people, and emails for local development.

## Useful SQL Queries

```sql
-- Check run status
SELECT id, status, label, created_at, finished_at
FROM runs ORDER BY created_at DESC LIMIT 10;

-- Count leads by verification status
SELECT verify_status, COUNT(*)
FROM v_emails_latest
WHERE tenant_id = 'dev'
GROUP BY verify_status;

-- Find unverified emails for a domain
SELECT e.email, e.created_at
FROM emails e
JOIN companies c ON c.id = e.company_id
LEFT JOIN verification_results vr ON vr.email_id = e.id
WHERE c.domain = 'example.com' AND vr.id IS NULL;

-- Check catch-all status for domains
SELECT company_name, chosen_domain, catch_all_status
FROM domain_resolutions
WHERE catch_all_status IS NOT NULL
ORDER BY created_at DESC LIMIT 20;

-- Queue health
SELECT origin, status, COUNT(*)
FROM rq_job  -- if using rq's built-in job table
GROUP BY origin, status;
```
