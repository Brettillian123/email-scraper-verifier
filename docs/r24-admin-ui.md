<!-- docs/r24-admin-ui.md -->

# R24 – Admin UI & Status View

R24 introduces a minimal admin surface for monitoring the email verifier pipeline, backed by the same metrics/analytics service used by the CLI (O20) and extended analytics (O17). This document describes:

- The **HTML dashboard** at `/admin/`
- The **JSON status API** at `/admin/metrics`
- The **JSON analytics API** at `/admin/analytics`
- The **CLI admin status command** (`email-scraper admin status`)
- **Auth** considerations (O23)

---

## 1. Overview

The admin UI is intentionally read-only and focused on pipeline health:

- Queue health (RQ queues + workers)
- Verification progress and valid rate
- Cost proxy counters (rough volume metrics)
- Basic analytics (per-day verification history, domain breakdown, error breakdown)

These views are implemented once in `src/admin/metrics.py` and then exposed via:

- FastAPI routes in `src/api/admin.py`
- HTML dashboard template in `src/api/templates/admin.html`
- CLI wrapper in `src/cli.py`

---

## 2. HTML dashboard – `/admin/`

### Route

- Method: `GET`
- Path: `/admin/`
- Module: `src/api/admin.py`
- Template: `src/api/templates/admin.html`

### Behavior

Renders a single-page HTML dashboard that:

- Auto-refreshes every 10 seconds.
- Calls:
  - `GET /admin/metrics`
  - `GET /admin/analytics?window_days=30&top_domains=10&top_errors=10`
- Displays:

1. **Queues**

   - Per-queue:
     - `name`
     - `queued`
     - `started`
     - `failed`
   - Aggregate:
     - Total queued
     - Total failed
   - Color cues:
     - Green when no failures
     - Yellow when some failures
     - Orange when failures dominate

2. **Workers**

   - For each RQ worker:
     - `name`
     - `state` (e.g. `busy`, `idle`, `unknown`)
     - `queues` (comma-separated list)

3. **Verification (summary)**

   - `total_emails`
   - `valid_rate` (% of valid among valid/invalid/risky_catch_all)
   - Per-status counts:
     - `valid`
     - `invalid`
     - `risky_catch_all`
     - Any other `verify_status` values

4. **Cost proxies**

   From `get_cost_counters`:

   - `smtp_probes` – number of verification_result rows
   - `catchall_checks` – number of domain_resolutions with `catch_all_checked_at` set
   - `domains_resolved` – number of domain_resolutions with `resolved_at` set
   - `pages_crawled` – count of rows in `sources`

5. **Verification trend (O17)**

   - Time-series table for the last N days (30 by default) with:
     - `date`
     - `total`
     - `valid`
     - `invalid`
     - `risky_catch_all`
     - `valid_rate`

6. **Domains & errors (O17)**

   - Top domains table:
     - `domain`
     - `total`
     - `valid_rate`
   - Top errors table:
     - error key (e.g. `mx_4xx`, `timeout`, etc.)
     - `count`

If either `/admin/metrics` or `/admin/analytics` fails, a red error banner is shown at the top of the page.

---

## 3. JSON status API – `/admin/metrics`

### Route

- Method: `GET`
- Path: `/admin/metrics`
- Module: `src/api/admin.py`
- Implementation: `get_admin_summary()` from `src/admin/metrics.py`

### Response shape

```jsonc
{
  "queues": [
    {
      "name": "ingest",
      "queued": 3,
      "started": 10,
      "failed": 1
    },
    {
      "name": "smtp",
      "queued": 0,
      "started": 25,
      "failed": 0
    }
  ],
  "workers": [
    {
      "name": "worker-1",
      "queues": ["ingest", "smtp"],
      "state": "busy",
      "last_heartbeat": "2025-01-01T12:34:56.789012"
    }
  ],
  "verification": {
    "total_emails": 42,
    "by_status": {
      "valid": 30,
      "invalid": 10,
      "risky_catch_all": 2
    },
    "valid_rate": 0.7142857142857143
  },
  "costs": {
    "smtp_probes": 42,
    "catchall_checks": 5,
    "domains_resolved": 12,
    "pages_crawled": 7
  }
}
Notes:

last_heartbeat may be null if not available.

valid_rate is a fraction in [0, 1] (clients may convert to %).

4. JSON analytics API – /admin/analytics (O17)
Route
Method: GET

Path: /admin/analytics

Module: src/api/admin.py

Implementation: get_analytics_summary() from src/admin/metrics.py

Query parameters
All parameters are optional; defaults are provided:

window_days (int, default: 30)

Rolling window of verification history to include.

top_domains (int, default: 20)

Number of domains to return in domain breakdown.

top_errors (int, default: 20)

Number of error keys to return in error breakdown.

Response shape
jsonc
Copy code
{
  "verification_time_series": [
    {
      "date": "2025-01-01",
      "total": 10,
      "valid": 7,
      "invalid": 2,
      "risky_catch_all": 1,
      "valid_rate": 0.7
    }
    // ...
  ],
  "domain_breakdown": [
    {
      "domain": "example.com",
      "total": 12,
      "valid": 9,
      "invalid": 2,
      "risky_catch_all": 1,
      "valid_rate": 0.75
    }
    // ...
  ],
  "error_breakdown": {
    "mx_4xx": 5,
    "timeout": 2
    // key -> count
  }
}
This endpoint is used by both:

The HTML dashboard (for tables).

The CLI (email-scraper admin status) for the analytics sections.

5. CLI – email-scraper admin status (O20)
Entry point
Module: src/cli.py

Recommended invocation from repo root:

powershell
Copy code
$PyExe = "python"  # or your venv Python
& $PyExe -m src.cli admin status
(If you register a console script in setup.cfg/pyproject.toml, the installed command name would be email-scraper.)

Arguments
text
Copy code
email-scraper admin status [--window-days N] [--top-domains N] [--top-errors N] [--json]
--window-days (default 30)

Passed through to get_analytics_summary.

--top-domains (default 20)

Passed through to get_analytics_summary.

--top-errors (default 20)

Passed through to get_analytics_summary.

--json

If present, prints JSON instead of human-readable tables.

Human-readable output
Default (no --json) prints:

A banner: Email Scraper – Admin status

Sections (in order):

=== Queues ===

=== Workers ===

=== Verification summary ===

=== Cost proxies ===

=== Verification time series ===

=== Top domains ===

=== Top errors ===

Example:

text
Copy code
Email Scraper – Admin status
============================

=== Queues ===
  name             queued   started    failed
  ------------  --------  --------  --------
  ingest               3        10         1
  smtp                 0        25         0

  Total queued: 3
  Total failed: 1

=== Workers ===
  name               state      queues
  ----------------  ----------  -------------
  worker-1           busy       ingest, smtp

=== Verification summary ===
  Total emails : 42
  Valid rate   : 71.4%

  status                  count
  --------------------  --------
  invalid                     10
  risky_catch_all              2
  valid                        30

=== Cost proxies ===
  SMTP probes     : 42
  Catch-all checks: 5
  Domains resolved: 12
  Pages crawled   : 7

=== Verification time series ===
  date               total    valid   invalid  catch_all   valid_rate
  ------------  ---------  -------  --------  ---------  -----------
  2025-01-01          10        7         2          1        70.0%

=== Top domains ===
  domain                         total   valid_rate
  ----------------------------  ------  -----------
  example.com                       12        75.0%

=== Top errors ===
  error                          count
  ----------------------------  ------
  mx_4xx                             5
  timeout                            2

Tip: use '--json' for machine-readable output.
JSON output
With --json, the CLI prints a combined payload:

jsonc
Copy code
{
  "summary": {
    "queues": [...],
    "workers": [...],
    "verification": {...},
    "costs": {...}
  },
  "analytics": {
    "verification_time_series": [...],
    "domain_breakdown": [...],
    "error_breakdown": {...}
  }
}
This is effectively the union of /admin/metrics and /admin/analytics in one call for scripting usage.

6. Auth & security (O23)
All /admin/* routes share the same security dependency:

Module: src/api/deps.py

Dependency: require_admin, attached at router level in src/api/admin.py:

python
Copy code
router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin)],
)
API key header
The admin API supports an API-key guard via environment configuration:

Env var: ADMIN_API_KEY

Header: x-admin-api-key

Behavior (at a high level):

If ADMIN_API_KEY is unset or empty:

require_admin is effectively a no-op (useful in local dev).

If ADMIN_API_KEY is set:

Requests to /admin/* must send:

x-admin-api-key: <ADMIN_API_KEY>

Otherwise, FastAPI responds with 401 Unauthorized.

Optional IP allowlist
If implemented (per O23 guidance), an additional IP gating layer can be configured:

Env var: ADMIN_ALLOWED_IPS (e.g. "127.0.0.1,10.0.0.0/8")

Typical behavior:

If ADMIN_ALLOWED_IPS is set:

require_admin checks Request.client.host against the allowlist.

Requests from non-allowed IPs receive 403 Forbidden even with a valid key.

If unset:

No IP-based restriction is applied.

Audit logging
O23 also introduces a minimal audit trail:

Table: admin_audit in db/schema.sql

sql
Copy code
CREATE TABLE IF NOT EXISTS admin_audit (
  id INTEGER PRIMARY KEY,
  ts TEXT NOT NULL,
  action TEXT NOT NULL,
  user_id TEXT,
  remote_ip TEXT,
  metadata TEXT
);
Optional helper: src/admin/audit.py (if enabled) can provide a log_admin_action(...) function, which admin endpoints can call with:

action (e.g. "view_metrics", "view_analytics")

user_id (derived from API key or future user identity)

remote_ip

metadata (JSON string of parameters, etc.)

This is designed to be low-friction and can be extended in later releases (R27, etc.) to include tenant IDs and richer user identities.

7. Acceptance & testing
Targeted tests
tests/test_r24_admin_ui.py

Verifies /admin/metrics response shape.

Verifies /admin/ returns HTML and references /admin/metrics.

tests/test_o17_admin_analytics.py (if present)

Verifies /admin/analytics shape and basic value behavior.

tests/test_o20_cli_admin_status.py

Unit tests for CLI admin status (both human and JSON modes), using monkeypatched summary/analytics.

tests/test_o23_admin_auth.py

Verifies require_admin behavior:

401 when API key is required but missing/incorrect.

200 when correct key is supplied.

IP allowlist behavior if configured.

Run them via:

powershell
Copy code
$PyExe = "python"
& $PyExe -m pytest `
  tests/test_r24_admin_ui.py `
  tests/test_o20_cli_admin_status.py `
  tests/test_o23_admin_auth.py
R24 acceptance script
Script: scripts/accept_r24.ps1

Responsibilities:

Runs focused tests (at least tests/test_r24_admin_ui.py).

Starts uvicorn src.api.app:app on a local port.

Hits GET /admin/metrics and prints the JSON summary.

Optionally hits GET /admin/analytics as O17 is available.

Stops the server and exits non-zero on failure.

Usage:

powershell
Copy code
.\scripts\accept_r24.ps1
If this script and the test suite are green, R24 (plus O17/O20/O23 wiring) is considered functionally complete.

makefile
Copy code
::contentReference[oaicite:0]{index=0}
