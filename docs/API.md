# API Reference

The Email Scraper exposes a REST API under `/api/` paths (for programmatic access) and additional endpoints for the web dashboard. All API endpoints require authentication via the configured `AUTH_MODE`.

## Authentication

Authentication depends on the `AUTH_MODE` environment variable:

**`AUTH_MODE=dev`** (development): Pass tenant and user identity via headers:
```
X-Tenant-Id: dev
X-User-Id: user_dev
```

**`AUTH_MODE=hs256`** (JWT): Pass a signed JWT in the Authorization header:
```
Authorization: Bearer <jwt_token>
```

The JWT must include `tenant_id` and `sub` (user ID) claims, signed with the `AUTH_HS256_SECRET`.

**`AUTH_MODE=session`** (production web): Uses session cookies set by the `/auth/login` flow. API calls from the web dashboard are automatically authenticated via the session cookie.

## Health Check

### `GET /health`

Returns service health status. No authentication required.

**Response** `200 OK`:
```json
{ "ok": true }
```

---

## Runs API

Runs represent a batch of domains to be processed through the lead discovery pipeline.

### `POST /runs`

Create a new run and enqueue it for processing.

**Request body**:
```json
{
  "domains": ["example.com", "acme.io"],
  "label": "Q1 prospects",
  "options": {
    "mode": "full",
    "skip_crawl": false,
    "skip_verify": false
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `domains` | `string[]` | Yes | List of company domains to process |
| `label` | `string` | No | Human-readable label for the run |
| `options` | `object` | No | Run configuration overrides |

**Options**:
| Key | Type | Default | Description |
|---|---|---|---|
| `mode` | `string` | `full` | Pipeline mode: `full`, `autodiscovery`, `generate`, `verify` |
| `skip_crawl` | `bool` | `false` | Skip the crawl/extract phase |
| `skip_verify` | `bool` | `false` | Skip the SMTP verification phase |

**Response** `200 OK`:
```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "queued",
  "tenant_id": "dev",
  "created_at": "2026-02-09T12:00:00Z"
}
```

---

### `GET /runs`

List runs for the current tenant.

**Query parameters**:
| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | `50` | Maximum results (1–200) |

**Response** `200 OK`:
```json
{
  "results": [
    {
      "id": "a1b2c3d4-...",
      "status": "succeeded",
      "label": "Q1 prospects",
      "domains_json": "[\"example.com\"]",
      "created_at": "2026-02-09T12:00:00Z",
      "finished_at": "2026-02-09T12:15:00Z"
    }
  ],
  "limit": 50
}
```

---

### `GET /runs/{run_id}`

Get detailed status and progress for a specific run.

**Response** `200 OK`:
```json
{
  "run_id": "a1b2c3d4-...",
  "tenant_id": "dev",
  "user_id": "user_dev",
  "label": "Q1 prospects",
  "status": "running",
  "domains": ["example.com", "acme.io"],
  "options": { "mode": "full" },
  "progress": {
    "domains_total": 2,
    "domains_completed": 1,
    "emails_found": 15,
    "emails_verified": 8
  },
  "error": null,
  "created_at": "2026-02-09T12:00:00Z",
  "updated_at": "2026-02-09T12:10:00Z",
  "started_at": "2026-02-09T12:00:05Z",
  "finished_at": null
}
```

Run statuses: `queued`, `running`, `succeeded`, `failed`, `cancelled`.

---

### `GET /runs/{run_id}/results`

Get lead results for a completed run.

**Query parameters**:
| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | `100` | Maximum results (1–500) |
| `offset` | `int` | `0` | Pagination offset |

**Response** `200 OK`:
```json
{
  "results": [
    {
      "email": "john.doe@example.com",
      "first_name": "John",
      "last_name": "Doe",
      "full_name": "John Doe",
      "title": "VP of Engineering",
      "company": "Example Corp",
      "company_id": 42,
      "company_domain": "example.com",
      "icp_score": 85,
      "verify_status": "valid",
      "verify_reason": "rcpt_2xx_non_catchall",
      "verified_at": "2026-02-09T12:12:00Z",
      "source_url": "https://example.com/team"
    }
  ],
  "limit": 100,
  "offset": 0
}
```

---

### `GET /runs/{run_id}/export`

Export run results as CSV or JSON file download.

**Query parameters**:
| Param | Type | Default | Description |
|---|---|---|---|
| `format` | `string` | `csv` | Export format: `csv` or `json` |
| `limit` | `int` | `10000` | Maximum rows (up to 100,000) |

**Response** (CSV): `200 OK` with `Content-Type: text/csv` and `Content-Disposition: attachment`.

**Response** (JSON): `200 OK` with JSON body containing `run_id`, `count`, and `results`.

---

## Lead Search API

### `GET /leads/search`

Full-text search across all leads with faceted filtering (R22/R23).

**Query parameters**:
| Param | Type | Required | Description |
|---|---|---|---|
| `q` | `string` | Yes | Search query (full-text) |
| `verify_status` | `string` | No | Comma-separated filter: `valid`, `risky_catch_all`, `invalid`, `unknown_timeout` |
| `icp_min` | `int` | No | Minimum ICP score (0–100) |
| `roles` | `string` | No | Comma-separated role families |
| `seniority` | `string` | No | Comma-separated seniority levels |
| `industries` | `string` | No | Comma-separated company industries |
| `sizes` | `string` | No | Comma-separated company size buckets |
| `tech` | `string` | No | Comma-separated technology keywords |
| `source` | `string` | No | Comma-separated source filters |
| `recency_days` | `int` | No | Only leads verified within N days |
| `sort` | `string` | No | Sort order: `icp_desc` (default), `icp_asc`, `verified_at_desc`, `verified_at_asc` |
| `limit` | `int` | No | Results per page (default 20, max 100) |
| `cursor` | `string` | No | Pagination cursor from a previous response |
| `facets` | `string` | No | Comma-separated facet names to return counts for |

**Response** `200 OK`:
```json
{
  "results": [
    {
      "email": "jane.smith@acme.io",
      "first_name": "Jane",
      "last_name": "Smith",
      "title": "CTO",
      "company_name": "Acme Inc",
      "company_domain": "acme.io",
      "icp_score": 92,
      "verify_status": "valid",
      "source_url": "https://acme.io/leadership"
    }
  ],
  "limit": 20,
  "sort": "icp_desc",
  "next_cursor": "eyJpY3AiOjkyLC4uLn0=",
  "facets": {
    "verify_status": {
      "valid": 45,
      "risky_catch_all": 12,
      "invalid": 8,
      "unknown_timeout": 3
    },
    "seniority": {
      "C-Level": 5,
      "VP": 12,
      "Director": 20,
      "Manager": 15
    }
  }
}
```

---

## Ingestion API

### `POST /ingest`

Ingest lead data via HTTP. Requires authentication.

**Request body**: Raw CSV or JSONL data.

**Headers**:
```
Content-Type: text/csv
```
or
```
Content-Type: application/x-ndjson
```

Body size is limited by `BODY_LIMIT_BYTES` (default 5 MiB).

**Response** `200 OK`:
```json
{ "ok": true, "received_bytes": 4096 }
```

For authenticated HTTP ingestion, the separate `src/ingest/http.py` endpoint also supports Bearer token authentication via `INGEST_HTTP_TOKEN`.

---

## Auth Routes

These routes serve HTML pages and handle form submissions for the session-based authentication flow. They are not typical JSON API endpoints.

| Route | Method | Description |
|---|---|---|
| `/auth/login` | GET/POST | Login page and form handler |
| `/auth/register` | GET/POST | Registration page (when `REGISTRATION_ENABLED=true`) |
| `/auth/logout` | GET | Clear session and redirect to login |
| `/auth/verify-email` | GET/POST | Email verification code entry |
| `/auth/forgot-password` | GET/POST | Password reset request |
| `/auth/reset-password` | GET/POST | Password reset with token |
| `/auth/pending` | GET | Pending approval page (for unapproved users) |

---

## Admin Routes

Admin routes are served under `/admin/` and require an authenticated session with admin privileges.

| Route | Method | Description |
|---|---|---|
| `/admin/dashboard` | GET | Admin dashboard with system metrics |
| `/admin/users` | GET | User management page |
| `/admin/users/{user_id}/approve` | POST | Approve a pending user |
| `/admin/users/{user_id}/role` | POST | Change a user's role |
| `/admin/metrics` | GET | JSON metrics endpoint |
| `/admin/analytics` | GET | JSON analytics data |

---

## Error Format

All error responses follow this structure:

```json
{
  "error": "error_code",
  "detail": "Human-readable description of what went wrong"
}
```

Common HTTP status codes:
- `400` — Invalid request parameters
- `401` — Authentication required or invalid credentials
- `403` — Insufficient permissions
- `404` — Resource not found
- `500` — Internal server error
