# Configuration Reference

All configuration is managed through environment variables, loaded from a `.env` file at the project root via `python-dotenv`. Copy `.env.example` to `.env` and customize as needed.

## Database

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///dev.db` | PostgreSQL connection string. Format: `postgresql://user:pass@host:port/dbname` |
| `DB_URL` | (falls back to `DATABASE_URL`) | Legacy alias — use `DATABASE_URL` instead |

**Production requirement**: PostgreSQL ≥ 14. SQLite is supported for development only.

## Redis

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis connection URL for job queues and caching |
| `RQ_REDIS_URL` | (falls back to `REDIS_URL`) | Explicit RQ Redis URL if different from cache Redis |

## Queue Configuration

| Variable | Default | Description |
|---|---|---|
| `QUEUE_NAME` | `verify` | Default verification queue name |
| `DLQ_NAME` | `verify_dlq` | Dead letter queue name |
| `RQ_QUEUE` | `orchestrator,crawl,generate,verify` | Comma-separated list of queues for workers to consume |
| `RUNS_QUEUE_NAME` | `orchestrator` | Queue for run orchestration jobs |
| `RQ_WORKER_CLASS` | (auto-detected) | Custom RQ worker class. On Windows, forced to `rq.SimpleWorker` |

## Crawling (R09/R10)

### HTTP Fetching

| Variable | Default | Description |
|---|---|---|
| `FETCH_USER_AGENT` | `EmailVerifierBot/0.9 (...)` | User-Agent header for HTTP requests |
| `FETCH_DEFAULT_DELAY_SEC` | `3` | Minimum delay between requests to the same domain |
| `FETCH_TIMEOUT_SEC` | `5` | HTTP read timeout (seconds) |
| `FETCH_CONNECT_TIMEOUT_SEC` | `5` | HTTP connection timeout (seconds) |
| `FETCH_CACHE_TTL_SEC` | `3600` | HTML response cache TTL (1 hour) |
| `ROBOTS_CACHE_TTL_SEC` | `86400` | robots.txt cache TTL (24 hours) |
| `FETCH_MAX_RETRIES` | `2` | Maximum fetch retry attempts |
| `FETCH_MAX_BODY_BYTES` | `2000000` | Maximum response body size (~2 MB) |
| `FETCH_ALLOWED_CONTENT_TYPES` | `text/html,text/plain` | Allowed MIME types for fetch responses |

### Crawler Behavior

| Variable | Default | Description |
|---|---|---|
| `CRAWL_MAX_PAGES_PER_DOMAIN` | `30` | Maximum pages to crawl per domain |
| `CRAWL_MAX_DEPTH` | `2` | Maximum link-follow depth from seed pages |
| `CRAWL_HTML_MAX_BYTES` | `1500000` | Maximum HTML size to process (~1.5 MB) |
| `CRAWL_CONNECT_TIMEOUT_S` | `10.0` | Crawler connection timeout (float seconds) |
| `CRAWL_READ_TIMEOUT_S` | `15.0` | Crawler read timeout (float seconds) |
| `CRAWL_SEEDS_LINKED_ONLY` | `false` | Only crawl seed paths discovered via link parsing |
| `CRAWL_DISCOVERY_PATHS` | `/,/about` | Comma-separated discovery pages for linked-only mode |
| `CRAWL_SEED_STOP_MIN_PEOPLE_PAGES` | `3` | Stop adding seed tiers after finding this many people-pages |
| `CRAWL_FOLLOW_KEYWORDS` | `team,people,staff,...` | Link text keywords that indicate people-relevant pages |

## SMTP Verification (R16)

### Identity

| Variable | Default | Description |
|---|---|---|
| `SMTP_HELO_DOMAIN` | `verifier.crestwellpartners.com` | HELO/EHLO domain for SMTP conversations |
| `SMTP_MAIL_FROM` | `bounce@{SMTP_HELO_DOMAIN}` | MAIL FROM address for SMTP probing |

### Probe Controls

| Variable | Default | Description |
|---|---|---|
| `SMTP_PROBES_ENABLED` | `true` | Master switch for SMTP probing |
| `SMTP_PROBES_ALLOWED_HOSTS` | (empty = all) | Comma-separated hostnames where probing is allowed. Empty means unrestricted |
| `SMTP_CONNECT_TIMEOUT` | `5` | SMTP connection timeout (seconds) |
| `SMTP_COMMAND_TIMEOUT` | `10` | SMTP command timeout (seconds) |

### Preflight

| Variable | Default | Description |
|---|---|---|
| `SMTP_PREFLIGHT_ENABLED` | `true` | Enable TCP port-25 preflight check before SMTP conversation |
| `SMTP_PREFLIGHT_TIMEOUT_SECONDS` | `1.5` | Preflight TCP connection timeout |
| `SMTP_PREFLIGHT_MAX_ADDRS` | `3` | Maximum MX addresses to try in preflight |
| `SMTP_PREFLIGHT_CACHE_TTL_SECONDS` | `300` | Preflight result cache TTL (5 minutes) |

### MX Resolution

| Variable | Default | Description |
|---|---|---|
| `SMTP_MX_MAX_ADDRS` | `3` | Maximum MX IP addresses to try per domain |
| `SMTP_PREFER_IPV4` | `true` | Prefer IPv4 addresses over IPv6 for MX resolution |

## Rate Limiting

| Variable | Default | Description |
|---|---|---|
| `GLOBAL_MAX_CONCURRENCY` | `12` | Maximum concurrent SMTP connections across all workers |
| `GLOBAL_RPS` | `6` | Global requests-per-second limit |
| `PER_MX_MAX_CONCURRENCY_DEFAULT` | `2` | Maximum concurrent connections per MX host |
| `PER_MX_RPS_DEFAULT` | `1` | Requests-per-second per MX host |

## Retries and Backoff

| Variable | Default | Description |
|---|---|---|
| `VERIFY_MAX_ATTEMPTS` | `5` | Maximum SMTP verification attempts per email |
| `VERIFY_BASE_BACKOFF_SECONDS` | `2` | Base backoff interval for retries |
| `VERIFY_MAX_BACKOFF_SECONDS` | `90` | Maximum backoff cap |
| `RETRY_SCHEDULE` | `5,15,45,90,180` | Comma-separated retry delay schedule (seconds) |

## Third-Party Fallback (O07)

| Variable | Default | Description |
|---|---|---|
| `THIRD_PARTY_VERIFY_URL` | (empty) | External verification API endpoint URL |
| `THIRD_PARTY_VERIFY_API_KEY` | (empty) | API key for third-party verification service |
| `THIRD_PARTY_VERIFY_ENABLED` | `false` | Auto-enabled when both URL and API key are set |

## AI People Extraction (O27)

| Variable | Default | Description |
|---|---|---|
| `OPENAI_API_KEY` | (required if enabled) | OpenAI API key for AI-powered extraction |
| `AI_PEOPLE_MODEL` | `gpt-4o-mini` | OpenAI model to use for extraction |
| `AI_PEOPLE_ENABLED` | `true` | Enable AI-assisted people extraction |
| `AI_PEOPLE_MAX_INPUT_TOKENS` | `1500` | Maximum token count sent to the AI model per page |

## Authentication

| Variable | Default | Description |
|---|---|---|
| `AUTH_MODE` | `session` | Auth mode: `session`, `dev`, `hs256`, or `none` |
| `SESSION_COOKIE_SECURE` | `true` | Set `Secure` flag on session cookies (requires HTTPS) |
| `REGISTRATION_ENABLED` | `true` | Allow new user registration |
| `DEFAULT_TENANT_ID` | `default` | Tenant ID assigned to new registrations |
| `APP_URL` | `http://localhost:8000` | Application base URL (used in emails) |

### Dev Mode

| Variable | Default | Description |
|---|---|---|
| `DEV_TENANT_ID` | `dev` | Default tenant ID in dev auth mode |
| `DEV_USER_ID` | `user_dev` | Default user ID in dev auth mode |

### JWT Mode (hs256)

| Variable | Default | Description |
|---|---|---|
| `AUTH_HS256_SECRET` | (required) | HS256 signing secret for JWT verification |
| `AUTH_JWT_ISSUER` | (optional) | Expected JWT issuer claim |
| `AUTH_JWT_AUDIENCE` | (optional) | Expected JWT audience claim |

## AWS / SES

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | AWS region for SES |
| `AWS_ACCESS_KEY_ID` | (optional) | AWS access key. Omit to use default credential chain |
| `AWS_SECRET_ACCESS_KEY` | (optional) | AWS secret key. Must be set together with access key |
| `SES_FROM_EMAIL` | (required for auth emails) | SES verified sender for auth emails (verification codes, password reset) |
| `SES_FROM_NAME` | `CrestwellIQ` | Display name for auth emails |
| `SES_AWS_REGION` | (falls back to `AWS_REGION`) | Override SES region if different from default |

### Test-Send (O26)

| Variable | Default | Description |
|---|---|---|
| `TEST_SEND_FROM` | (required if used) | FROM address for test-send verification emails |
| `TEST_SEND_REPLY_TO` | (optional) | Reply-To address for test emails |
| `TEST_SEND_MAIL_FROM_DOMAIN` | (required if used) | MAIL FROM / return-path domain |
| `TEST_SEND_BOUNCE_PREFIX` | `bounce` | Bounce address prefix |
| `TEST_SEND_SUBJECT_PREFIX` | `[IQVerifier Test]` | Subject line prefix for test emails |
| `TEST_SEND_BOUNCES_SQS_URL` | (optional) | SQS queue URL for bounce notifications |

## Ingestion

| Variable | Default | Description |
|---|---|---|
| `INGEST_HTTP_TOKEN` | (required for HTTP ingest) | Bearer token for the HTTP ingestion endpoint |
| `BODY_LIMIT_BYTES` | `5242880` | Maximum request body size (5 MiB) |

## Search and Facets

| Variable | Default | Description |
|---|---|---|
| `FACET_USE_MV` | `false` | Use materialized view for facet queries (faster but requires periodic refresh) |

## ICP Scoring (R14)

ICP scoring configuration is loaded from `docs/icp-schema.yaml` (if present and PyYAML is installed). See `src/scoring/icp.py` for the scoring algorithm.

## Admin

| Variable | Default | Description |
|---|---|---|
| `ADMIN_API_KEY` | (empty) | API key for admin endpoints |
| `ADMIN_ALLOWED_IPS` | (empty) | Comma-separated IP allowlist for admin access |
| `DEBUG` | `false` | Enable debug mode (verbose logging, stack traces) |
