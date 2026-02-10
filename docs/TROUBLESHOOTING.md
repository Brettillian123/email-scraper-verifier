# Troubleshooting

## Common Issues

### Workers Not Processing Jobs

**Symptom**: Runs stay in "queued" status indefinitely.

**Diagnosis**:
```bash
# Check worker status
sudo systemctl status email-scraper-worker@1

# Check Redis connectivity
redis-cli ping

# Check queue depth
redis-cli llen rq:queue:orchestrator
redis-cli llen rq:queue:crawl
redis-cli llen rq:queue:verify

# Check for failed jobs
redis-cli llen rq:queue:failed
```

**Common causes**:
- Worker not listening on the right queues — verify `RQ_QUEUE` env var matches the queues being enqueued to (default: `orchestrator,crawl,generate,verify`)
- Redis connection refused — ensure Redis is running and `REDIS_URL` is correct
- Worker crashed — check `journalctl -u email-scraper-worker@1 -n 50`
- Import error on startup — run `python -m src.queueing.worker` manually to see the traceback

### SMTP Verification Returns All "unknown_timeout"

**Symptom**: Every email gets `verify_status = 'unknown_timeout'`.

**Diagnosis**:
```bash
# Test outbound port 25 connectivity
telnet gmail-smtp-in.l.google.com 25

# Test SMTP probe manually
python scripts/probe_smtp.py test@gmail.com
```

**Common causes**:
- **Port 25 blocked**: Most cloud providers block outbound SMTP by default. Check with your provider (AWS requires an EC2 SMTP unblock request, GCP blocks it entirely on standard VMs)
- **Firewall rules**: Ensure your server's firewall allows outbound TCP port 25
- **SMTP_PROBES_ENABLED=false**: Check your `.env`
- **SMTP_PROBES_ALLOWED_HOSTS too restrictive**: If set, only probes from the listed hostnames are executed. Set to empty for unrestricted access
- **DNS resolution failing**: Ensure the server can resolve MX records (`dig MX gmail.com`)

### All Emails Classified as "risky_catch_all"

**Symptom**: Verification works but every result is `risky_catch_all`.

**Explanation**: The target domain is a catch-all — it accepts mail for any local part. This is common with small businesses using services like Google Workspace with a catch-all enabled, or with email forwarding services.

**Options**:
- This is expected behavior for catch-all domains and is not a bug
- Enable the test-send feature (O26) to confirm delivery and upgrade `risky_catch_all` to `valid`
- Use the third-party fallback (O07) for a second opinion

### Crawling Returns No People

**Symptom**: Autodiscovery crawls pages but finds zero people.

**Diagnosis**:
```bash
# Crawl a domain manually and inspect output
python scripts/crawl_domain.py example.com

# Test extraction on a specific URL
python scripts/extract_candidates.py https://example.com/team
```

**Common causes**:
- **robots.txt blocking**: The site blocks crawlers. Check with `python scripts/fetch_url.py https://example.com/robots.txt`
- **JavaScript-rendered content**: The people are loaded dynamically via JavaScript. The crawler only sees server-rendered HTML. Consider enabling AI extraction (O27) which handles some edge cases better
- **Non-standard page structure**: The extraction heuristics may not recognize the page layout. Check the HTML manually
- **Seed paths miss team pages**: The site uses non-standard URLs for team pages. Add custom seed paths via `CRAWL_DISCOVERY_PATHS`

### Database Connection Errors

**Symptom**: `psycopg2.OperationalError: connection refused` or similar.

**Diagnosis**:
```bash
# Test PostgreSQL connection
psql "$DATABASE_URL" -c "SELECT 1"

# Check PostgreSQL is running
sudo systemctl status postgresql

# Check pg_hba.conf allows your connection method
sudo cat /etc/postgresql/*/main/pg_hba.conf | grep -v '^#'
```

**Common causes**:
- PostgreSQL not running
- Wrong credentials in `DATABASE_URL`
- `pg_hba.conf` doesn't allow password auth for the user/database
- PostgreSQL only listening on a Unix socket — ensure `listen_addresses = 'localhost'` in `postgresql.conf`

### Registration Emails Not Arriving

**Symptom**: Users register but never receive the verification code email.

**Diagnosis**:
```bash
# Check SES configuration
python -c "
from src.auth.ses import send_verification_code
# Check if it raises on import
print('SES module loaded OK')
"
```

**Common causes**:
- `SES_FROM_EMAIL` not verified in AWS SES
- AWS credentials not set or incorrect
- SES still in sandbox mode (can only send to verified addresses)
- `AWS_REGION` / `SES_AWS_REGION` misconfigured

### Run Fails Immediately with "company limit exceeded"

**Symptom**: Run transitions to "failed" with an error about company limits.

**Explanation**: The pipeline enforces a hard limit of 1000 companies per 24-hour rolling window. This is a safety guardrail to prevent runaway runs.

**Resolution**: Wait for the 24-hour window to roll over, or check if a previous run consumed the budget with:

```sql
SELECT COUNT(DISTINCT domain) as domains_used, MIN(created_at), MAX(created_at)
FROM companies
WHERE created_at > NOW() - INTERVAL '24 hours'
AND tenant_id = 'your_tenant_id';
```

## Debugging Techniques

### Inspecting the Dead Letter Queue

Failed jobs that exhaust retries or hit permanent errors are moved to the DLQ:

```bash
python scripts/peek_dlq.py
```

To requeue failed jobs after fixing the underlying issue:

```bash
python scripts/requeue_dlq.py
```

### Enabling Debug Logging

Set `DEBUG=true` in `.env` and restart services. For more granular control:

```python
# In any script
import logging
logging.basicConfig(level=logging.DEBUG)

# For specific modules
logging.getLogger('src.verify.smtp').setLevel(logging.DEBUG)
logging.getLogger('src.crawl.runner').setLevel(logging.DEBUG)
logging.getLogger('src.fetch.client').setLevel(logging.DEBUG)
```

### Manual Pipeline Steps

You can run individual pipeline stages in isolation for debugging:

```bash
# Step 1: Resolve a domain
python scripts/resolve_domains.py example.com

# Step 2: Crawl and inspect pages
python scripts/crawl_domain.py example.com

# Step 3: Extract candidates from a URL
python scripts/extract_candidates.py https://example.com/team

# Step 3b: AI extraction (if enabled)
python scripts/extract_candidates_ai.py https://example.com/team

# Step 4: Generate email permutations
python scripts/generate_permutations.py --first John --last Doe --domain example.com

# Step 5: Probe a specific email via SMTP
python scripts/probe_smtp.py john.doe@example.com

# Step 6: Check catch-all status
python scripts/probe_catchall.py example.com
```

### Diagnosing Catch-All Detection

```bash
python scripts/diagnose_catchall.py example.com
```

This runs the full catch-all detection flow with verbose output showing each MX host tried, the random local part used, and the SMTP response.

### Checking Configuration

```bash
python scripts/print_settings.py
```

Prints all resolved configuration values including defaults and environment overrides.

### Database Inspection

```bash
# Check the latest verification view
psql "$DATABASE_URL" -c "
  SELECT email, verify_status, verify_reason, verified_at
  FROM v_emails_latest
  WHERE tenant_id = 'dev'
  ORDER BY verified_at DESC
  LIMIT 20;
"

# Check run progress
psql "$DATABASE_URL" -c "
  SELECT id, status, label,
         progress_json::json->>'emails_found' as found,
         progress_json::json->>'emails_verified' as verified,
         error
  FROM runs
  ORDER BY created_at DESC
  LIMIT 10;
"
```

## Performance Issues

### Slow Verification

**Symptom**: Verification takes hours for a small batch.

**Tuning**:
- Increase `GLOBAL_MAX_CONCURRENCY` (default 12) if your server can handle more outbound connections
- Increase `PER_MX_MAX_CONCURRENCY_DEFAULT` (default 2) cautiously — too high risks blocking by target MX servers
- Ensure `SMTP_PREFLIGHT_ENABLED=true` to skip unreachable MX hosts quickly
- Reduce `SMTP_CONNECT_TIMEOUT` from 20s to 10s if most servers respond quickly
- Run multiple worker instances

### Slow Crawling

**Symptom**: Crawling phase is the bottleneck.

**Tuning**:
- Reduce `CRAWL_MAX_PAGES_PER_DOMAIN` (default 30 → 8-15 for most sites)
- Reduce `CRAWL_MAX_DEPTH` (default 2 → 1 for shallow crawls)
- Set `CRAWL_SEEDS_LINKED_ONLY=true` to only crawl pages linked from discovery pages
- Lower `FETCH_DEFAULT_DELAY_SEC` (default 3s) if robots.txt doesn't specify a crawl-delay
- Run multiple workers on the `crawl` queue

### High Memory Usage

**Symptom**: Worker processes consume excessive memory.

**Common causes**:
- Large HTML pages in memory during extraction — `CRAWL_HTML_MAX_BYTES` (default 1.5 MB) caps this
- AI extraction sending large payloads — `AI_PEOPLE_MAX_INPUT_TOKENS` (default 1500) limits input size
- Redis memory growth — monitor with `redis-cli info memory` and set `maxmemory` in Redis config

## Error Reference

| Error | Meaning | Resolution |
|---|---|---|
| `PermanentSMTPError` | SMTP server permanently rejected the probe (5xx) | Email is invalid — this is expected for non-existent addresses |
| `TemporarySMTPError` | SMTP server returned a temporary error (4xx) | Will be retried automatically per retry schedule |
| `RobotsBlockInfo` | robots.txt disallows crawling this path | Respected by design — the page will be skipped |
| `Connection refused on port 25` | Target MX not reachable | Check if outbound port 25 is blocked on your server |
| `SMTP timeout` | MX server didn't respond in time | May indicate server is slow or blocking; retries will attempt |
| `No MX records found` | Domain has no mail exchange records | Domain probably doesn't receive email |
| `company limit exceeded` | 24-hour run budget exhausted | Wait for the window to roll over |
