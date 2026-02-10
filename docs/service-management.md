# Email Scraper Operations Guide

## VPS Service Management

This guide covers how to manage and use the Email Scraper system deployed on your VPS with systemd services.

---

## Table of Contents

1. [Service Control (esctl)](#service-control-esctl)
2. [API Reference](#api-reference)
3. [Running Jobs](#running-jobs)
4. [Monitoring Workers](#monitoring-workers)
5. [Viewing Logs](#viewing-logs)
6. [Troubleshooting](#troubleshooting)
7. [Remote Access](#remote-access)

---

## Service Control (esctl)

The `esctl` command is your primary tool for managing the Email Scraper services.

### Quick Reference

| Command | Description |
|---------|-------------|
| `esctl all start` | Start API and all workers |
| `esctl all stop` | Stop everything |
| `esctl all restart` | Restart everything |
| `esctl all status` | Show status of all services |
| `esctl api start` | Start API server only |
| `esctl api stop` | Stop API server |
| `esctl api restart` | Restart API server |
| `esctl api status` | Show API status |
| `esctl api logs` | Tail API logs (Ctrl+C to exit) |
| `esctl worker start` | Start all workers |
| `esctl worker stop` | Stop all workers |
| `esctl worker restart` | Restart all workers |
| `esctl worker status` | Show worker statuses |
| `esctl worker logs` | Tail worker logs |
| `esctl health` | Quick health check |

### Service Architecture

```
┌─────────────────────────────────────────────────────┐
│                      VPS                            │
│                                                     │
│   ┌─────────────────────────────────────────────┐  │
│   │         email-scraper-api.service           │  │
│   │         (FastAPI on port 8000)              │  │
│   └─────────────────────────────────────────────┘  │
│                         │                          │
│                         ▼                          │
│                  ┌─────────────┐                   │
│                  │    Redis    │                   │
│                  │   :6379     │                   │
│                  └─────────────┘                   │
│                    │         │                     │
│            ┌───────┘         └───────┐             │
│            ▼                         ▼             │
│   ┌─────────────────┐     ┌─────────────────┐     │
│   │   worker@1      │     │   worker@2      │     │
│   │  (RQ Worker)    │     │  (RQ Worker)    │     │
│   └─────────────────┘     └─────────────────┘     │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## API Reference

The API runs on `http://localhost:8000` (or your VPS IP/domain).

### Authentication

With `AUTH_MODE=dev`, include these headers:

```
X-Tenant-ID: dev
X-User-ID: user_dev
```

### Endpoints

#### Health Check

```bash
# Check if API is running
curl http://localhost:8000/health
```

Response:
```json
{"ok": true}
```

#### Create a Run (Start Scraping)

```bash
curl -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev" \
  -d '{
    "domains": ["example.com", "another-company.com"],
    "label": "My first run",
    "options": {}
  }'
```

Response:
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "tenant_id": "dev",
  "created_at": "2026-01-21T00:30:00Z"
}
```

#### List All Runs

```bash
curl "http://localhost:8000/runs?limit=50" \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev"
```

Response:
```json
{
  "results": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "status": "running",
      "label": "My first run",
      "created_at": "2026-01-21T00:30:00Z"
    }
  ],
  "limit": 50
}
```

#### Get Run Details

```bash
curl http://localhost:8000/runs/{run_id} \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev"
```

Response:
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "tenant_id": "dev",
  "user_id": "user_dev",
  "label": "My first run",
  "status": "succeeded",
  "domains": ["example.com"],
  "options": {},
  "progress": {
    "phase": "complete",
    "companies_processed": 1,
    "people_found": 5,
    "emails_verified": 5
  },
  "error": null,
  "created_at": "2026-01-21T00:30:00Z",
  "started_at": "2026-01-21T00:30:01Z",
  "finished_at": "2026-01-21T00:32:15Z"
}
```

#### Get Run Results

```bash
curl http://localhost:8000/runs/{run_id}/results \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev"
```

#### Export Results (CSV)

```bash
curl http://localhost:8000/runs/{run_id}/export \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev" \
  -o results.csv
```

#### Search Leads

```bash
curl "http://localhost:8000/leads/search?q=john&domain=example.com" \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev"
```

### Admin Endpoints

#### View Metrics

```bash
curl http://localhost:8000/metrics
```

#### View Analytics

```bash
curl http://localhost:8000/analytics
```

#### Admin Dashboard

Open in browser: `http://your-vps-ip:8000/`

---

## Running Jobs

### Starting a Scraping Job

**Option 1: Via curl**

```bash
curl -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev" \
  -d '{
    "domains": ["targetcompany.com"],
    "label": "Target Company Scrape"
  }'
```

**Option 2: Via Python script**

```python
import requests

response = requests.post(
    "http://localhost:8000/runs",
    headers={
        "X-Tenant-ID": "dev",
        "X-User-ID": "user_dev",
    },
    json={
        "domains": ["targetcompany.com"],
        "label": "Target Company Scrape",
    }
)

run_id = response.json()["run_id"]
print(f"Started run: {run_id}")
```

### Run Statuses

| Status | Meaning |
|--------|---------|
| `queued` | Job is waiting in queue |
| `running` | Job is being processed |
| `succeeded` | Job completed successfully |
| `failed` | Job encountered an error |
| `completed_with_warnings` | Job completed but with some issues |

### Checking Run Progress

```bash
# Watch a run's progress
watch -n 5 'curl -s http://localhost:8000/runs/{run_id} \
  -H "X-Tenant-ID: dev" -H "X-User-ID: user_dev" | jq'
```

---

## Monitoring Workers

### Check Worker Status

```bash
esctl worker status
```

### Check Queue Depth

```bash
# How many jobs are waiting?
redis-cli llen rq:queue:verify

# Check all queues
redis-cli keys "rq:queue:*"
```

### Check Active Jobs

```bash
# List workers
redis-cli keys "rq:worker:*"

# Check worker info
redis-cli hgetall "rq:worker:{worker_id}"
```

### Dead Letter Queue (Failed Jobs)

```bash
# Check DLQ depth
redis-cli llen rq:queue:verify_dlq

# View failed job IDs
redis-cli lrange rq:queue:verify_dlq 0 10
```

---

## Viewing Logs

### API Logs

```bash
# Follow API logs in real-time
esctl api logs

# Or use journalctl directly
sudo journalctl -u email-scraper-api -f

# Last 100 lines
sudo journalctl -u email-scraper-api -n 100 --no-pager

# Logs from last hour
sudo journalctl -u email-scraper-api --since "1 hour ago"

# Logs from specific time
sudo journalctl -u email-scraper-api --since "2026-01-21 00:00:00"
```

### Worker Logs

```bash
# Follow all worker logs
esctl worker logs

# Or specific worker
sudo journalctl -u email-scraper-worker@1 -f

# All workers with journalctl
sudo journalctl -u "email-scraper-worker@*" -f
```

### Combined Logs

```bash
# All Email Scraper logs
sudo journalctl -u "email-scraper-*" -f

# Filter for errors only
sudo journalctl -u "email-scraper-*" -p err
```

---

## Troubleshooting

### API Won't Start

```bash
# Check status
esctl api status

# Check logs for errors
sudo journalctl -u email-scraper-api -n 50 --no-pager

# Common issues:
# - Port 8000 already in use
# - .env file misconfigured
# - Python dependencies missing
```

**Port in use fix:**

```bash
# Find what's using port 8000
sudo lsof -i :8000

# Kill the process
sudo kill -9 {PID}

# Restart API
esctl api restart
```

### Workers Not Processing

```bash
# Check worker status
esctl worker status

# Check Redis connection
redis-cli ping

# Check queue has jobs
redis-cli llen rq:queue:verify

# Restart workers
esctl worker restart
```

### Jobs Stuck in Queue

```bash
# Check if workers are listening
esctl worker status

# Check for failed jobs
redis-cli llen rq:queue:verify_dlq

# Clear stuck jobs (CAUTION: loses data)
redis-cli del rq:queue:verify
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
psql -h 127.0.0.1 -U scraper_user -d email_scraper_db -c "SELECT 1;"

# Check PostgreSQL is running
sudo systemctl status postgresql
```

### Redis Connection Issues

```bash
# Test Redis
redis-cli ping

# Check Redis is running
sudo systemctl status redis-server

# Restart Redis
sudo systemctl restart redis-server
```

### Memory Issues

```bash
# Check memory usage
free -h

# Check service memory
systemctl status email-scraper-api
systemctl status email-scraper-worker@1

# Restart services to free memory
esctl all restart
```

---

## Remote Access

### From Your Local Machine

```bash
# Check health
ssh root@your-vps "esctl health"

# Restart everything
ssh root@your-vps "esctl all restart"

# Start a run
ssh root@your-vps 'curl -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: dev" \
  -H "X-User-ID: user_dev" \
  -d "{\"domains\": [\"example.com\"]}"'

# Check run status
ssh root@your-vps 'curl -s http://localhost:8000/runs/{run_id} \
  -H "X-Tenant-ID: dev" -H "X-User-ID: user_dev" | jq'
```

### SSH Aliases (Add to ~/.bashrc or ~/.zshrc)

```bash
# Add these to your local machine's shell config
alias vps='ssh root@your-vps'
alias eshealth='ssh root@your-vps "esctl health"'
alias esrestart='ssh root@your-vps "esctl all restart"'
alias eslogs='ssh root@your-vps "esctl api logs"'
alias eswlogs='ssh root@your-vps "esctl worker logs"'
```

Then use:

```bash
eshealth      # Quick health check
esrestart     # Restart all services
eslogs        # Watch API logs
eswlogs       # Watch worker logs
```

### Port Forwarding (Access API Locally)

```bash
# Forward VPS port 8000 to local port 8000
ssh -L 8000:localhost:8000 root@your-vps

# Now access API at http://localhost:8000 from your machine
curl http://localhost:8000/health
```

---

## Configuration Reference

### Environment Variables (.env)

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `REDIS_URL` | Redis connection string | `redis://127.0.0.1:6379/0` |
| `AUTH_MODE` | Authentication mode (none/dev/hs256) | `dev` |
| `SMTP_HELO_DOMAIN` | SMTP identity for verification | Required |
| `CRAWL_MAX_PAGES_PER_DOMAIN` | Max pages to crawl | `8` |
| `CRAWL_MAX_DEPTH` | Max crawl depth | `1` |
| `AI_PEOPLE_ENABLED` | Enable AI extraction | `true` |
| `OPENAI_API_KEY` | OpenAI API key for AI extraction | Optional |

### File Locations

| Path | Description |
|------|-------------|
| `/opt/email-scraper/` | Application root |
| `/opt/email-scraper/.env` | Environment configuration |
| `/opt/email-scraper/.venv/` | Python virtual environment |
| `/opt/email-scraper/data/` | Data files |
| `/etc/systemd/system/email-scraper-*.service` | Systemd service files |
| `/usr/local/bin/esctl` | Management script |

---

## Quick Start Checklist

1. ✅ SSH into VPS: `ssh root@your-vps`
2. ✅ Check services: `esctl health`
3. ✅ Start if needed: `esctl all start`
4. ✅ Test API: `curl http://localhost:8000/health`
5. ✅ Create a run: `curl -X POST http://localhost:8000/runs ...`
6. ✅ Monitor progress: `esctl worker logs`
7. ✅ Export results: `curl http://localhost:8000/runs/{id}/export`

---

## Support

For issues:

1. Check logs: `esctl api logs` or `esctl worker logs`
2. Verify services: `esctl all status`
3. Test connections: `esctl health`
4. Review configuration: `cat /opt/email-scraper/.env`
