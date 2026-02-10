# Deployment Guide

This guide covers deploying the Email Scraper to a production Linux server. The reference setup uses Ubuntu 24.04, PostgreSQL 16, Redis 7, Caddy (for automatic HTTPS), and systemd for process management.

## Prerequisites

- A Linux VPS or dedicated server with at least 2 GB RAM and 2 CPU cores
- A registered domain with DNS pointing to your server's IP
- Python 3.12+ installed
- PostgreSQL 14+ installed and running
- Redis 7+ installed and running
- Outbound port 25 (SMTP) not blocked by your hosting provider — verify with `telnet gmail-smtp-in.l.google.com 25`

> **Important**: Many cloud providers (AWS, GCP, Azure) block outbound port 25 by default. You may need to request an exception or use a dedicated SMTP relay server.

## Step 1: System Setup

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.12 and build dependencies
sudo apt install -y python3.12 python3.12-venv python3.12-dev \
    build-essential libpq-dev git

# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Install Redis
sudo apt install -y redis-server
sudo systemctl enable redis-server

# Install Caddy
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install -y caddy
```

## Step 2: Database Setup

```bash
# Create the database user and database
sudo -u postgres psql <<EOF
CREATE USER scraper_user WITH PASSWORD 'YOUR_SECURE_PASSWORD'; # pragma: allowlist secret
CREATE DATABASE email_scraper_db OWNER scraper_user;
GRANT ALL PRIVILEGES ON DATABASE email_scraper_db TO scraper_user;
EOF
```

## Step 3: Application Setup

```bash
# Create application user
sudo useradd -m -s /bin/bash scraper
sudo su - scraper

# Clone repository
git clone https://github.com/your-org/email-scraper.git
cd email-scraper

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

## Step 4: Environment Configuration

```bash
cp .env.example .env
chmod 600 .env   # Restrict permissions — contains secrets
```

Edit `.env` with production values:

```ini
# Database
DATABASE_URL=postgresql://scraper_user:YOUR_SECURE_PASSWORD@127.0.0.1:5432/email_scraper_db <!-- pragma: allowlist secret -->

# Redis
REDIS_URL=redis://127.0.0.1:6379/0

# SMTP Identity — use a domain you control with proper SPF/DKIM
SMTP_HELO_DOMAIN=verifier.yourdomain.com
SMTP_MAIL_FROM=bounce@verifier.yourdomain.com

# Queue config
RQ_QUEUE=orchestrator,crawl,generate,verify
RUNS_QUEUE_NAME=orchestrator

# Auth — production mode
AUTH_MODE=session
SESSION_COOKIE_SECURE=true
APP_URL=https://leads.yourdomain.com

# AWS SES (for auth emails — verification, password reset)
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
SES_FROM_EMAIL=noreply@yourdomain.com
SES_FROM_NAME=YourApp

# AI extraction (optional)
OPENAI_API_KEY=sk-...
AI_PEOPLE_ENABLED=true

# Security
ADMIN_API_KEY=a_long_random_string
DEBUG=false
```

## Step 5: Initialize Database

```bash
source .venv/bin/activate
cd /home/scraper/email-scraper

# Apply main schema
python scripts/apply_schema.py

# Apply auth tables
python scripts/apply_auth_migration.py

# Apply email verification
python scripts/apply_003_verification_code.py

# Apply run metrics and activity tables
psql "$DATABASE_URL" -f db/add_run_metrics_and_user_activity.sql
```

## Step 6: Configure Caddy (Reverse Proxy + HTTPS)

Edit `/etc/caddy/Caddyfile`:

```caddyfile
leads.yourdomain.com {
    reverse_proxy localhost:8000 {
        health_uri /health
        health_interval 30s
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
    }

    header {
        Strict-Transport-Security "max-age=31536000; includeSubDomains; preload"
        X-Frame-Options "SAMEORIGIN"
        X-Content-Type-Options "nosniff"
        X-XSS-Protection "1; mode=block"
        Referrer-Policy "strict-origin-when-cross-origin"
        -Server
    }

    encode gzip

    log {
        output file /var/log/caddy/access.log {
            roll_size 100mb
            roll_keep 5
        }
    }
}

www.leads.yourdomain.com {
    redir https://leads.yourdomain.com{uri} permanent
}
```

```bash
sudo systemctl restart caddy
```

Caddy will automatically obtain and renew Let's Encrypt TLS certificates.

## Step 7: Systemd Services

Create three service units for the web server, worker, and an optional second worker.

### Web Server

Create `/etc/systemd/system/email-scraper-web.service`:

```ini
[Unit]
Description=Email Scraper Web Server
After=network.target postgresql.service redis.service
Requires=postgresql.service redis.service

[Service]
Type=simple
User=scraper
Group=scraper
WorkingDirectory=/home/scraper/email-scraper
Environment=PATH=/home/scraper/email-scraper/.venv/bin:/usr/local/bin:/usr/bin
EnvironmentFile=/home/scraper/email-scraper/.env
ExecStart=/home/scraper/email-scraper/.venv/bin/uvicorn src.api.app:app \
    --host 127.0.0.1 --port 8000 --workers 2 --log-level info
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### RQ Worker

Create `/etc/systemd/system/email-scraper-worker@.service` (template unit for multiple workers):

```ini
[Unit]
Description=Email Scraper Worker %i
After=network.target postgresql.service redis.service
Requires=redis.service

[Service]
Type=simple
User=scraper
Group=scraper
WorkingDirectory=/home/scraper/email-scraper
Environment=PATH=/home/scraper/email-scraper/.venv/bin:/usr/local/bin:/usr/bin
EnvironmentFile=/home/scraper/email-scraper/.env
ExecStart=/home/scraper/email-scraper/.venv/bin/python -m src.queueing.worker
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Enable and Start Services

```bash
sudo systemctl daemon-reload

sudo systemctl enable email-scraper-web
sudo systemctl enable email-scraper-worker@1
sudo systemctl enable email-scraper-worker@2   # Optional second worker

sudo systemctl start email-scraper-web
sudo systemctl start email-scraper-worker@1
sudo systemctl start email-scraper-worker@2
```

### Check Status

```bash
sudo systemctl status email-scraper-web
sudo systemctl status email-scraper-worker@1
sudo journalctl -u email-scraper-web -f         # Follow web logs
sudo journalctl -u email-scraper-worker@1 -f    # Follow worker logs
```

## Step 8: DNS Configuration

For SMTP verification to work reliably, configure these DNS records for your verifier domain:

```
; A record for your verifier domain
verifier.yourdomain.com.    A       YOUR_SERVER_IP

; SPF — authorize your server to send from this domain
verifier.yourdomain.com.    TXT     "v=spf1 ip4:YOUR_SERVER_IP -all"

; Reverse DNS (PTR) — set via your hosting provider's control panel
; YOUR_SERVER_IP should resolve back to verifier.yourdomain.com

; MX record (optional, but helps deliverability of test-sends)
verifier.yourdomain.com.    MX  10  verifier.yourdomain.com.
```

If using AWS SES for test-sends, also configure DKIM and verify the sending domain in the SES console.

## Step 9: First Admin User

```bash
# Register via the web UI at https://leads.yourdomain.com/auth/register
# Then approve the user via the admin CLI:
source /home/scraper/email-scraper/.venv/bin/activate
python scripts/manage_users.py approve <user_email>
```

## Maintenance

### Updating

```bash
sudo su - scraper
cd email-scraper
git pull origin main
source .venv/bin/activate
pip install -r requirements.txt

# Apply any new migrations
python scripts/apply_schema.py

# Restart services
sudo systemctl restart email-scraper-web
sudo systemctl restart email-scraper-worker@1
sudo systemctl restart email-scraper-worker@2
```

### Backups

```bash
# Database backup
pg_dump -U scraper_user email_scraper_db | gzip > backup_$(date +%Y%m%d).sql.gz

# Restore
gunzip -c backup_20260101.sql.gz | psql -U scraper_user email_scraper_db
```

### Log Rotation

Systemd journal handles log rotation automatically. For Caddy access logs, the `roll_size` and `roll_keep` directives in the Caddyfile manage rotation.

### Monitoring Checklist

- **Web server**: `curl -s https://leads.yourdomain.com/health` should return 200
- **Redis**: `redis-cli ping` should return `PONG`
- **PostgreSQL**: `pg_isready` should indicate accepting connections
- **Workers**: `sudo systemctl is-active email-scraper-worker@1` should return `active`
- **Queue depth**: Monitor via the admin dashboard or `rq info` CLI
- **Disk space**: Monitor the PostgreSQL data directory and Redis dump files

## Scaling Considerations

- **More workers**: Enable additional worker instances (`email-scraper-worker@3`, etc.) to increase throughput
- **Separate queues**: Run dedicated workers per queue type (`RQ_QUEUE=verify` on one, `RQ_QUEUE=crawl` on another) to isolate workloads
- **Database connection pooling**: Use PgBouncer if you exceed ~50 concurrent connections
- **Redis memory**: Monitor with `redis-cli info memory`. The default `maxmemory` policy should be `allkeys-lru` for cache safety
- **Multiple web workers**: Increase Uvicorn `--workers` count (1 per CPU core is a reasonable starting point)
