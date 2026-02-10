#!/bin/bash
# quick_start.sh - Start workers and create a run
#
# Usage: bash quick_start.sh [domain]

DOMAIN=${1:-"brandtcpa.com"}

echo "=============================================="
echo "Quick Start Pipeline for: $DOMAIN"
echo "=============================================="
echo ""

cd /opt/email-scraper

# Create log directory if needed
echo "[1/5] Creating log directory..."
mkdir -p /var/log/email-scraper
echo "  ✓ Done"

# Check if uvicorn is running
echo ""
echo "[2/5] Checking API server..."
if pgrep -f "uvicorn.*app:app" > /dev/null; then
    echo "  ✓ API server is running"
else
    echo "  Starting API server..."
    nohup uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload > /var/log/email-scraper/api.log 2>&1 &
    sleep 3
    if pgrep -f "uvicorn.*app:app" > /dev/null; then
        echo "  ✓ API server started"
    else
        echo "  ⚠ API server may not have started"
        echo "  Check: tail /var/log/email-scraper/api.log"
    fi
fi

# Start workers
echo ""
echo "[3/5] Starting workers..."
pkill -f "rq worker" 2>/dev/null
sleep 1

# Start workers with proper logging
cd /opt/email-scraper
source .venv/bin/activate 2>/dev/null || true

nohup rq worker orchestrator crawl generate verify -v > /var/log/email-scraper/worker.log 2>&1 &
WORKER_PID=$!
sleep 2

if ps -p $WORKER_PID > /dev/null 2>&1; then
    echo "  ✓ Workers started (PID: $WORKER_PID)"
else
    echo "  ⚠ Workers may have failed to start"
    echo "  Trying direct start..."
    rq worker orchestrator crawl generate verify &
    sleep 2
fi

# Check worker count
WORKER_COUNT=$(pgrep -c -f "rq worker" 2>/dev/null || echo "0")
echo "  Workers running: $WORKER_COUNT"

# Wait for API to be ready
echo ""
echo "[4/5] Waiting for API to be ready..."
for i in {1..10}; do
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "  ✓ API is ready"
        break
    elif curl -s http://localhost:8000/ > /dev/null 2>&1; then
        echo "  ✓ API is ready"
        break
    fi
    echo "  Waiting... ($i/10)"
    sleep 2
done

# Create the run
echo ""
echo "[5/5] Creating pipeline run..."

# Try different API endpoints
RESPONSE=""

# Try /api/browser/runs first
RESPONSE=$(curl -s -X POST http://localhost:8000/api/browser/runs \
    -H "Content-Type: application/json" \
    -d "{\"domains\": [\"${DOMAIN}\"], \"ai_enabled\": true}" 2>/dev/null)

if [ -z "$RESPONSE" ] || echo "$RESPONSE" | grep -q "Not Found\|404\|error"; then
    # Try /api/runs
    RESPONSE=$(curl -s -X POST http://localhost:8000/api/runs \
        -H "Content-Type: application/json" \
        -d "{\"domains\": [\"${DOMAIN}\"], \"ai_enabled\": true}" 2>/dev/null)
fi

if [ -z "$RESPONSE" ] || echo "$RESPONSE" | grep -q "Not Found\|404\|error"; then
    # Try /runs directly
    RESPONSE=$(curl -s -X POST http://localhost:8000/runs \
        -H "Content-Type: application/json" \
        -d "{\"domains\": [\"${DOMAIN}\"]}" 2>/dev/null)
fi

if [ -z "$RESPONSE" ]; then
    echo "  ⚠ No response from API"
    echo ""
    echo "  Try creating run manually via dashboard:"
    echo "  http://YOUR_IP:8000/admin/dashboard"
    echo ""
    echo "  Or via CLI:"
    echo "  python -m src.cli run ${DOMAIN}"
else
    echo "  Response: $RESPONSE"

    RUN_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('run_id') or d.get('id') or '')" 2>/dev/null)

    if [ -n "$RUN_ID" ] && [ "$RUN_ID" != "None" ]; then
        echo "  ✓ Run created: $RUN_ID"
    fi
fi

echo ""
echo "=============================================="
echo "Status Check"
echo "=============================================="
echo ""
echo "Queue status:"
rq info 2>/dev/null | head -20

echo ""
echo "=============================================="
echo "Monitor Commands"
echo "=============================================="
echo ""
echo "  # Watch worker output"
echo "  tail -f /var/log/email-scraper/worker.log"
echo ""
echo "  # Check results"
echo "  psql \$DATABASE_URL -c \"SELECT first_name, last_name FROM people WHERE company_id=66;\""
echo ""
