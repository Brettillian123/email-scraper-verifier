#!/bin/bash
# reset_and_rerun.sh - Reset AI flags and re-run discovery for a domain
#
# This script:
# 1. Resets the ai_people_extracted flag so AI can run again
# 2. Optionally deletes existing people/emails to start fresh
# 3. Triggers a new pipeline run
#
# Usage:
#   bash reset_and_rerun.sh [domain]
#   bash reset_and_rerun.sh [domain] --full  # Also delete people/emails

DOMAIN=${1:-"brandtcpa.com"}
FULL_RESET=${2:-""}

echo "=============================================="
echo "Reset & Re-run Discovery for: $DOMAIN"
echo "=============================================="
echo ""

cd /opt/email-scraper 2>/dev/null || true

# Check current state
echo "[1/5] Current state for $DOMAIN:"
psql $DATABASE_URL -c "
SELECT
    c.id as company_id,
    c.domain,
    c.attrs::json->>'ai_people_extracted' as ai_extracted,
    (SELECT COUNT(*) FROM people WHERE company_id = c.id) as people_count,
    (SELECT COUNT(*) FROM emails WHERE company_id = c.id) as emails_count,
    (SELECT COUNT(*) FROM sources WHERE company_id = c.id) as pages_count
FROM companies c
WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}';
"

# Get company ID
COMPANY_ID=$(psql $DATABASE_URL -t -c "
SELECT id FROM companies
WHERE domain = '${DOMAIN}' OR official_domain = '${DOMAIN}'
LIMIT 1;
" | tr -d ' ')

if [ -z "$COMPANY_ID" ]; then
    echo "ERROR: Company not found for domain $DOMAIN"
    exit 1
fi

echo ""
echo "Company ID: $COMPANY_ID"
echo ""

# Reset AI flag
echo "[2/5] Resetting AI extraction flag..."
psql $DATABASE_URL -c "
UPDATE companies
SET attrs = COALESCE(attrs::jsonb, '{}'::jsonb) - 'ai_people_extracted' - 'ai_extraction_timestamp'
WHERE id = ${COMPANY_ID};
"
echo "  ✓ AI flag reset"

# Full reset if requested
if [ "$FULL_RESET" = "--full" ]; then
    echo ""
    echo "[3/5] Performing FULL reset (deleting people, emails, verification results)..."

    # Delete verification results first (foreign key)
    psql $DATABASE_URL -c "
    DELETE FROM verification_results
    WHERE email_id IN (SELECT id FROM emails WHERE company_id = ${COMPANY_ID});
    "
    echo "  ✓ Deleted verification results"

    # Delete emails
    psql $DATABASE_URL -c "
    DELETE FROM emails WHERE company_id = ${COMPANY_ID};
    "
    echo "  ✓ Deleted emails"

    # Delete people
    psql $DATABASE_URL -c "
    DELETE FROM people WHERE company_id = ${COMPANY_ID};
    "
    echo "  ✓ Deleted people"

    # Optionally delete crawled pages too
    read -p "Delete crawled pages too? (y/N): " DELETE_PAGES
    if [ "$DELETE_PAGES" = "y" ] || [ "$DELETE_PAGES" = "Y" ]; then
        psql $DATABASE_URL -c "
        DELETE FROM sources WHERE company_id = ${COMPANY_ID};
        "
        echo "  ✓ Deleted crawled pages"
    fi
else
    echo ""
    echo "[3/5] Skipping full reset (use --full to delete people/emails)"
fi

# Ensure official_domain is set
echo ""
echo "[4/5] Ensuring official_domain is set..."
psql $DATABASE_URL -c "
UPDATE companies
SET official_domain = domain
WHERE id = ${COMPANY_ID}
  AND (official_domain IS NULL OR official_domain = '');
"
echo "  ✓ official_domain set"

# Create new run
echo ""
echo "[5/5] Creating new pipeline run..."

RUN_RESPONSE=$(curl -s -X POST http://localhost:8000/api/browser/runs \
    -H "Content-Type: application/json" \
    -d "{\"domains\": [\"${DOMAIN}\"], \"ai_enabled\": true, \"force_discovery\": true}")

echo "$RUN_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    if d.get('ok'):
        print(f'  ✓ Run created: {d.get(\"run_id\")}')
        print(f'  Job ID: {d.get(\"job_id\")}')
    else:
        print(f'  ✗ Failed: {d}')
except:
    print('  ✗ Failed to parse response')
" 2>/dev/null || echo "  Run response: $RUN_RESPONSE"

echo ""
echo "=============================================="
echo "Reset Complete!"
echo "=============================================="
echo ""
echo "Monitor progress with:"
echo "  watch -n5 'psql \$DATABASE_URL -c \"SELECT status, progress_json::json->>\\\"phase\\\" as phase FROM runs ORDER BY created_at DESC LIMIT 1;\"'"
echo ""
echo "Or check the dashboard:"
echo "  http://YOUR_IP:8000/admin/dashboard"
echo ""
echo "Watch worker logs:"
echo "  tail -f /var/log/email-scraper/*.log"
echo ""
