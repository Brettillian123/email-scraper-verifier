#!/bin/bash
# test_pipeline.sh - Test the email scraper pipeline
#
# Usage: bash test_pipeline.sh [domain]
# Example: bash test_pipeline.sh crestwellpartners.com

DOMAIN=${1:-"crestwellpartners.com"}

echo "=============================================="
echo "Testing Pipeline for: $DOMAIN"
echo "=============================================="
echo ""

cd /opt/email-scraper

# Check services are running
echo "[1/4] Checking services..."
if pgrep -f "uvicorn" > /dev/null; then
    echo "  ✓ Uvicorn is running"
else
    echo "  ✗ Uvicorn is NOT running"
    echo "    Start it with: uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload"
    exit 1
fi

if pgrep -f "rq worker" > /dev/null; then
    echo "  ✓ RQ workers are running"
else
    echo "  ✗ RQ workers are NOT running"
    echo "    Start them with: rq worker orchestrator crawl generate verify -v"
    exit 1
fi

# Create a run
echo ""
echo "[2/4] Creating pipeline run for $DOMAIN..."
RESPONSE=$(curl -s -X POST http://localhost:8000/api/browser/runs \
    -H "Content-Type: application/json" \
    -d "{\"domains\": [\"$DOMAIN\"], \"ai_enabled\": true, \"force_discovery\": true}")

RUN_ID=$(echo $RESPONSE | python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null)

if [ -z "$RUN_ID" ]; then
    echo "  ✗ Failed to create run"
    echo "  Response: $RESPONSE"
    exit 1
fi

echo "  ✓ Run created: $RUN_ID"

# Wait and check progress
echo ""
echo "[3/4] Waiting for pipeline to complete (max 60s)..."
for i in {1..12}; do
    sleep 5
    STATUS=$(curl -s "http://localhost:8000/api/browser/runs/$RUN_ID" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','unknown'))" 2>/dev/null)
    echo "  Status after ${i}0s: $STATUS"
    
    if [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "completed" ]; then
        break
    fi
    if [ "$STATUS" = "failed" ]; then
        echo "  ✗ Run failed!"
        ERROR=$(curl -s "http://localhost:8000/api/browser/runs/$RUN_ID" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error',''))" 2>/dev/null)
        echo "  Error: $ERROR"
        break
    fi
done

# Check results
echo ""
echo "[4/4] Checking database results..."
echo ""
psql $DATABASE_URL << SQLCHECK
SELECT '=== Companies ===' as section;
SELECT id, domain, official_domain, 
       (attrs::json->>'ai_people_extracted')::text as ai_extracted 
FROM companies 
WHERE domain = '$DOMAIN' OR official_domain = '$DOMAIN';

SELECT '=== People ===' as section;
SELECT p.id, p.first_name, p.last_name, p.title
FROM people p
JOIN companies c ON c.id = p.company_id
WHERE c.domain = '$DOMAIN' OR c.official_domain = '$DOMAIN';

SELECT '=== Emails ===' as section;
SELECT e.id, e.email, vr.verify_status, vr.verify_reason
FROM emails e
LEFT JOIN verification_results vr ON vr.email_id = e.id
JOIN companies c ON c.id = e.company_id
WHERE c.domain = '$DOMAIN' OR c.official_domain = '$DOMAIN';

SELECT '=== Summary ===' as section;
SELECT 
    (SELECT COUNT(*) FROM companies WHERE domain = '$DOMAIN' OR official_domain = '$DOMAIN') as companies,
    (SELECT COUNT(*) FROM people p JOIN companies c ON c.id = p.company_id WHERE c.domain = '$DOMAIN' OR c.official_domain = '$DOMAIN') as people,
    (SELECT COUNT(*) FROM emails e JOIN companies c ON c.id = e.company_id WHERE c.domain = '$DOMAIN' OR c.official_domain = '$DOMAIN') as emails,
    (SELECT COUNT(*) FROM verification_results vr JOIN emails e ON e.id = vr.email_id JOIN companies c ON c.id = e.company_id WHERE c.domain = '$DOMAIN' OR c.official_domain = '$DOMAIN') as verifications;
SQLCHECK

echo ""
echo "=============================================="
echo "Test Complete"
echo "=============================================="
echo ""
echo "Expected results:"
echo "  - Companies: 1"
echo "  - People: 4+ (for crestwellpartners.com)"
echo "  - Emails: 12-24 (3-6 per person)"
echo "  - Verifications: Same as emails (or in progress)"
echo ""
echo "If emails = 0, check worker logs for errors."
echo "If verifications = 0, check if verify queue is processing."
echo ""
