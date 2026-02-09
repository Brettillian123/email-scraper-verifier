#!/bin/bash
# diagnose.sh - Diagnose email scraper pipeline issues
#
# Usage: bash diagnose.sh

echo "=============================================="
echo "Email Scraper Pipeline Diagnostics"
echo "=============================================="
echo ""

cd /opt/email-scraper

echo "[1] Service Status"
echo "-------------------"
if pgrep -f "uvicorn" > /dev/null; then
    echo "✓ Uvicorn: Running"
else
    echo "✗ Uvicorn: NOT running"
fi

if pgrep -f "rq worker" > /dev/null; then
    echo "✓ RQ Workers: Running"
    echo "  $(pgrep -af 'rq worker' | head -1)"
else
    echo "✗ RQ Workers: NOT running"
fi

if redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "✓ Redis: Running"
else
    echo "✗ Redis: NOT running"
fi

echo ""
echo "[2] Queue Status"
echo "-----------------"
rq info 2>/dev/null || echo "Could not get queue info"

echo ""
echo "[3] Database Counts"
echo "-------------------"
psql $DATABASE_URL -t << 'SQL'
SELECT 'Companies: ' || COUNT(*) FROM companies;
SELECT 'People: ' || COUNT(*) FROM people;
SELECT 'Emails: ' || COUNT(*) FROM emails;
SELECT 'Verifications: ' || COUNT(*) FROM verification_results;
SELECT 'Runs: ' || COUNT(*) FROM runs;
SQL

echo ""
echo "[4] Recent Run Status"
echo "---------------------"
psql $DATABASE_URL << 'SQL'
SELECT id, status, 
       (progress_json::json->>'phase') as phase,
       created_at
FROM runs 
ORDER BY created_at DESC 
LIMIT 3;
SQL

echo ""
echo "[5] Code Check: upsert_generated_email call"
echo "--------------------------------------------"
if grep -q "pattern_used=pattern_key" src/queueing/tasks.py; then
    echo "✗ BUG PRESENT: pattern_used=pattern_key found"
    echo "  This is the bug that prevents emails from being generated!"
    echo "  Run: bash fix_all.sh"
elif grep -q "domain=dom," src/queueing/tasks.py; then
    echo "✓ Fixed: domain=dom found"
else
    echo "? Unknown state - check manually"
fi

echo ""
echo "[6] Companies with official_domain"
echo "-----------------------------------"
psql $DATABASE_URL -t << 'SQL'
SELECT 'With official_domain: ' || COUNT(*) FROM companies WHERE official_domain IS NOT NULL AND official_domain != '';
SELECT 'Without official_domain: ' || COUNT(*) FROM companies WHERE official_domain IS NULL OR official_domain = '';
SQL

echo ""
echo "[7] Email Generation Check (Last 5 People)"
echo "-------------------------------------------"
psql $DATABASE_URL << 'SQL'
SELECT p.id, p.first_name, p.last_name, c.domain,
       (SELECT COUNT(*) FROM emails WHERE person_id = p.id) as email_count
FROM people p
JOIN companies c ON c.id = p.company_id
ORDER BY p.id DESC
LIMIT 5;
SQL

echo ""
echo "[8] Verification Status Breakdown"
echo "----------------------------------"
psql $DATABASE_URL << 'SQL'
SELECT COALESCE(verify_status, 'NULL') as status, COUNT(*) 
FROM verification_results 
GROUP BY verify_status
ORDER BY COUNT(*) DESC;
SQL

echo ""
echo "=============================================="
echo "Diagnostics Complete"
echo "=============================================="
echo ""
echo "Common issues:"
echo "  - If 'pattern_used=pattern_key' found: Run fix_all.sh"
echo "  - If 'Without official_domain' > 0: Run fix_all.sh"
echo "  - If Workers NOT running: rq worker orchestrator crawl generate verify -v"
echo "  - If Redis NOT running: redis-server"
echo ""
