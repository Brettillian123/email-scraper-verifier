#!/bin/bash
# master_fix.sh - Apply all fixes and rerun pipeline
#
# This script:
# 1. Applies all Python fixes to the codebase
# 2. Clears failed jobs and caches
# 3. Resets the target domain
# 4. Creates a new pipeline run
#
# Usage: bash master_fix.sh [domain]

DOMAIN=${1:-"brandtcpa.com"}

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║              EMAIL SCRAPER MASTER FIX                            ║"
echo "║              Domain: $DOMAIN"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

cd /opt/email-scraper

# ============================================================================
# STEP 1: Apply Python fixes
# ============================================================================
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ STEP 1: Applying Code Fixes                                      │"
echo "└──────────────────────────────────────────────────────────────────┘"

# Fix 1.1: Lower people_cards classifier threshold
echo "[1.1] Lowering people_cards classifier threshold (8 → 4)..."
sed -i 's/PEOPLE_CARDS_CLASSIFY_MIN_SCORE", 8)/PEOPLE_CARDS_CLASSIFY_MIN_SCORE", 4)/g' \
    src/extract/people_cards.py 2>/dev/null && echo "  ✓ Done" || echo "  ⚠ Skipped"

# Fix 1.2: Add team page URL bypass (if not already present)
echo "[1.2] Adding team page URL bypass..."
if ! grep -q "BYPASS: Always allow obvious team URLs" src/extract/people_cards.py 2>/dev/null; then
    python3 << 'PYFIX12'
import re

filepath = "/opt/email-scraper/src/extract/people_cards.py"
with open(filepath, 'r') as f:
    content = f.read()

old = '''def _classifier_allows_people_cards(html: str, source_url: str) -> bool:
    if _HAS_SOURCE_FILTERS and classify_page_for_people_extraction is not None:'''

new = '''def _classifier_allows_people_cards(html: str, source_url: str) -> bool:
    # BYPASS: Always allow obvious team URLs regardless of classifier score
    url_lower = source_url.lower()
    if any(p in url_lower for p in ['/our-team', '/the-team', '/team/', '/staff', '/people', '/leadership', '/partners', '/about-us']):
        log.debug("Bypassing classifier for team URL: %s", source_url)
        return True
    
    if _HAS_SOURCE_FILTERS and classify_page_for_people_extraction is not None:'''

if old in content:
    content = content.replace(old, new)
    with open(filepath, 'w') as f:
        f.write(content)
    print("  ✓ Done")
else:
    print("  ⚠ Pattern not found or already applied")
PYFIX12
else
    echo "  ✓ Already applied"
fi

# Fix 1.3: Make AI prefilter less aggressive
echo "[1.3] Making AI prefilter less aggressive..."
python3 << 'PYFIX13'
import re

filepath = "/opt/email-scraper/src/extract/ai_candidates_wrapper.py"
try:
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Make the press URL pattern more conservative
    old_pattern = r'_PRESS_URL_PATTERNS = re\.compile\(\s*r"[^"]+"\s*r"[^"]+"\s*r"[^"]+",\s*re\.IGNORECASE,\s*\)'
    
    if '_PRESS_URL_PATTERNS' in content:
        # Check if it contains the broad patterns
        if 'blog|articles' in content and 'resources' in content:
            # Replace with narrower pattern
            new_pattern = '''_PRESS_URL_PATTERNS = re.compile(
    r"/(press-release|press-releases|in-the-news|newsroom|"
    r"case-stud|customer-stor|testimonial)(/|$|\\?)",
    re.IGNORECASE,
)'''
            # Find and replace the pattern
            content = re.sub(
                r'_PRESS_URL_PATTERNS = re\.compile\([^)]+\)[^)]*\)',
                new_pattern.replace('\\', '\\\\'),
                content,
                flags=re.DOTALL
            )
            with open(filepath, 'w') as f:
                f.write(content)
            print("  ✓ Done")
        else:
            print("  ✓ Already narrowed")
    else:
        print("  ⚠ Pattern not found")
except Exception as e:
    print(f"  ⚠ Error: {e}")
PYFIX13

# Fix 1.4: Add safety fallback when AI returns empty
echo "[1.4] Adding safety fallback for AI empty results..."
if ! grep -q "ai_empty_with_quality_input" src/extract/ai_candidates_wrapper.py 2>/dev/null; then
    echo "  ℹ Skipping (complex patch - run fix_extraction_comprehensive.py manually if needed)"
else
    echo "  ✓ Already applied"
fi

# Fix 1.5: Fix SMTP timeouts
echo "[1.5] Fixing SMTP timeouts..."
sed -i 's/"SMTP_COMMAND_TIMEOUT_CLAMP", "10.0"/"SMTP_COMMAND_TIMEOUT_CLAMP", "25.0"/g' \
    src/queueing/tasks.py 2>/dev/null
sed -i 's/"SMTP_CONNECT_TIMEOUT_CLAMP", "6.0"/"SMTP_CONNECT_TIMEOUT_CLAMP", "12.0"/g' \
    src/queueing/tasks.py 2>/dev/null
sed -i 's/"TCP25_PROBE_TIMEOUT_SECONDS", "1.5"/"TCP25_PROBE_TIMEOUT_SECONDS", "3.0"/g' \
    src/queueing/tasks.py 2>/dev/null
echo "  ✓ Done"

# Fix 1.6: Increase job timeout for probes
echo "[1.6] Increasing probe job timeout (20 → 45s)..."
sed -i 's/job_timeout=20,/job_timeout=45,/g' src/queueing/tasks.py 2>/dev/null && \
    echo "  ✓ Done" || echo "  ⚠ Skipped"

echo ""

# ============================================================================
# STEP 2: Clear caches and failed jobs
# ============================================================================
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ STEP 2: Clearing Caches and Failed Jobs                          │"
echo "└──────────────────────────────────────────────────────────────────┘"

# Clear TCP25 cache
echo "[2.1] Clearing TCP25 preflight cache..."
redis-cli KEYS "tcp25_preflight:*" 2>/dev/null | xargs -r redis-cli DEL 2>/dev/null
echo "  ✓ Done"

# Clear failed jobs
echo "[2.2] Clearing failed job registries..."
python3 << 'PYCLEAR'
import os, sys
sys.path.insert(0, "/opt/email-scraper")
from redis import Redis
from rq import Queue
from rq.registry import FailedJobRegistry

redis = Redis.from_url(os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0")
total = 0
for qname in ["verify", "generate", "crawl", "orchestrator", "mx"]:
    try:
        q = Queue(name=qname, connection=redis)
        reg = FailedJobRegistry(queue=q)
        jobs = reg.get_job_ids()
        for jid in jobs:
            try:
                reg.remove(jid, delete_job=True)
            except:
                pass
        if jobs:
            total += len(jobs)
    except:
        pass
print(f"  ✓ Cleared {total} failed jobs")
PYCLEAR

echo ""

# ============================================================================
# STEP 3: Reset domain data
# ============================================================================
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ STEP 3: Resetting Domain Data                                    │"
echo "└──────────────────────────────────────────────────────────────────┘"

# Get company ID
COMPANY_ID=$(psql $DATABASE_URL -t -c "
SELECT id FROM companies 
WHERE domain = '${DOMAIN}' OR official_domain = '${DOMAIN}' 
LIMIT 1;
" 2>/dev/null | tr -d ' ')

if [ -z "$COMPANY_ID" ]; then
    echo "  Company not found for $DOMAIN"
    echo "  Will be created on first run"
else
    echo "  Company ID: $COMPANY_ID"
    
    echo "[3.1] Resetting AI extraction flag..."
    psql $DATABASE_URL -c "
    UPDATE companies 
    SET attrs = COALESCE(attrs::jsonb, '{}'::jsonb) - 'ai_people_extracted'
    WHERE id = ${COMPANY_ID};
    " 2>/dev/null >/dev/null && echo "  ✓ Done"
    
    echo "[3.2] Deleting verification results..."
    psql $DATABASE_URL -c "
    DELETE FROM verification_results 
    WHERE email_id IN (SELECT id FROM emails WHERE company_id = ${COMPANY_ID});
    " 2>/dev/null >/dev/null && echo "  ✓ Done"
    
    echo "[3.3] Deleting emails..."
    psql $DATABASE_URL -c "
    DELETE FROM emails WHERE company_id = ${COMPANY_ID};
    " 2>/dev/null >/dev/null && echo "  ✓ Done"
    
    echo "[3.4] Deleting people..."
    psql $DATABASE_URL -c "
    DELETE FROM people WHERE company_id = ${COMPANY_ID};
    " 2>/dev/null >/dev/null && echo "  ✓ Done"
    
    echo "[3.5] Ensuring official_domain is set..."
    psql $DATABASE_URL -c "
    UPDATE companies SET official_domain = domain 
    WHERE id = ${COMPANY_ID} AND (official_domain IS NULL OR official_domain = '');
    " 2>/dev/null >/dev/null && echo "  ✓ Done"
fi

echo ""

# ============================================================================
# STEP 4: Restart workers
# ============================================================================
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ STEP 4: Restarting Workers                                       │"
echo "└──────────────────────────────────────────────────────────────────┘"

echo "[4.1] Stopping workers..."
pkill -f "rq worker" 2>/dev/null
sleep 2
echo "  ✓ Done"

echo "[4.2] Starting workers..."
nohup rq worker orchestrator crawl generate verify -v > /var/log/email-scraper/worker.log 2>&1 &
sleep 2
if pgrep -f "rq worker" > /dev/null; then
    echo "  ✓ Workers started"
else
    echo "  ⚠ Workers may not have started - check manually"
fi

echo ""

# ============================================================================
# STEP 5: Create new pipeline run
# ============================================================================
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ STEP 5: Creating Pipeline Run                                    │"
echo "└──────────────────────────────────────────────────────────────────┘"

RUN_RESPONSE=$(curl -s -X POST http://localhost:8000/api/browser/runs \
    -H "Content-Type: application/json" \
    -d "{\"domains\": [\"${DOMAIN}\"], \"ai_enabled\": true, \"force_discovery\": true}")

RUN_ID=$(echo "$RUN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null)

if [ -z "$RUN_ID" ]; then
    echo "  ⚠ Failed to create run"
    echo "  Response: $RUN_RESPONSE"
else
    echo "  ✓ Run created: $RUN_ID"
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                        ALL DONE!                                 ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Monitor progress:"
echo ""
echo "  # Watch worker logs"
echo "  tail -f /var/log/email-scraper/worker.log"
echo ""
echo "  # Check queue status"
echo "  rq info"
echo ""
echo "  # Check results after completion"
echo "  psql \$DATABASE_URL -c \"SELECT first_name, last_name, title FROM people WHERE company_id=${COMPANY_ID:-'?'};\""
echo ""
echo "  # Check emails generated"
echo "  psql \$DATABASE_URL -c \"SELECT email, verify_status FROM emails e LEFT JOIN verification_results v ON v.email_id = e.id WHERE e.company_id=${COMPANY_ID:-'?'};\""
echo ""
