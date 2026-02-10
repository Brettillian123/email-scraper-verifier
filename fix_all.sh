#!/bin/bash
# fix_all.sh - Complete fix for Email Scraper pipeline
#
# This script fixes ALL known issues:
# 1. The critical bug in task_generate_emails (wrong upsert_generated_email call)
# 2. Sets official_domain for all companies
# 3. Restarts services
#
# Usage: bash fix_all.sh

set -e

echo "=============================================="
echo "Email Scraper - Complete Pipeline Fix"
echo "=============================================="
echo ""

cd /opt/email-scraper

# Backup
echo "[1/5] Creating backups..."
cp src/queueing/tasks.py src/queueing/tasks.py.bak.$(date +%s) 2>/dev/null || true
echo "  Done"

# Fix the critical bug in tasks.py
echo ""
echo "[2/5] Fixing task_generate_emails (critical bug)..."

# The old buggy code:
# upsert_generated_email(
#     conn=con,
#     person_id=person_id,
#     email=email_addr,
#     pattern_used=pattern_key,
#     pattern_rank=rank,
# )

# Use Python for the fix since sed can't handle multiline easily
python3 << 'PYFIX'
import re

filepath = "/opt/email-scraper/src/queueing/tasks.py"

with open(filepath, 'r') as f:
    content = f.read()

# Pattern to match the buggy call
old_pattern = r'''upsert_generated_email\(
                conn=con,
                person_id=person_id,
                email=email_addr,
                pattern_used=pattern_key,
                pattern_rank=rank,
            \)'''

new_code = '''upsert_generated_email(
                conn=con,
                person_id=person_id,
                email=email_addr,
                domain=dom,
                source_note=f"generated:pattern={pattern_key}:rank={rank}",
            )'''

# Check if bug exists
if "pattern_used=pattern_key" in content and "pattern_rank=rank" in content:
    content = re.sub(old_pattern, new_code, content, flags=re.MULTILINE)
    with open(filepath, 'w') as f:
        f.write(content)
    print("  ✓ Fixed upsert_generated_email call")
elif "domain=dom," in content and 'source_note=f"generated:' in content:
    print("  ✓ Already fixed")
else:
    print("  ⚠ Pattern not found - trying alternative fix...")
    # Try line-by-line replacement
    lines = content.split('\n')
    new_lines = []
    i = 0
    fixed = False
    while i < len(lines):
        line = lines[i]
        if 'pattern_used=pattern_key,' in line:
            new_lines.append(line.replace('pattern_used=pattern_key,', 'domain=dom,'))
            fixed = True
        elif 'pattern_rank=rank,' in line:
            new_lines.append(line.replace('pattern_rank=rank,', 'source_note=f"generated:pattern={pattern_key}:rank={rank}",'))
        else:
            new_lines.append(line)
        i += 1

    if fixed:
        with open(filepath, 'w') as f:
            f.write('\n'.join(new_lines))
        print("  ✓ Fixed with alternative method")
    else:
        print("  ✗ Could not fix automatically")
PYFIX

# Verify the fix
echo ""
echo "[3/5] Verifying fix..."
if grep -q "domain=dom," src/queueing/tasks.py && ! grep -q "pattern_used=pattern_key," src/queueing/tasks.py; then
    echo "  ✓ Fix verified successfully"
else
    echo "  ⚠ Fix verification failed - check manually"
    echo "    Look for upsert_generated_email in src/queueing/tasks.py around line 2186"
fi

# Fix database
echo ""
echo "[4/5] Fixing database (setting official_domain)..."
psql $DATABASE_URL -c "UPDATE companies SET official_domain = domain WHERE (official_domain IS NULL OR official_domain = '') AND domain IS NOT NULL;" 2>/dev/null && echo "  ✓ Database updated" || echo "  ⚠ Database update skipped"

# Restart services
echo ""
echo "[5/5] Restarting services..."
echo "  Stopping workers..."
pkill -f "rq worker" 2>/dev/null || true
sleep 2

echo "  Stopping uvicorn..."
pkill -f "uvicorn" 2>/dev/null || true
sleep 2

echo ""
echo "=============================================="
echo "Fix Complete!"
echo "=============================================="
echo ""
echo "Now manually restart the services:"
echo ""
echo "  # Terminal 1: Start uvicorn"
echo "  cd /opt/email-scraper"
echo "  uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload"
echo ""
echo "  # Terminal 2: Start workers (with verbose output)"
echo "  cd /opt/email-scraper"
echo "  rq worker orchestrator crawl generate verify -v"
echo ""
echo "Then test from the dashboard:"
echo "  http://$(hostname -I | awk '{print $1}'):8000/admin/dashboard"
echo ""
echo "Watch the worker output for:"
echo "  - 'R12 generated emails' with count > 0"
echo "  - Verification jobs being processed"
echo ""
