#!/bin/bash
# diagnose_issues.sh - Diagnose the three specific pipeline issues
#
# Issue 1: Autodiscovery not finding all people
# Issue 2: Unknown timeouts on SMTP probes
# Issue 3: Stops probing on hard invalid
#
# Usage: bash diagnose_issues.sh [domain]

DOMAIN=${1:-"brandtcpa.com"}

echo "=============================================="
echo "Pipeline Issue Diagnostics for: $DOMAIN"
echo "=============================================="
echo ""

cd /opt/email-scraper 2>/dev/null || cd /home/email-scraper 2>/dev/null || true

# Always quote DATABASE_URL for safety (spaces/escaping) and avoid reading .psqlrc.
PSQL_BASE=(psql -X "$DATABASE_URL")

run_psql() {
  # Run a SQL statement; never fail the whole script on SQL errors.
  "${PSQL_BASE[@]}" -c "$1" 2>/dev/null || "${PSQL_BASE[@]}" -c "$1" || true
}

# Resolve company_id once to avoid domain/official_domain drift.
COMPANY_ROW=$("${PSQL_BASE[@]}" -At -c "
SELECT id, domain, official_domain
FROM companies
WHERE domain = '${DOMAIN}' OR official_domain = '${DOMAIN}'
ORDER BY (official_domain = '${DOMAIN}') DESC, id DESC
LIMIT 1;
" 2>/dev/null || true)

COMPANY_ID=""
COMPANY_DOMAIN=""
OFFICIAL_DOMAIN=""

if [[ -n "$COMPANY_ROW" ]]; then
  IFS='|' read -r COMPANY_ID COMPANY_DOMAIN OFFICIAL_DOMAIN <<<"$COMPANY_ROW"
  echo "Resolved company:"
  echo "  company_id: $COMPANY_ID"
  echo "  domain: $COMPANY_DOMAIN"
  echo "  official_domain: $OFFICIAL_DOMAIN"
  echo ""
else
  echo "WARNING: No matching row in companies for domain='$DOMAIN'."
  echo "Diagnostics will fall back to URL matching where possible."
  echo ""
fi

# =============================================================================
# ISSUE 1: Autodiscovery not finding all people
# =============================================================================

echo "═══════════════════════════════════════════════"
echo "ISSUE 1: Autodiscovery / People Extraction"
echo "═══════════════════════════════════════════════"
echo ""

echo "[1.1] Pages crawled for domain:"
# Prefer matching the effective domain (official_domain if present), anchored to URL host.
EFFECTIVE_DOMAIN="${OFFICIAL_DOMAIN:-$DOMAIN}"
run_psql "
SELECT id, source_url, LENGTH(html) as html_bytes, fetched_at
FROM sources
WHERE source_url ILIKE '%://%${EFFECTIVE_DOMAIN}%'
   OR source_url ILIKE '%://%${DOMAIN}%'
ORDER BY fetched_at DESC;
"

echo ""
echo "[1.2] People found for domain:"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT p.id, p.first_name, p.last_name, p.title, p.source_url
  FROM people p
  WHERE p.company_id = ${COMPANY_ID}
  ORDER BY p.id;
  "
else
  run_psql "
  SELECT p.id, p.first_name, p.last_name, p.title, p.source_url
  FROM people p
  JOIN companies c ON c.id = p.company_id
  WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}'
  ORDER BY p.id;
  "
fi

echo ""
echo "[1.3] Company AI extraction status:"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT id, domain, official_domain,
         attrs::json->>'ai_people_extracted' as ai_extracted,
         attrs::json->>'ai_extraction_timestamp' as ai_timestamp
  FROM companies
  WHERE id = ${COMPANY_ID};
  "
else
  run_psql "
  SELECT id, domain, official_domain,
         attrs::json->>'ai_people_extracted' as ai_extracted,
         attrs::json->>'ai_extraction_timestamp' as ai_timestamp
  FROM companies
  WHERE domain = '${DOMAIN}' OR official_domain = '${DOMAIN}';
  "
fi

echo ""
echo "[1.4] Check page classifier config (if too aggressive):"
grep -n "job_board\|press_release\|testimonial\|news\|careers" src/queueing/tasks.py 2>/dev/null | head -5 || echo "  Could not check tasks.py"

# =============================================================================
# ISSUE 2: Unknown timeouts
# =============================================================================

echo ""
echo "═══════════════════════════════════════════════"
echo "ISSUE 2: SMTP Probe Timeouts"
echo "═══════════════════════════════════════════════"
echo ""

echo "[2.1] Current timeout configuration:"
echo "  SMTP_CONNECT_TIMEOUT: $(grep 'SMTP_CONNECT_TIMEOUT' src/config.py 2>/dev/null | head -1 || echo 'unknown')"
echo "  SMTP_COMMAND_TIMEOUT: $(grep 'SMTP_COMMAND_TIMEOUT' src/config.py 2>/dev/null | head -1 || echo 'unknown')"
echo "  SMTP_PREFLIGHT_TIMEOUT: $(grep 'SMTP_PREFLIGHT_TIMEOUT' src/config.py 2>/dev/null | head -1 || echo 'unknown')"

echo ""
echo "[2.2] Environment overrides (if any):"
echo "  SMTP_CONNECT_TIMEOUT=${SMTP_CONNECT_TIMEOUT:-not set}"
echo "  SMTP_COMMAND_TIMEOUT=${SMTP_COMMAND_TIMEOUT:-not set}"
echo "  SMTP_PROBES_ENABLED=${SMTP_PROBES_ENABLED:-not set}"

echo ""
echo "[2.3] Verification results for domain (showing status breakdown):"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT
      vr.verify_status,
      vr.verify_reason,
      COUNT(*) as count
  FROM verification_results vr
  JOIN emails e ON e.id = vr.email_id
  WHERE e.company_id = ${COMPANY_ID}
  GROUP BY vr.verify_status, vr.verify_reason
  ORDER BY count DESC;
  "
else
  run_psql "
  SELECT
      vr.verify_status,
      vr.verify_reason,
      COUNT(*) as count
  FROM verification_results vr
  JOIN emails e ON e.id = vr.email_id
  JOIN companies c ON c.id = e.company_id
  WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}'
  GROUP BY vr.verify_status, vr.verify_reason
  ORDER BY count DESC;
  "
fi

echo ""
echo "[2.4] Emails with unknown_timeout status:"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT e.email, vr.verify_status, vr.verify_reason, vr.verified_at
  FROM verification_results vr
  JOIN emails e ON e.id = vr.email_id
  WHERE e.company_id = ${COMPANY_ID}
    AND vr.verify_status = 'unknown_timeout'
  ORDER BY vr.verified_at DESC
  LIMIT 10;
  "
else
  run_psql "
  SELECT e.email, vr.verify_status, vr.verify_reason, vr.verified_at
  FROM verification_results vr
  JOIN emails e ON e.id = vr.email_id
  JOIN companies c ON c.id = e.company_id
  WHERE (c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}')
    AND vr.verify_status = 'unknown_timeout'
  ORDER BY vr.verified_at DESC
  LIMIT 10;
  "
fi

echo ""
echo "[2.5] MX host for domain:"
# FIX: Postgres schema likely keys domain_resolutions by company_id, not by a 'domain' column.
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT
      ${COMPANY_ID} as company_id,
      dr.mx_host,
      dr.catch_all_status,
      dr.catch_all_checked_at,
      dr.resolved_at
  FROM domain_resolutions dr
  WHERE dr.company_id = ${COMPANY_ID}
  ORDER BY dr.resolved_at DESC
  LIMIT 1;
  "
else
  echo "  Skipped: company_id not resolved; cannot query domain_resolutions reliably."
fi

# =============================================================================
# ISSUE 3: Stops probing on hard invalid
# =============================================================================

echo ""
echo "═══════════════════════════════════════════════"
echo "ISSUE 3: Probe Continuation"
echo "═══════════════════════════════════════════════"
echo ""

echo "[3.1] Emails generated vs verified for domain:"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT
      'Emails generated' as metric,
      COUNT(DISTINCT e.id) as count
  FROM emails e
  WHERE e.company_id = ${COMPANY_ID}
  UNION ALL
  SELECT
      'Emails verified' as metric,
      COUNT(DISTINCT vr.email_id) as count
  FROM verification_results vr
  JOIN emails e ON e.id = vr.email_id
  WHERE e.company_id = ${COMPANY_ID};
  "
else
  run_psql "
  SELECT
      'Emails generated' as metric,
      COUNT(DISTINCT e.id) as count
  FROM emails e
  JOIN companies c ON c.id = e.company_id
  WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}'
  UNION ALL
  SELECT
      'Emails verified' as metric,
      COUNT(DISTINCT vr.email_id) as count
  FROM verification_results vr
  JOIN emails e ON e.id = vr.email_id
  JOIN companies c ON c.id = e.company_id
  WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}';
  "
fi

echo ""
echo "[3.2] Per-person email generation count:"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT
      p.id as person_id,
      p.first_name,
      p.last_name,
      COUNT(e.id) as emails_generated,
      COUNT(vr.id) as emails_verified
  FROM people p
  LEFT JOIN emails e ON e.person_id = p.id
  LEFT JOIN verification_results vr ON vr.email_id = e.id
  WHERE p.company_id = ${COMPANY_ID}
  GROUP BY p.id, p.first_name, p.last_name
  ORDER BY p.id;
  "
else
  run_psql "
  SELECT
      p.id as person_id,
      p.first_name,
      p.last_name,
      COUNT(e.id) as emails_generated,
      COUNT(vr.id) as emails_verified
  FROM people p
  JOIN companies c ON c.id = p.company_id
  LEFT JOIN emails e ON e.person_id = p.id
  LEFT JOIN verification_results vr ON vr.email_id = e.id
  WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}'
  GROUP BY p.id, p.first_name, p.last_name
  ORDER BY p.id;
  "
fi

echo ""
echo "[3.3] Verification status per person:"
if [[ -n "$COMPANY_ID" ]]; then
  run_psql "
  SELECT
      p.first_name || ' ' || p.last_name as person,
      vr.verify_status,
      COUNT(*) as count
  FROM people p
  LEFT JOIN emails e ON e.person_id = p.id
  LEFT JOIN verification_results vr ON vr.email_id = e.id
  WHERE p.company_id = ${COMPANY_ID}
  GROUP BY p.first_name, p.last_name, vr.verify_status
  ORDER BY p.first_name, p.last_name, vr.verify_status;
  "
else
  run_psql "
  SELECT
      p.first_name || ' ' || p.last_name as person,
      vr.verify_status,
      COUNT(*) as count
  FROM people p
  JOIN companies c ON c.id = p.company_id
  LEFT JOIN emails e ON e.person_id = p.id
  LEFT JOIN verification_results vr ON vr.email_id = e.id
  WHERE c.domain = '${DOMAIN}' OR c.official_domain = '${DOMAIN}'
  GROUP BY p.first_name, p.last_name, vr.verify_status
  ORDER BY p.first_name, p.last_name, vr.verify_status;
  "
fi

echo ""
echo "[3.4] Queue status (check if verify queue has pending jobs):"
rq info 2>/dev/null || echo "  Could not get RQ info"

echo ""
echo "[3.5] Check for failed jobs in verify queue:"
rq info --raw 2>/dev/null | grep -A5 "verify" || echo "  Could not check failed jobs"

# =============================================================================
# Summary
# =============================================================================

echo ""
echo "═══════════════════════════════════════════════"
echo "SUMMARY & RECOMMENDATIONS"
echo "═══════════════════════════════════════════════"
echo ""

echo "Based on the diagnostics above:"
echo ""
echo "Issue 1 (Autodiscovery):"
echo "  - Check if pages were crawled (1.1)"
echo "  - Check if AI extraction ran (1.3)"
echo "  - If ai_extracted=true but few people, the classifier may be too strict"
echo "  - Run: python3 fix_pipeline_issues.py"
echo ""
echo "Issue 2 (Timeouts):"
echo "  - Check verification results (2.3)"
echo "  - If many unknown_timeout, increase timeouts in config.py"
echo "  - Check MX resolution (2.5) - if no MX, probes will fail"
echo "  - Run: python3 fix_pipeline_issues.py"
echo ""
echo "Issue 3 (Stops on invalid):"
echo "  - Compare emails_generated vs emails_verified (3.1)"
echo "  - If generated > verified, check queue status (3.4)"
echo "  - Make sure workers run: rq worker orchestrator crawl generate verify"
echo "  - Check for failed/stuck jobs (3.5)"
echo ""
