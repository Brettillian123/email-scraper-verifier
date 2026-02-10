#!/bin/bash
# clear_tcp25_cache.sh - Clear TCP25 preflight cache from Redis
#
# This clears the cached TCP25 probe results that might be blocking
# email verification. Use this if you're seeing many "tcp25_blocked" errors.
#
# Usage: bash clear_tcp25_cache.sh

echo "=============================================="
echo "Clearing TCP25 Preflight Cache"
echo "=============================================="
echo ""

# Find all tcp25_preflight keys and delete them
echo "[1/2] Finding tcp25_preflight keys..."
KEYS=$(redis-cli KEYS "tcp25_preflight:*" 2>/dev/null)

if [ -z "$KEYS" ]; then
    echo "  No tcp25_preflight keys found"
else
    echo "  Found keys:"
    echo "$KEYS" | head -20

    echo ""
    echo "[2/2] Deleting keys..."
    echo "$KEYS" | xargs redis-cli DEL 2>/dev/null
    echo "  ✓ Deleted"
fi

echo ""
echo "=============================================="
echo "Cache Cleared!"
echo "=============================================="
echo ""
echo "The next verification probes will re-check TCP/25 connectivity."
echo ""
echo "If you're still seeing tcp25_blocked errors, check:"
echo "  1. Your VPS can reach port 25 on target mail servers"
echo "  2. SMTP_PROBES_ENABLED=1 is set"
echo "  3. Outbound port 25 is not blocked by your hosting provider"
echo ""
echo "Test connectivity manually:"
echo "  nc -zv mail.example.com 25"
echo "  telnet mail.example.com 25"
echo ""
