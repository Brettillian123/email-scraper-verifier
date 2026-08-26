#!/usr/bin/env bash
# Run the test suite against a disposable Postgres + Redis.
#
# WHY. Without a database the autouse fixture in tests/conftest.py skips nearly
# everything -- "10 passed, 772 skipped" -- so 98.7% of the suite never runs and
# "tests pass" means almost nothing. That same gap blocked the run-completion fix,
# which cannot be validated without real Redis and RQ.
#
# The test database is NOT production. It listens on 55432/56379, lives on tmpfs,
# dies with the container, and sits on its own network where cw-iq-db is
# unreachable.
#
# pytest runs INSIDE a container built from crestwell-iq:local, so this works on
# the VPS (no host Python) and on a workstation alike, and the runtime deps are
# already baked -- only the test-only packages are installed per run.
#
# Usage:
#   scripts/run_tests.sh                     # whole suite
#   scripts/run_tests.sh tests/test_x.py -v  # args pass straight to pytest
#   KEEP_UP=1 scripts/run_tests.sh           # leave containers up afterwards
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

COMPOSE="docker compose -f docker-compose.test.yml"
NET="cw-iq-test-net"
IMAGE="${TEST_IMAGE:-crestwell-iq:local}"
PGURL="postgresql://test:test@test-db:5432/iq_test"
REDISURL="redis://test-redis:6379/0"

cleanup() {
  if [ "${KEEP_UP:-0}" = "1" ]; then
    echo "KEEP_UP=1 -- test containers left running"
  else
    echo "--- tearing down test infrastructure ---"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "--- starting disposable postgres + redis ---"
$COMPOSE up -d --wait

echo "--- loading schema ---"
docker exec -i cw-iq-test-db psql -q -U test -d iq_test -v ON_ERROR_STOP=1 < db/schema.sql

echo "--- applying migrations ---"
for f in db/migrations/*.sql; do
  # Migrations are historical and some assume state a fresh database lacks.
  # A failure is reported, not fatal: the goal is a database the tests can run
  # against, not a byte-perfect replay of production history.
  if docker exec -i cw-iq-test-db psql -q -U test -d iq_test -v ON_ERROR_STOP=1 < "$f" >/dev/null 2>&1; then
    echo "  ok    $(basename "$f")"
  else
    echo "  SKIP  $(basename "$f")  (did not apply cleanly to a fresh database)"
  fi
done

# Probing stays ENABLED. The SMTP tests patch smtplib and assert on the
# classifier, so disabling it makes assert_smtp_probing_allowed() raise before
# the code under test is reached. No real connection is made -- every socket in
# the suite is a mock -- and ALLOWED_HOSTS is cleared so the hostname gate does
# not fire either.
echo "--- running pytest in $IMAGE ---"
set +e
docker run --rm \
  --network "$NET" \
  -v "$HERE":/app -w /app \
  -e DATABASE_URL="$PGURL" -e DB_URL="$PGURL" \
  -e REDIS_URL="$REDISURL" -e RQ_REDIS_URL="$REDISURL" \
  -e SMTP_PROBES_ENABLED=true -e SMTP_PROBES_ALLOWED_HOSTS= \
  -e PYTHONPATH=/app \
  "$IMAGE" \
  sh -c "pip install -q pytest ruff fakeredis respx pytest-asyncio 2>/dev/null; python -m pytest -p no:cacheprovider $*"
RC=$?
set -e

echo "--- pytest exit code: $RC ---"
exit $RC
