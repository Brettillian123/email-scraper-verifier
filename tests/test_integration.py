#!/usr/bin/env python3
"""
test_integration.py - Integration test for Email Scraper web-app wiring

Run: python test_integration.py

Prerequisites:
  - API server running: uvicorn src.api.app:app --port 8000
  - Redis running
  - Database migrated
"""

import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_HOST = os.getenv("API_HOST", "http://localhost:8000")
TENANT_ID = os.getenv("TENANT_ID", "dev")
USER_ID = os.getenv("USER_ID", "test_user")

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

passed = 0
failed = 0


def log_pass(msg: str):
    global passed
    print(f"{GREEN}✓ PASS{RESET}: {msg}")
    passed += 1


def log_fail(msg: str):
    global failed
    print(f"{RED}✗ FAIL{RESET}: {msg}")
    failed += 1


def log_warn(msg: str):
    print(f"{YELLOW}⚠ WARN{RESET}: {msg}")


def api_request(method: str, path: str, data: dict | None = None) -> dict | None:
    """Make API request and return JSON response."""
    url = f"{API_HOST}{path}"
    headers = {
        "Content-Type": "application/json",
        "X-Tenant-Id": TENANT_ID,
        "X-User-Id": USER_ID,
    }

    body = json.dumps(data).encode() if data else None
    req = Request(url, data=body, headers=headers, method=method)

    try:
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        return {"error": e.code, "detail": e.read().decode()}
    except URLError as e:
        return {"error": "connection_failed", "detail": str(e)}


def test_module_imports():
    """Test that all modules can be imported."""
    print("\n=== Module Import Tests ===\n")

    modules = [
        ("src.api.runs_v2", "router"),
        ("src.admin.run_metrics", "RunMetricsSummary"),
        ("src.admin.user_activity", "log_user_activity"),
        ("src.queueing.pipeline_v2", "pipeline_start_v2"),
    ]

    for module, attr in modules:
        try:
            mod = __import__(module, fromlist=[attr])
            if hasattr(mod, attr):
                log_pass(f"Import {module}.{attr}")
            else:
                log_warn(f"{module} loaded but missing {attr}")
        except ImportError as e:
            log_warn(f"Cannot import {module}: {e}")


def test_database_tables():
    """Test that required database tables exist."""
    print("\n=== Database Table Tests ===\n")

    try:
        from src.db import get_conn

        conn = get_conn()

        tables = ["runs", "companies", "people", "emails", "run_metrics", "user_activity"]
        for table in tables:
            try:
                conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
                log_pass(f"Table '{table}' exists")
            except Exception:
                if table in ("run_metrics", "user_activity"):
                    log_warn(f"Table '{table}' not found (optional)")
                else:
                    log_fail(f"Table '{table}' not found")

        conn.close()
    except Exception as e:
        log_fail(f"Database connection failed: {e}")


def test_api_endpoints():
    """Test API endpoints."""
    print("\n=== API Endpoint Tests ===\n")

    # Health check
    resp = api_request("GET", "/api/v2/health")
    if resp and resp.get("status") == "ok":
        log_pass("GET /api/v2/health")
    else:
        log_fail(f"GET /api/v2/health - {resp}")
        return  # Skip remaining tests if API not available

    # Create run
    resp = api_request(
        "POST",
        "/api/v2/runs",
        {
            "domains": ["example.com", "test.com"],
            "options": {"modes": ["autodiscovery"], "company_limit": 10},
            "label": "Python integration test",
        },
    )

    if resp and "run_id" in resp:
        log_pass(f"POST /api/v2/runs - created {resp['run_id']}")
        run_id = resp["run_id"]
    else:
        log_fail(f"POST /api/v2/runs - {resp}")
        return

    # Get run
    resp = api_request("GET", f"/api/v2/runs/{run_id}")
    if resp and "status" in resp:
        log_pass(f"GET /api/v2/runs/{run_id} - status: {resp['status']}")
    else:
        log_fail(f"GET /api/v2/runs/{run_id} - {resp}")

    # Get metrics
    resp = api_request("GET", f"/api/v2/runs/{run_id}/metrics")
    if resp and "run_id" in resp:
        log_pass(f"GET /api/v2/runs/{run_id}/metrics")
    else:
        log_fail(f"GET /api/v2/runs/{run_id}/metrics - {resp}")

    # User activity
    resp = api_request("GET", "/api/v2/users/me/activity")
    if resp and "user_id" in resp:
        log_pass("GET /api/v2/users/me/activity")
    else:
        log_fail(f"GET /api/v2/users/me/activity - {resp}")

    # User usage
    resp = api_request("GET", "/api/v2/users/me/usage")
    if resp and "user_id" in resp:
        log_pass("GET /api/v2/users/me/usage")
    else:
        log_fail(f"GET /api/v2/users/me/usage - {resp}")

    # Test company limit enforcement
    resp = api_request(
        "POST",
        "/api/v2/runs",
        {"domains": ["a.com", "b.com", "c.com", "d.com", "e.com"], "options": {"company_limit": 3}},
    )

    if resp and resp.get("domains_count") == 3:
        log_pass("Company limit enforced (5 -> 3)")
    else:
        log_fail(f"Company limit not enforced - {resp}")


def test_redis_connection():
    """Test Redis connection."""
    print("\n=== Redis Connection Test ===\n")

    try:
        from redis import Redis

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = Redis.from_url(redis_url)
        r.ping()
        log_pass("Redis connection OK")

        # Check for workers
        workers = r.smembers("rq:workers")
        if workers:
            log_pass(f"Found {len(workers)} RQ worker(s)")
        else:
            log_warn("No RQ workers running")
    except Exception as e:
        log_fail(f"Redis connection failed: {e}")


def main():
    print("=" * 50)
    print("Email Scraper Integration Tests")
    print("=" * 50)
    print(f"\nAPI: {API_HOST}")
    print(f"Tenant: {TENANT_ID}")
    print(f"User: {USER_ID}")

    test_module_imports()
    test_database_tables()
    test_redis_connection()
    test_api_endpoints()

    print("\n" + "=" * 50)
    print(f"Results: {GREEN}{passed} passed{RESET}, {RED}{failed} failed{RESET}")
    print("=" * 50)

    if failed > 0:
        print(f"\n{RED}Some tests failed. Check the output above.{RESET}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}All tests passed!{RESET}")
        sys.exit(0)


if __name__ == "__main__":
    main()
