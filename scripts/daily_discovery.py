#!/usr/bin/env python
"""
scripts/daily_discovery.py

Cron-triggered Google Custom Search lead discovery.

Runs the discovery task synchronously (not via RQ queue) so cron
output and exit codes are straightforward to monitor.

Usage:
    python scripts/daily_discovery.py
    python scripts/daily_discovery.py --tenant dev

Crontab example (run at 6 AM UTC daily):
    0 6 * * * cd /opt/email-scraper && /opt/email-scraper/venv/bin/python scripts/daily_discovery.py >> logs/discovery.log 2>&1
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily Google CSE lead discovery")
    parser.add_argument("--tenant", default="dev", help="Tenant ID (default: dev)")
    args = parser.parse_args()

    # Ensure project root is on sys.path so 'src' is importable
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.queueing.google_discovery_task import task_google_discovery

    log.info("Starting daily Google discovery for tenant=%s", args.tenant)

    result = task_google_discovery(
        tenant_id=args.tenant,
        trigger_type="cron",
    )

    if result.get("skipped"):
        log.info("Discovery is disabled, skipping (reason: %s)", result.get("reason"))
        return

    if result.get("ok"):
        log.info(
            "Discovery complete: %d companies queried, %d people found, "
            "%d inserted, %d emails generated, %d queries used",
            result.get("companies_queried", 0),
            result.get("people_found", 0),
            result.get("people_inserted", 0),
            result.get("emails_generated", 0),
            result.get("queries_used", 0),
        )
        if result.get("errors"):
            log.warning("Errors during discovery: %s", result["errors"][:5])
    else:
        log.error("Discovery failed: %s", result.get("error", "unknown"))
        sys.exit(1)


if __name__ == "__main__":
    main()
