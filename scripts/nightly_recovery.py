"""Nightly recovery pass: re-confirm addresses lost to the 2026-08-25 outage.

Runs in the 01:00-06:00 window. Enqueues per-PERSON generation jobs up to the
daily probe budget, then stops.

THREE THINGS THIS DOES DIFFERENTLY FROM THE BACKFILL THAT CAUSED THE OUTAGE
--------------------------------------------------------------------------
1. **Port 25 preflight.** If outbound SMTP is blocked the job logs and exits
   without enqueuing anything. Hammering a blocked port nightly while the host
   decides whether to lift a block is the one thing guaranteed to keep it in
   place.

2. **Person-level targeting.** The backfill selected COMPANIES containing
   unprobed people, and the pipeline then re-probed EVERY person at those
   companies -- 3,473 against the 1,937 intended, which is what put already
   confirmed addresses back in front of a throttling provider. This enqueues
   `task_generate_emails` per person, so the scope is exactly what was chosen.

3. **Sized to the budget, not to the work.** The batch is capped by whatever
   remains of `SMTP_DAILY_PROBE_CAP` for the day. Recovery takes as many nights
   as it takes.

Anything the per-MX ceiling refuses mid-run comes back `unknown`, never
`invalid`, so an over-large batch degrades into no-ops rather than data loss.

Usage:
    python scripts/nightly_recovery.py [--dry-run] [--max-people N]
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys

from rq import Queue

from src.db import get_conn
from src.queueing.rate_limit import (
    SMTP_DAILY_PROBE_CAP,
    daily_probe_usage,
)
from src.queueing.redis_conn import get_redis

log = logging.getLogger("nightly_recovery")

PROBES_PER_PERSON = int(os.getenv("MAX_PROBES_PER_PERSON", "3"))
PREFLIGHT_MX = os.getenv("RECOVERY_PREFLIGHT_MX", "aspmx.l.google.com")
GENERATE_QUEUE = os.getenv("RECOVERY_QUEUE", "generate")

#: Priority order.
#:
#: NOTE ON WHAT CANNOT BE DONE HERE. The obvious ordering would be "companies
#: that lost an address in the outage, first". That is not expressible. When an
#: address is deleted its `emails` row, its `verification_results` rows and any
#: pattern evidence go with it, so a company that lost everything leaves no trace
#: distinguishing it from one that never had anything. `first_valid_at` only
#: exists on surviving rows, and `company_email_pattern` was created during this
#: same session, so neither can see backwards. An earlier version of this query
#: tried it anyway with a clause that could never be true -- `had_before` is a
#: strict subset of `confirmed_now` -- and silently ordered by nothing.
#:
#: What IS expressible: a company whose pattern we have CONFIRMED but which holds
#: no address today must have lost one, because the confirmation required an
#: accept. That is a small set now (the pattern table is young) and will be the
#: precise signal if this ever happens again.
SQL = """
WITH confirmed_pattern AS (
  SELECT DISTINCT company_id FROM company_email_pattern WHERE confirmed_count > 0
),
has_address_now AS (
  SELECT DISTINCT p.company_id
    FROM emails e JOIN people p ON p.id = e.person_id
   WHERE e.first_valid_at IS NOT NULL
)
SELECT p.id, p.first_name, p.last_name, lower(c.domain)
  FROM people p
  JOIN companies c ON c.id = p.company_id
 WHERE c.domain IS NOT NULL AND c.domain <> ''
   AND p.first_name <> '' AND p.last_name <> ''
   -- nothing currently on file for this person
   AND NOT EXISTS (SELECT 1 FROM emails e WHERE e.person_id = p.id)
   -- and not already answered by a previous night
   AND (p.generation_status IS NULL OR p.generation_status IN ('deferred', 'exhausted'))
 ORDER BY
   -- 1. proven pattern, nothing on file: it worked once and is gone now
   (p.company_id IN (SELECT company_id FROM confirmed_pattern)
    AND p.company_id NOT IN (SELECT company_id FROM has_address_now)) DESC,
   -- 2. companies with a confirmed pattern: these land on probe #1, so they are
   --    the cheapest results available per unit of provider patience
   (p.company_id IN (SELECT company_id FROM confirmed_pattern)) DESC,
   -- 3. decision-makers
   (p.title ~* '(ceo|chief executive|founder|co-?founder|owner|president|managing)') DESC,
   p.company_id, p.id
 LIMIT %s
"""


def port25_open(host: str, timeout: float = 8.0) -> bool:
    """Is outbound SMTP actually usable from this box?

    The 2026-08-25 block surfaced as connection failures on port 25, recorded as
    `port25_unreachable`. Until that clears there is nothing useful for this job
    to do, and plenty of harm it could do by trying.
    """
    try:
        with socket.create_connection((host, 25), timeout=timeout) as s:
            banner = s.recv(256)
        return banner.startswith(b"2")
    except Exception as exc:
        log.warning("port 25 preflight failed against %s: %s", host, exc)
        return False


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="select and report, enqueue nothing")
    ap.add_argument("--max-people", type=int, default=0, help="override the budget-derived cap")
    args = ap.parse_args()

    redis = get_redis()

    usage = daily_probe_usage(redis)
    used = int(usage.get("global_used") or 0)
    remaining_probes = max(0, SMTP_DAILY_PROBE_CAP - used)
    budget_people = remaining_probes // max(1, PROBES_PER_PERSON)
    cap = args.max_people if args.max_people > 0 else budget_people

    log.info(
        "budget: %s/%s probes used today, %s remaining -> %s people",
        used,
        SMTP_DAILY_PROBE_CAP,
        remaining_probes,
        budget_people,
    )
    if cap <= 0:
        log.info("daily probe budget already spent; nothing to do")
        return 0

    if not port25_open(PREFLIGHT_MX):
        log.error(
            "outbound SMTP is blocked; enqueuing nothing. "
            "This job stays a no-op until port 25 works again."
        )
        return 0

    con = get_conn()
    try:
        rows = con.execute(SQL, (cap,)).fetchall()
    finally:
        try:
            con.close()
        except Exception:
            pass

    log.info("selected %s people (cap %s)", len(rows), cap)
    if not rows:
        log.info("no unprobed people remain")
        return 0

    if args.dry_run:
        for pid, fn, ln, dom in rows[:15]:
            log.info("  would probe person=%s %s %s @ %s", pid, fn, ln, dom)
        log.info("DRY RUN -- nothing enqueued")
        return 0

    from src.queueing.tasks import task_generate_emails

    q = Queue(name=GENERATE_QUEUE, connection=redis)
    enqueued = 0
    for person_id, first, last, domain in rows:
        try:
            q.enqueue(
                task_generate_emails,
                person_id=person_id,
                first=first,
                last=last,
                domain=domain,
                job_timeout=600,
                meta={"stage": "nightly_recovery", "person_id": person_id, "domain": domain},
            )
            enqueued += 1
        except Exception:
            log.warning("failed to enqueue person %s", person_id, exc_info=True)

    log.info("enqueued %s person-generation jobs on queue '%s'", enqueued, GENERATE_QUEUE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
