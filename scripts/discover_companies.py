"""Run one company-discovery pass. Cron entry point.

    python scripts/discover_companies.py [--dry-run]

Sources live in the `discovery_source` table, so adding a fund or a search
phrase is a row, not a deploy. Seed some with --add:

    python scripts/discover_companies.py --add vc_portfolio https://fund.com/portfolio
    python scripts/discover_companies.py --add serper "seed round announced fintech"
"""

from __future__ import annotations

import argparse
import logging
import sys

from src.db import get_conn
from src.discover.run import (
    AUTO_QUEUE_THRESHOLD,
    MAX_CANDIDATES_PER_RUN,
    MAX_PROMOTE_PER_DAY,
    discover,
)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="score but write nothing")
    ap.add_argument("--add", nargs=2, metavar=("KIND", "TARGET"), help="add a source and exit")
    ap.add_argument("--list", action="store_true", help="list configured sources and exit")
    args = ap.parse_args()

    if args.add:
        kind, target = args.add
        con = get_conn()
        try:
            con.execute(
                "INSERT INTO discovery_source (kind, target) VALUES (?, ?)"
                " ON CONFLICT (kind, target) DO NOTHING",
                (kind, target),
            )
            con.commit()
            print(f"added source: {kind} -> {target}")
        finally:
            con.close()
        return 0

    if args.list:
        con = get_conn()
        try:
            rows = con.execute(
                "SELECT kind, target, enabled, last_run_at, last_found"
                " FROM discovery_source ORDER BY kind, id"
            ).fetchall()
            if not rows:
                print("no sources configured -- add one with --add")
            for k, t, en, ts, n in rows:
                print(f"  [{'on ' if en else 'off'}] {k:14s} {str(t)[:60]:60s} last={ts} found={n}")
        finally:
            con.close()
        return 0

    print(
        f"caps: threshold>={AUTO_QUEUE_THRESHOLD}, "
        f"max {MAX_CANDIDATES_PER_RUN} candidates/run, {MAX_PROMOTE_PER_DAY} promoted/day"
    )
    r = discover(dry_run=args.dry_run)
    print(
        f"\nseen={r.seen} already_known={r.already_known} profiled={r.profiled}\n"
        f"promoted={r.promoted} parked={r.parked} rejected={r.rejected} truncated={r.truncated}"
    )
    if r.promoted_domains:
        print("promoted: " + ", ".join(r.promoted_domains))
    if r.truncated:
        print(f"NOTE: {r.truncated} candidates dropped at the per-run cap.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
