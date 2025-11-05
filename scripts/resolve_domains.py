#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from contextlib import closing


def _official_col(con: sqlite3.Connection) -> str:
    cols = {row[1] for row in con.execute("PRAGMA table_info(companies)")}
    for c in ("domain_official", "official_domain"):
        if c in cols:
            return c
    raise SystemExit(
        "companies table missing both 'domain_official' and 'official_domain'. "
        "Add one of those columns or run your migrations."
    )


def _iter_targets(con: sqlite3.Connection, limit: int, only_missing: bool = True):
    col = _official_col(con)
    where = f"WHERE {col} IS NULL" if only_missing else ""
    sql = f"""
        SELECT id, name, user_supplied_domain
        FROM companies
        {where}
        ORDER BY id ASC
        LIMIT :limit
    """
    with closing(con.cursor()) as cur:
        cur.execute(sql, {"limit": int(limit)})
        return cur.fetchall()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Resolve official domains for companies (manual/backlog runs)."
    )
    ap.add_argument("--limit", type=int, default=100, help="Max companies to process.")
    ap.add_argument(
        "--db",
        default=os.getenv("DATABASE_PATH", "dev.db"),
        help="SQLite DB file. Defaults to $DATABASE_PATH or dev.db",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Process all rows (ignore unresolved filter).",
    )
    ap.add_argument(
        "--busy-timeout-ms",
        type=int,
        default=5000,
        help="SQLite busy_timeout in milliseconds.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do everything except writing resolver output.",
    )
    args = ap.parse_args()

    # Ensure the resolver writes to the SAME DB we read from.
    os.environ["DATABASE_PATH"] = args.db

    # Import after env is set so the task's _conn() picks the right path.
    from src.queueing.tasks import resolve_company_domain

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute(f"PRAGMA busy_timeout={int(args.busy_timeout_ms)}")

    rows = _iter_targets(con, args.limit, only_missing=(not args.all))
    if not rows:
        print("[]")
        return 0

    for r in rows:
        cid, name, hint = r["id"], r["name"], r["user_supplied_domain"]
        try:
            if args.dry_run:
                res = {
                    "company_id": cid,
                    "chosen": None,
                    "method": "dry_run",
                    "confidence": 0,
                }
            else:
                res = resolve_company_domain(cid, name, hint)
            print(json.dumps(res, separators=(",", ":")), flush=True)
        except Exception as e:
            err = {"company_id": cid, "error": type(e).__name__, "message": str(e)}
            print(json.dumps(err, separators=(",", ":")), file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
