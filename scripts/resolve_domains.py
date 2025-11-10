#!/usr/bin/env python3
# scripts/resolve_domains.py
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from contextlib import closing
from datetime import UTC, datetime

# Best-effort import of resolver version for audit metadata
try:
    from src.resolve.domain import RESOLVER_VERSION as _RESOLVER_VERSION
except Exception:  # pragma: no cover
    _RESOLVER_VERSION = "r08.cli"


def _official_col(con: sqlite3.Connection) -> str:
    """Return the canonical companies column used to store the official domain."""
    cols = {row[1] for row in con.execute("PRAGMA table_info(companies)")}
    for col in ("domain_official", "official_domain"):
        if col in cols:
            return col
    raise SystemExit(
        "companies table missing both 'domain_official' and 'official_domain'. "
        "Add one of those columns or run your migrations."
    )


def _find_audit_table(con: sqlite3.Connection) -> str | None:
    """Find an existing audit table name, if any."""
    for cand in ("domain_resolutions", "domain_resolution_audit", "domain_resolution_log"):
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
            (cand,),
        ).fetchone()
        if row:
            return cand
    return None


def _audit_resolution(
    con: sqlite3.Connection,
    company_id: int,
    company_name: str,
    domain: str | None,
    confidence: int | None,
    method: str | None,
) -> None:
    """
    Idempotently insert an audit row for a resolved domain.

    Silently no-ops if:
      * audit table is missing,
      * inputs are incomplete (no domain/method/confidence),
      * or an equivalent audit row already exists.
    """
    if not domain or confidence is None or not method:
        return

    audit_tbl = _find_audit_table(con)
    if not audit_tbl:
        return  # no audit table present; nothing to do

    # Discover audit columns
    info = [dict(r) for r in con.execute(f"PRAGMA table_info({audit_tbl})")]
    names = {r["name"] for r in info}

    # Resolve optional columns
    domain_col = next(
        (
            c
            for c in (
                "domain",
                "resolved_domain",
                "official_domain",
                "selected_domain",
                "chosen_domain",
                "result_domain",
                "value",
                "candidate_domain",
            )
            if c in names
        ),
        None,
    )
    created_at_col = "created_at" if "created_at" in names else None
    resolver_version_col = (
        "resolver_version"
        if "resolver_version" in names
        else ("version" if "version" in names else None)
    )
    source_col = "source" if "source" in names else ("origin" if "origin" in names else None)
    company_name_col = "company_name" if "company_name" in names else None

    # Idempotency check
    if domain_col:
        exists = con.execute(
            f"SELECT 1 FROM {audit_tbl} WHERE company_id=? AND {domain_col}=? AND method=? AND confidence=? LIMIT 1",
            (company_id, domain, method, int(confidence)),
        ).fetchone()
    else:
        exists = con.execute(
            f"SELECT 1 FROM {audit_tbl} WHERE company_id=? AND method=? AND confidence=? LIMIT 1",
            (company_id, method, int(confidence)),
        ).fetchone()
    if exists:
        return

    # Build insert
    cols = ["company_id", "method", "confidence"]
    vals: list[object] = [company_id, method, int(confidence)]
    if domain_col:
        cols.append(domain_col)
        vals.append(domain)
    if resolver_version_col:
        cols.append(resolver_version_col)
        vals.append(_RESOLVER_VERSION)
    if source_col:
        cols.append(source_col)
        vals.append("resolver")
    if company_name_col:
        cols.append(company_name_col)
        vals.append(company_name)
    if created_at_col:
        cols.append(created_at_col)
        vals.append(datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"))

    q = f"INSERT INTO {audit_tbl} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(vals))})"
    with closing(con.cursor()) as cur:
        cur.execute(q, vals)
    con.commit()


def _iter_targets(con: sqlite3.Connection, limit: int, only_missing: bool = True):
    """Return candidate companies to resolve, optionally filtering to only unresolved."""
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

            # Emit a compact JSON line for every processed company
            print(json.dumps(res, separators=(",", ":")), flush=True)

            # Best-effort audit write (idempotent) for real runs
            if not args.dry_run and isinstance(res, dict):
                _audit_resolution(
                    con=con,
                    company_id=cid,
                    company_name=name,
                    domain=res.get("chosen"),
                    confidence=res.get("confidence"),
                    method=res.get("method"),
                )
        except Exception as e:  # pylint: disable=broad-except
            err = {"company_id": cid, "error": type(e).__name__, "message": str(e)}
            print(json.dumps(err, separators=(",", ":")), file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
