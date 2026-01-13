#!/usr/bin/env python3
# scripts/resolve_domains.py
"""
Resolve official domains for companies (manual/backlog runs).

This script is PostgreSQL-native and uses src.db.get_conn() for database access.
The CompatConnection layer handles SQL translation automatically.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

from src.db import get_conn

# Best-effort import of resolver version for audit metadata
try:
    from src.resolve.domain import RESOLVER_VERSION as _RESOLVER_VERSION
except Exception:  # pragma: no cover
    _RESOLVER_VERSION = "r08.cli"


def _table_columns(conn: Any, table: str) -> set[str]:
    """
    Return column names for a table using PRAGMA table_info(...).

    Works for both SQLite and Postgres via CompatCursor PRAGMA emulation.
    """
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall() or []
    except Exception:
        return set()

    cols: set[str] = set()
    for row in rows:
        try:
            # row[1] is the column name in PRAGMA table_info output
            name = row[1] if isinstance(row, tuple) else row.get("name", row[1])
        except Exception:
            continue
        if name:
            cols.add(str(name))
    return cols


def _official_col(conn: Any) -> str:
    """Return the canonical companies column used to store the official domain."""
    cols = _table_columns(conn, "companies")
    for col in ("domain_official", "official_domain"):
        if col in cols:
            return col
    raise SystemExit(
        "companies table missing both 'domain_official' and 'official_domain'. "
        "Add one of those columns or run your migrations."
    )


def _table_exists(conn: Any, table: str) -> bool:
    """
    Check if a table exists.

    Uses sqlite_master query which CompatCursor emulates for Postgres.
    """
    try:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _find_audit_table(conn: Any) -> str | None:
    """Find an existing audit table name, if any."""
    for cand in ("domain_resolutions", "domain_resolution_audit", "domain_resolution_log"):
        if _table_exists(conn, cand):
            return cand
    return None


def _audit_resolution(
    conn: Any,
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

    audit_tbl = _find_audit_table(conn)
    if not audit_tbl:
        return  # no audit table present; nothing to do

    # Discover audit columns
    names = _table_columns(conn, audit_tbl)

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
        exists = conn.execute(
            f"SELECT 1 FROM {audit_tbl} WHERE company_id=? AND {domain_col}=? AND method=? AND confidence=? LIMIT 1",
            (company_id, domain, method, int(confidence)),
        ).fetchone()
    else:
        exists = conn.execute(
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

    # Use ? placeholders - CompatCursor translates to %s for Postgres
    q = f"INSERT INTO {audit_tbl} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(vals))})"
    conn.execute(q, tuple(vals))
    conn.commit()


def _iter_targets(conn: Any, limit: int, only_missing: bool = True) -> list[Any]:
    """Return candidate companies to resolve, optionally filtering to only unresolved."""
    col = _official_col(conn)
    where = f"WHERE {col} IS NULL" if only_missing else ""
    sql = f"""
        SELECT id, name, user_supplied_domain
        FROM companies
        {where}
        ORDER BY id ASC
        LIMIT ?
    """
    cur = conn.execute(sql, (int(limit),))
    return cur.fetchall() or []


def _is_postgres_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Resolve official domains for companies (manual/backlog runs)."
    )
    ap.add_argument("--limit", type=int, default=100, help="Max companies to process.")
    ap.add_argument(
        "--db",
        dest="db_url",
        default=None,
        help=(
            "Database URL (postgresql://... or legacy sqlite:///path). "
            "Overrides DATABASE_URL/DB_URL for this run."
        ),
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Process all rows (ignore unresolved filter).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do everything except writing resolver output.",
    )
    args = ap.parse_args()

    # Override DATABASE_URL if --db provided
    if args.db_url:
        os.environ["DATABASE_URL"] = args.db_url

    # Import after env is set so the task's get_conn() picks the right DB.
    from src.queueing.tasks import resolve_company_domain

    with get_conn() as conn:
        rows = _iter_targets(conn, args.limit, only_missing=(not args.all))
        if not rows:
            print("[]")
            return 0

        for r in rows:
            # Handle both tuple and dict-like row access
            if isinstance(r, tuple):
                cid, name, hint = r[0], r[1], r[2]
            else:
                cid = r.get("id", r[0])
                name = r.get("name", r[1])
                hint = r.get("user_supplied_domain", r[2])

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
                        conn=conn,
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
