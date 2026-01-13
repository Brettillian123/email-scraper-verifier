# scripts/migrate_r14_add_icp.py
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Any

from src.db import get_conn


def _apply_dsn_override(dsn: str | None) -> None:
    if not dsn:
        return
    os.environ["DATABASE_URL"] = dsn
    os.environ["PG_DSN"] = dsn


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def col_exists(cur: Any, *, schema: str, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
           AND column_name = %s
         LIMIT 1
        """,
        (schema, table, col),
    )
    return cur.fetchone() is not None


def ensure_columns(cur: Any, *, schema: str) -> None:
    people = f"{_qi(schema)}.{_qi('people')}"
    if not col_exists(cur, schema=schema, table="people", col="icp_score"):
        cur.execute(f"ALTER TABLE {people} ADD COLUMN icp_score INTEGER")
    if not col_exists(cur, schema=schema, table="people", col="icp_reasons"):
        cur.execute(f"ALTER TABLE {people} ADD COLUMN icp_reasons TEXT")  # JSON list
    if not col_exists(cur, schema=schema, table="people", col="last_scored_at"):
        cur.execute(f"ALTER TABLE {people} ADD COLUMN last_scored_at TEXT")  # ISO8601 UTC


def load_cfg() -> dict[str, Any]:
    try:
        from src.config import load_icp_config

        cfg = load_icp_config()
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def score_row(rf: str | None, sn: str | None, cfg: dict[str, Any]) -> tuple[int, list[str]]:
    cap = int(cfg.get("cap", 100))
    weights = cfg.get("weights") or cfg.get("signals") or {}
    labels = cfg.get("reason_labels") or {}
    w_rf = weights.get("role_family") or {}
    w_sn = weights.get("seniority") or {}

    reasons: list[str] = []
    matched: set[str] = set()
    score = 0

    if rf and rf in w_rf:
        pts = int(w_rf[rf])
        score += pts
        reasons.append(f"{labels.get('role_family', 'role_family')}:{rf}+{pts}")
        matched.add("role_family")

    if sn and sn in w_sn:
        pts = int(w_sn[sn])
        score += pts
        reasons.append(f"{labels.get('seniority', 'seniority')}:{sn}+{pts}")
        matched.add("seniority")

    required = cfg.get("min_required") or []
    if any(req not in matched for req in required):
        return 0, ["missing_min_required"]

    score = max(0, min(cap, score))
    if not reasons:
        return 0, ["missing_min_required"]
    return score, reasons


def backfill(cur: Any, *, schema: str, cfg: dict[str, Any], verbose: bool = False) -> int:
    people = f"{_qi(schema)}.{_qi('people')}"
    cur.execute(f"SELECT id, role_family, seniority FROM {people}")
    rows = cur.fetchall()
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated = 0
    for pid, rf, sn in rows:
        s, rs = score_row(rf, sn, cfg)
        cur.execute(
            f"""
            UPDATE {people}
               SET icp_score = %s,
                   icp_reasons = %s,
                   last_scored_at = %s
             WHERE id = %s
            """,
            (s, json.dumps(rs), now, pid),
        )
        updated += int(cur.rowcount or 0)
    if verbose:
        print(f"Backfilled ICP for {updated} people")
    return updated


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN/URL (optional; overrides DATABASE_URL for this run).",
    )
    ap.add_argument("--no-backfill", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument(
        "--schema",
        default=os.getenv("PGSCHEMA", "public"),
        help="Target Postgres schema (default: public, or PGSCHEMA env var).",
    )
    args = ap.parse_args()
    _apply_dsn_override(args.dsn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            ensure_columns(cur, schema=args.schema)
            if not args.no_backfill:
                cfg = load_cfg()
                backfill(cur, schema=args.schema, cfg=cfg, verbose=args.verbose)
        finally:
            try:
                cur.close()
            except Exception:
                pass
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
