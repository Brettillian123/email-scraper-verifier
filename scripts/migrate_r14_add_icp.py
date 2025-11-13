# scripts/migrate_r14_add_icp.py
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import pathlib
import sqlite3
from typing import Any


def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def ensure_columns(cur: sqlite3.Cursor) -> None:
    if not col_exists(cur, "people", "icp_score"):
        cur.execute("ALTER TABLE people ADD COLUMN icp_score INTEGER")
    if not col_exists(cur, "people", "icp_reasons"):
        cur.execute("ALTER TABLE people ADD COLUMN icp_reasons TEXT")  # JSON list
    if not col_exists(cur, "people", "last_scored_at"):
        cur.execute("ALTER TABLE people ADD COLUMN last_scored_at TEXT")  # ISO8601 UTC


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


def backfill(conn: sqlite3.Connection, cfg: dict[str, Any], verbose: bool = False) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id, role_family, seniority FROM people")
    rows = cur.fetchall()
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated = 0
    for pid, rf, sn in rows:
        s, rs = score_row(rf, sn, cfg)
        cur.execute(
            "UPDATE people SET icp_score=?, icp_reasons=?, last_scored_at=? WHERE id=?",
            (s, json.dumps(rs), now, pid),
        )
        updated += cur.rowcount
    conn.commit()
    if verbose:
        print(f"Backfilled ICP for {updated} people")
    return updated


def main(db: str, no_backfill: bool = False, verbose: bool = False) -> None:
    p = pathlib.Path(db)
    if not p.exists():
        raise SystemExit(f"Database not found: {db}")
    with contextlib.closing(sqlite3.connect(db)) as conn:
        cur = conn.cursor()
        ensure_columns(cur)
        conn.commit()
        if not no_backfill:
            cfg = load_cfg()
            backfill(conn, cfg, verbose=verbose)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/dev.db")
    ap.add_argument("--no-backfill", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    main(args.db, no_backfill=args.no_backfill, verbose=args.verbose)
