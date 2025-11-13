# scripts/backfill_r14_icp.py
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from src.config import load_icp_config
from src.scoring.icp import compute_icp


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {db_path}")


# Precompiled patterns/keyword maps to keep the fallback simple and reduce complexity.
_SENIORITY_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(c(eo|to|oo)|chief)\b", re.I), "C"),
    (re.compile(r"\bvice president\b|\bvp\b", re.I), "VP"),
    (re.compile(r"\bhead\b", re.I), "Head"),
    (re.compile(r"\bdirector\b", re.I), "Director"),
    (re.compile(r"\bmanager\b|\bmgr\b", re.I), "Manager"),
]

_ROLE_KEYWORDS: list[tuple[tuple[str, ...], str]] = [
    (("sales",), "Sales"),
    (("marketing", "growth", "demand gen"), "Marketing"),
    (("product",), "Product"),
    (("engineering", "engineer", "developer"), "Engineering"),
    (("data", "analytics", "scientist"), "Data"),
    (("finance", "cfo", "accounting"), "Finance"),
    (("hr", "human resources", "people"), "HR"),
    (("it", "information technology"), "IT"),
    (("operations", "ops"), "Operations"),
    (("design", "ux"), "Design"),
]


def _infer_role_family_and_seniority(title_norm: str | None) -> tuple[str | None, str | None]:
    """
    Minimal, deterministic fallback for O02 when people.role_family/seniority are empty.
    Keeps acceptance simple without depending on a separate O02 backfill.
    """
    if not title_norm:
        return None, None

    t = title_norm.lower()

    # Seniority
    seniority = next((val for pat, val in _SENIORITY_PATTERNS if pat.search(t)), None)

    # Role family (very coarse keyword presence)
    role_family = next((val for keys, val in _ROLE_KEYWORDS if any(k in t for k in keys)), None)

    return role_family, seniority


def main(db: str = "data/dev.db") -> None:
    """
    Backfill ICP scores (R14) for existing people/companies.

    Reads minimal fields from people + companies and writes:
      - people.icp_score (INTEGER)
      - people.icp_reasons (TEXT JSON list[str])
      - people.last_scored_at (UTC ISO8601)
    """
    _ensure_db_exists(db)

    cfg: dict[str, Any] = load_icp_config()
    if not cfg:
        raise SystemExit("ICP config is empty or missing; expected docs/icp-schema.yaml")

    conn = sqlite3.connect(db)
    try:
        cur = conn.cursor()

        # Join via company_id (domain is not stored as a column on people/companies).
        # O03 provides companies.attrs (TEXT JSON). No dedicated columns for industry/size.
        cur.execute(
            """
            SELECT
                p.id,                   -- 0
                p.role_family,          -- 1
                p.seniority,            -- 2
                p.title_norm,           -- 3
                p.company_id,           -- 4
                c.attrs                 -- 5 (TEXT JSON from O03)
            FROM people p
            LEFT JOIN companies c
              ON c.id = p.company_id
            """
        )
        rows = cur.fetchall()

        # Aware UTC timestamp like 2025-11-13T12:34:56Z
        now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        for pid, rf, sr, title_norm, company_id, attrs_json in rows:
            # Parse company attrs safely
            try:
                attrs_dict = json.loads(attrs_json or "{}")
                if not isinstance(attrs_dict, dict):
                    attrs_dict = {}
            except Exception:
                attrs_dict = {}

            # Fill missing O02 fields from title_norm (fallback only)
            rf_fallback, sr_fallback = _infer_role_family_and_seniority(title_norm)
            role_family = (rf or rf_fallback) or None
            seniority = (sr or sr_fallback) or None

            company = {
                "id": company_id,
                "industry": attrs_dict.get("industry"),
                "size": attrs_dict.get("size"),
                "attrs": attrs_dict,
            }
            person = {
                "role_family": role_family,
                "seniority": seniority,
                "title_norm": title_norm,
                "domain": None,  # not stored; keep key for scorer API compatibility
            }

            res = compute_icp(person, company, cfg)

            cur.execute(
                """
                UPDATE people
                   SET icp_score = ?,
                       icp_reasons = ?,
                       last_scored_at = ?,
                       role_family = COALESCE(role_family, ?),
                       seniority   = COALESCE(seniority, ?)
                 WHERE id = ?
                """,
                (
                    int(res.score),
                    json.dumps(res.reasons, ensure_ascii=False),
                    now,
                    role_family,
                    seniority,
                    pid,
                ),
            )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill R14 ICP scores for existing data.")
    parser.add_argument(
        "--db", default="data/dev.db", help="Path to SQLite database (default: data/dev.db)"
    )
    args = parser.parse_args()
    main(args.db)
