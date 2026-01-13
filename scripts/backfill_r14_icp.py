# scripts/backfill_r14_icp.py
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from contextlib import closing
from typing import Any

from src.config import load_icp_config
from src.db import get_conn
from src.scoring.icp import compute_icp

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


def _has_column(conn, table: str, column: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table, column))
        return cur.fetchone() is not None


def _safe_attrs_dict(raw: Any) -> dict[str, Any]:
    """
    Normalize companies.attrs into a dict regardless of storage type:
      - json/jsonb may come back as dict (driver-dependent)
      - text/json string needs json.loads
    """
    if not raw:
        return {}

    if isinstance(raw, dict):
        return raw

    if isinstance(raw, str):
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    # Some drivers may return a specialized JSON wrapper; try best-effort.
    try:
        s = str(raw)
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def main() -> None:
    """
    Backfill ICP scores (R14) for existing people/companies.

    Reads minimal fields from people + companies and writes:
      - people.icp_score (INTEGER)
      - people.icp_reasons (TEXT/JSON)
      - people.last_scored_at (UTC ISO8601)
    """
    parser = argparse.ArgumentParser(description="Backfill R14 ICP scores for existing data.")
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope the backfill to one tenant. Default: all tenants.",
    )
    parser.add_argument(
        "--dsn",
        dest="dsn",
        default=None,
        help="Optional Postgres DSN/URL override. If provided, sets DATABASE_URL for this run.",
    )
    args = parser.parse_args()

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    cfg: dict[str, Any] = load_icp_config()
    if not cfg:
        raise SystemExit("ICP config is empty or missing; expected docs/icp-schema.yaml")

    with closing(get_conn()) as conn:
        people_has_tenant = _has_column(conn, "people", "tenant_id")
        companies_has_tenant = _has_column(conn, "companies", "tenant_id")
        companies_has_attrs = _has_column(conn, "companies", "attrs")

        join_pred = "c.id = p.company_id"
        if people_has_tenant and companies_has_tenant:
            join_pred += " AND c.tenant_id = p.tenant_id"

        select_attrs = "c.attrs" if companies_has_attrs else "NULL"

        sql = f"""
            SELECT
                p.id,                   -- 0
                p.role_family,          -- 1
                p.seniority,            -- 2
                p.title_norm,           -- 3
                p.company_id,           -- 4
                {select_attrs}          -- 5
                {", p.tenant_id" if people_has_tenant else ""}
            FROM people p
            LEFT JOIN companies c
              ON {join_pred}
        """

        params: list[Any] = []
        if args.tenant_id and people_has_tenant:
            sql += " WHERE p.tenant_id = %s"
            params.append(args.tenant_id)

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        update_sql_base = """
            UPDATE people
               SET icp_score = %s,
                   icp_reasons = %s,
                   last_scored_at = %s,
                   role_family = COALESCE(role_family, %s),
                   seniority   = COALESCE(seniority, %s)
             WHERE id = %s
        """

        updated = 0
        with conn:
            with conn.cursor() as cur:
                for row in rows:
                    if people_has_tenant:
                        pid, rf, sr, title_norm, company_id, attrs_raw, tid = row
                    else:
                        pid, rf, sr, title_norm, company_id, attrs_raw = row
                        tid = None

                    attrs_dict = _safe_attrs_dict(attrs_raw)

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

                    if people_has_tenant:
                        cur.execute(
                            update_sql_base + " AND tenant_id = %s",
                            (
                                int(res.score),
                                json.dumps(res.reasons, ensure_ascii=False),
                                now,
                                role_family,
                                seniority,
                                pid,
                                tid,
                            ),
                        )
                    else:
                        cur.execute(
                            update_sql_base,
                            (
                                int(res.score),
                                json.dumps(res.reasons, ensure_ascii=False),
                                now,
                                role_family,
                                seniority,
                                pid,
                            ),
                        )

                    updated += cur.rowcount or 0

        print(f"R14: updated {updated} people row(s).")


if __name__ == "__main__":
    main()
