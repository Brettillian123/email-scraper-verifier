# scripts/backfill_o02_roles.py
from __future__ import annotations

import argparse
import importlib
import os
import re
from contextlib import closing
from typing import Any

from src.db import get_conn  # type: ignore[import]


def classify_heuristic(title: str):
    t = (title or "").lower()

    # seniority
    if re.search(r"\bchief\b|^c[eo]o\b|^c[a-z]{1,3}o\b", t) or re.search(
        r"\bceo|cto|cfo|cmo|cso|cio\b", t
    ):
        seniority = "C"
    elif "vice president" in t or re.search(r"\bvp\b", t):
        seniority = "VP"
    elif re.search(r"\bhead\b", t):
        seniority = "Head"
    elif "director" in t:
        seniority = "Director"
    elif "manager" in t:
        seniority = "Manager"
    else:
        seniority = "IC"

    # role family
    if "sales" in t:
        role_family = "Sales"
    elif "marketing" in t:
        role_family = "Marketing"
    elif "engineering" in t or "engineer" in t or "devops" in t:
        role_family = "Engineering"
    elif "customer success" in t or ("customer" in t and "success" in t):
        role_family = "Customer Success"
    elif "operations" in t or re.search(r"\bops\b", t):
        role_family = "Operations"
    else:
        role_family = None

    return role_family, seniority


def classify_with_module(title: str):
    """
    Try to use src.ingest.title_norm if available.
    Supports a few likely function shapes; falls back to heuristics.
    """
    try:
        m = importlib.import_module("src.ingest.title_norm")
    except Exception:
        return None, None

    candidates = (
        "classify",
        "infer",
        "normalize",
        "normalize_title",
        "classify_title",
        "infer_role_seniority",
    )

    for name in candidates:
        f = getattr(m, name, None)
        if callable(f):
            try:
                out = f(title)

                # tuple(shape) -> (role_family, seniority) or (seniority, role) – try both
                if isinstance(out, tuple) and len(out) >= 2:
                    a, b = out[0], out[1]
                    if str(a).lower() in {"c", "vp", "head", "director", "manager", "ic"}:
                        return (b, a)
                    return (a, b)

                # dataclass / object with attributes
                role_family = getattr(out, "role_family", None)
                seniority = getattr(out, "seniority", None)
                if role_family or seniority:
                    return role_family, seniority
            except Exception:
                continue

    return None, None


def _people_has_tenant_id(conn) -> bool:
    q = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'people'
          AND column_name = 'tenant_id'
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchone() is not None


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="O02: Backfill role_family/seniority for people rows.")
    ap.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope updates. Default: all tenants.",
    )
    ap.add_argument(
        "--dsn",
        dest="dsn",
        default=None,
        help="Optional Postgres DSN/URL override. If provided, sets DATABASE_URL for this run.",
    )
    args = ap.parse_args(argv)

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    with closing(get_conn()) as conn:
        has_tenant = _people_has_tenant_id(conn)

        q = (
            """
            SELECT
              id,
              {tenant_sel}
              COALESCE(NULLIF(title_norm, ''), NULLIF(title, '')) AS t,
              NULLIF(role_family, '') AS rf,
              NULLIF(seniority, '')   AS sn
            FROM people
            """
        ).format(tenant_sel="tenant_id AS tenant_id, " if has_tenant else "")

        params: list[Any] = []
        if args.tenant_id and has_tenant:
            q += " WHERE tenant_id = %s"
            params.append(args.tenant_id)

        with conn.cursor() as cur:
            cur.execute(q, tuple(params))
            rows = cur.fetchall()

        updated = 0
        with conn:
            with conn.cursor() as cur:
                for row in rows:
                    if has_tenant:
                        pid, tid, t, rf, sn = row
                    else:
                        pid, t, rf, sn = row
                        tid = None

                    if not t:
                        continue

                    new_rf, new_sn = rf, sn
                    if not (rf and sn):
                        mod_rf, mod_sn = classify_with_module(t)
                        if not mod_rf and not mod_sn:
                            mod_rf, mod_sn = classify_heuristic(t)

                        new_rf = new_rf or mod_rf
                        new_sn = new_sn or mod_sn

                    if (new_rf != rf) or (new_sn != sn):
                        if has_tenant:
                            cur.execute(
                                "UPDATE people SET role_family = %s, seniority = %s WHERE id = %s AND tenant_id = %s",
                                (new_rf, new_sn, pid, tid),
                            )
                        else:
                            cur.execute(
                                "UPDATE people SET role_family = %s, seniority = %s WHERE id = %s",
                                (new_rf, new_sn, pid),
                            )
                        updated += cur.rowcount or 0

        print(f"Backfill complete. Rows updated: {updated}")


if __name__ == "__main__":
    main()
