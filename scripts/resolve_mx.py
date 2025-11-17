# scripts/resolve_mx.py
from __future__ import annotations

"""
R15 — DNS/MX Lookup Service: CLI

Usage examples:
  python scripts/resolve_mx.py --domain crestwellpartners.com
  python scripts/resolve_mx.py --company-id 12 --domain foo.com --force
  python scripts/resolve_mx.py --domain пример.рф  # IDN handled

Prints:
  - lowest MX
  - all MX (sorted by preference)
  - preference map
  - status (cached / fresh)
  - failure (if any)

Notes:
  - Uses DATABASE_URL=sqlite:///path/to.db if set; otherwise --db or data/dev.db.
  - company_id is optional; if omitted we try to locate a company row by domain.
    If not found, we use 0 (no FK is enforced in R15 migration).
"""

import argparse
import json
import os
import sqlite3
import sys
from typing import Any

# Import resolver
from src.resolve.mx import DEFAULT_DB_PATH, DEFAULT_TTL_SECONDS, norm_domain, resolve_mx


def _db_path_from_env_or_flag(flag: str | None) -> str:
    if flag:
        return flag
    url = os.getenv("DATABASE_URL", "").strip()
    if url.startswith("sqlite:///"):
        return url[len("sqlite:///") :]
    # Fallback to resolver default
    return DEFAULT_DB_PATH


def _lookup_company_id(con: sqlite3.Connection, domain: str) -> int:
    """
    Best-effort lookup for a company id by domain. We try both 'companies.domain'
    and 'companies.official_domain' if present. Returns 0 if not found.
    """
    try:
        cur = con.cursor()
        # Prefer exact domain column
        row = cur.execute("SELECT id FROM companies WHERE domain = ? LIMIT 1", (domain,)).fetchone()
        if row:
            return int(row[0])

        # Try official_domain if column exists
        try:
            cols = {r[1] for r in cur.execute("PRAGMA table_info(companies)").fetchall()}
            if "official_domain" in cols:
                row2 = cur.execute(
                    "SELECT id FROM companies WHERE official_domain = ? LIMIT 1", (domain,)
                ).fetchone()
                if row2:
                    return int(row2[0])
        except Exception:
            pass
    except Exception:
        pass
    return 0


def _pretty_print(res: dict[str, Any]) -> None:
    domain = res.get("domain") or ""
    lowest = res.get("lowest_mx")
    hosts = list(res.get("mx_hosts") or [])
    prefmap = dict(res.get("preference_map") or {})
    failure = res.get("failure")
    cached = bool(res.get("cached"))

    # Sort hosts by preference map, then by name for determinism
    hosts_sorted = sorted(hosts, key=lambda h: (prefmap.get(h, 10**9), h))

    status = "cached" if cached else "fresh"
    print(f"Domain:        {domain}")
    print(f"Status:        {status}")
    print(f"Lowest MX:     {lowest if lowest else '(none)'}")
    print("All MX:")
    if hosts_sorted:
        for h in hosts_sorted:
            print(f"  - {h} (pref {prefmap.get(h, '?')})")
    else:
        print("  - (none)")

    # Stable JSON for preference map (sorted by preference then key)
    pref_items = sorted(prefmap.items(), key=lambda kv: (kv[1], kv[0]))
    pref_sorted = {k: v for k, v in pref_items}
    print("Preference map (JSON):")
    print(json.dumps(pref_sorted, ensure_ascii=False, separators=(",", ":"), indent=2))

    if failure:
        print(f"Failure:       {failure}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="R15: Resolve MX for a domain (with DB cache).")
    ap.add_argument("--domain", required=True, help="Domain to resolve (Unicode or ASCII).")
    ap.add_argument(
        "--company-id",
        type=int,
        default=None,
        help="Company ID to associate with this resolution (optional).",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Force refresh (ignore cache and re-resolve).",
    )
    ap.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB. Defaults to DATABASE_URL or data/dev.db.",
    )
    ap.add_argument(
        "--ttl",
        type=int,
        default=DEFAULT_TTL_SECONDS,
        help=f"TTL seconds for cache writes (default {DEFAULT_TTL_SECONDS}).",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON instead of pretty text.",
    )

    args = ap.parse_args(argv)

    db_path = _db_path_from_env_or_flag(args.db)
    canon = norm_domain(args.domain)
    if not canon:
        print("error: empty or invalid --domain", file=sys.stderr)
        return 2

    company_id: int
    if args.company_id is not None:
        company_id = int(args.company_id)
    else:
        # best-effort lookup; use 0 if not found
        try:
            con = sqlite3.connect(db_path)
            with con:
                company_id = _lookup_company_id(con, canon)
        except Exception:
            company_id = 0
        finally:
            try:
                con.close()
            except Exception:
                pass

    try:
        res_obj = resolve_mx(
            company_id=company_id,
            domain=canon,
            force=bool(args.force),
            db_path=db_path,
            ttl_seconds=int(args.ttl),
        )
        res = {
            "company_id": res_obj.company_id,
            "domain": res_obj.domain,
            "mx_hosts": res_obj.mx_hosts,
            "preference_map": res_obj.preference_map,
            "lowest_mx": res_obj.lowest_mx,
            "resolved_at": res_obj.resolved_at,
            "ttl": res_obj.ttl,
            "failure": res_obj.failure,
            "cached": res_obj.cached,
            "row_id": res_obj.row_id,
        }
    except Exception as e:
        err = {"error": f"{type(e).__name__}: {e}", "domain": canon}
        if args.json:
            print(json.dumps(err, ensure_ascii=False))
        else:
            print(f"Domain:  {canon}")
            print("Status:  error")
            print(f"Failure: {err['error']}")
        return 1

    if args.json:
        # Emit deterministic JSON (sort preference_map by pref for readability)
        if isinstance(res.get("preference_map"), dict):
            pm = res["preference_map"]
            res["preference_map"] = {k: pm[k] for k in sorted(pm, key=lambda h: (pm[h], h))}
        print(json.dumps(res, ensure_ascii=False, separators=(",", ":"), indent=2))
    else:
        _pretty_print(res)

    # Non-zero exit if failure and no A/AAAA fallback succeeded
    if res.get("failure") and not res.get("lowest_mx"):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
