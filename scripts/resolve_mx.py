# scripts/resolve_mx.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.resolve.mx import (
    _update_latest_resolution_behavior,  # private but fine for CLI wiring
    get_mx_behavior_hint,
)

# R15/O06 helpers
from src.resolve.mx import (
    resolve_mx as _resolve_mx,
)

DEFAULT_DB = "data/dev.db"
DEFAULT_TTL = 86400  # 24h


def _abspath(p: str | None) -> str:
    if not p:
        return str(Path(DEFAULT_DB).resolve())
    return str(Path(p).resolve())


def _print_human(
    *,
    domain: str,
    status: str,
    lowest_mx: str | None,
    mx_hosts: list[str],
    preference_map: dict[str, int],
    behavior: dict[str, Any] | None = None,
) -> None:
    print(f"Domain:        {domain}")
    print(f"Status:        {status}")
    print(f"Lowest MX:     {lowest_mx}")
    print("All MX:")
    if mx_hosts:
        for h in mx_hosts:
            pref = preference_map.get(h, None)
            if pref is None:
                print(f"  - {h}")
            else:
                print(f"  - {h} (pref {pref})")
    else:
        print("  - (none)")
    print("Preference map (JSON):")
    print(json.dumps(preference_map, indent=2, ensure_ascii=False))
    if behavior is not None:
        print("Behavior hint (O06, JSON):")
        print(json.dumps(behavior, indent=2, ensure_ascii=False))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", required=True, help="Domain to resolve")
    ap.add_argument(
        "--company-id", type=int, default=0, help="Company ID for cache row (default 0)"
    )
    ap.add_argument("--force", action="store_true", help="Bypass TTL and refresh cache")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path (default: data/dev.db)")
    ap.add_argument("--ttl", type=int, default=DEFAULT_TTL, help="TTL seconds for cache rows")
    ap.add_argument("--json", action="store_true", help="Output JSON instead of human text")
    ap.add_argument(
        "--refresh-behavior",
        action="store_true",
        help="Summarize recent probe stats and write to domain_resolutions.mx_behavior",
    )
    args = ap.parse_args()

    db_path = _abspath(args.db)

    # Do the MX resolution (R15)
    res = _resolve_mx(
        company_id=int(args.company_id),
        domain=args.domain.strip(),
        force=bool(args.force),
        db_path=db_path,
        ttl_seconds=int(args.ttl),
    )
    status = "cached" if res.cached else "fresh"

    behavior: dict[str, Any] | None = None
    if args.refresh_behavior:
        # Summarize recent RCPT probe behavior for the lowest MX (O06)
        lowest = res.lowest_mx or args.domain.strip().lower()
        behavior = get_mx_behavior_hint(lowest, db_path=db_path)
        # Persist into the latest domain_resolutions row for this domain (best-effort)
        try:
            _update_latest_resolution_behavior(
                args.domain.strip().lower(), behavior, db_path=db_path
            )
        except Exception:
            # Do not fail CLI on write issues
            pass

    if args.json:
        out = {
            "domain": res.domain,
            "status": status,
            "company_id": res.company_id,
            "lowest_mx": res.lowest_mx,
            "mx_hosts": res.mx_hosts,
            "preference_map": res.preference_map,
            "resolved_at": res.resolved_at,
            "ttl": res.ttl,
            "failure": res.failure,
            "behavior": behavior,
        }
        print(json.dumps(out, ensure_ascii=False))
    else:
        _print_human(
            domain=res.domain,
            status=status,
            lowest_mx=res.lowest_mx,
            mx_hosts=res.mx_hosts,
            preference_map=res.preference_map,
            behavior=behavior,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
