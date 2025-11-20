# scripts/probe_catchall.py
from __future__ import annotations

"""
R17 CLI — domain-level catch-all probe

Usage examples (PowerShell):

  # Minimal (uses R15 MX resolver + R16 SMTP pipeline under the hood)
  #   $PyExe .\scripts\probe_catchall.py --domain gmail.com
  #
  # Second run should be cached:
  #   $PyExe .\scripts\probe_catchall.py --domain gmail.com
  #
  # Force bypass the cache and re-probe:
  #   $PyExe .\scripts\probe_catchall.py --domain gmail.com --force

Behavior:
  - Uses src.verify.catchall.check_catchall_for_domain().
  - Reuses R15 MX resolution and R16 SMTP RCPT pipeline.
  - Prints a human-friendly summary including status, MX, RCPT code,
    whether the result came from cache, and the random address used.
"""

import argparse
import sys

from src.verify.catchall import (
    CATCHALL_TTL_SECONDS,
    check_catchall_for_domain,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="R17: Domain-level catch-all probe using cached MX + SMTP RCPT.",
    )
    p.add_argument(
        "--domain",
        "-d",
        required=True,
        help="Domain to probe (e.g. gmail.com)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Bypass cache TTL and force a fresh SMTP probe.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    domain = (args.domain or "").strip().lower()
    if not domain:
        parser.error("Domain must be a non-empty string (e.g. --domain gmail.com)")

    try:
        res = check_catchall_for_domain(domain, force=bool(args.force))
    except Exception as exc:  # pragma: no cover - defensive
        sys.stderr.write(f"Error: catch-all probe failed for {domain}: {exc}\n")
        return 1

    ttl_hours = CATCHALL_TTL_SECONDS / 3600.0
    rcpt_code = res.rcpt_code if res.rcpt_code is not None else "-"
    mx_host = res.mx_host or "-"
    result_source = "cached" if res.cached else "fresh"
    elapsed_ms = int(res.elapsed_ms)
    random_addr = f"{res.localpart}@{res.domain}" if res.localpart else "-"

    print(f"Domain:         {res.domain}")
    print(f"Status:         {res.status}")
    print(f"MX host:        {mx_host}")
    print(f"RCPT code:      {rcpt_code}")
    print(f"Result source:  {result_source}")
    print(f"Cache TTL:      {int(ttl_hours)}h ({CATCHALL_TTL_SECONDS} seconds)")
    print(f"Random address: {random_addr}")
    print(f"Elapsed:        {elapsed_ms} ms")
    if res.error:
        print(f"Error:          {res.error}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
