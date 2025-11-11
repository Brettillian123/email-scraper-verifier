# scripts/fetch_url.py
"""
Quick manual fetcher for R09 testing.

Usage (Windows PowerShell 7.x):

  .\.venv\Scripts\python.exe .\scripts\fetch_url.py --url "https://example.com/about" --verbose
  .\.venv\Scripts\python.exe .\scripts\fetch_url.py -u "https://example.com/" --print-body
  .\.venv\Scripts\python.exe .\scripts\fetch_url.py -u "https://example.com/" --out page.html

Notes:
- Designed to work from repo root without installing the package (adds project root to sys.path).
- Prints a human summary and a final single-line RESULT that scripts can parse.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

# --- make src/ importable when running from repo root ------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Import the fetcher
try:
    from src.fetch import FetcherClient, RobotsDisallowed  # type: ignore
    from src.fetch import robots as robots_mod  # type: ignore
except Exception as e:  # pragma: no cover
    print(f"ERROR: failed to import fetcher package: {e}", file=sys.stderr)
    sys.exit(2)


def _split_host_path(url: str) -> tuple[str, str]:
    parts = urlsplit(url)
    host = (parts.netloc or "").lower()
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    return host, path


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch a URL with robots/throttle/cache enforcement.")
    ap.add_argument(
        "-u", "--url", required=True, help="URL to fetch, e.g. https://example.com/about"
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    ap.add_argument(
        "--print-body", action="store_true", help="Write response body to stdout (binary-safe)"
    )
    ap.add_argument("-o", "--out", help="Write body to file instead of stdout")
    ap.add_argument(
        "--raise-on-disallow", action="store_true", help="Raise if robots disallows (debug)"
    )
    args = ap.parse_args()

    url: str = args.url
    verbose: bool = args.verbose
    out_path: Path | None = Path(args.out) if args.out else None

    host, path = _split_host_path(url)

    if verbose:
        print(f"[cli] URL ..........: {url}")
        print(f"[cli] Host / Path ..: {host} {path}")
        # Pre-check robots (client will also enforce)
        try:
            allowed = robots_mod.is_allowed(host, path)
            delay = robots_mod.get_crawl_delay(host)
            print(f"[cli] Robots .......: allowed={allowed} crawl_delay={delay:.3f}s")
        except Exception as e:
            print(f"[cli] Robots check ..: ERROR ({e})")

    start_wall = time.time()
    try:
        with FetcherClient(raise_on_disallow=args.raise_on_disallow) as fc:
            res = fc.fetch(url)
    except RobotsDisallowed as e:
        elapsed = time.time() - start_wall
        completed_at = time.time()
        # Standardized single-line result for scripts to parse
        print(
            "RESULT status=451 reason=blocked-by-robots from_cache=False "
            f"elapsed_s={elapsed:.3f} completed_at={int(completed_at)}"
        )
        if verbose:
            print(f"[cli] Blocked by robots: {e}")
        return 0  # treat as expected behavior for this CLI

    elapsed = time.time() - start_wall
    completed_at = time.time()

    if verbose:
        print(f"[cli] Effective URL .: {res.effective_url}")
        print(f"[cli] Status ........: {res.status}")
        print(f"[cli] Reason ........: {res.reason}")
        print(f"[cli] From cache ....: {res.from_cache}")
        print(f"[cli] Content-Type ..: {res.content_type or '-'}")
        print(f"[cli] Body bytes ....: {len(res.body or b'')}")
        print(f"[cli] Elapsed (s) ...: {elapsed:.3f}")

    # Always print a final single-line RESULT for scripts to parse.
    # Accept both "blocked-by-robots" and "blocked_by_robots" semantics in external scripts.
    print(
        "RESULT "
        f"status={res.status} "
        f"reason={res.reason} "
        f"from_cache={str(res.from_cache)} "
        f"elapsed_s={elapsed:.3f} "
        f"completed_at={int(completed_at)}"
    )

    # Output body if requested
    if res.body:
        if args.print_body and not out_path:
            # Write raw bytes to stdout.buffer
            try:
                sys.stdout.buffer.write(res.body)
                # Ensure RESULT line stays visible; add a newline if body didn't end with one
                if not res.body.endswith(b"\n"):
                    sys.stdout.write("\n")
            except BrokenPipeError:
                # Allow piping
                pass
        elif out_path:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(res.body)
            if verbose:
                print(f"[cli] Wrote body -> {out_path}")

    # Exit code policy:
    #  - 0 on success or expected throttling/robots behavior (we're a diagnostic tool)
    #  - 1 on 5xx/transport errors
    if res.status >= 500 or res.reason.startswith("error"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
