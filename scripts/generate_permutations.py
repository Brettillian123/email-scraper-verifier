#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the project root (parent of /scripts) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import after path fix
from src.generate.permutations import (  # noqa: E402
    generate_permutations,
    infer_domain_pattern,
)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Generate email permutations, optionally constrained by inferred pattern."
    )
    p.add_argument("--first", required=True)
    p.add_argument("--last", required=True)
    p.add_argument("--domain", required=True)
    p.add_argument(
        "--infer-from",
        nargs="*",
        default=[],
        metavar="EMAIL",
        help="Published emails from the same domain to infer the pattern (e.g., jane.doe@example.com).",
    )
    args = p.parse_args()

    pattern = None
    if args.infer_from:
        pattern = infer_domain_pattern(args.infer_from, args.first, args.last)

    result = generate_permutations(args.first, args.last, args.domain, only_pattern=pattern)
    for e in sorted(result):
        print(e)


if __name__ == "__main__":
    main()
