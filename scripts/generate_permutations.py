# scripts/generate_permutations.py
from __future__ import annotations

"""
Convenience exports and a tiny CLI for generating email permutations.

O09: adds normalization/transliteration so names are particle-aware and ASCII
before applying patterns.
"""

# --- Ensure project root is on sys.path so `src` imports work when run directly ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Re-export canonical utilities for scripting convenience.
from src.generate.patterns import (  # noqa: F401
    PATTERNS,
    ROLE_ALIASES,
    Inference,
    LPFn,
    apply_pattern,
    infer_domain_pattern,
    norm_name,
)

# O09: normalized generation wrapper
from src.generate.permutations import generate_permutations
from src.ingest.normalize import normalize_split_parts

__all__ = [
    "LPFn",
    "PATTERNS",
    "ROLE_ALIASES",
    "norm_name",
    "apply_pattern",
    "Inference",
    "infer_domain_pattern",
    "generate_for",
]


def generate_for(first: str, last: str, domain: str, only_pattern: str | None = None) -> set[str]:
    """
    Generate permutations for first/last@domain using O09 normalization.

    Args:
        first: Raw first-name string (may include diacritics or particles).
        last: Raw last-name string.
        domain: Email domain.
        only_pattern: Optional canonical pattern key (e.g., "first.last").

    Returns:
        A set of candidate emails.
    """
    nf, nl = normalize_split_parts(first, last)
    dom = (domain or "").lower().strip()
    if not dom:
        return set()
    return generate_permutations(nf, nl, dom, only_pattern=only_pattern)


if __name__ == "__main__":
    import argparse
    import json

    p = argparse.ArgumentParser(
        description="Generate email permutations with normalized names (O09)."
    )
    p.add_argument("--first", required=True, help="Raw first name")
    p.add_argument("--last", required=True, help="Raw last name")
    p.add_argument("--domain", required=True, help="Email domain (e.g., example.com)")
    p.add_argument(
        "--only-pattern",
        choices=sorted(PATTERNS.keys()),
        help="Restrict to a single canonical pattern key (e.g., first.last).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Output JSON array instead of newline-separated text.",
    )
    args = p.parse_args()

    candidates = sorted(generate_for(args.first, args.last, args.domain, args.only_pattern))
    if args.json:
        sys.stdout.write(json.dumps(candidates, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write("\n".join(candidates) + ("\n" if candidates else ""))
