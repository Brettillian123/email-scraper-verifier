# src/generate/permutations.py
from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

# Reuse role aliases from R11 if available (non-fatal if not present for unit tests)
try:
    from src.extract.candidates import ROLE_ALIASES  # type: ignore
except Exception:  # pragma: no cover
    ROLE_ALIASES: set[str] = set()

# O01 canonical toolkit (keys like "first.last"); used when an explicit key is supplied.
from src.generate.patterns import (
    PATTERNS as CANON_PATTERNS,  # dict[str, LPFn]
)
from src.generate.patterns import (
    apply_pattern,  # (first, last, key) -> local
)

# ---------------------------------------------------------------------------
# Legacy R12 pattern templates (kept for backward compatibility with tests)
# Placeholders: {first}, {last}, {f}, {l}
# ---------------------------------------------------------------------------
PATTERNS: tuple[str, ...] = (
    "{first}.{last}",
    "{first}{last}",
    "{f}{last}",
    "{first}{l}",
    "{f}.{last}",
    "{first}.{l}",
    "{first}_{last}",
    "{first}-{last}",
    "{last}{first}",
    "{last}.{first}",
    "{last}{f}",
    "{l}{first}",
)


# ------------------------------
# Name normalization (R12 legacy)
# ------------------------------
def _to_ascii_lower(s: str) -> str:
    """ASCII-fold and lower-case."""
    nfkd = unicodedata.normalize("NFKD", s)
    return nfkd.encode("ascii", "ignore").decode("ascii").lower()


def normalize_name_parts(first: str, last: str) -> tuple[str, str, str, str]:
    """
    Return normalized (first, last, f, l):
      - lower-cased
      - ASCII-only
      - alphanumeric only
      - f/l are single-letter initials

    NOTE: we avoid a variable literally named `l` to satisfy Ruff E741,
    but we still return four items where the 4th is the last initial.
    """
    first = _to_ascii_lower(first or "")
    last = _to_ascii_lower(last or "")
    first = re.sub(r"[^a-z0-9]", "", first)
    last = re.sub(r"[^a-z0-9]", "", last)
    f_initial = first[:1]
    last_initial = last[:1]
    return first, last, f_initial, last_initial


# -----------------------
# Candidate generation
# -----------------------
def generate_permutations(
    first: str,
    last: str,
    domain: str,
    *,
    # If provided, this takes precedence. Accepts either a canonical key
    # (e.g., 'first.last') OR a legacy format string (e.g., '{first}.{last}').
    only_pattern: str | None = None,
    # (optional; unused here but accepted to avoid breaking callers that pass it)
    examples: Iterable[tuple[str, str, str]] | None = None,  # noqa: ARG001
) -> set[str]:
    """
    Make email candidates for first/last@domain.

    Resolution order:
      1) If only_pattern is a canonical key in CANON_PATTERNS, generate exactly one
         using apply_pattern().
      2) Else if only_pattern is a legacy brace-template string, render with the
         legacy normalization context.
      3) Else fall back to the legacy R12 PATTERNS list.

    Role/distribution aliases are always skipped when known.
    """
    if not (first or last) or not domain:
        return set()

    dom = domain.lower().strip()
    out: set[str] = set()

    # 1) Canonical key path (O01)
    if only_pattern and only_pattern in CANON_PATTERNS:
        local = apply_pattern(first, last, only_pattern)
        if local and local not in ROLE_ALIASES:
            out.add(f"{local}@{dom}")
        return out

    # Prepare legacy formatting context
    first_n, last_n, f_initial, l_initial = normalize_name_parts(first, last)
    ctx = {"first": first_n, "last": last_n, "f": f_initial, "l": l_initial}

    # 2) Legacy single-template path
    if only_pattern and only_pattern not in CANON_PATTERNS:
        try:
            local = only_pattern.format(**ctx)
        except Exception:
            local = ""
        if local and local not in ROLE_ALIASES:
            out.add(f"{local}@{dom}")
        return out

    # 3) Legacy full-set fallback
    for pattern in PATTERNS:
        try:
            local = pattern.format(**ctx)
        except Exception:
            continue
        if not local or local in ROLE_ALIASES:
            continue
        out.add(f"{local}@{dom}")

    return out


# -----------------------------------------
# Legacy heuristic used by some R12 tests
# -----------------------------------------
def infer_domain_pattern(
    emails: Iterable[str],
    first: str,
    last: str,
) -> str | None:
    """
    R12-era heuristic inference that looks only at published localparts,
    returning a *legacy* format string like '{first}.{last}' when a clear
    shape dominates. Kept for backward compatibility with tests.

    Priority:
      1) first.last / first_last / first-last (separator-based)
      2) f + last
      3) first + last
      4) first + l  (rare; after f+last)
    """
    locals_published = {e.split("@", 1)[0].lower() for e in emails if "@" in e}
    if not locals_published:
        return None

    def two_token(local: str, sep: str) -> bool:
        if sep not in local or local.count(sep) != 1:
            return False
        a, b = local.split(sep, 1)
        return bool(a) and bool(b)

    # 1) Separator-based patterns (strongest indicator)
    if any(two_token(lc, ".") for lc in locals_published):
        return "{first}.{last}"
    if any(two_token(lc, "_") for lc in locals_published):
        return "{first}_{last}"
    if any(two_token(lc, "-") for lc in locals_published):
        return "{first}-{last}"

    # 2) Initial + last (e.g., jdoe)
    if any(re.fullmatch(r"[a-z][a-z0-9]+", lc) for lc in locals_published):
        return "{f}{last}"

    # 3) firstlast (no separator; weak heuristic)
    if any(re.fullmatch(r"[a-z0-9]{6,}", lc) for lc in locals_published):
        return "{first}{last}"

    # 4) first + last initial (e.g., john.d) — rare; keep last
    if any(re.fullmatch(r"[a-z0-9]+[a-z]", lc) for lc in locals_published):
        return "{first}{l}"

    return None
