from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

# Reuse role aliases from R11 if available (non-fatal if not present for unit tests)
try:
    from src.extract.candidates import ROLE_ALIASES  # type: ignore
except Exception:  # pragma: no cover
    ROLE_ALIASES: set[str] = set()

# Common local-part patterns. Placeholders: {first}, {last}, {f}, {l}
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


def generate_permutations(
    first: str,
    last: str,
    domain: str,
    only_pattern: str | None = None,
) -> set[str]:
    """
    Make email candidates for first/last@domain using common patterns.
    If only_pattern is provided (e.g., '{first}.{last}'), use just that one.
    """
    first, last, f_initial, last_initial = normalize_name_parts(first, last)
    if not (first or last) or not domain:
        return set()

    locals_seen: set[str] = set()
    patterns = (only_pattern,) if only_pattern else PATTERNS
    ctx = {"first": first, "last": last, "f": f_initial, "l": last_initial}

    for pattern in patterns:
        try:
            local = pattern.format(**ctx)
        except Exception:
            continue
        if not local:
            continue
        if local in ROLE_ALIASES:
            # Skip obvious role/distribution addresses if aliases were provided
            continue
        locals_seen.add(local)

    dom = domain.lower()
    return {f"{local}@{dom}" for local in locals_seen}


def infer_domain_pattern(
    emails: Iterable[str],
    first: str,
    last: str,
) -> str | None:
    """
    Given known valid emails for a domain and a name, try to infer which of our
    PATTERNS the domain uses by formatting each pattern with the normalized name
    and checking whether the formatted local-part appears among the emails.
    """
    first, last, f_initial, last_initial = normalize_name_parts(first, last)
    locals_published = {e.split("@", 1)[0].lower() for e in emails if "@" in e}

    if not locals_published:
        return None

    ctx = {"first": first, "last": last, "f": f_initial, "l": last_initial}
    for pattern in PATTERNS:
        try:
            local = pattern.format(**ctx)
        except Exception:
            continue
        if local in locals_published:
            return pattern
    return None
