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

# Central O26 role/placeholder classifier (preferred).
# We fall back to ROLE_ALIASES if this is not available (e.g., older tests).
try:
    from src.emails.classify import is_role_or_placeholder_email
except Exception:  # pragma: no cover

    def is_role_or_placeholder_email(addr: str) -> bool:  # type: ignore[no-redef]
        return False


# O01/O26 canonical toolkit (keys like "first.last"); used when an explicit key is supplied
# or when generating the full candidate set in priority order.
from src.generate.patterns import (
    PATTERNS as CANON_PATTERNS,  # dict[str, LPFn]
)
from src.generate.patterns import (
    apply_pattern,  # (first, last, key) -> local
    generate_candidate_emails_for_person,  # (first, last, domain, company_pattern) -> list[str]
)

# Optional ranking + cap (may not exist in older branches/tests).
try:
    from src.generate.patterns import (  # type: ignore
        DEFAULT_MAX_PERMUTATIONS_PER_PERSON,
        PATTERN_RANKS,
    )
except Exception:  # pragma: no cover
    DEFAULT_MAX_PERMUTATIONS_PER_PERSON = 6  # sensible default
    PATTERN_RANKS: dict[str, int] = {
        "first.last": 1,
        "first_last": 2,
        "firstlast": 3,
        "first": 4,
        "flast": 5,
        "firstl": 6,
        "f.last": 7,
        "first.l": 8,
        "first-last": 10,
        "first_l": 11,
        "f_last": 12,
        # anything unknown ranks worse
    }

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

# Map legacy templates to canonical keys (where possible) so we can apply ranks.
_TEMPLATE_TO_CANON_KEY: dict[str, str] = {
    "{first}.{last}": "first.last",
    "{first}{last}": "firstlast",
    "{f}{last}": "flast",
    "{first}{l}": "firstl",
    "{f}.{last}": "f.last",
    "{first}.{l}": "first.l",
    "{first}_{last}": "first_last",
    "{first}-{last}": "first-last",
    "{last}{first}": "lastfirst",
    "{last}.{first}": "last.first",
    "{last}{f}": "lastf",
    "{l}{first}": "lfirst",
}


def _pattern_rank_for_template(template: str) -> int:
    """
    Rank a legacy template using PATTERN_RANKS when mappable; otherwise push late.
    Lower rank = more preferred.
    """
    key = _TEMPLATE_TO_CANON_KEY.get(template, "")
    if key and key in PATTERN_RANKS:
        return int(PATTERN_RANKS[key])
    # Unknown / uncommon shapes should be least preferred.
    return 10_000


def _is_role_or_placeholder(addr: str, *, local_hint: str | None = None) -> bool:
    """
    Internal helper used by permutation generation to decide whether a candidate
    local-part/email should be treated as a generic/role/placeholder address.

    Resolution:
      1) Prefer the central classifier in src.emails.classify (captures info@,
         support@, hello@, example@, noreply@, info+foo@, etc.).
      2) Fall back to legacy ROLE_ALIASES checks on the local-part.

    This ensures we never generate or keep obviously non-personal addresses
    as permutations for a specific person.
    """
    # 1) Central classifier (uses full addr, including '+' patterns and prefixes)
    try:
        if is_role_or_placeholder_email(addr):
            return True
    except Exception:
        # If classifier is missing or misconfigured, we fall back to aliases only.
        pass

    # 2) Legacy alias-based check on local-part
    local = local_hint
    if not local:
        local = addr.split("@", 1)[0]
    local = local.lower()

    return local in ROLE_ALIASES


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


def _compute_cap(max_permutations_per_person: int | None) -> int:
    cap = (
        DEFAULT_MAX_PERMUTATIONS_PER_PERSON
        if max_permutations_per_person is None
        else int(max_permutations_per_person)
    )
    return cap if cap > 0 else 0


def _legacy_ctx(first: str, last: str) -> dict[str, str]:
    first_n, last_n, f_initial, l_initial = normalize_name_parts(first, last)
    return {"first": first_n, "last": last_n, "f": f_initial, "l": l_initial}


def _render_legacy_local(template: str, ctx: dict[str, str]) -> str:
    try:
        return template.format(**ctx)
    except Exception:
        return ""


class _EmailAccumulator:
    def __init__(self, cap: int) -> None:
        self._cap = cap
        self._set: set[str] = set()
        self._list: list[str] = []

    def full(self) -> bool:
        return len(self._list) >= self._cap

    def add(self, email: str) -> None:
        if not email or "@" not in email:
            return
        if email in self._set:
            return

        local = email.split("@", 1)[0]
        if not local:
            return
        if _is_role_or_placeholder(email, local_hint=local):
            return
        if self.full():
            return

        self._set.add(email)
        self._list.append(email)

    def as_set(self) -> set[str]:
        return set(self._list)


def _add_only_pattern(
    *,
    acc: _EmailAccumulator,
    first: str,
    last: str,
    dom: str,
    only_pattern: str,
    ctx: dict[str, str],
) -> None:
    if only_pattern in CANON_PATTERNS:
        local = apply_pattern(first, last, only_pattern)
    else:
        local = _render_legacy_local(only_pattern, ctx)

    if local:
        acc.add(f"{local}@{dom}")


def _add_canonical_candidates(
    *,
    acc: _EmailAccumulator,
    first: str,
    last: str,
    dom: str,
    company_pattern: str | None,
) -> None:
    for email in generate_candidate_emails_for_person(
        first_name=first,
        last_name=last,
        domain=dom,
        company_pattern=company_pattern,
    ):
        acc.add(email)
        if acc.full():
            return


def _add_legacy_fallback(
    *,
    acc: _EmailAccumulator,
    dom: str,
    ctx: dict[str, str],
) -> None:
    for template in sorted(PATTERNS, key=_pattern_rank_for_template):
        if acc.full():
            return
        local = _render_legacy_local(template, ctx)
        if local:
            acc.add(f"{local}@{dom}")


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
    # Optional company-level pattern (canonical key) to *prefer* when generating
    # the full candidate set. Ignored when only_pattern is supplied.
    company_pattern: str | None = None,
    # Hard cap to prevent permutation explosion. If None, uses the project default.
    max_permutations_per_person: int | None = None,
) -> set[str]:
    """
    Make email candidates for first/last@domain.

    Resolution order:
      1) If only_pattern is a canonical key in CANON_PATTERNS, generate exactly one
         using apply_pattern().
      2) Else if only_pattern is a legacy brace-template string, render with the
         legacy normalization context.
      3) Else:
           - Generate canonical candidates using the O26 priority list
             (via generate_candidate_emails_for_person), optionally preferring
             company_pattern first.
           - Add any remaining legacy R12 PATTERNS templates as fallback.

    Role/distribution aliases and other placeholder addresses are always skipped
    when known, using the central classifier plus ROLE_ALIASES.

    IMPORTANT:
      - Enforces a strict per-person cap (DEFAULT_MAX_PERMUTATIONS_PER_PERSON, or
        max_permutations_per_person if provided) after dedupe/normalization.
    """
    if not (first or last) or not domain:
        return set()

    dom = domain.lower().strip()
    cap = _compute_cap(max_permutations_per_person)
    if cap <= 0:
        return set()

    acc = _EmailAccumulator(cap)
    ctx = _legacy_ctx(first, last)

    if only_pattern:
        _add_only_pattern(
            acc=acc,
            first=first,
            last=last,
            dom=dom,
            only_pattern=only_pattern,
            ctx=ctx,
        )
        return acc.as_set()

    _add_canonical_candidates(
        acc=acc,
        first=first,
        last=last,
        dom=dom,
        company_pattern=company_pattern,
    )
    if acc.full():
        return acc.as_set()

    _add_legacy_fallback(acc=acc, dom=dom, ctx=ctx)
    return acc.as_set()


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
