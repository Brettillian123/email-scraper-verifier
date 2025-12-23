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
    """
    if not (first or last) or not domain:
        return set()

    dom = domain.lower().strip()
    out: set[str] = set()

    # 1) Canonical key path (O01/O26 "only this pattern")
    if only_pattern and only_pattern in CANON_PATTERNS:
        local = apply_pattern(first, last, only_pattern)
        if local and not _is_role_or_placeholder(f"{local}@{dom}", local_hint=local):
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
        if local and not _is_role_or_placeholder(f"{local}@{dom}", local_hint=local):
            out.add(f"{local}@{dom}")
        return out

    # 3) Canonical multi-pattern generator (O26) + legacy full-set fallback

    # 3a) Canonical candidates in O26 priority order. This covers:
    #       flast, first.last, first, firstl, firstlast, f.last, last,
    #       first_last, first-last, lastfirst
    #     and will prefer company_pattern first if provided.
    for email in generate_candidate_emails_for_person(
        first_name=first,
        last_name=last,
        domain=dom,
        company_pattern=company_pattern,
    ):
        local = email.split("@", 1)[0]
        if not local or _is_role_or_placeholder(email, local_hint=local):
            continue
        out.add(email)

    # 3b) Legacy R12 templates as fallback (adds any shapes not covered above,
    #     such as `{last}.{first}`, `{last}{f}`, `{l}{first}`).
    for pattern in PATTERNS:
        try:
            local = pattern.format(**ctx)
        except Exception:
            continue
        if not local or _is_role_or_placeholder(f"{local}@{dom}", local_hint=local):
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
