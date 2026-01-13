from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass

# Local-part builder type
LPFn = Callable[[str, str], str]

# Canonical pattern set (ASCII, lowercase, separators normalized)
PATTERNS: dict[str, LPFn] = {
    "first.last": lambda fn, ln: f"{fn}.{ln}",
    "f.last": lambda fn, ln: f"{fn[:1]}.{ln}",
    "firstl": lambda fn, ln: f"{fn}{ln[:1]}",
    "flast": lambda fn, ln: f"{fn[:1]}{ln}",
    "first": lambda fn, ln: fn,
    "last": lambda fn, ln: ln,
    "first_last": lambda fn, ln: f"{fn}_{ln}",
    "first-last": lambda fn, ln: f"{fn}-{ln}",
    "firstlast": lambda fn, ln: f"{fn}{ln}",
    "lastfirst": lambda fn, ln: f"{ln}{fn}",
    # Additional (supported) patterns referenced by PATTERN_RANKS
    "first.l": lambda fn, ln: f"{fn}.{ln[:1]}",
    "first_l": lambda fn, ln: f"{fn}_{ln[:1]}",
    "f_last": lambda fn, ln: f"{fn[:1]}_{ln}",
}

# Pattern priority (lower = more common/preferred)
PATTERN_RANKS: dict[str, int] = {
    "first.last": 1,
    "first_last": 2,
    "firstlast": 3,
    "first": 4,
    "flast": 5,
    "firstl": 6,
    "f.last": 7,
    "first.l": 8,
    # ... less common patterns get higher ranks
    "first-last": 10,
    "first_l": 11,
    "f_last": 12,
}

DEFAULT_MAX_PERMUTATIONS_PER_PERSON = 6


def _pattern_rank(key: str) -> int:
    return int(PATTERN_RANKS.get(key, 999))


# Default priority order for generation / inference (most common first).
# Derived from PATTERN_RANKS, with unranked patterns appended after ranked ones.
PATTERN_PRIORITY: tuple[str, ...] = tuple(
    sorted(PATTERNS.keys(), key=lambda k: (_pattern_rank(k), k))
)

ROLE_ALIASES = {"info", "sales", "support", "hello", "marketing", "press", "admin"}


def _safe(s: str) -> list[str]:
    # Keep only [a-z0-9], collapse runs
    out: list[str] = []
    for ch in s.lower():
        out.append(ch if ("a" <= ch <= "z") or ("0" <= ch <= "9") else " ")
    return "".join(out).split()


def norm_name(first: str, last: str) -> tuple[str, str]:
    fn = "".join(_safe(first))
    ln = "".join(_safe(last))
    return fn, ln


def apply_pattern(first: str, last: str, key: str) -> str:
    """
    Legacy helper: normalize (first, last) and apply a named pattern.

    This always returns a string and does *not* indicate when the pattern is
    inappropriate for the given names (e.g. missing first/last). For new
    code (O26), prefer build_localpart(), which can return None.
    """
    fn, ln = norm_name(first, last)
    return PATTERNS[key](fn, ln)


def build_localpart(pattern: str, first: str, last: str) -> str | None:
    """
    Build a single local-part (left side of '@') from a pattern and names.

    Returns None if the pattern cannot be applied (e.g. missing first/last
    for a pattern that requires both).

    Uses the same normalization as apply_pattern(), but is *safe* in the
    presence of missing data.
    """
    if pattern not in PATTERNS:
        return None

    fn, ln = norm_name(first, last)

    # Patterns that require both first and last names.
    both_required = {
        "first.last",
        "f.last",
        "firstl",
        "flast",
        "first_last",
        "first-last",
        "firstlast",
        "lastfirst",
        "first.l",
        "first_l",
        "f_last",
    }

    if pattern in both_required:
        if not fn or not ln:
            return None
        return PATTERNS[pattern](fn, ln)

    # Single-name patterns.
    if pattern == "first":
        return fn or None
    if pattern == "last":
        return ln or None

    # Fallback for any future patterns (should not normally be hit).
    return PATTERNS[pattern](fn, ln)


def generate_localparts_for_person(
    first_name: str,
    last_name: str,
    preferred_pattern: str | None = None,
    *,
    max_permutations: int = DEFAULT_MAX_PERMUTATIONS_PER_PERSON,
) -> list[str]:
    """
    Generate a deduplicated, prioritized list of candidate local-parts for a
    person, optionally preferring a known company-level pattern.

    Parameters
    ----------
    first_name:
        Person's first name (raw or normalized).
    last_name:
        Person's last name (raw or normalized).
    preferred_pattern:
        A pattern key (see PATTERNS) to try first, typically sourced from
        companies.attrs["email_pattern"].
    max_permutations:
        Maximum number of local-parts to return (after dedupe).

    Returns
    -------
    list[str]
        Local-parts like ["banderson", "brett.anderson", "brett", ...].
    """
    if max_permutations <= 0:
        return []

    patterns: list[str] = list(PATTERN_PRIORITY)

    if preferred_pattern and preferred_pattern in PATTERNS:
        # Move preferred pattern to the front while preserving relative order.
        if preferred_pattern in patterns:
            patterns.remove(preferred_pattern)
        patterns.insert(0, preferred_pattern)

    seen: set[str] = set()
    result: list[str] = []

    for key in patterns:
        lp = build_localpart(key, first_name, last_name)
        if not lp:
            continue
        if lp in seen:
            continue
        seen.add(lp)
        result.append(lp)
        if len(result) >= max_permutations:
            break

    return result


def generate_candidate_emails_for_person(
    first_name: str,
    last_name: str,
    domain: str,
    company_pattern: str | None = None,
    *,
    max_permutations: int = DEFAULT_MAX_PERMUTATIONS_PER_PERSON,
) -> list[str]:
    """
    Generate full candidate email addresses for a person at a given domain.

    Parameters
    ----------
    first_name:
        Person's first name.
    last_name:
        Person's last name.
    domain:
        Email domain (e.g. "crestwellpartners.com").
    company_pattern:
        Optional stored pattern to prioritize (e.g. from companies.attrs).
    max_permutations:
        Maximum number of emails to return (after dedupe of local-parts).

    Returns
    -------
    list[str]
        Email addresses like ["banderson@example.com", "brett.anderson@example.com", ...].
    """
    domain = (domain or "").strip().lower()
    if not domain:
        return []

    locals_ = generate_localparts_for_person(
        first_name=first_name,
        last_name=last_name,
        preferred_pattern=company_pattern,
        max_permutations=max_permutations,
    )
    return [f"{lp}@{domain}" for lp in locals_]


@dataclass(frozen=True)
class Inference:
    pattern: str | None
    confidence: float
    samples: int


def infer_domain_pattern(examples: Iterable[tuple[str, str, str]]) -> Inference:
    """
    Infer the dominant pattern from a set of (first, last, email_localpart)
    examples for a domain.

    examples:
        iterable of (first, last, email_localpart)

    Returns
    -------
    Inference
        pattern:
            Best-fitting pattern key if it clearly dominates, else None.
        confidence:
            Fraction of (non-role) examples that matched the best pattern.
        samples:
            Number of examples considered after filtering role aliases.

    Rule:
        ≥ 2 hits AND ≥ 0.8 of non-role examples must match the same pattern.
    """
    # Filter out role aliases
    ex = [(fn, ln, lp) for fn, ln, lp in examples if lp not in ROLE_ALIASES]
    n = len(ex)
    if n < 2:
        return Inference(None, 0.0, n)

    scores: dict[str, int] = {k: 0 for k in PATTERNS}
    for first, last, lp in ex:
        for key in PATTERNS:
            candidate = build_localpart(key, first, last)
            if candidate == lp:
                scores[key] += 1

    # Tie-break by rank (prefer lower rank when hit counts are equal).
    best, hits = max(scores.items(), key=lambda kv: (kv[1], -_pattern_rank(kv[0])))
    conf = (hits / n) if n else 0.0
    if hits >= 2 and conf >= 0.80:
        return Inference(best, conf, n)
    return Inference(None, conf, n)


# ---------------------------------------------------------------------------
# Company-level helpers for companies.attrs["email_pattern"] (O26)
# ---------------------------------------------------------------------------


def _select_company_attrs_row(
    conn: sqlite3.Connection,
    company_id: int,
) -> sqlite3.Row | tuple | None:
    return conn.execute(
        "SELECT attrs FROM companies WHERE id = ?",
        (company_id,),
    ).fetchone()


def _extract_attrs_value(row: sqlite3.Row | tuple | None) -> str:
    if row is None:
        return ""
    if hasattr(row, "keys"):
        # sqlite3.Row
        return row["attrs"]  # type: ignore[index]
    # plain tuple
    return row[0]  # type: ignore[index]


def get_company_email_pattern(
    conn: sqlite3.Connection,
    company_id: int,
) -> str | None:
    """
    Read companies.attrs["email_pattern"] for a given company.

    Returns the pattern key if present and non-empty, otherwise None.
    """
    row = _select_company_attrs_row(conn, company_id)
    raw = _extract_attrs_value(row)
    if not raw:
        return None

    try:
        attrs = json.loads(raw)
    except json.JSONDecodeError:
        return None

    value = attrs.get("email_pattern")
    if isinstance(value, str) and value:
        return value
    return None


def _save_company_pattern(
    conn: sqlite3.Connection,
    company_id: int,
    pattern: str,
) -> None:
    """
    Persist companies.attrs["email_pattern"] = pattern and commit.
    """
    row = _select_company_attrs_row(conn, company_id)
    if row is None:
        raise ValueError(f"Company not found: id={company_id}")

    raw = _extract_attrs_value(row)
    if raw:
        try:
            attrs = json.loads(raw)
        except json.JSONDecodeError:
            attrs = {}
    else:
        attrs = {}

    attrs["email_pattern"] = pattern
    new_raw = json.dumps(attrs, separators=(",", ":"), sort_keys=True)

    conn.execute(
        "UPDATE companies SET attrs = ? WHERE id = ?",
        (new_raw, company_id),
    )
    conn.commit()


def set_company_email_pattern(
    conn: sqlite3.Connection,
    company_id: int,
    pattern: str,
) -> None:
    """
    Public helper to set companies.attrs["email_pattern"] directly.
    """
    _save_company_pattern(conn, company_id, pattern)


def infer_pattern_for_company(
    conn: sqlite3.Connection,
    company_id: int,
    *,
    force: bool = False,
) -> str | None:
    """
    Infer and persist the email pattern for a company based on the first
    available "valid" email for that company's people.

    Behavior
    --------
    - If force is False and a pattern is already present in
      companies.attrs["email_pattern"], that pattern is returned immediately.
    - Otherwise this function:
        1. Looks up the earliest verification_results row with
           vr.verify_status = 'valid' for any email belonging to people
           at this company.
        2. Extracts (localpart, first_name, last_name).
        3. Tries patterns in PATTERN_PRIORITY order via build_localpart().
        4. When a pattern reproduces the observed localpart exactly, it is
           written to companies.attrs["email_pattern"] and returned.
    - If no matching pattern is found or no valid emails exist yet, the
      function returns None and does not modify the DB.

    Parameters
    ----------
    conn:
        Open sqlite3 connection.
    company_id:
        Target company id.
    force:
        When True, ignores any existing stored pattern and recomputes.

    Returns
    -------
    str | None
        The inferred pattern key, or None if no pattern could be inferred.
    """
    if not force:
        existing = get_company_email_pattern(conn, company_id)
        if existing:
            return existing

    row = conn.execute(
        """
        SELECT
          e.email,
          p.first_name,
          p.last_name
        FROM emails AS e
        JOIN people AS p
          ON p.id = e.person_id
        JOIN verification_results AS vr
          ON vr.email_id = e.id
        WHERE p.company_id = ?
          AND vr.verify_status = 'valid'
        ORDER BY vr.id
        LIMIT 1
        """,
        (company_id,),
    ).fetchone()

    if row is None:
        return None

    if hasattr(row, "keys"):
        email = row["email"]  # type: ignore[index]
        first_name = row["first_name"]  # type: ignore[index]
        last_name = row["last_name"]  # type: ignore[index]
    else:
        email = row[0]  # type: ignore[index]
        first_name = row[1]  # type: ignore[index]
        last_name = row[2]  # type: ignore[index]

    if not isinstance(email, str):
        return None

    localpart = email.split("@", 1)[0].lower()

    for key in PATTERN_PRIORITY:
        candidate = build_localpart(key, first_name or "", last_name or "")
        if candidate and candidate.lower() == localpart:
            _save_company_pattern(conn, company_id, key)
            return key

    # No pattern matched the existing valid email.
    return None
