# src/verify/candidates.py
"""
Probe planning: which addresses to try for a person, and in what order.

The old behaviour was one blind guess per person -- 2,985 of the 3,070 people
who have an address have exactly one, so a miss lost that person permanently.
This module turns that single guess into a short, evidence-ranked list that the
sequential verifier walks until it gets a `valid`.

Ranking (see ``rank_patterns``):

  1. Patterns this company has CONFIRMED at least twice (``trusted``), best
     evidence first.
  2. An operator override from ``companies.attrs["email_pattern"]``, if set.
  3. Patterns confirmed exactly once (``candidate``).
  4. Patterns implied by the SHAPE of the company's published addresses
     (``inferred_count``), which need no matching person record.
  5. The global base rate, ADJUSTED FOR COMPANY SIZE.
  6. Patterns with exactly one recorded failure and nothing confirming them.

Patterns this company has failed ``REFUTED_MIN_FAILURES`` times or more are
skipped entirely -- that is the whole point of the evidence store, because the
invalid address rows themselves are deleted by CLEANUP_INVALID_GENERATED.

Company size matters more than anything else in the base rate. Published
distributions put ``first.last`` at 38% for 1-10 employees but 74% at 10,001+,
while ``first@`` is ~17% at the small end and ~0% at the large end. Crestwell
prospects seed-to-Series A companies -- the 2025 median Series A raiser had
~14 employees -- so ``SMALL_COMPANY_ORDER`` is the DEFAULT, not a special case.
It is also what this system's own numbers say: flast 95%, first.last 90%,
first 89%, firstlast 65%, firstl 25% on decidable results.

``first_last`` is never generated: 116 attempts, 0 valid addresses.

Everything above ``company_pattern_evidence`` is pure -- no I/O, no DB -- so the
ranking can be exercised without a database.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from src.generate.patterns import (
    RENDERABLE_PATTERNS,
    STATUS_CANDIDATE,
    STATUS_REFUTED,
    STATUS_TRUSTED,
    get_domain_patterns,
    pattern_confidence,
    render_pattern,
    render_pattern_variants,
)

log = logging.getLogger(__name__)

#: Patterns that are never generated regardless of evidence.
#: first_last: 116 attempts in production, 18 of them decidable, 0 valid.
NEVER_GENERATE: frozenset[str] = frozenset({"first_last"})

#: Base-rate order for small companies -- the DEFAULT, and Crestwell's whole ICP.
#: Ordered by the FREQUENCY measured on 963 real published addresses -- flast
#: 68.0%, first.last 17.2%, first 10.2%, firstlast 3.9%, last.first 0.8% -- not
#: by per-try yield, which only says how often a format that IS in use answers.
#: [flast, first.last, first, firstlast] solves 99.2% of decidable domains and
#: is the cheapest arrangement of them: expected 1.49 probes vs 1.56 with
#: `first` in second place, and 1.84 if first.last leads.
SMALL_COMPANY_ORDER: tuple[str, ...] = (
    "flast",
    "first.last",
    "first",
    "firstlast",
    "f.last",
    "firstl",
)

#: Base-rate order once a company is big enough for a directory convention.
#: first.last dominates at enterprise scale and bare first@ collapses to ~0%.
LARGE_COMPANY_ORDER: tuple[str, ...] = (
    "first.last",
    "flast",
    "firstlast",
    "f.last",
    "firstl",
    "first",
)

#: Employee count at or below which SMALL_COMPANY_ORDER applies. Well above the
#: ~14-person Series A median so that a stale or inflated headcount does not
#: flip a startup into the enterprise ordering.
SMALL_COMPANY_MAX_EMPLOYEES = 200

#: Confidence grades persisted for catch-all addresses.
GRADE_HIGH = "high"
GRADE_MEDIUM = "medium"
GRADE_LOW = "low"

_GRADE_BY_STATUS = {
    STATUS_TRUSTED: GRADE_HIGH,
    STATUS_CANDIDATE: GRADE_MEDIUM,
}

#: Independent failures needed before a pattern is dropped from the plan.
#: One miss is not proof -- a stale title, a departed person or a middle name
#: in the first_name field all produce a hard bounce on a format the company
#: really does use. Laplace puts 0-of-1 at 0.25 confidence, not 0, so a
#: once-failed pattern is demoted to the tail of the plan instead of removed;
#: at two independent failures it is gone.
REFUTED_MIN_FAILURES = 2

#: Default SMTP probe budget per person. Kept deliberately small: walking many
#: addresses at one domain is the directory-harvest signature that gets a
#: probing IP throttled or blocked.
DEFAULT_MAX_PROBES_PER_PERSON = 3

#: Company attrs keys that may carry a headcount.
_EMPLOYEE_COUNT_KEYS = (
    "employee_count",
    "employees",
    "headcount",
    "employee_range_min",
    "size",
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def max_probes_per_person() -> int:
    """SMTP probe budget per person, from MAX_PROBES_PER_PERSON."""
    try:
        return max(0, int(os.getenv("MAX_PROBES_PER_PERSON", str(DEFAULT_MAX_PROBES_PER_PERSON))))
    except (TypeError, ValueError):
        return DEFAULT_MAX_PROBES_PER_PERSON


# ---------------------------------------------------------------------------
# Ranking (pure)
# ---------------------------------------------------------------------------


def base_rate_order(employee_count: int | None = None) -> tuple[str, ...]:
    """
    Global fallback order for a company with no pattern evidence.

    Defaults to SMALL_COMPANY_ORDER whenever the size is unknown, because an
    unknown prospect here is overwhelmingly likely to be a small one.
    """
    if employee_count is not None and employee_count > SMALL_COMPANY_MAX_EMPLOYEES:
        return LARGE_COMPANY_ORDER
    return SMALL_COMPANY_ORDER


def rank_patterns(
    evidence: list[dict[str, Any]] | None,
    *,
    employee_count: int | None = None,
    override: str | None = None,
) -> list[str]:
    """
    Ordered, deduped pattern names to try for one person.

    ``evidence`` is ``get_domain_patterns()`` output (already ranked by
    confirmed_count desc). ``override`` is an operator-set pattern, which
    outranks statistical guesses but not confirmed evidence.
    """
    rows = list(evidence or [])
    failures = {
        r["pattern"]: int(r.get("failed_count") or 0)
        for r in rows
        if r.get("status") == STATUS_REFUTED
    }
    refuted = {p for p, n in failures.items() if n >= REFUTED_MIN_FAILURES}
    demoted = {p for p in failures if p not in refuted}

    out: list[str] = []

    def add(pattern: str | None, *, allow_demoted: bool = False) -> None:
        if not pattern or pattern in out:
            return
        if pattern in NEVER_GENERATE or pattern in refuted:
            return
        if pattern in demoted and not allow_demoted:
            return
        if pattern not in RENDERABLE_PATTERNS:
            return
        out.append(pattern)

    for row in rows:
        if row.get("status") == STATUS_TRUSTED:
            add(row.get("pattern"))
    add(override)
    for row in rows:
        if row.get("status") == STATUS_CANDIDATE:
            add(row.get("pattern"))
    # Shape-inferred from the company's own published addresses: weaker than a
    # verified hit, stronger than the global base rate.
    for row in rows:
        if int(row.get("inferred_count") or 0) > 0:
            add(row.get("pattern"))
    for pattern in base_rate_order(employee_count):
        add(pattern)
    # Second pass appends only the once-failed patterns, at the tail.
    for pattern in base_rate_order(employee_count):
        add(pattern, allow_demoted=True)

    return out


def rank_candidates(
    evidence: list[dict[str, Any]] | None,
    first_name: str,
    last_name: str,
    domain: str,
    *,
    employee_count: int | None = None,
    override: str | None = None,
    limit: int | None = None,
) -> list[tuple[str, str]]:
    """
    Ordered ``(email, pattern)`` candidates for one person at one domain.

    Canonical renderings come first, one per ranked pattern, so a small probe
    budget is spent distinguishing FORMATS rather than re-spelling one surname.
    Alternative spellings of compound names ("garcialopez" vs "lopez") follow
    in the same pattern order and are only reached with a larger budget.
    """
    patterns = rank_patterns(evidence, employee_count=employee_count, override=override)

    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(email: str | None, pattern: str) -> None:
        if not email or email in seen:
            return
        seen.add(email)
        out.append((email, pattern))

    for pattern in patterns:
        add(render_pattern(pattern, first_name, last_name, domain), pattern)
    for pattern in patterns:
        for email in render_pattern_variants(pattern, first_name, last_name, domain):
            add(email, pattern)

    if limit is not None and limit >= 0:
        return out[:limit]
    return out


def has_trusted_pattern(evidence: list[dict[str, Any]] | None) -> bool:
    """True if the company has at least one pattern confirmed twice or more."""
    return any(
        row.get("status") == STATUS_TRUSTED and row.get("pattern") not in NEVER_GENERATE
        for row in (evidence or [])
    )


def catchall_confidence(
    evidence: list[dict[str, Any]] | None,
    pattern: str | None,
) -> tuple[float, str]:
    """
    Grade a guessed address on a catch-all domain.

    A true catch-all is undecidable at the SMTP layer -- every local-part gets
    a 250 -- so there is no verdict to be had. What CAN be graded is how well
    the company's own evidence supports the format we guessed:

        trusted pattern (>=2 confirmed)  -> high
        candidate pattern (1 confirmed)  -> medium
        no evidence, or refuted          -> low

    The float is the Laplace-smoothed rate from ``pattern_confidence`` (0.5 with
    no evidence at all). Sort on the GRADE; use the float to break ties within
    a grade.
    """
    for row in evidence or []:
        if row.get("pattern") == pattern:
            confidence = float(row.get("confidence") or 0.0)
            return confidence, _GRADE_BY_STATUS.get(str(row.get("status")), GRADE_LOW)
    return pattern_confidence(0, 0)[0], GRADE_LOW


# ---------------------------------------------------------------------------
# Database reads (best-effort; never raise into the probe loop)
# ---------------------------------------------------------------------------


def _rollback(conn: Any) -> None:
    """
    Clear an aborted transaction after a swallowed statement failure.

    Postgres aborts the WHOLE transaction on any failed statement: every later
    statement raises InFailedSqlTransaction until rollback(). Reading evidence
    from an unmigrated pattern table would otherwise silently discard every
    probe verdict the caller writes afterwards on the same connection.
    """
    try:
        conn.rollback()
    except Exception:
        pass


def company_pattern_evidence(conn: Any, company_id: int | None) -> list[dict[str, Any]]:
    """
    ``get_domain_patterns()`` for a company, or [] if it is unavailable.

    Not filtered by mail domain: company_id is the key, and rows written before
    a domain was known carry NULL there.

    Swallows errors on purpose -- the evidence store is Postgres-only, and a
    legacy SQLite dev database must degrade to the base-rate order rather than
    kill the generation job.
    """
    if not company_id:
        return []
    try:
        return get_domain_patterns(conn, int(company_id))
    except Exception:
        _rollback(conn)
        log.debug(
            "pattern evidence unavailable",
            exc_info=True,
            extra={"company_id": company_id},
        )
        return []


def company_employee_count(conn: Any, company_id: int | None) -> int | None:
    """
    Headcount for a company from ``companies.attrs``, or None.

    None is the normal answer -- nothing populates a headcount today. It means
    "use the small-company order", which is the right default for this ICP.
    """
    if not company_id:
        return None
    try:
        row = conn.execute(
            "SELECT attrs FROM companies WHERE id = ?", (int(company_id),)
        ).fetchone()
    except Exception:
        _rollback(conn)
        return None
    if not row or not row[0]:
        return None

    attrs = row[0]
    if isinstance(attrs, (str, bytes)):
        try:
            attrs = json.loads(attrs)
        except Exception:
            return None
    if not isinstance(attrs, dict):
        return None

    for key in _EMPLOYEE_COUNT_KEYS:
        value = attrs.get(key)
        if value is None:
            continue
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        if count > 0:
            return count
    return None


__all__ = [
    "DEFAULT_MAX_PROBES_PER_PERSON",
    "GRADE_HIGH",
    "GRADE_LOW",
    "GRADE_MEDIUM",
    "LARGE_COMPANY_ORDER",
    "NEVER_GENERATE",
    "REFUTED_MIN_FAILURES",
    "SMALL_COMPANY_MAX_EMPLOYEES",
    "SMALL_COMPANY_ORDER",
    "base_rate_order",
    "catchall_confidence",
    "company_employee_count",
    "company_pattern_evidence",
    "has_trusted_pattern",
    "max_probes_per_person",
    "rank_candidates",
    "rank_patterns",
]
