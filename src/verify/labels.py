# src/verify/labels.py
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypedDict

"""
O26 — Verification labels and primary/alternate selection.

This module provides a second dimension on top of the coarse verify_status
(valid / invalid / risky_catch_all / unknown_timeout):

  - Distinguish *how* we reached "valid":
        * valid_native
        * valid_catchall_tested  (O26 test-send upgrade)

  - Distinguish *which* valid is the canonical address for a person:
        * valid_native_primary
        * valid_native_alternate
        * valid_catchall_tested_primary
        * valid_catchall_tested_alternate

The intent is:

  - Keep verify_status exactly as-is for compatibility with R18, export,
    search filters, etc.

  - Add a lightweight, derived verify_label that callers can show in UI,
    expose via APIs, or optionally persist in denormalized tables
    (e.g. lead_search_docs) if needed.

Typical usage patterns:

  1) To derive a label for a single row, when you already know whether
     it's primary for the person:

         label = compute_verify_label_from_row(row, is_primary=True)

  2) To choose a primary/alternate split for one person with multiple
     valid addresses:

         primary_idx = choose_primary_index(candidates_for_person)
         for idx, row in enumerate(candidates_for_person):
             is_primary = idx == primary_idx
             label = compute_verify_label_from_row(row, is_primary=is_primary)

This module is intentionally free of any DB access; callers are expected
to supply already-joined rows (e.g. from v_emails_latest + people) as
simple Mapping[str, Any] objects or dicts.
"""

# We avoid importing src.verify.status at runtime to keep this module
# decoupled; callers can pass any string-like verify_status.
VerifyStatusStr = str


# Canonical label codes we emit.
class VerifyLabel:
    VALID_NATIVE = "valid_native"
    VALID_NATIVE_PRIMARY = "valid_native_primary"
    VALID_NATIVE_ALTERNATE = "valid_native_alternate"

    VALID_CATCHALL_TESTED = "valid_catchall_tested"
    VALID_CATCHALL_TESTED_PRIMARY = "valid_catchall_tested_primary"
    VALID_CATCHALL_TESTED_ALTERNATE = "valid_catchall_tested_alternate"

    INVALID = "invalid"
    RISKY_CATCH_ALL = "risky_catch_all"
    UNKNOWN_TIMEOUT = "unknown_timeout"
    UNKNOWN = "unknown"


TEST_SEND_UPGRADE_REASONS: set[str] = {"no_bounce_after_test_send"}


class LabelRow(TypedDict, total=False):
    """
    Minimal structural type for rows passed into label helpers.

    This matches the shape you'd get from v_emails_latest joined to people,
    but is intentionally loose so you can pass plain dicts or sqlite3.Row.
    """

    # Verification / O26 fields
    verify_status: VerifyStatusStr
    verify_reason: str | None
    test_send_status: str | None
    bounce_code: str | None

    # Email / provenance
    email: str
    source: str | None  # e.g. "extracted", "generated", "seed"
    verified_at: str | None  # ISO timestamp as string (for tie-breaks)

    # Person info (for human-looking localpart heuristics)
    first_name: str | None
    last_name: str | None


@dataclass(frozen=True)
class PrimarySelectionConfig:
    """
    Tunable knobs for choosing a primary email among multiple valids.

    You can override these from tests if you want to explore alternate
    heuristics without changing the core algorithm.
    """

    # Preferred email.source values in ascending priority order
    source_preference: tuple[str, ...] = ("extracted", "seed", "generated")

    # Role-like localparts that should almost never be the primary
    role_localparts: tuple[str, ...] = (
        "info",
        "contact",
        "support",
        "sales",
        "hello",
        "team",
        "office",
    )


DEFAULT_PRIMARY_CONFIG = PrimarySelectionConfig()


def is_test_send_upgrade(row: Mapping[str, Any]) -> bool:
    """
    Return True if a valid row looks like it was upgraded by O26 test-send.

    Current heuristic:
      - verify_status == "valid"
      - verify_reason is one of TEST_SEND_UPGRADE_REASONS

    We *could* also consider test_send_status / bounce_code here, but
    verify_reason is the clearest single flag and is already present in
    v_emails_latest.
    """
    status = str(row.get("verify_status") or "").lower()
    reason = (row.get("verify_reason") or "").lower()
    return status == "valid" and reason in TEST_SEND_UPGRADE_REASONS


def classify_base_label(
    verify_status: VerifyStatusStr,
    verify_reason: str | None = None,
) -> str:
    """
    Map coarse verify_status (+ optional reason) to a base label.

    For valid addresses we distinguish:

      * valid_native
      * valid_catchall_tested  (O26 upgrade)

    For all other statuses, we mostly return the status itself, falling
    back to "unknown" if we don't recognise it.
    """
    status = (verify_status or "").lower()
    reason_norm = (verify_reason or "").lower()

    if status == "valid":
        if reason_norm in TEST_SEND_UPGRADE_REASONS:
            return VerifyLabel.VALID_CATCHALL_TESTED
        return VerifyLabel.VALID_NATIVE

    if status == "invalid":
        return VerifyLabel.INVALID
    if status == "risky_catch_all":
        return VerifyLabel.RISKY_CATCH_ALL
    if status == "unknown_timeout":
        return VerifyLabel.UNKNOWN_TIMEOUT

    # Future-proofing: if R18 grows more statuses, they can still be
    # surfaced as a generic "unknown" label instead of breaking callers.
    return VerifyLabel.UNKNOWN


def compute_verify_label(
    verify_status: VerifyStatusStr,
    verify_reason: str | None = None,
    *,
    is_primary: bool | None = None,
    is_test_send_upgraded: bool | None = None,
) -> str:
    """
    Compute a full verify_label code from status/reason + primary flag.

    Parameters
    ----------
    verify_status:
        The coarse R18 status ("valid", "invalid", "risky_catch_all",
        "unknown_timeout", ...).
    verify_reason:
        The R18 reason string, used here only to detect O26 upgrades
        (no_bounce_after_test_send).
    is_primary:
        Whether this address is considered the canonical primary address
        for the person. If None, the label will not encode primary vs
        alternate; you'll just get the base label.
    is_test_send_upgraded:
        Optional explicit flag for "O26-upgraded via test-send". If you
        pass None (the default), we infer it from verify_reason.

    Returns
    -------
    A string from the VerifyLabel.* set.
    """
    # For non-valid statuses, we ignore is_primary and is_test_send_upgraded.
    if (verify_status or "").lower() != "valid":
        return classify_base_label(verify_status, verify_reason)

    # Determine whether this is a catch-all-tested upgrade.
    if is_test_send_upgraded is None:
        is_test_send_upgraded = (verify_reason or "").lower() in TEST_SEND_UPGRADE_REASONS

    if is_test_send_upgraded:
        base = VerifyLabel.VALID_CATCHALL_TESTED
        if is_primary is True:
            return VerifyLabel.VALID_CATCHALL_TESTED_PRIMARY
        if is_primary is False:
            return VerifyLabel.VALID_CATCHALL_TESTED_ALTERNATE
        return base

    # Native valids
    base = VerifyLabel.VALID_NATIVE
    if is_primary is True:
        return VerifyLabel.VALID_NATIVE_PRIMARY
    if is_primary is False:
        return VerifyLabel.VALID_NATIVE_ALTERNATE
    return base


def compute_verify_label_from_row(
    row: Mapping[str, Any],
    *,
    is_primary: bool | None = None,
) -> str:
    """
    Convenience wrapper around compute_verify_label() for DB rows.

    Expects at minimum verify_status and verify_reason columns; for
    valid rows, will also use verify_reason to infer whether this is an
    O26 test-send upgrade.
    """
    status = str(row.get("verify_status") or "")
    reason = row.get("verify_reason")
    upgraded = is_test_send_upgrade(row)
    return compute_verify_label(
        status,
        reason,
        is_primary=is_primary,
        is_test_send_upgraded=upgraded,
    )


def _normalize_name_part(value: str | None) -> str:
    if not value:
        return ""
    # Strip non-letters and lowercase, to improve fuzzy matching.
    return re.sub(r"[^a-z]", "", value.lower())


def _localpart(email: str) -> str:
    return email.split("@", 1)[0].lower()


def _source_rank(source: str | None, order: Iterable[str]) -> int:
    # Lower rank is better.
    if not source:
        return len(tuple(order)) + 1
    src = source.lower()
    for idx, candidate in enumerate(order):
        if src == candidate:
            return idx
    return len(tuple(order))


def _role_localpart_penalty(localpart: str, role_localparts: Iterable[str]) -> int:
    base = 0
    if localpart in role_localparts:
        base += 40
    for role in role_localparts:
        if localpart.startswith(role):
            base += 20
            break
    return base


def _human_pattern_score(
    email: str,
    first_name: str | None,
    last_name: str | None,
    role_localparts: Iterable[str],
) -> int:
    """
    Heuristic numeric score for "how human-looking" an email localpart is.

    Lower scores are *better* (more likely to be the primary).

    Rough rules:
      - Penalise role-like addresses (info@, sales@, etc.).
      - Prefer localparts that match common name-based patterns.
      - Slightly penalise digits and very short localparts.
    """
    local = _localpart(email)
    score = 0

    score += _role_localpart_penalty(local, role_localparts)

    # Penalise obviously short/opaque addresses a bit
    if len(local) <= 3:
        score += 5

    # Penalise digits (tracking/aliases)
    if any(ch.isdigit() for ch in local):
        score += 5

    first_norm = _normalize_name_part(first_name)
    last_norm = _normalize_name_part(last_name)

    # Strong preference for exact common patterns when we know the name.
    common_patterns: set[str] = set()
    if first_norm and last_norm:
        common_patterns.update(
            {
                f"{first_norm}.{last_norm}",
                f"{first_norm}_{last_norm}",
                f"{first_norm}{last_norm}",
                f"{first_norm[0]}{last_norm}",
                f"{first_norm[0]}.{last_norm}",
                f"{first_norm[0]}_{last_norm}",
            }
        )

    if common_patterns and local in common_patterns:
        score -= 15
    else:
        # Mild preference if it at least contains the last name / first name.
        if last_norm and last_norm in local:
            score -= 5
        if first_norm and first_norm in local:
            score -= 3

    return score


def choose_primary_index(
    rows: Sequence[Mapping[str, Any]],
    config: PrimarySelectionConfig = DEFAULT_PRIMARY_CONFIG,
) -> int | None:
    """
    Given all email rows for a single person, pick the index of the primary.

    Only rows with verify_status == "valid" are considered; if there are
    no valid rows, returns None.

    Priority order:

      1) Email.source — prefer extracted > seed > generated.
      2) Base validity tier — prefer valid_native over valid_catchall_tested.
      3) "Human-looking" localpart — prefer name-like patterns over role-like.
      4) verified_at timestamp (earlier wins) if present.
      5) Original index as final stable tie-breaker.

    Inputs
    ------
    rows:
        Sequence of Mapping objects (sqlite3.Row, dict, etc.) representing
        all email candidates for a single person.
    config:
        Optional PrimarySelectionConfig to tweak source preferences and
        role-like localparts.

    Returns
    -------
    int | None:
        The index into `rows` that should be treated as the primary valid
        address, or None if there are no valid rows.
    """
    candidates: list[tuple[tuple[Any, ...], int]] = []

    for idx, row in enumerate(rows):
        status = str(row.get("verify_status") or "").lower()
        if status != "valid":
            continue

        email = str(row.get("email") or "")
        source = row.get("source")
        first_name = row.get("first_name")
        last_name = row.get("last_name")
        verify_reason = row.get("verify_reason")

        base_label = classify_base_label(status, verify_reason)

        # valid_native (0) vs valid_catchall_tested (1)
        if base_label == VerifyLabel.VALID_NATIVE:
            tier_rank = 0
        elif base_label == VerifyLabel.VALID_CATCHALL_TESTED:
            tier_rank = 1
        else:
            # Shouldn't happen for status == "valid", but keep it stable.
            tier_rank = 2

        src_rank = _source_rank(source, config.source_preference)
        pattern_score = _human_pattern_score(
            email,
            first_name,
            last_name,
            config.role_localparts,
        )

        # verified_at is stored as an ISO string; lexical order is fine for
        # chronological comparison. Missing timestamps get a neutral rank.
        verified_at = row.get("verified_at") or ""

        sort_key = (
            src_rank,
            tier_rank,
            pattern_score,
            verified_at,
            idx,
        )
        candidates.append((sort_key, idx))

    if not candidates:
        return None

    candidates.sort(key=lambda pair: pair[0])
    return candidates[0][1]
