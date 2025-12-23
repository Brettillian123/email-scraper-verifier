from __future__ import annotations

import re

"""
Email address classification helpers.

This module centralizes simple heuristics for identifying
generic / role / placeholder email addresses such as:

  - info@company.com
  - support@company.com
  - hello@company.com
  - example@brandtcpa.com
  - noreply@company.com
  - support+foo@company.com

These are treated as *non-personal* addresses and should not:

  - Prevent permutation generation for real people.
  - Be attached to a specific person record (unless you have
    very strong evidence they are in fact personal).
  - Be counted as "this person already has an email" when
    deciding whether to generate permutations.

They can still be stored as company-level emails (person_id = NULL)
and surfaced in search/export as "found but not linked to a person".
"""

# Localparts that are almost never a real, individual person.
# This list is intentionally conservative and can be extended over time.
_ROLE_LOCALPARTS: set[str] = {
    "info",
    "hello",
    "support",
    "help",
    "sales",
    "contact",
    "team",
    "office",
    "careers",
    "jobs",
    "hr",
    "billing",
    "accounts",
    "privacy",
    "legal",
    "security",
    "admin",
    "webmaster",
    "newsletter",
    "press",
    "media",
    "example",
}

# Common "no reply" prefixes that often appear as:
#   noreply@, noreply+foo@, no-reply@
_ROLE_PREFIXES: tuple[str, ...] = (
    "no-reply",
    "noreply",
)

_PLUS_ALIAS_RE = re.compile(r"^([^+]+)\+.+$")


def is_role_or_placeholder_email(addr: str) -> bool:
    """
    Return True for generic / role / placeholder email addresses.

    Examples that should return True:
      - info@company.com
      - support@company.com
      - hello@company.com
      - example@brandtcpa.com
      - noreply@company.com
      - support+foo@company.com
      - info+newsletter@company.com

    The intent is to capture addresses that are clearly not tied to a
    single named person and should therefore:

      - Not block permutation generation for people at the same company.
      - Not be auto-attached to a specific person record.
    """
    if "@" not in addr:
        return False

    local, _domain = addr.split("@", 1)
    lp = local.lower()

    # Exact matches: info@, support@, example@, etc.
    if lp in _ROLE_LOCALPARTS:
        return True

    # Obvious prefixes like noreply@ / no-reply@
    if any(lp.startswith(pref) for pref in _ROLE_PREFIXES):
        return True

    # Generic catch-alls like "support+foo", "info+bar", "sales+xyz".
    # If the part before '+' is a known role local-part, treat as role.
    m = _PLUS_ALIAS_RE.match(lp)
    if not m:
        return False

    base = m.group(1)
    return base in _ROLE_LOCALPARTS or any(base.startswith(pref) for pref in _ROLE_PREFIXES)


__all__ = ["is_role_or_placeholder_email"]
