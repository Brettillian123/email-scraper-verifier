from __future__ import annotations

from typing import Final

#: Canonical set of generic role / distribution localparts.
#:
#: These are matched after:
#:   - lowercasing
#:   - removing any "+tag" suffix
#:   - removing all dots from the localpart
ROLE_LOCALPARTS: Final[set[str]] = {
    "info",
    "sales",
    "support",
    "help",
    "hello",
    "contact",
    "billing",
    "customerservice",
    "customersuccess",
    "careers",
    "jobs",
    "hr",
    "admin",
    "office",
    "team",
    "marketing",
    "press",
    "media",
}


def _normalize_localpart(local: str) -> str:
    """
    Normalize the localpart into a base key used for ROLE_LOCALPARTS lookup.

    - Lowercase
    - Strip whitespace
    - Drop any "+tag" suffix
    - Remove dots (so "customer.service" → "customerservice")
    """
    base = local.strip().lower()
    if not base:
        return ""

    # Strip plus-tagging if present.
    base = base.split("+", 1)[0]

    # Remove dots to catch variants like "customer.service".
    base = base.replace(".", "")
    return base


def is_role_address(email: str) -> bool:
    """
    Return True if email looks like a generic role/distribution address.

    This is intentionally conservative and only returns True for a fixed
    list of known role localparts after simple normalization. It does
    *not* attempt to classify arbitrary strings by semantics.
    """
    local, _, _ = email.strip().lower().partition("@")
    if not local:
        return False

    base_local = _normalize_localpart(local)
    if not base_local:
        return False

    return base_local in ROLE_LOCALPARTS
