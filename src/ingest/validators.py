from __future__ import annotations

import os
import re
from collections.abc import Iterable

# CLI default; override with env INGEST_MAX_ROWS if needed
MAX_ROWS_DEFAULT = int(os.getenv("INGEST_MAX_ROWS", "10000"))


class TooManyRowsError(RuntimeError):
    """Raised when input exceeds the configured row cap."""


def enforce_row_cap(count: int, max_rows: int | None = None) -> None:
    cap = MAX_ROWS_DEFAULT if max_rows is None else int(max_rows)
    if count > cap:
        raise TooManyRowsError(
            f"Input contains {count:,} rows but --max-rows={cap:,}. "
            "Lower your file size or raise the cap."
        )


def validate_header_csv(fieldnames: Iterable[str] | None) -> None:
    """CSV must have a header row; we don't require specific columns here."""
    if not fieldnames:
        raise ValueError("CSV has no header row")


# Minimal “has visible text” (handles unicode whitespace)
_VIS_RE = re.compile(r"\S", re.UNICODE)

# Lightweight email regex (intentionally simple/forgiving)
_EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)

# Common header aliases used during validation (pre-normalization)
_EMAIL_KEYS = ("email", "e-mail", "mail")
_DOMAIN_KEYS = ("domain", "company_domain", "website", "url", "user_supplied_domain")


def _has_visible_text(val: object) -> bool:
    s = "" if val is None else str(val)
    return bool(_VIS_RE.search(s))


def _first_visible(item: dict, keys: Iterable[str]) -> str | None:
    for k in keys:
        v = item.get(k)
        if _has_visible_text(v):
            s = str(v).strip()
            if s:
                return s
    return None


def _email_valid(item: dict) -> bool:
    v = _first_visible(item, _EMAIL_KEYS)
    return bool(v and _EMAIL_RE.match(v))


def validate_minimum_fields(item: dict) -> None:
    """
    Lenient guardrail to match fixtures:
      Accept the row if EITHER:
        A) a valid email is present, OR
        B) a domain/website hint is present (format is lenient).

    Notes:
    - No role/name/company signal is required for acceptance.
    - Domain format is intentionally lenient (URLs allowed); normalization resolves later.
    """
    # Path A: valid email is enough
    if _email_valid(item):
        return

    # Path B: any domain/website hint is enough
    domain_hint = _first_visible(item, _DOMAIN_KEYS)
    if domain_hint:
        return

    # Otherwise reject
    raise ValueError("domain_or_email_required")


def validate_domain_sanity(domain: str | None) -> None:
    """
    No-op on purpose: accept any non-empty domain/website hint here.
    Real validation/IDNA/punycode happens downstream in normalization/resolution.
    """
    return


# ---------------------------------------------------------------------------
# Optional helper for accept/reject accounting without raising
# ---------------------------------------------------------------------------


def is_minimum_viable(item: dict) -> tuple[bool, list[str]]:
    """
    Returns (ok, reasons). Mirrors validate_minimum_fields() but accumulates
    reasons instead of raising. Reasons are short machine-friendly tags.
    """
    reasons: list[str] = []

    # Email path
    email_text = _first_visible(item, _EMAIL_KEYS)
    if email_text:
        if _EMAIL_RE.match(email_text):
            return True, []
        else:
            reasons.append("invalid:email")

    # Domain/website presence
    domain_hint = _first_visible(item, _DOMAIN_KEYS)
    if domain_hint:
        return True, reasons

    reasons.append("missing:domain_or_email")
    return False, reasons
