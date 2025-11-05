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
        # Keep the message helpful and actionable
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
_ROLE_PLACEHOLDERS = {"-", "—", "--", "na", "n/a", "none", "null"}


def _has_visible_text(val: object) -> bool:
    s = "" if val is None else str(val)
    return bool(_VIS_RE.search(s))


def validate_minimum_fields(item: dict) -> None:
    """
    Guardrail: row must have ROLE (visible, not a placeholder)
    AND at least one of (company, domain).
    """
    company = (item.get("company") or "").strip()
    domain = (item.get("domain") or "").strip()
    role_raw = item.get("role")

    if not (company or domain):
        raise ValueError("row must have at least company or domain")

    if not _has_visible_text(role_raw):
        raise ValueError("role is required")

    role_clean = str(role_raw).strip().lower()
    if role_clean in _ROLE_PLACEHOLDERS:
        raise ValueError("role cannot be a placeholder")


def validate_domain_sanity(domain: str | None) -> None:
    """
    Lenient check: allow blank; if present, reject obvious junk like spaces/slashes.
    A proper resolver/normalizer can IDNA/punycode later.
    """
    if not domain:
        return
    dom = str(domain).strip()
    if not dom:
        return
    if " " in dom or "/" in dom:
        raise ValueError("domain looks invalid")
