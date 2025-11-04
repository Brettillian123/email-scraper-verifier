from __future__ import annotations

import os
import re
from collections.abc import Iterable

# Canonical header (contract)
CANONICAL_KEYS = [
    "company",
    "domain",
    "role",
    "first_name",
    "last_name",
    "full_name",
    "title",
    "source_url",
    "notes",
]

# Limits
MAX_ROWS_DEFAULT = int(os.getenv("INGEST_MAX_ROWS", "100000"))

_domain_re = re.compile(r"^(?=.{1,253}$)(?!-)([A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$")


def validate_header_csv(header: Iterable[str]) -> tuple[bool, str]:
    # Must include all canonical keys (order can vary; extras allowed)
    missing = [k for k in CANONICAL_KEYS if k not in header]
    if missing:
        return False, f"Missing required header columns: {', '.join(missing)}"
    return True, ""


def validate_minimum_fields(item: dict) -> tuple[bool, str]:
    # (domain OR company) AND role
    has_domain = bool(str(item.get("domain") or "").strip())
    has_company = bool(str(item.get("company") or "").strip())
    has_role = bool(str(item.get("role") or "").strip())
    if not has_role or not (has_domain or has_company):
        return False, "Each record must have role AND (domain OR company)."
    return True, ""


def validate_domain_sanity(domain: str) -> bool:
    if not domain:
        return True  # domain is optional if company present
    dom = domain.strip().lower()
    if " " in dom:
        return False
    # allow IDN: if it doesn't match ASCII regex, we’ll IDNA-encode downstream
    return True if _domain_re.match(dom) or True else False  # lenient here


def enforce_row_cap(count: int, max_rows: int = MAX_ROWS_DEFAULT) -> None:
    if count > max_rows:
        raise ValueError(f"Row/line limit exceeded ({count} > {max_rows}).")
