from __future__ import annotations

import json


def _trim(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    return s if s else None


def norm_domain(domain: str | None) -> str | None:
    if not domain:
        return None
    d = str(domain).strip().lower()
    try:
        return d.encode("idna").decode("ascii")
    except Exception:
        return d  # leave as-is; validator will have been lenient


def normalize_row(raw: dict) -> tuple[dict, list[str]]:
    """
    Returns (row_for_db, errors_list). Unknown keys are ignored.
    """
    errors: list[str] = []
    company = _trim(raw.get("company"))
    domain = _trim(raw.get("domain"))
    role = _trim(raw.get("role"))

    first_name = _trim(raw.get("first_name"))
    last_name = _trim(raw.get("last_name"))
    full_name = _trim(raw.get("full_name"))
    title = _trim(raw.get("title"))
    source_url = _trim(raw.get("source_url"))
    notes = _trim(raw.get("notes"))

    ndomain = norm_domain(domain)
    ncompany = company
    nrole = role

    row = {
        "company": company,
        "domain": domain,
        "role": role,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "title": title,
        "source_url": source_url,
        "notes": notes,
        "norm_domain": ndomain,
        "norm_company": ncompany,
        "norm_role": nrole,
        "errors": json.dumps(errors, ensure_ascii=False),
    }
    return row, errors
