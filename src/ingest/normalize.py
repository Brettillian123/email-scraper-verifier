from __future__ import annotations

import json
import unicodedata

try:
    # optional but improves non-Latin handling; add to requirements if used
    from unidecode import unidecode as _latinize
except Exception:  # pragma: no cover
    _latinize = None


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


# ---------------------------------------------------------------------------
# O09 — Internationalization & advanced name parsing helpers
# ---------------------------------------------------------------------------

# Particles to keep with the surname (normalized lowercase)
# Non-exhaustive but pragmatic set; tune over time.
SURNAME_PARTICLES = {
    "da",
    "das",
    "de",
    "del",
    "della",
    "di",
    "dos",
    "du",
    "la",
    "le",
    "van",
    "von",
    "bin",
    "binti",
    "ibn",
    "al",
    "el",
    "de la",
    "de los",
    "de las",
}


def strip_diacritics(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def transliterate(s: str) -> str:
    """
    Best-effort transliteration to ASCII.
    - Prefer Unidecode when available.
    - Fallback strips diacritics and drops non-ASCII characters so we don't
      generate separator-only locals (e.g., '-@example.com').
    """
    if _latinize:
        return _latinize(s)
    # Fallback: remove diacritics first, then drop any remaining non-ASCII
    base = strip_diacritics(s)
    try:
        return base.encode("ascii", "ignore").decode("ascii")
    except Exception:  # very defensive
        return base


def is_cjk(s: str) -> bool:
    # coarse heuristic: any CJK block character → treat as CJK
    return any(
        "\u4e00" <= ch <= "\u9fff" or "\u3040" <= ch <= "\u30ff" or "\uac00" <= ch <= "\ud7af"
        for ch in s
    )


def _collapse_ws(s: str) -> str:
    return " ".join(str(s).strip().split())


def split_name_hard(full_name: str) -> tuple[str, str]:
    """
    Returns (first, last) with particle-aware splitting.
    """
    raw = _collapse_ws(full_name)
    if not raw:
        return "", ""

    # If CJK: treat first token as surname (last), remaining tokens as given name (first)
    if is_cjk(raw):
        toks = _collapse_ws(transliterate(raw)).split()
        if len(toks) >= 2:
            return " ".join(toks[1:]), toks[0]
        return (toks[0], "") if toks else ("", "")

    toks = raw.split()
    if len(toks) == 1:
        return toks[0], ""

    # Try to keep trailing particles with surname
    # Walk from the end backward, accreting known particles
    last_parts: list[str] = [toks[-1]]
    i = len(toks) - 2
    while i >= 0:
        # single-token particles
        if toks[i].lower() in SURNAME_PARTICLES:
            last_parts.insert(0, toks[i])
            i -= 1
            continue
        # multi-word particles (e.g., "de la", "de los")
        if i - 1 >= 0:
            two = f"{toks[i - 1].lower()} {toks[i].lower()}"
            if two in SURNAME_PARTICLES:
                last_parts = [toks[i - 1], toks[i], *last_parts]
                i -= 2
                continue
        break

    first = " ".join(toks[: i + 1]).strip()
    last = " ".join(last_parts).strip()
    return first, last


def normalize_name_parts(full_name: str) -> tuple[str, str, str]:
    """
    Returns (first_name, last_name, raw_name).
    Applies transliteration, diacritic stripping, and particle-aware splitting.
    """
    first, last = split_name_hard(full_name)
    norm_first = _collapse_ws(transliterate(first)).lower()
    norm_last = _collapse_ws(transliterate(last)).lower()
    return norm_first, norm_last, full_name


def normalize_split_parts(first: str | None, last: str | None) -> tuple[str, str]:
    """
    Normalize ALREADY-SPLIT name parts without reordering.

    Use this when you *know* the inputs are (given_name, surname) in the desired
    order (e.g., from separate form fields or DB columns). We still apply
    transliteration/diacritic stripping and whitespace collapsing.

    Returns:
        (first_norm, last_norm)
    """
    f_raw = _collapse_ws(first or "")
    l_raw = _collapse_ws(last or "")
    first_norm = _collapse_ws(transliterate(f_raw)).lower()
    last_norm = _collapse_ws(transliterate(l_raw)).lower()
    return first_norm, last_norm
