# src/ingest/normalize.py
from __future__ import annotations

import json
import re
import unicodedata

try:
    # optional but improves non-Latin handling; add to requirements if used
    from unidecode import unidecode as _latinize
except Exception:  # pragma: no cover
    _latinize = None


# ---------------------------------------------------------------------------
# Small utils
# ---------------------------------------------------------------------------


def _to_nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def _trim(s: str | None) -> str | None:
    if s is None:
        return None
    s = _to_nfkc(str(s)).strip()
    return s if s else None


def _collapse_ws(s: str) -> str:
    return " ".join(str(s).strip().split())


def norm_domain(domain: str | None) -> str | None:
    if not domain:
        return None
    d = _to_nfkc(str(domain)).strip().lower()
    try:
        return d.encode("idna").decode("ascii")
    except Exception:
        return d  # leave as-is; validator will have been lenient


# ---------------------------------------------------------------------------
# O09 — Internationalization & advanced name parsing helpers
# ---------------------------------------------------------------------------

# Particles to keep with the surname (normalized lowercase)
# Non-exhaustive but pragmatic set; tune over time.
SURNAME_PARTICLES = {
    # Single-token particles
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
    # Multi-token particles
    "de la",
    "de los",
    "de las",
    "van der",
    "van de",
    "van den",
    "von der",
    "von den",
    "von dem",
    "auf der",
    "op de",
    "op den",
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


def split_name_hard(full_name: str) -> tuple[str, str]:
    """
    Returns (first, last) with particle-aware splitting.
    """
    raw = _collapse_ws(_to_nfkc(full_name))
    if not raw:
        return "", ""

    # If CJK: treat first token as surname (last), remaining tokens as given name (first).
    # We split on the original string so this still works even if Unidecode is not
    # installed; transliteration happens later in normalize_name_parts.
    if is_cjk(raw):
        toks = raw.split()
        if len(toks) >= 2:
            first_raw = " ".join(toks[1:])
            last_raw = toks[0]
            return first_raw, last_raw
        if toks:
            return toks[0], ""
        return "", ""

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
        # multi-word particles (e.g., "de la", "van der")
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
    Applies transliteration, diacritic stripping, and particle-aware splitting,
    with family-first handling for CJK names (e.g., "王 小明" → first "xiaoming",
    last "wang" after transliteration).
    """
    first, last = split_name_hard(full_name)
    norm_first = _collapse_ws(transliterate(first)).lower()
    norm_last = _collapse_ws(transliterate(last)).lower()
    return norm_first, norm_last, full_name


def normalize_split_parts(first: str | None, last: str | None) -> tuple[str, str]:
    """
    Normalize ALREADY-SPLIT name parts without reordering.
    Returns: (first_norm, last_norm) both ASCII-latinized lowercase.
    """
    f_raw = _collapse_ws(first or "")
    l_raw = _collapse_ws(last or "")
    first_norm = _collapse_ws(transliterate(f_raw)).lower()
    last_norm = _collapse_ws(transliterate(l_raw)).lower()
    return first_norm, last_norm


# ---------------------------------------------------------------------------
# R13 — Normalization engine (names/title/company) + provenance preservation
# ---------------------------------------------------------------------------

# Title/role abbreviation safe-list → canonical casing
_ABBR_CANON = {
    # C-suite
    "ceo": "CEO",
    "cfo": "CFO",
    "coo": "COO",
    "cmo": "CMO",
    "cto": "CTO",
    "cio": "CIO",
    "chro": "CHRO",
    "cro": "CRO",
    # Leadership
    "vp": "VP",
    "svp": "SVP",
    "evp": "EVP",
    "gm": "GM",
    "md": "MD",
    # Credentials
    "phd": "PhD",
    "cpa": "CPA",
    "jd": "JD",
    "mba": "MBA",
    # Roman numerals (commonly used with roles)
    "ii": "II",
    "iii": "III",
    "iv": "IV",
}

# Words to keep lowercase in titles unless first token
_LOWER_TITLE_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "but",
    "by",
    "for",
    "from",
    "in",
    "into",
    "nor",
    "of",
    "on",
    "or",
    "over",
    "per",
    "the",
    "to",
    "via",
    "vs",
    "with",
}

# Company suffix canon (display, whether to add comma before)
# If suffix is recognized, we'll standardize spacing/punctuation in display,
# and drop it when building norm_key.
_COMPANY_SUFFIXES: list[tuple[tuple[str, ...], str, bool]] = [
    # US canonical with comma
    (("inc", "inc.", "incorporated"), "Inc.", True),
    (("corp", "corp.", "corporation"), "Corp.", True),
    (("co", "co.", "company"), "Co.", True),
    (("llc", "l.l.c.", "lc"), "LLC", True),
    (("ltd", "ltd.", "limited"), "Ltd.", True),
    # EU/Intl (no comma in display)
    (("gmbh",), "GmbH", False),
    (("s.a.", "s.a", "sa"), "S.A.", False),
    (("sarl", "sàrl", "s.a.r.l."), "Sàrl", False),
    (("pty ltd", "pty", "pte ltd", "pte"), "Pty Ltd", False),
    (("bv",), "BV", False),
    (("nv",), "NV", False),
    (("plc",), "PLC", False),
    (("ag",), "AG", False),
    (("oy",), "Oy", False),
    (("oyj",), "Oyj", False),
]


_APOSTROPHES = {"'", "’", "ʼ", "ʹ", "ꞌ"}  # common apostrophe-like chars
_HYPHEN_CHARS = {"-", "–"}  # hyphen/minus + non-breaking hyphen


def _cap_after_separators(token: str) -> str:
    """
    Capitalize a token while preserving hyphens/apostrophes:
    - First alphabetic char → uppercase
    - After hyphen/apostrophe → uppercase next alphabetic
    - Others → lowercase
    """
    out = []
    cap_next = True
    for ch in token:
        if ch.isalpha():
            out.append(ch.upper() if cap_next else ch.lower())
            cap_next = False
        else:
            out.append(ch)
            if ch in _APOSTROPHES or ch in _HYPHEN_CHARS:
                cap_next = True
    return "".join(out)


def _lower_if_particle(tokens: list[str], idx: int) -> str | None:
    """
    If tokens[idx] participates in a known (possibly multi-word) surname particle,
    return the particle in lowercase; otherwise None.
    """
    t = tokens[idx]
    low = t.lower()
    # multi-word particles (look back one)
    if idx > 0:
        prev = tokens[idx - 1].lower()
        two = f"{prev} {low}"
        if two in SURNAME_PARTICLES:
            # only the second token returns here; the first was handled at its position
            return low
    # single token particle — lowercase even at start of the surname
    if low in SURNAME_PARTICLES:
        return low
    return None


def _name_title_case(first: str, last: str) -> tuple[str, str]:
    """
    Title-case names with particle rules and preserved diacritics.
    """
    # First
    f_toks = _collapse_ws(first).split() if first else []
    f_out = [_cap_after_separators(tok) for tok in f_toks]
    first_norm = " ".join(f_out)

    # Last with particles
    l_toks = _collapse_ws(last).split() if last else []
    l_out: list[str] = []
    for i, tok in enumerate(l_toks):
        as_particle = _lower_if_particle(l_toks, i)
        if as_particle is not None:
            l_out.append(as_particle)
        else:
            l_out.append(_cap_after_separators(tok))
    last_norm = " ".join(l_out)

    return first_norm, last_norm


def norm_person_name(first: str | None, last: str | None) -> tuple[str, str, list[str]]:
    """
    Normalize person name parts to display-form (keep diacritics).
    - Unicode NFKC
    - Trim/collapse whitespace
    - Title-case with particle rules
    - Preserve hyphens/apostrophes
    Returns: (first_norm, last_norm, errors)
    """
    errs: list[str] = []
    f_raw = _trim(first) or ""
    l_raw = _trim(last) or ""

    first_norm, last_norm = _name_title_case(f_raw, l_raw)

    # Surface soft warnings (do not fail ingestion)
    if not first_norm and not last_norm:
        errs.append("name.empty")
    return first_norm, last_norm, errs


def _strip_trailing_punct(word: str) -> tuple[str, str]:
    """
    Split trailing punctuation we want to preserve (commas, periods).
    Returns (base, trailing)
    """
    m = re.match(r"^(.*?)([.,:;!?]+)$", word)
    if m:
        return m.group(1), m.group(2)
    return word, ""


def _canon_abbrev(word: str) -> str | None:
    """
    Map abbreviations (with or without dots) to canonical forms.
    """
    base = word.lower()
    base_nodots = base.replace(".", "")
    if base in _ABBR_CANON:
        return _ABBR_CANON[base]
    if base_nodots in _ABBR_CANON:
        return _ABBR_CANON[base_nodots]
    return None


def _titlecase_token(word: str, position: int) -> str:
    """
    Title-case a title token with abbreviation & small-word rules.
    """
    if not word:
        return word
    if word in {"&", "/", "-"}:
        return word

    base, trail = _strip_trailing_punct(word)

    # Early return for abbreviations
    canon = _canon_abbrev(base)
    if canon:
        return canon + trail

    low = base.lower()
    if position > 0 and low in _LOWER_TITLE_WORDS:
        return low + trail

    # Hyphenated compounds → titlecase each side
    parts = re.split(r"([-–])", base)
    parts_out = []
    for p in parts:
        if p in _HYPHEN_CHARS:
            parts_out.append(p)
        else:
            parts_out.append(_cap_after_separators(p))
    return "".join(parts_out) + trail


def norm_title(title_raw: str | None) -> tuple[str | None, list[str]]:
    """
    Normalize job titles to clean display form.
    - Keep safe-list abbreviations uppercased (e.g., VP, CEO, PhD)
    - Lowercase small words (e.g., 'of') when not leading
    - Preserve symbols like '&' and separators
    """
    errs: list[str] = []
    if not title_raw:
        return None, errs
    t = _collapse_ws(_to_nfkc(title_raw))
    if not t:
        return None, errs

    tokens = t.split(" ")
    out_tokens = [_titlecase_token(tok, i) for i, tok in enumerate(tokens)]
    title_norm = _collapse_ws(" ".join(out_tokens))
    return title_norm, errs


def _detect_suffix(tokens: list[str]) -> tuple[int | None, str | None, bool]:
    """
    If the end of tokens matches a known company suffix, return
    (start_index, display, add_comma). Handles multi-token suffixes
    like "Pty Ltd".
    """
    if not tokens:
        return None, None, False

    # Consider last 2 tokens for multi-word matches
    def norm_tok(tok: str) -> str:
        t = _to_nfkc(tok).strip()
        t = t.lower()
        t = t.rstrip(",.")  # tolerate trailing punctuation in tokens
        t = t.replace(".", "")
        return t

    n = len(tokens)

    # Try 2-token suffixes first
    if n >= 2:
        last2 = f"{norm_tok(tokens[-2])} {norm_tok(tokens[-1])}"
        for variants, disp, add_comma in _COMPANY_SUFFIXES:
            if any(last2 == v for v in variants):
                return n - 2, disp, add_comma

    # Then single-token suffixes
    last1 = norm_tok(tokens[-1])
    for variants, disp, add_comma in _COMPANY_SUFFIXES:
        if any(last1 == v for v in variants):
            return n - 1, disp, add_comma

    return None, None, False


def _standardize_company_display(name: str) -> str:
    """
    Standardize corporate suffix spacing/punctuation for display.
    Keeps original base name text; applies canonical suffix casing and comma rule.
    """
    tokens = _collapse_ws(_to_nfkc(name)).split()
    if not tokens:
        return ""

    start, disp, add_comma = _detect_suffix(tokens)
    if start is None:
        return " ".join(tokens)

    base = " ".join(tokens[:start]).rstrip(",")
    # If base is empty (e.g., name is just "LLC"), leave as-is sans comma logic
    if not base:
        return " ".join(tokens)

    if add_comma:
        return f"{base}, {disp}"
    else:
        return f"{base} {disp}"


def _suffix_token_set() -> set[str]:
    """
    Build a lowercase/ascii set of suffix tokens for norm_key stripping.
    For multi-word suffixes, include their components, but we'll remove them
    only when they appear at the end.
    """
    toks: set[str] = set()
    for variants, _, _ in _COMPANY_SUFFIXES:
        for v in variants:
            for part in v.split():
                toks.add(part)
    # Add common expansions
    toks.update({"limited", "incorporated", "corporation", "company"})
    return toks


_SUFFIX_TOKENS = _suffix_token_set()


def _company_norm_key(name: str) -> str:
    """
    Build a norm_key by:
    - lowercasing
    - removing diacritics
    - stripping punctuation
    - removing trailing corporate suffixes (including dotted/letter-by-letter
      forms like S.A., L.L.C.)
    - collapsing spaces
    """
    base = _to_nfkc(name)
    base = strip_diacritics(base).lower()

    # Tokenize with punctuation stripped
    # Replace any non-alphanumeric with space
    base = re.sub(r"[^0-9a-z]+", " ", base)
    tokens = _collapse_ws(base).split()

    # Strip trailing suffix tokens (supports multi-token and letter-by-letter suffixes)
    i = len(tokens) - 1
    while i >= 0:
        # Handle explicit 2-word forms like "pty ltd" / "pte ltd"
        if i >= 1 and f"{tokens[i - 1]} {tokens[i]}" in {"pty ltd", "pte ltd"}:
            i -= 2
            continue

        # Handle concatenated single-letter tails like "s a" → "sa", "l l c" → "llc"
        j = i
        letters: list[str] = []
        while j >= 0 and len(tokens[j]) == 1 and tokens[j].isalpha():
            letters.append(tokens[j])
            j -= 1
        if letters:
            candidate = "".join(reversed(letters))
            if candidate in _SUFFIX_TOKENS or candidate in {
                "llc",
                "sa",
                "plc",
                "ag",
                "bv",
                "nv",
                "oy",
                "oyj",
                "gmbh",
            }:
                i = j
                continue

        # Single-token suffixes (inc, corp, co, ltd, gmbh, etc.)
        if tokens[i] in _SUFFIX_TOKENS:
            i -= 1
            continue

        break

    core = tokens[: i + 1] if i >= 0 else []
    # If everything was stripped (unlikely), fall back to original tokens
    if not core and tokens:
        core = tokens
    return " ".join(core)


def norm_company_name(name_raw: str | None) -> tuple[str | None, str | None, list[str]]:
    """
    Normalize company display name and compute a merge-safe norm_key.
    - Trim/collapse whitespace; NFKC
    - Standardize common corporate suffix display (Inc., LLC, Ltd., GmbH,
      S.A., Sàrl, Pty Ltd, BV, NV)
    - Do NOT rewrite semantics (& vs 'and' left as-is for display)
    - norm_key: lowercase, diacritics removed, punctuation stripped,
      corporate suffixes removed
    Returns: (name_norm, norm_key, errors)
    """
    errs: list[str] = []
    if not name_raw:
        return None, None, errs
    s = _collapse_ws(_to_nfkc(name_raw))
    if not s:
        return None, None, errs

    name_norm = _standardize_company_display(s)
    norm_key = _company_norm_key(s)

    # Soft warning if norm_key is empty after processing
    if not norm_key:
        errs.append("company.norm_key.empty")
    return name_norm, norm_key, errs


# ---------------------------------------------------------------------------
# Row normalizer (wire R13 in)
# ---------------------------------------------------------------------------


def normalize_row(raw: dict) -> tuple[dict, list[str]]:
    """
    Returns (row_for_db, errors_list). Unknown keys are ignored.

    R13 wiring:
      - Copy inbound title to title_raw; compute title_norm
      - Normalize first/last name (display-case; keep diacritics)
      - Normalize company display and compute company_norm_key
      - Preserve provenance: never drop/overwrite source_url
    """
    errors: list[str] = []

    company_in = _trim(raw.get("company"))
    domain_in = _trim(raw.get("domain"))
    role_in = _trim(raw.get("role"))

    first_in = _trim(raw.get("first_name"))
    last_in = _trim(raw.get("last_name"))
    full_name_in = _trim(raw.get("full_name"))
    title_in = _trim(raw.get("title"))
    source_url = _trim(raw.get("source_url"))
    notes = _trim(raw.get("notes"))

    # Derive missing name parts from full_name if needed
    if full_name_in and (not first_in or not last_in):
        first_part, last_part = split_name_hard(full_name_in)
        first_in = first_in or first_part
        last_in = last_in or last_part

    # Names (display-form)
    first_norm, last_norm, name_errs = norm_person_name(first_in, last_in)
    errors.extend(name_errs)

    # Title
    title_norm, title_errs = norm_title(title_in)
    errors.extend(title_errs)

    # Company
    company_name_norm, company_norm_key, comp_errs = norm_company_name(company_in)
    errors.extend(comp_errs)

    # Domain
    ndomain = norm_domain(domain_in)

    # Pass-through role (O02 will canonicalize later)
    nrole = role_in

    row = {
        # --- originals / display ---
        "company": company_in,
        "domain": domain_in,
        "role": role_in,
        "first_name": first_norm or (first_in or ""),
        "last_name": last_norm or (last_in or ""),
        "full_name": full_name_in,  # keep original if provided
        "title": title_in,  # original title preserved
        "title_raw": title_in,  # explicit raw copy for audit
        "title_norm": title_norm,  # normalized display
        "source_url": source_url,  # provenance must be preserved
        "notes": notes,
        # --- normalized helpers (back-compat + new R13) ---
        "norm_domain": ndomain,
        "norm_company": company_name_norm or company_in,  # back-compat display-normalized
        "norm_role": nrole,
        # Company normalization targets for upsert into companies table
        "company_name_norm": company_name_norm,
        "company_norm_key": company_norm_key,
        # Aggregate errors snapshot for ingestion
        "errors": json.dumps(errors, ensure_ascii=False),
    }
    return row, errors
