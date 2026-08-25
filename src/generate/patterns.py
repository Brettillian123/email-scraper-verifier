# src/generate/patterns.py
"""
Canonical email-format vocabulary + per-company pattern evidence.

Two layers live here:

1. **Vocabulary** (pure functions, no I/O).
   ``classify_pattern`` and ``render_pattern`` are exact inverses over
   ``PATTERN_VOCABULARY``. Everything in the codebase that names an email
   format should use these strings, not ad-hoc templates.

2. **Evidence store** (Postgres, table ``company_email_pattern``).
   One row per ``(company_id, pattern)`` -- multi-pattern companies are the
   norm, not the exception: of the 311 companies with a confirmed-valid
   address today, 100 use 2 patterns, 17 use 3 and 2 use 4.

   Invalid address rows are deleted by design (CLEANUP_INVALID_GENERATED=1),
   so ``failed_count`` on this table is the *only* memory of a guess that
   missed. That is what stops the system re-guessing the same dead format.

Postgres only. Pass a psycopg2 connection (or ``src.db.get_conn()``);
placeholders are ``%s``. No SQLite code paths.

Out of scope / future work
--------------------------
* Nickname expansion (Robert -> Bob, William -> Bill). Deliberately NOT
  implemented: it needs a curated dataset, and a wrong nickname burns an
  SMTP probe on a name the company never used.
* Locale-specific ordering beyond the ``last.first`` / ``last`` members of
  the vocabulary.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# 1. Canonical vocabulary
# ---------------------------------------------------------------------------

#: Sentinel for a local-part that is a real address but matches no known form.
#: Recorded like any other pattern -- "this company does something we cannot
#: render" is useful evidence -- but ``render_pattern`` returns None for it.
PATTERN_OTHER = "other"

# Local-part builders over ALREADY-NORMALISED name parts.
# Return None when the pattern cannot be applied to the given parts.
_RENDERERS: dict[str, Callable[[str, str], str | None]] = {
    "first.last": lambda fn, ln: f"{fn}.{ln}" if fn and ln else None,
    "flast": lambda fn, ln: f"{fn[:1]}{ln}" if fn and ln else None,
    "first": lambda fn, ln: fn or None,
    "firstlast": lambda fn, ln: f"{fn}{ln}" if fn and ln else None,
    "firstl": lambda fn, ln: f"{fn}{ln[:1]}" if fn and ln else None,
    "f.last": lambda fn, ln: f"{fn[:1]}.{ln}" if fn and ln else None,
    "first.l": lambda fn, ln: f"{fn}.{ln[:1]}" if fn and ln else None,
    "last.first": lambda fn, ln: f"{ln}.{fn}" if fn and ln else None,
    "last": lambda fn, ln: ln or None,
    "first_last": lambda fn, ln: f"{fn}_{ln}" if fn and ln else None,
}

#: Patterns that can be rendered back into an address.
RENDERABLE_PATTERNS: tuple[str, ...] = tuple(_RENDERERS)

#: The full vocabulary, including the non-renderable ``other`` sentinel.
PATTERN_VOCABULARY: tuple[str, ...] = (*RENDERABLE_PATTERNS, PATTERN_OTHER)

#: Measured hit rate on DECIDABLE (non-catch-all) verification results, taken
#: from production on 2026-08-19. Catch-all domains are excluded because an
#: SMTP RCPT probe cannot decide them at all.
#:
#: Read this next to MEASURED_PATTERN_TRIES: ``firstl`` is a 26-try sample and
#: should not be treated as firm. ``first_last`` is 0/18 decidable across 116
#: attempts -- there is no evidence it has ever worked here.
MEASURED_PATTERN_YIELD: dict[str, float] = {
    "flast": 0.95,
    "first.last": 0.90,
    "first": 0.89,
    "firstlast": 0.65,
    "firstl": 0.25,
    "first_last": 0.00,
}

#: Total attempts behind MEASURED_PATTERN_YIELD (decidable + undecidable).
MEASURED_PATTERN_TRIES: dict[str, int] = {
    "first.last": 1969,
    "first": 540,
    "flast": 378,
    "first_last": 116,
    "firstlast": 55,
    "firstl": 26,
}

#: Evidence-ordered fallback for a company with NO recorded pattern, and the
#: tie-break order used by classify_pattern().
#:
#: Measured winners first; f.last / last.first / last have no local sample and
#: sit conservatively mid-pack; first_last is last because 116 attempts have
#: produced zero valid addresses.
#:
#: NOTE this is deliberately NOT the legacy PATTERN_RANKS order below, which
#: puts first.last and first_last at the top -- an enterprise distribution
#: applied to 14-person startups.
PATTERN_PRIOR_ORDER: tuple[str, ...] = (
    "flast",
    "first.last",
    "first",
    "firstlast",
    "f.last",
    "firstl",
    "first.l",
    "last.first",
    "last",
    "first_last",
)

# ---------------------------------------------------------------------------
# 2. Role addresses -- never generated for a named person
# ---------------------------------------------------------------------------

#: RFC 2142 reserved mailbox names.
RFC2142_ROLES: frozenset[str] = frozenset(
    {
        "info",
        "sales",
        "support",
        "admin",
        "postmaster",
        "abuse",
        "webmaster",
        "hostmaster",
        "noc",
        "security",
    }
)

#: Non-RFC role mailboxes that are just as obviously not a person.
EXTRA_ROLE_LOCALPARTS: frozenset[str] = frozenset(
    {
        "hello",
        "contact",
        "team",
        "careers",
        "jobs",
        "press",
        "help",
        "billing",
        "marketing",
    }
)

#: Union used by the generator and the recorder.
ROLE_LOCALPARTS: frozenset[str] = RFC2142_ROLES | EXTRA_ROLE_LOCALPARTS

#: Legacy alias (imported by scripts/generate_permutations.py). Same set.
ROLE_ALIASES: frozenset[str] = ROLE_LOCALPARTS

_ROLE_TOKEN_RE = re.compile(r"[^a-z]+")


def is_role_localpart(local_part: str) -> bool:
    """
    True if ``local_part`` is a role/shared mailbox rather than a person.

    Matches the whole local-part ("info") and any alphabetic token inside it
    ("sales-team", "info_us", "support2").

    A person whose name collides with a role word (Steve *Jobs*) loses that
    one candidate -- ``jobs@`` is dropped, ``sjobs@`` is not.
    """
    lp = localpart_of(local_part)
    if not lp:
        return True
    if lp in ROLE_LOCALPARTS:
        return True
    return any(tok in ROLE_LOCALPARTS for tok in _ROLE_TOKEN_RE.split(lp) if tok)


# ---------------------------------------------------------------------------
# 3. Name / address normalisation
# ---------------------------------------------------------------------------

# Letters that NFD cannot decompose into base + combining mark.
_TRANSLIT = str.maketrans(
    {
        "ø": "o",
        "æ": "ae",
        "œ": "oe",
        "ß": "ss",
        "đ": "d",
        "ð": "d",
        "ł": "l",
        "þ": "th",
        "ħ": "h",
        "ı": "i",
        "ŋ": "n",
    }
)

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
# whitespace, underscore, ASCII hyphen, and the U+2010..U+2015 dash block
_NAME_SPLIT_RE = re.compile(r"[\s_\-‐-―]+")
_LOCALPART_KEEP_RE = re.compile(r"[^a-z0-9._-]+")


def _fold(value: str) -> str:
    """NFD-fold diacritics, transliterate the leftovers, lowercase."""
    s = unicodedata.normalize("NFD", str(value or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().translate(_TRANSLIT)


def fold_name(raw: str) -> str:
    """
    Fold one name part to a bare ASCII token.

    ``"José"`` -> ``"jose"``, ``"O'Brien"`` -> ``"obrien"``,
    ``"Müller-Schmidt"`` -> ``"mullerschmidt"``.
    """
    return _NON_ALNUM_RE.sub("", _fold(raw))


def name_tokens(raw: str) -> list[str]:
    """Split a name field on whitespace/hyphens/underscores, folded."""
    parts = (fold_name(p) for p in _NAME_SPLIT_RE.split(str(raw or "").strip()))
    return [p for p in parts if p]


def name_variants(raw: str, *, keep: str = "last") -> list[str]:
    """
    Ordered, deduped candidate forms for ONE name field.

    Hyphenated and multi-part names yield BOTH the compound form and the
    single significant token, as separate candidates::

        name_variants("García-López")            -> ["garcialopez", "lopez"]
        name_variants("van der Berg")            -> ["vanderberg", "berg"]
        name_variants("Mary Jane", keep="first") -> ["maryjane", "mary"]

    The first element is always the compound form; ``render_pattern`` uses
    only that, so classify/render stay exact inverses.
    """
    toks = name_tokens(raw)
    if not toks:
        return []
    out = ["".join(toks)]
    tok = toks[0] if keep == "first" else toks[-1]
    if tok and tok != out[0]:
        out.append(tok)
    return out


def localpart_of(email: str) -> str:
    """
    Normalised local-part of an address (or of a bare local-part).

    Strips any domain, drops plus-addressing, folds diacritics, and keeps only
    ``[a-z0-9._-]`` -- the separators are pattern-significant.
    """
    s = str(email or "").strip()
    if "@" in s:
        s = s.split("@", 1)[0]
    if "+" in s:
        s = s.split("+", 1)[0]
    return _LOCALPART_KEEP_RE.sub("", _fold(s))


def normalize_domain(domain: str) -> str:
    """Lowercase a mail domain and strip ``@`` / ``www.`` / trailing dot."""
    d = _fold(domain).strip().strip("@").strip().rstrip(".")
    if d.startswith("www."):
        d = d[4:]
    return d


def domain_of(email: str) -> str:
    """Normalised domain of a full address (``""`` if there is no ``@``)."""
    s = str(email or "").strip()
    if "@" not in s:
        return ""
    return normalize_domain(s.rsplit("@", 1)[1])


# ---------------------------------------------------------------------------
# 4. classify / render -- exact inverses
# ---------------------------------------------------------------------------


def classify_pattern(local_part: str, first_name: str, last_name: str) -> str:
    """
    Name the format of ``local_part`` given the person it belongs to.

    Accepts a bare local-part or a full address. Tries every name variant
    (compound and single-token), so a surname recorded as "García-López" is
    recognised whether the company used ``garcialopez`` or ``lopez``.

    Ambiguous local-parts (a one-letter first name makes ``flast`` and
    ``firstlast`` identical) resolve in ``PATTERN_PRIOR_ORDER``, i.e. the
    format that actually wins more often here.

    Returns a member of ``PATTERN_VOCABULARY``; ``PATTERN_OTHER`` if nothing
    matches. Never raises.
    """
    lp = localpart_of(local_part)
    if not lp:
        return PATTERN_OTHER

    firsts = name_variants(first_name, keep="first") or [""]
    lasts = name_variants(last_name, keep="last") or [""]

    for pattern in PATTERN_PRIOR_ORDER:
        render = _RENDERERS[pattern]
        for fn in firsts:
            for ln in lasts:
                if render(fn, ln) == lp:
                    return pattern
    return PATTERN_OTHER


def render_pattern(pattern: str, first_name: str, last_name: str, domain: str) -> str | None:
    """
    Build the address ``pattern`` implies for this person. Inverse of
    ``classify_pattern``::

        classify_pattern(render_pattern(p, fn, ln, d), fn, ln) == p

    Returns None when the address cannot or must not be built:
      * ``pattern`` is ``other`` or not in the vocabulary
      * a required name part is missing after folding
      * ``domain`` is empty
      * the result would be a role mailbox (never generated for a person)

    Uses the compound form of each name part. For the hyphenated/multi-part
    alternatives use ``render_pattern_variants``.
    """
    dom = normalize_domain(domain)
    if not dom:
        return None
    render = _RENDERERS.get(pattern)
    if render is None:
        return None

    firsts = name_variants(first_name, keep="first")
    lasts = name_variants(last_name, keep="last")
    local = render(firsts[0] if firsts else "", lasts[0] if lasts else "")
    if not local or is_role_localpart(local):
        return None
    return f"{local}@{dom}"


def render_pattern_variants(
    pattern: str,
    first_name: str,
    last_name: str,
    domain: str,
) -> list[str]:
    """
    Every address ``pattern`` implies, across all name variants.

    For a simple name this is exactly ``[render_pattern(...)]``. For a
    hyphenated or multi-part surname it also carries the single-token form::

        render_pattern_variants("flast", "José", "García-López", "x.com")
        -> ["jgarcialopez@x.com", "jlopez@x.com"]

    Deduped, role-filtered, most-canonical first.
    """
    dom = normalize_domain(domain)
    render = _RENDERERS.get(pattern)
    if not dom or render is None:
        return []

    firsts = name_variants(first_name, keep="first") or [""]
    lasts = name_variants(last_name, keep="last") or [""]

    out: list[str] = []
    seen: set[str] = set()
    for fn in firsts:
        for ln in lasts:
            local = render(fn, ln)
            if not local or local in seen or is_role_localpart(local):
                continue
            seen.add(local)
            out.append(f"{local}@{dom}")
    return out


# ---------------------------------------------------------------------------
# 5. Shape inference -- name the format of a local-part with NO person record
# ---------------------------------------------------------------------------
#
# classify_pattern() needs the person the address belongs to. Most harvested
# addresses have no matching people row, so their format was thrown away.
# A local-part largely reveals its own shape: "j.smith" is an initial and a
# surname whoever owns it. Measured on 171 published addresses: 94.2% correct
# overall (161/171), and 100% (92/92) at confidence >= 0.75.
#
# The 0.70 band is 88.3% and its errors are almost all firstl <-> flast
# ("jsmith" could be J. Smith or Jay Smith). That is not fixable from the
# local-part alone, so it is priced in as a low confidence rather than papered
# over. Only cross the 0.75 line if you have measured the change.

#: Local-parts outside this alphabet carry no shape signal we trust.
_SHAPE_ALLOWED_RE = re.compile(r"^[a-z0-9._+-]+$")

#: "jsmith2" / "asmith01" -- a disambiguating suffix, not part of the format.
_SHAPE_TRAILING_DIGITS_RE = re.compile(r"\d+$")

#: Shortest prefix tried when splitting a run-together "firstlast".
_SHAPE_MIN_FIRST_LEN = 3


def infer_pattern_from_localpart(
    local_part: str,
    first_names: set[str] | frozenset[str],
) -> tuple[str | None, float]:
    """
    Name the format of ``local_part`` without knowing whose address it is.

    ``first_names`` is a set of folded first names -- use
    ``first_name_dictionary(conn)``, which is cached.

    Returns ``(pattern, confidence)``; ``(None, 0.0)`` when the local-part is
    unusable (role mailbox, odd characters) or genuinely ambiguous. ``pattern``
    is always a member of ``PATTERN_VOCABULARY``.

    Treat >= 0.75 as safe to act on and 0.70 as a hint only; see the section
    comment for the measured accuracy behind those numbers. Never raises.
    """
    lp = str(local_part or "").strip().lower()
    if "@" in lp:
        lp = lp.split("@", 1)[0]
    if not _SHAPE_ALLOWED_RE.match(lp):
        return None, 0.0
    if "+" in lp:
        lp = lp.split("+", 1)[0]
    lp = _SHAPE_TRAILING_DIGITS_RE.sub("", lp)
    if not lp or is_role_localpart(lp):
        return None, 0.0

    known = first_names or frozenset()

    if "." in lp:
        a, _, b = lp.partition(".")
        if not (a.isalpha() and b.isalpha()):
            return None, 0.0
        if len(a) == 1:
            # "j.smith" -- initial, dot, surname. Called f.last here; the
            # dotless "jsmith" is flast.
            return "f.last", 0.95
        if len(b) == 1:
            return "first.l", 0.90
        a_known, b_known = a in known, b in known
        if a_known and not b_known:
            return "first.last", 0.95
        if b_known and not a_known:
            return "last.first", 0.80
        # Neither name recognised, or both are plausible first names: the prior
        # says first.last by a wide margin (17.2% vs 0.8% for last.first).
        return "first.last", 0.75

    if "_" in lp:
        a, _, b = lp.partition("_")
        if not (a.isalpha() and b.isalpha()):
            return None, 0.0
        return "first_last", 0.85 if a in known else 0.60

    if not lp.isalpha():
        return None, 0.0

    if lp in known:
        return "first", 0.85
    if lp[1:] in known:
        # "mmartin": flast of M. Martin, or firstlast of Mmartin? Undecidable.
        return None, 0.0
    for i in range(_SHAPE_MIN_FIRST_LEN, len(lp)):
        if lp[:i] in known:
            return "firstlast", 0.70
    if len(lp) >= 4:
        return "flast", 0.70
    return None, 0.0


#: Folded first names seen in the people table. Loaded once per process.
_FIRST_NAME_CACHE: frozenset[str] | None = None

_SELECT_FIRST_NAMES = """
    SELECT DISTINCT first_name
    FROM people
    WHERE first_name IS NOT NULL AND first_name <> ''
"""


def first_name_dictionary(conn: Any, *, refresh: bool = False) -> frozenset[str]:
    """
    Folded first names from the people table (~2,175 distinct from 5,569 rows).

    Cached for the life of the process -- ``infer_pattern_from_localpart`` is
    called once per harvested address and must not hit the database each time.
    Pass ``refresh=True`` after an ingest to rebuild it.

    Returns an empty set if the table is unreadable; shape inference still
    works, it just loses the dictionary-backed branches.
    """
    global _FIRST_NAME_CACHE
    if _FIRST_NAME_CACHE is not None and not refresh:
        return _FIRST_NAME_CACHE
    names: set[str] = set()
    try:
        for row in _execute(conn, _SELECT_FIRST_NAMES, ()):
            for token in name_tokens(row[0]):
                if len(token) >= 2 and token.isalpha():
                    names.add(token)
    except Exception:  # pragma: no cover -- degrade, never break generation
        return _FIRST_NAME_CACHE or frozenset()
    _FIRST_NAME_CACHE = frozenset(names)
    return _FIRST_NAME_CACHE


# ---------------------------------------------------------------------------
# 6. Confidence grading
# ---------------------------------------------------------------------------

#: Evidence grades returned by ``pattern_confidence``.
STATUS_TRUSTED = "trusted"
STATUS_CANDIDATE = "candidate"
STATUS_REFUTED = "refuted"
STATUS_UNKNOWN = "unknown"


def pattern_confidence(confirmed_count: int, failed_count: int) -> tuple[float, str]:
    """
    Grade one pattern's evidence.

    Returns ``(confidence, status)``.

    ``status`` is the decision the caller should act on:
      * ``trusted``   -- >= 2 independent confirmed people
      * ``candidate`` -- exactly 1 confirmed person
      * ``refuted``   -- 0 confirmed and >= 1 failure
      * ``unknown``   -- no evidence either way

    ``confidence`` is a Laplace-smoothed success rate,
    ``(confirmed + 0.5) / (confirmed + failed + 1)``, so it is defined at zero
    evidence (0.5) and never saturates at 0.0/1.0 on a single observation. Use
    it for ranking, use ``status`` for decisions.
    """
    c = max(0, int(confirmed_count or 0))
    f = max(0, int(failed_count or 0))
    confidence = round((c + 0.5) / (c + f + 1.0), 4)
    if c >= 2:
        status = STATUS_TRUSTED
    elif c == 1:
        status = STATUS_CANDIDATE
    elif f >= 1:
        status = STATUS_REFUTED
    else:
        status = STATUS_UNKNOWN
    return confidence, status


# ---------------------------------------------------------------------------
# 7. Evidence store (Postgres: company_email_pattern)
# ---------------------------------------------------------------------------

_SELECT_PATTERNS = """
    SELECT pattern, domain, confirmed_count, failed_count, first_seen_at, last_seen_at,
           inferred_count
    FROM company_email_pattern
    WHERE company_id = %s
"""

_ORDER_BY_EVIDENCE = " ORDER BY confirmed_count DESC, failed_count ASC, pattern ASC"

# Increment one outcome. Counters accumulate; domain is refreshed when known.
_UPSERT_OUTCOME = """
    INSERT INTO company_email_pattern
        (company_id, domain, pattern, confirmed_count, failed_count)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (company_id, pattern) DO UPDATE SET
        confirmed_count = company_email_pattern.confirmed_count + EXCLUDED.confirmed_count,
        failed_count = company_email_pattern.failed_count + EXCLUDED.failed_count,
        domain = COALESCE(EXCLUDED.domain, company_email_pattern.domain),
        last_seen_at = NOW()
    RETURNING id
"""

# Absolute rewrite used by recomputation. confirmed_count is fully derivable
# from surviving valid addresses so it is SET; failed_count is NOT -- invalid
# rows are deleted, so accumulated failure memory can only ever go up.
_UPSERT_COUNTS = """
    INSERT INTO company_email_pattern
        (company_id, domain, pattern, confirmed_count, failed_count)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (company_id, pattern) DO UPDATE SET
        confirmed_count = EXCLUDED.confirmed_count,
        failed_count = GREATEST(company_email_pattern.failed_count, EXCLUDED.failed_count),
        domain = COALESCE(EXCLUDED.domain, company_email_pattern.domain),
        last_seen_at = NOW()
    RETURNING id
"""

# Shape-inferred evidence. SET, not incremented: the caller re-derives it from
# the company's whole published-address set, so a re-run is idempotent.
_UPSERT_INFERRED = """
    INSERT INTO company_email_pattern
        (company_id, domain, pattern, inferred_count)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (company_id, pattern) DO UPDATE SET
        inferred_count = EXCLUDED.inferred_count,
        domain = COALESCE(EXCLUDED.domain, company_email_pattern.domain),
        last_seen_at = NOW()
    RETURNING id
"""

# Latest verification per address for one company, with the person behind it.
_SELECT_COMPANY_OUTCOMES = """
    SELECT e.email, p.first_name, p.last_name, p.id, vr.verify_status
    FROM emails AS e
    JOIN people AS p ON p.id = e.person_id
    JOIN LATERAL (
        SELECT v.verify_status
        FROM verification_results AS v
        WHERE v.email_id = e.id
        ORDER BY v.id DESC
        LIMIT 1
    ) AS vr ON TRUE
    WHERE p.company_id = %s
      AND vr.verify_status IN ('valid', 'invalid')
"""


def _execute(conn: Any, sql: str, params: tuple[Any, ...]) -> list[Any]:
    """Run one statement and return its rows (empty list if none)."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        try:
            rows = cur.fetchall()
        except Exception:  # statement returned nothing
            rows = []
        return list(rows or [])
    finally:
        try:
            cur.close()
        except Exception:
            pass


def record_pattern_outcome(
    conn: Any,
    *,
    company_id: int,
    domain: str,
    email: str,
    first_name: str,
    last_name: str,
    ok: bool,
    pattern: str | None = None,
) -> str | None:
    """
    Remember what happened to one guessed address.

    Classifies ``email`` against the person's name and increments
    ``confirmed_count`` (``ok=True``) or ``failed_count`` (``ok=False``) for
    that ``(company_id, pattern)``.

    Pass ``pattern`` when the caller GENERATED the address and already knows
    the format. Re-deriving it can land on ``other`` (a middle name in the
    first_name field, a married surname), and a refutation filed under
    ``other`` never stops the format that actually missed from being tried
    again -- which is the whole point of storing the failure.

    This is how a failure survives: the invalid address row itself is deleted
    by CLEANUP_INVALID_GENERATED, but the fact that *this format* missed at
    *this company* stays here.

    Call once per person per outcome -- ``confirmed_count`` is read as
    "independent confirmed people".

    Returns the pattern recorded, or None if the address carries no naming
    signal (empty, or a role mailbox such as ``info@``).

    **Does not commit.** The caller owns the transaction, matching the rest of
    the codebase; a caller that never commits silently loses the evidence.
    """
    local = localpart_of(email)
    if not local or is_role_localpart(local):
        return None

    if pattern not in PATTERN_VOCABULARY:
        pattern = classify_pattern(local, first_name, last_name)
    dom = normalize_domain(domain) or domain_of(email)
    _execute(
        conn,
        _UPSERT_OUTCOME,
        (int(company_id), dom or None, pattern, 1 if ok else 0, 0 if ok else 1),
    )
    return pattern


def upsert_pattern_counts(
    conn: Any,
    *,
    company_id: int,
    domain: str | None,
    pattern: str,
    confirmed_count: int,
    failed_count: int = 0,
) -> None:
    """
    Write absolute counts for one ``(company_id, pattern)``.

    Used by recomputation (``rebuild_company_patterns`` and the backfill
    script). ``confirmed_count`` is SET, because it is fully derivable from
    the valid addresses still in the database. ``failed_count`` is raised to
    the given value but never lowered -- failure memory has no source to be
    recomputed from, so a re-run must not erase it.

    Idempotent. Does not commit.
    """
    if pattern not in PATTERN_VOCABULARY:
        raise ValueError(f"Unknown pattern: {pattern!r}")
    _execute(
        conn,
        _UPSERT_COUNTS,
        (
            int(company_id),
            normalize_domain(domain or "") or None,
            pattern,
            max(0, int(confirmed_count)),
            max(0, int(failed_count)),
        ),
    )


def record_inferred_patterns(
    conn: Any,
    *,
    company_id: int,
    domain: str | None,
    local_parts: Iterable[str],
    first_names: set[str] | frozenset[str],
    min_confidence: float = 0.75,
) -> dict[str, int]:
    """
    Store what a company's PUBLISHED addresses say about its format.

    Runs ``infer_pattern_from_localpart`` over every harvested local-part for
    one company and writes the surviving counts to ``inferred_count``. This is
    the coverage win: on the same data it lifts companies with a known format
    from 35 to 128, because it needs no matching people row.

    ``min_confidence`` defaults to 0.75, the band measured 100% accurate
    (92/92). Lowering it admits the 88.3% band -- do that only with numbers.

    Kept strictly out of ``confirmed_count``: a published address proves what
    the company writes on its website, not that SMTP accepts it. Counts are
    SET from the full input, so re-running on the same addresses is idempotent.

    Returns ``{pattern: count}``. Does not commit.
    """
    counts: dict[str, int] = {}
    for local_part in local_parts or ():
        pattern, confidence = infer_pattern_from_localpart(local_part, first_names)
        if pattern and confidence >= min_confidence:
            counts[pattern] = counts.get(pattern, 0) + 1

    dom = normalize_domain(domain or "") or None
    for pattern, count in counts.items():
        _execute(conn, _UPSERT_INFERRED, (int(company_id), dom, pattern, count))
    return counts


def get_domain_patterns(
    conn: Any,
    company_id: int,
    *,
    domain: str | None = None,
) -> list[dict[str, Any]]:
    """
    Every pattern known for a company, ranked by evidence.

    Ordered by ``confirmed_count`` desc, then ``failed_count`` asc, then
    pattern name for stability. Each dict carries::

        pattern          str   member of PATTERN_VOCABULARY
        domain           str|None
        confirmed_count  int
        failed_count     int
        inferred_count   int    published addresses whose SHAPE implies this
        confidence       float  see pattern_confidence()
        status           str    trusted | candidate | refuted | unknown
        renderable       bool   False for "other"
        first_seen_at    datetime
        last_seen_at     datetime

    Returns [] for an unknown company. Pass ``domain`` to restrict to one mail
    domain for a company that mails from several.
    """
    sql = _SELECT_PATTERNS
    params: tuple[Any, ...] = (int(company_id),)
    if domain:
        sql += " AND domain = %s"
        params = (int(company_id), normalize_domain(domain))
    sql += _ORDER_BY_EVIDENCE

    out: list[dict[str, Any]] = []
    for row in _execute(conn, sql, params):
        pattern = str(row[0])
        confirmed = int(row[2] or 0)
        failed = int(row[3] or 0)
        confidence, status = pattern_confidence(confirmed, failed)
        out.append(
            {
                "pattern": pattern,
                "domain": row[1],
                "confirmed_count": confirmed,
                "failed_count": failed,
                "inferred_count": int(row[6] or 0),
                "confidence": confidence,
                "status": status,
                "renderable": pattern in _RENDERERS,
                "first_seen_at": row[4],
                "last_seen_at": row[5],
            }
        )
    return out


def rebuild_company_patterns(conn: Any, company_id: int) -> list[dict[str, Any]]:
    """
    Recompute one company's confirmed counts from its verified addresses.

    Reads the latest verification per address for the company, classifies each
    against the person's name, and writes absolute ``confirmed_count`` values
    (distinct people per pattern). Existing ``failed_count`` values are
    preserved -- see ``upsert_pattern_counts``.

    Idempotent. Does not commit. Returns the refreshed ranking.
    """
    confirmed: dict[str, set[int]] = {}
    failed: dict[str, set[int]] = {}
    domains: dict[str, str] = {}

    for row in _execute(conn, _SELECT_COMPANY_OUTCOMES, (int(company_id),)):
        email, first_name, last_name = row[0], row[1], row[2]
        person_id, verify_status = row[3], row[4]
        local = localpart_of(email)
        if not local or is_role_localpart(local):
            continue
        pattern = classify_pattern(local, first_name or "", last_name or "")
        bucket = confirmed if str(verify_status) == "valid" else failed
        bucket.setdefault(pattern, set()).add(int(person_id))
        dom = domain_of(email)
        if dom:
            domains[pattern] = dom

    for pattern in set(confirmed) | set(failed):
        upsert_pattern_counts(
            conn,
            company_id=company_id,
            domain=domains.get(pattern),
            pattern=pattern,
            confirmed_count=len(confirmed.get(pattern, ())),
            failed_count=len(failed.get(pattern, ())),
        )

    return get_domain_patterns(conn, company_id)


def get_company_email_pattern(conn: Any, company_id: int) -> str | None:
    """
    Single best pattern for a company, or None.

    Back-compat shim for callers that can only hold one pattern. Returns the
    top-ranked pattern that is renderable and not ``refuted``.

    Prefer ``get_domain_patterns`` -- 38% of companies with known addresses
    use more than one format, and this function is blind to the rest.
    """
    for row in get_domain_patterns(conn, company_id):
        if row["renderable"] and row["status"] in (STATUS_TRUSTED, STATUS_CANDIDATE):
            return str(row["pattern"])
    return None


def set_company_email_pattern(
    conn: Any,
    company_id: int,
    pattern: str,
    *,
    domain: str | None = None,
) -> None:
    """
    Operator override: assert that a company uses ``pattern``.

    Counts as one confirmation, so the pattern grades as ``candidate`` (never
    ``trusted`` -- only real verifications earn that). Re-asserting does not
    inflate the count.

    Raises ValueError for a pattern outside ``PATTERN_VOCABULARY``. Does not
    commit.
    """
    if pattern not in PATTERN_VOCABULARY:
        raise ValueError(f"Unknown pattern: {pattern!r}")
    existing = {row["pattern"]: row for row in get_domain_patterns(conn, company_id)}
    current = existing.get(pattern)
    upsert_pattern_counts(
        conn,
        company_id=company_id,
        domain=domain,
        pattern=pattern,
        confirmed_count=max(1, int(current["confirmed_count"]) if current else 0),
        failed_count=int(current["failed_count"]) if current else 0,
    )


def infer_pattern_for_company(
    conn: Any,
    company_id: int,
    *,
    force: bool = False,
) -> str | None:
    """
    Derive and persist a company's patterns from its verified addresses.

    With ``force=False`` (default) a company that already has usable evidence
    is returned as-is. With ``force=True`` the counts are recomputed from the
    verification results, which is safe to repeat.

    Returns the single best pattern, or None if nothing could be inferred.
    Does not commit.
    """
    if not force:
        existing = get_company_email_pattern(conn, company_id)
        if existing:
            return existing
    rebuild_company_patterns(conn, company_id)
    return get_company_email_pattern(conn, company_id)


# ---------------------------------------------------------------------------
# 8. Legacy generation surface (DEPRECATED)
# ---------------------------------------------------------------------------
#
# Kept verbatim for existing importers: src/generate/permutations.py,
# src/queueing/tasks.py, src/verify/test_send.py, scripts/*.
#
# New code should use classify_pattern / render_pattern / render_pattern_variants
# and PATTERN_PRIOR_ORDER. In particular PATTERN_RANKS below encodes an
# enterprise-shaped distribution (first.last #1, first_last #2) that does not
# match this system's own measured yields; see MEASURED_PATTERN_YIELD.

# Local-part builder type
LPFn = Callable[[str, str], str]

PATTERNS: dict[str, LPFn] = {
    "first.last": lambda fn, ln: f"{fn}.{ln}",
    "f.last": lambda fn, ln: f"{fn[:1]}.{ln}",
    "firstl": lambda fn, ln: f"{fn}{ln[:1]}",
    "flast": lambda fn, ln: f"{fn[:1]}{ln}",
    "first": lambda fn, ln: fn,
    "last": lambda fn, ln: ln,
    "first_last": lambda fn, ln: f"{fn}_{ln}",
    "first-last": lambda fn, ln: f"{fn}-{ln}",
    "firstlast": lambda fn, ln: f"{fn}{ln}",
    "lastfirst": lambda fn, ln: f"{ln}{fn}",
    "first.l": lambda fn, ln: f"{fn}.{ln[:1]}",
    "first_l": lambda fn, ln: f"{fn}_{ln[:1]}",
    "f_last": lambda fn, ln: f"{fn[:1]}_{ln}",
}

# Pattern priority (lower = more common/preferred).
PATTERN_RANKS: dict[str, int] = {
    "first.last": 1,
    "first_last": 2,
    "firstlast": 3,
    "first": 4,
    "flast": 5,
    "firstl": 6,
    "f.last": 7,
    "first.l": 8,
    "first-last": 10,
    "first_l": 11,
    "f_last": 12,
}

DEFAULT_MAX_PERMUTATIONS_PER_PERSON = 6

# Patterns that need both name parts.
_BOTH_REQUIRED = frozenset(
    {
        "first.last",
        "f.last",
        "firstl",
        "flast",
        "first_last",
        "first-last",
        "firstlast",
        "lastfirst",
        "first.l",
        "first_l",
        "f_last",
    }
)


def _pattern_rank(key: str) -> int:
    return int(PATTERN_RANKS.get(key, 999))


#: Default generation order derived from PATTERN_RANKS (deprecated; see
#: PATTERN_PRIOR_ORDER).
PATTERN_PRIORITY: tuple[str, ...] = tuple(
    sorted(PATTERNS.keys(), key=lambda k: (_pattern_rank(k), k))
)


def norm_name(first: str, last: str) -> tuple[str, str]:
    """Deprecated: fold a (first, last) pair to bare ASCII tokens."""
    return fold_name(first), fold_name(last)


def apply_pattern(first: str, last: str, key: str) -> str:
    """Deprecated: normalise names and apply a legacy pattern key."""
    fn, ln = norm_name(first, last)
    return PATTERNS[key](fn, ln)


def build_localpart(pattern: str, first: str, last: str) -> str | None:
    """Deprecated: safe variant of apply_pattern(). Prefer render_pattern()."""
    if pattern not in PATTERNS:
        return None
    fn, ln = norm_name(first, last)
    if pattern in _BOTH_REQUIRED:
        if not fn or not ln:
            return None
        return PATTERNS[pattern](fn, ln)
    if pattern == "first":
        return fn or None
    if pattern == "last":
        return ln or None
    return PATTERNS[pattern](fn, ln)


def generate_localparts_for_person(
    first_name: str,
    last_name: str,
    preferred_pattern: str | None = None,
    *,
    max_permutations: int = DEFAULT_MAX_PERMUTATIONS_PER_PERSON,
) -> list[str]:
    """Deprecated: prioritised, deduped local-parts for one person."""
    if max_permutations <= 0:
        return []

    patterns: list[str] = list(PATTERN_PRIORITY)
    if preferred_pattern and preferred_pattern in PATTERNS:
        if preferred_pattern in patterns:
            patterns.remove(preferred_pattern)
        patterns.insert(0, preferred_pattern)

    seen: set[str] = set()
    result: list[str] = []
    for key in patterns:
        lp = build_localpart(key, first_name, last_name)
        if not lp or lp in seen:
            continue
        seen.add(lp)
        result.append(lp)
        if len(result) >= max_permutations:
            break
    return result


def generate_candidate_emails_for_person(
    first_name: str,
    last_name: str,
    domain: str,
    company_pattern: str | None = None,
    *,
    max_permutations: int = DEFAULT_MAX_PERMUTATIONS_PER_PERSON,
) -> list[str]:
    """Deprecated: full candidate addresses for one person at a domain."""
    domain = (domain or "").strip().lower()
    if not domain:
        return []
    locals_ = generate_localparts_for_person(
        first_name=first_name,
        last_name=last_name,
        preferred_pattern=company_pattern,
        max_permutations=max_permutations,
    )
    return [f"{lp}@{domain}" for lp in locals_]


@dataclass(frozen=True)
class Inference:
    pattern: str | None
    confidence: float
    samples: int


def infer_domain_pattern(examples: Iterable[tuple[str, str, str]]) -> Inference:
    """
    Deprecated: infer the dominant pattern from (first, last, localpart)
    examples. Requires >= 2 hits and >= 0.80 agreement.

    Prefer get_domain_patterns(), which keeps every pattern a company uses
    instead of collapsing to one.
    """
    ex = [(fn, ln, lp) for fn, ln, lp in examples if lp not in ROLE_ALIASES]
    n = len(ex)
    if n < 2:
        return Inference(None, 0.0, n)

    scores: dict[str, int] = dict.fromkeys(PATTERNS, 0)
    for first, last, lp in ex:
        for key in PATTERNS:
            if build_localpart(key, first, last) == lp:
                scores[key] += 1

    best, hits = max(scores.items(), key=lambda kv: (kv[1], -_pattern_rank(kv[0])))
    conf = (hits / n) if n else 0.0
    if hits >= 2 and conf >= 0.80:
        return Inference(best, conf, n)
    return Inference(None, conf, n)


__all__ = [
    # vocabulary
    "PATTERN_OTHER",
    "PATTERN_VOCABULARY",
    "RENDERABLE_PATTERNS",
    "PATTERN_PRIOR_ORDER",
    "MEASURED_PATTERN_YIELD",
    "MEASURED_PATTERN_TRIES",
    "classify_pattern",
    "infer_pattern_from_localpart",
    "first_name_dictionary",
    "render_pattern",
    "render_pattern_variants",
    # roles
    "RFC2142_ROLES",
    "EXTRA_ROLE_LOCALPARTS",
    "ROLE_LOCALPARTS",
    "ROLE_ALIASES",
    "is_role_localpart",
    # normalisation
    "fold_name",
    "name_tokens",
    "name_variants",
    "localpart_of",
    "domain_of",
    "normalize_domain",
    # confidence
    "STATUS_TRUSTED",
    "STATUS_CANDIDATE",
    "STATUS_REFUTED",
    "STATUS_UNKNOWN",
    "pattern_confidence",
    # evidence store
    "record_pattern_outcome",
    "record_inferred_patterns",
    "upsert_pattern_counts",
    "get_domain_patterns",
    "rebuild_company_patterns",
    "get_company_email_pattern",
    "set_company_email_pattern",
    "infer_pattern_for_company",
    # legacy
    "LPFn",
    "PATTERNS",
    "PATTERN_RANKS",
    "PATTERN_PRIORITY",
    "DEFAULT_MAX_PERMUTATIONS_PER_PERSON",
    "Inference",
    "norm_name",
    "apply_pattern",
    "build_localpart",
    "generate_localparts_for_person",
    "generate_candidate_emails_for_person",
    "infer_domain_pattern",
]
