# src/ingest/company_enrich.py
"""
O03 — Company enrichment (lightweight, free cues)

Heuristic extraction from already-fetched page text (e.g., R10 /about, /jobs).
No external APIs. Designed to be cheap and deterministic.

Outputs (example):
    {
      "size_bucket": "51-200",
      "industry": ["B2B SaaS", "Healthcare"],
      "tech": ["Salesforce", "HubSpot", "AWS"]
    }

Notes
-----
- Keep rules conservative: prefer precision over recall.
- Accept raw HTML or plain text input; we'll strip tags crudely if needed.
- Buckets are aligned with docs/icp-schema.yaml:
    ["1-10","11-50","51-200","201-1000","1001+"]
- Plus-handling: "N+ employees" rolls up to the next bucket (i.e., use N+1).
"""

from __future__ import annotations

import html
import re
from collections.abc import Iterable

BUCKETS = ["1-10", "11-50", "51-200", "201-1000", "1001+"]

# ---- Keyword dictionaries ---------------------------------------------------

INDUSTRY_KEYWORDS: dict[str, Iterable[str]] = {
    "B2B SaaS": [r"\bsaas\b", r"\bsoftware\s+as\s+a\s+service\b", r"\bb2b\b"],
    "Healthcare": [r"\bhealth(care)?\b", r"\bmed(ical|tech)\b", r"\bpatient\b"],
    "Finance": [r"\bfintech\b", r"\bfinance\b", r"\binsurtech\b", r"\bbanking?\b"],
    "Manufacturing": [
        r"\bmanufactur(e|ing)\b",
        r"\bplant\b",
        r"\bfactory\b",
        r"\bsupply\s+chain\b",
    ],
    "Retail/E-commerce": [r"\be-?commerce\b", r"\bretail\b", r"\bshop(ping)?\b"],
    "Education": [r"\bed(tech|ucation(al)?)\b", r"\bschool\b", r"\buniversity\b"],
    "Government/Public": [r"\bpublic\s+sector\b", r"\bgovernment\b", r"\bgovtech\b"],
}

TECH_KEYWORDS: dict[str, Iterable[str]] = {
    "Salesforce": [r"\bsalesforce\b"],
    "HubSpot": [r"\bhubspot\b"],
    "Marketo": [r"\bmarketo\b"],
    "Pardot": [r"\bpardot\b"],
    "Intercom": [r"\bintercom\b"],
    "Zendesk": [r"\bzendesk\b"],
    "AWS": [r"\bamazon\s+web\s+services\b", r"\baws\b"],
    "GCP": [r"\bgoogle\s+cloud\b", r"\bgcp\b"],
    "Azure": [r"\bazure\b", r"\bmicrosoft\s+azure\b"],
    "Snowflake": [r"\bsnowflake\b"],
    "Databricks": [r"\bdatabricks\b"],
    "PostgreSQL": [r"\bpostgres(ql)?\b"],
    "MySQL": [r"\bmysql\b"],
}


# ---- Helpers ----------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+", re.MULTILINE)


def _to_text(s: str) -> str:
    """Best-effort convert HTML-ish content to plain text."""
    if not s:
        return ""
    # Unescape & strip tags; collapse whitespace
    out = html.unescape(_TAG_RE.sub(" ", s))
    return _WS_RE.sub(" ", out).strip()


def _norm(s: str) -> str:
    return _WS_RE.sub(" ", s).strip().lower()


# ---- Size bucket heuristics -------------------------------------------------

# Patterns like:
#   "11–50 employees", "51-200 employees", "200+ employees"
#   "more than 1,000 employees", "over 1000 employees"
#   "approx. 75 employees" -> map to nearest bucket (coarse)
_RANGE_RE = re.compile(
    r"\b(?P<a>\d{1,3})(?:\s*[–-]\s*(?P<b>\d{1,3}))?(?P<plus>\s*\+)?\s*"
    r"(?:employees|staff|people)\b",
    re.IGNORECASE,
)
_OVER_1000_RE = re.compile(
    r"\b(1[, ]?0{3,}|over\s+1000|more\s+than\s+1000)\s+(employees|staff|people)\b",
    re.IGNORECASE,
)


def _bucket_from_number(n: int) -> str:
    if n <= 10:
        return "1-10"
    if n <= 50:
        return "11-50"
    if n <= 200:
        return "51-200"
    if n <= 1000:
        return "201-1000"
    return "1001+"


def _guess_size_bucket(text: str) -> str | None:
    # Direct ranges or "N+ employees"
    for m in _RANGE_RE.finditer(text):
        a = int(m.group("a"))
        b = m.group("b")
        plus = m.group("plus") is not None

        if b is not None:
            # Range: choose the bucket covering the upper bound for conservatism
            return _bucket_from_number(int(b))

        # Single number; if "N+" present, roll up to next bucket (use N+1)
        return _bucket_from_number(a + 1) if plus else _bucket_from_number(a)

    # Over 1000 style (e.g., "more than 1000 employees")
    if _OVER_1000_RE.search(text):
        return "1001+"

    # Fallback: simple "approx. N employees" pattern
    approx = re.search(
        r"\b(?:approx(?:imately)?\s+)?(\d{2,4})\s+(employees|staff|people)\b",
        text,
        re.IGNORECASE,
    )
    if approx:
        n = int(approx.group(1))
        return _bucket_from_number(n)

    return None


# ---- Keyword extraction -----------------------------------------------------


def _match_keywords(text: str, mapping: dict[str, Iterable[str]]) -> list[str]:
    found: list[str] = []
    for label, patterns in mapping.items():
        for pat in patterns:
            if re.search(pat, text, flags=re.IGNORECASE):
                found.append(label)
                break
    return found


# ---- Public API -------------------------------------------------------------


def enrich_company_from_text(html_or_text: str) -> dict[str, object]:
    """
    Extract a small set of attributes from textual content.

    Args:
      html_or_text: raw HTML or plain text from About/Jobs/News pages.

    Returns:
      dict with optional keys: size_bucket, industry (list), tech (list)
    """
    text = _to_text(html_or_text)
    if not text:
        return {}

    size = _guess_size_bucket(text)
    industries = _match_keywords(text, INDUSTRY_KEYWORDS)
    tech = _match_keywords(text, TECH_KEYWORDS)

    out: dict[str, object] = {}
    if size:
        out["size_bucket"] = size
    if industries:
        # Deduplicate while preserving order
        seen = set()
        uniq = [x for x in industries if not (x in seen or seen.add(x))]
        out["industry"] = uniq
    if tech:
        seen = set()
        uniq = [x for x in tech if not (x in seen or seen.add(x))]
        out["tech"] = uniq

    return out
