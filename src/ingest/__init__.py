# src/ingest/__init__.py
"""
Compatibility shim for legacy callers (R13-ready).

This module keeps the old public API surface while delegating
to the new R13 normalization/persist pipeline under src.ingest.*.

Exports:
  - normalize_domain(raw) -> str
  - normalize_company(raw) -> str
  - split_name(full) -> (first, last)
  - map_role(raw_role_or_title) -> str
  - ingest_row(row: dict) -> bool
  - enqueue(task, payload) -> None
  - _sqlite_path_from_env() -> str | None
"""

from __future__ import annotations

import os
import re
import sqlite3
from typing import Any

# R13 normalization primitives
from src.ingest.normalize import (
    norm_company_name,
    norm_domain,
    norm_person_name,
    normalize_row,
    split_name_hard,
)

# Lightweight acceptance gate
from src.ingest.validators import is_minimum_viable

# NOTE: Do **not** import persist/upsert_row at module import time.
# Persist imports queueing.tasks (for R08 enqueue), and tasks may import us.
# Import lazily inside ingest_row() to avoid circular imports.

# Optional O02 canonicalizer (safe if missing)
try:
    from src.ingest.title_norm import canonicalize as _canonicalize  # type: ignore
except Exception:  # pragma: no cover
    _canonicalize = None  # type: ignore


__all__ = [
    "normalize_domain",
    "normalize_company",
    "split_name",
    "map_role",
    "ingest_row",
    "enqueue",
    "_sqlite_path_from_env",
]


# -------------------------------------------------------------------
# Legacy helpers delegating to R13 engines
# -------------------------------------------------------------------


def normalize_domain(raw: str | None) -> str:
    """
    Legacy wrapper around R13 norm_domain().
    Returns "" instead of None for backward compatibility with older tests.
    """
    nd = norm_domain(raw)
    return nd or ""


def normalize_company(raw: str | None) -> str:
    """
    Legacy wrapper: return the display-normalized company name.
    (Suffix punctuation/spacing standardized; whitespace collapsed.)
    """
    name_norm, _key, _errs = norm_company_name(raw)
    return name_norm or ""


# Common honorifics/suffixes for light cleanup in split_name()
_PREFIXES = {"mr", "mrs", "ms", "miss", "mx", "dr", "prof"}
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "md", "mba", "cpa", "esq"}


def _strip_affixes(full: str) -> str:
    toks = re.split(r"[^\w\-'.]+", full)
    toks = [t for t in toks if t]  # drop empties
    # drop leading prefixes
    out = []
    it = iter(toks)
    for t in it:
        if t.strip(".").lower() in _PREFIXES:
            continue
        out.append(t)
        break
    # keep the rest and drop trailing suffix run
    out.extend(list(it))
    while out and out[-1].strip(".").lower() in _SUFFIXES:
        out.pop()
    return " ".join(out)


def split_name(full: str | None) -> tuple[str, str]:
    """
    Legacy API: split a full name into (first, last) with reasonable casing.

    Implementation:
      - Remove common prefixes/suffixes (Dr., Jr., III, …)
      - Delegate tokenization to split_name_hard (particle-aware)
      - Title/case with R13 norms via norm_person_name()
    """
    if not full:
        return ("", "")
    cleaned = _strip_affixes(str(full).strip())
    first_raw, last_raw = split_name_hard(cleaned)
    first, last, _errs = norm_person_name(first_raw, last_raw)
    return (first, last)


# -------------------------------------------------------------------
# Role mapping — keep a simple, deterministic fallback
# -------------------------------------------------------------------

_ROLE_MAP: dict[str, list[str]] = {
    "executive": [
        "ceo",
        "chief executive officer",
        "chief executive",
        "president",
        "managing director",
    ],
    "engineering": [
        "cto",
        "chief technology officer",
        "vp engineering",
        "engineering",
        "engineer",
        "developer",
        "software",
        "tech lead",
        "head of engineering",
        "cio",
        "information officer",
    ],
    "sales": [
        "sales",
        "account executive",
        "business development",
        "bd",
        "head of sales",
        "vp sales",
        "cro",
        "chief revenue officer",
    ],
    "marketing": ["marketing", "vp marketing", "growth", "demand gen", "content", "cmo"],
    "finance": ["cfo", "finance", "financial", "controller"],
    "it": ["it", "information technology", "sysadmin", "systems administrator", "it manager"],
    "operations": ["operations", "coo", "chief operating officer", "ops"],
    "founder": ["founder", "co-founder", "cofounder", "owner", "principal"],
}
_ROLE_PLACEHOLDERS = {"-", "—", "--", "na", "n/a", "none", "null"}


def map_role(raw: str | None) -> str:
    """
    Legacy coarse bucketing for a role/title string.

    Behavior:
      - If O02 canonicalizer is available, use its role_family (lowercased).
      - Otherwise, apply keyword buckets from _ROLE_MAP.
      - Returns one of:
        executive, engineering, sales, marketing, finance,
        it, operations, founder, other
    """
    if raw is None:
        return "other"
    s = str(raw).strip()
    if not s or s.lower() in _ROLE_PLACEHOLDERS:
        return "other"

    # Prefer O02 if present
    if _canonicalize is not None:
        try:
            role_family, _seniority = _canonicalize(s)
            return role_family.lower() if role_family else "other"
        except Exception:
            pass  # fall through to keyword mapping

    # Keyword mapping (case/space-insensitive)
    t = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip().lower()
    for bucket, keys in _ROLE_MAP.items():
        for k in keys:
            if k in t:
                return bucket

    # Heuristics
    if "chief" in t and "executive" in t:
        return "executive"
    if "chief" in t and "operat" in t:
        return "operations"
    if "chief" in t and ("tech" in t or "information" in t):
        return "engineering"
    return "other"


# -------------------------------------------------------------------
# Persistence & queue shims
# -------------------------------------------------------------------


def ingest_row(row: dict[str, Any]) -> bool:
    """
    Back-compat ingest entrypoint.

    R13 pipeline:
      - Accept a raw row (any keys)
      - **Gate** via validators.is_minimum_viable() to avoid DB writes for empties
      - Normalize via normalize_row()
      - Persist via src.ingest.persist.upsert_row()
      - Never drop provenance (source_url)

    Returns:
      True on success, False if rejected by the gate or on DB/config errors.
      (Queue/Redis outages are degraded inside persist and must not cause False.)
    """
    ok, _reasons = is_minimum_viable(row or {})
    if not ok:
        return False

    # Pre-normalize for callers that inspect it (persist will normalize again)
    _normalized, _errs = normalize_row(row or {})

    # Lazy import to avoid circular imports with queueing.tasks <-> persist
    from src.ingest.persist import upsert_row as _persist_upsert_row

    try:
        _persist_upsert_row(row or {})  # accepts RAW; normalizes internally
        return True
    except (sqlite3.Error, RuntimeError):
        # Treat SQLite/DB errors or bad DATABASE_URL as a hard failure
        return False


def enqueue(task: str, payload: dict[str, Any]) -> None:
    """
    Production path: enqueue into RQ if available; otherwise no-op.
    Provided for backwards compatibility with older pipelines/tests.
    """
    try:
        from rq import Queue  # type: ignore

        from src.queueing.redis_conn import get_redis  # type: ignore

        q = Queue(name="verify", connection=get_redis())
        # Keep a generic handler; downstream can route by payload['task'] if needed.
        q.enqueue("src.queueing.tasks.handle_task", {"task": task, "payload": payload})
    except Exception:
        # No RQ/Redis in local runs; tests may monkeypatch this.
        return


def _sqlite_path_from_env() -> str | None:
    """
    Legacy helper — prefer DATABASE_URL=sqlite:///path.db, otherwise None.
    """
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url.lower().startswith("sqlite:///"):
        return url[len("sqlite:///") :]
    return None
