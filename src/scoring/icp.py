# src/scoring/icp.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ScoreResult:
    """Result of an ICP scoring operation."""

    score: int
    reasons: list[str]


def _get(d: dict[str, Any] | None, path: str, default: Any = None) -> Any:
    """Safely get a dotted-path value from a (possibly None) dict.

    Examples:
      _get(company, "attrs.tech_keywords", []) -> list[str] | []
    """
    cur: Any = d or {}
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def compute_icp(
    person: dict[str, Any],
    company: dict[str, Any] | None,
    cfg: dict[str, Any],
) -> ScoreResult:
    """Compute a simple, null-safe ICP score for a person/company pair.

    Inputs:
      person:  dict containing at least:
                 - domain
                 - role_family (from O02)
                 - seniority   (from O02)
      company: dict (or None) containing:
                 - size
                 - industry
                 - attrs.tech_keywords (list[str]) or tech_keywords
      cfg:     parsed docs/icp-schema.yaml, using keys:
                 - min_required: [domain, role_family, ...]
                 - weights.role_family
                 - weights.seniority
                 - weights.company_size
                 - weights.industry_bonus
                 - weights.tech_keywords
                 - cap
    """
    reasons: list[str] = []
    score = 0

    cap = int(cfg.get("cap", 100))
    weights: dict[str, Any] = cfg.get("weights", {})
    min_required = set(cfg.get("min_required", []))

    # ------------------------------------------------------------------
    # Gate: require basic fields (do NOT crash on nulls).
    # If we don't meet min_required, return a zero score with a reason.
    # ------------------------------------------------------------------
    have = {
        k for k in ["domain", "role_family", "seniority"] if (person.get(k) or _get(company, k))
    }
    if not min_required.issubset(have):
        return ScoreResult(score=0, reasons=["missing_min_required"])

    # ------------------------------------------------------------------
    # Role family (from O02)
    # ------------------------------------------------------------------
    rf = person.get("role_family")
    if rf:
        rfw = weights.get("role_family", {}).get(rf)
        if rfw:
            score += int(rfw)
            reasons.append(f"role_family:{rf}+{int(rfw)}")

    # ------------------------------------------------------------------
    # Seniority (from O02)
    # ------------------------------------------------------------------
    sr = person.get("seniority")
    if sr:
        srw = weights.get("seniority", {}).get(sr)
        if srw:
            score += int(srw)
            reasons.append(f"seniority:{sr}+{int(srw)}")

    # ------------------------------------------------------------------
    # Company size / industry (from O03)
    # ------------------------------------------------------------------
    size = _get(company, "size")
    if size is not None:
        sw = weights.get("company_size", {}).get(str(size))
        if sw:
            score += int(sw)
            reasons.append(f"company_size:{size}+{int(sw)}")

    industry = _get(company, "industry")
    if industry is not None:
        iw = weights.get("industry_bonus", {}).get(str(industry))
        if iw:
            score += int(iw)
            reasons.append(f"industry:{industry}+{int(iw)}")

    # ------------------------------------------------------------------
    # Tech keywords (from O03 attrs or O08)
    # ------------------------------------------------------------------
    # Prefer nested attrs.tech_keywords; fall back to top-level tech_keywords.
    kws = _get(company, "attrs.tech_keywords", []) or _get(company, "tech_keywords", [])
    tw: dict[str, Any] = weights.get("tech_keywords", {}) or {}

    if isinstance(kws, list):
        for kw in kws:
            key = str(kw).lower()
            w = tw.get(key)
            if w:
                score += int(w)
                reasons.append(f"tech:{kw}+{int(w)}")

    # Clamp and return
    score = max(0, min(cap, score))
    return ScoreResult(score=score, reasons=reasons)
