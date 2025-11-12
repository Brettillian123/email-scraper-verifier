# src/ingest/title_norm.py
"""
O02 — Title/role normalization

Canonicalize a display-normalized title (from R13) into:
  - role_family: coarse function area (e.g., "Sales", "Marketing", "Engineering")
  - seniority:   ladder level (C, VP, Director, Manager, IC)

Design:
  1) Load optional rules from docs/title_map.yaml (if present).
  2) Apply deterministic rule-based heuristics as a fallback/default.

Notes:
  - Input should already be R13-normalized display text (e.g., "VP, Sales & Marketing").
  - We do not persist anything here; callers should write outputs to:
      people.role_family, people.seniority   (added by scripts/migrate_o02_title_fields.py)
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

# Optional dependency; degrade gracefully if missing
try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


# ----------------------------------
# Canonical enums (TEXT)
# ----------------------------------

SENIORITY_ORDER = ("C", "VP", "Director", "Manager", "IC")

# Keep role_family short, stable buckets.
ROLE_FAMILIES = {
    "Executive",
    "Founder",
    "Sales",
    "Marketing",
    "Customer Success",
    "Support",
    "Finance",
    "Operations",
    "Engineering",
    "Product",
    "Data",
    "IT",
    "Security",
    "HR",
    "Legal",
    "Design",
    "General Management",
}


# ----------------------------------
# Optional YAML rule loading
# ----------------------------------


@dataclass(frozen=True)
class MapEntry:
    match: str  # substring or regex (lowercased)
    role_family: str
    seniority: str


def _default_map() -> list[MapEntry]:
    """
    A compact, opinionated default mapping that covers common ICP titles.
    Users can extend/override via docs/title_map.yaml.
    """
    rules: list[MapEntry] = []

    def add(patterns: Iterable[str], role: str, seniority: str):
        for p in patterns:
            rules.append(MapEntry(p.lower(), role, seniority))

    # Executives / CxO / Founder
    add(["chief executive officer", "ceo", "president"], "Executive", "C")
    add(["chief operating officer", "coo"], "Operations", "C")
    add(["chief technology officer", "cto", "head of engineering"], "Engineering", "C")
    add(["chief information officer", "cio"], "IT", "C")
    add(["chief marketing officer", "cmo"], "Marketing", "C")
    add(["chief revenue officer", "cro"], "Sales", "C")
    add(["chief human resources officer", "chro", "chief people officer"], "HR", "C")
    add(["chief information security officer", "ciso"], "Security", "C")
    add(["founder", "co-founder"], "Founder", "C")

    # VP layer
    add(
        ["svp", "evp", "senior vice president", "executive vice president", "vice president", "vp"],
        "General Management",
        "VP",
    )

    # Directors / Heads
    add(["director", "head of", "head,"], "General Management", "Director")

    # Managers
    add(["manager"], "General Management", "Manager")

    # Leads / Principals / IC-senior
    add(
        ["principal", "staff", "lead", "architect", "individual contributor"],
        "General Management",
        "IC",
    )

    # Functional areas (role family) by keywords — leave seniority to prior matches
    # Sales
    add(["sales", "revenue", "business development", "bd"], "Sales", "IC")
    # Marketing
    add(["marketing", "growth", "demand generation", "brand"], "Marketing", "IC")
    # Customer success / support
    add(
        ["customer success", "customer experience", "cx", "account management"],
        "Customer Success",
        "IC",
    )
    add(["support", "helpdesk", "service desk"], "Support", "IC")
    # Finance / Ops / HR / Legal
    add(["finance", "financial", "accounting", "controller"], "Finance", "IC")
    add(["operations", "ops", "supply chain"], "Operations", "IC")
    add(["human resources", "people", "talent", "recruit"], "HR", "IC")
    add(["legal", "counsel", "attorney", "jd"], "Legal", "IC")
    # Product / Engineering / IT / Data / Security / Design
    add(["product"], "Product", "IC")
    add(["engineering", "software", "developer", "devops", "sre"], "Engineering", "IC")
    add(["information technology", "it", "sysadmin", "systems administrator"], "IT", "IC")
    add(["data", "analytics", "bi", "machine learning", "ml", "ai"], "Data", "IC")
    add(["security", "infosec", "application security"], "Security", "IC")
    add(["design", "ux", "ui", "user experience"], "Design", "IC")

    return rules


def _load_yaml_rules(path: Path | None) -> list[MapEntry]:
    """
    docs/title_map.yaml structure (example):

    rules:
      - match: "Chief Revenue Officer"
        role_family: "Sales"
        seniority: "C"
      - match: "Head of Sales"
        role_family: "Sales"
        seniority: "Director"
      - match: "VP, Sales"
        role_family: "Sales"
        seniority: "VP"
    """
    if path is None:
        path = Path(__file__).resolve().parents[2] / "docs" / "title_map.yaml"
    if not path.exists() or yaml is None:
        return []

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:  # pragma: no cover
        return []

    out: list[MapEntry] = []
    for item in data.get("rules") or []:
        try:
            out.append(
                MapEntry(
                    match=str(item.get("match", "")).lower(),
                    role_family=str(item.get("role_family", "")),
                    seniority=str(item.get("seniority", "")),
                )
            )
        except Exception:
            continue
    return out


# ----------------------------------
# Core logic
# ----------------------------------


def _detect_seniority_ladder(text: str) -> str | None:
    # Order matters: detect higher seniorities first
    # Explicit C-suite tokens (include CEO and General Counsel)
    if (
        re.search(r"\bchief\b", text)
        or re.search(r"\bceo\b", text)
        or re.search(r"\bcfo\b", text)
        or re.search(r"\bcoo\b", text)
        or re.search(r"\bcto\b", text)
        or re.search(r"\bcmo\b", text)
        or re.search(r"\bcio\b", text)
        or re.search(r"\bciso\b", text)
        or re.search(r"\bpresident\b", text)
        or re.search(r"\bgeneral counsel\b", text)
        or re.search(r"\bgc\b", text)
    ):
        return "C"
    if re.search(r"\b(svp|evp|senior vice president|executive vice president)\b", text):
        return "VP"
    if re.search(r"\b(vice president|vp)\b", text):
        return "VP"
    if re.search(r"\b(director|head of|head,)\b", text):
        return "Director"
    if re.search(r"\b(manager|mgr)\b", text):
        return "Manager"
    # Senior IC cues (leave as IC)
    if re.search(r"\b(principal|staff|lead|architect)\b", text):
        return "IC"
    return None


def _detect_role_family(text: str) -> str | None:
    """
    Lightweight, data-driven family detection to keep cyclomatic complexity low.
    The first matching pattern wins.
    """
    patterns: list[tuple[str, str]] = [
        # Executive/founder first
        (r"\b(founder|co[- ]?founder)\b", "Founder"),
        (r"\b(ceo|president)\b", "Executive"),
        # Map common C-suite abbreviations to functional families
        (r"\b(cto|chief technology officer)\b", "Engineering"),
        (r"\b(cio|chief information officer)\b", "IT"),
        (r"\b(cmo|chief marketing officer)\b", "Marketing"),
        (r"\b(cfo|chief financial officer)\b", "Finance"),
        (r"\b(cro|chief revenue officer)\b", "Sales"),
        (r"\b(chro|chief human resources officer|chief people officer)\b", "HR"),
        (r"\b(ciso|chief information security officer)\b", "Security"),
        (r"\bgeneral counsel\b", "Legal"),
        # Functional areas
        (r"\b(sales|revenue|business development|bd)\b", "Sales"),
        (r"\b(marketing|growth|demand generation|brand)\b", "Marketing"),
        (r"\b(customer success|customer experience|cx|account management)\b", "Customer Success"),
        (r"\b(support|helpdesk|service desk)\b", "Support"),
        (r"\b(finance|accounting|controller)\b", "Finance"),
        (r"\b(operations|ops|supply chain)\b", "Operations"),
        (r"\b(human resources|people|talent|recruit(ing)?)\b", "HR"),
        (r"\b(legal|counsel|attorney|lawyer|jd)\b", "Legal"),
        (r"\b(product)\b", "Product"),
        (r"\b(engineering|software|developer|devops|sre)\b", "Engineering"),
        (r"\b(information technology|it|sysadmin|systems administrator)\b", "IT"),
        (r"\b(data|analytics|business intelligence|bi|machine learning|ml|ai)\b", "Data"),
        (r"\b(security|infosec|application security|ciso)\b", "Security"),
        (r"\b(design|ux|ui|user experience)\b", "Design"),
        # General management catch-all
        (r"\b(general manager|gm)\b", "General Management"),
    ]
    for pat, family in patterns:
        if re.search(pat, text):
            return family
    return None


def canonicalize(title_norm: str | None) -> tuple[str, str]:
    """
    Return (role_family, seniority) for a given display-normalized title.
    Fallbacks:
      - role_family defaults to "General Management" if nothing fits.
      - seniority defaults to "IC".
    """
    if not title_norm:
        return ("General Management", "IC")

    t = title_norm.strip().lower()

    # Special exact cases first
    exact: dict[str, tuple[str, str]] = {
        "ceo": ("Executive", "C"),
        "cto": ("Engineering", "C"),
        "cfo": ("Finance", "C"),
        "coo": ("Operations", "C"),
        "cio": ("IT", "C"),
        "cmo": ("Marketing", "C"),
        "cro": ("Sales", "C"),
        "chro": ("HR", "C"),
        "ciso": ("Security", "C"),
        "general counsel": ("Legal", "C"),
        "product manager": ("Product", "IC"),
    }
    if t in exact:
        return exact[t]

    # 1) YAML exact/substring rules (first match wins)
    for entry in _load_yaml_rules(None) or []:
        if entry.match and entry.match in t:
            role = entry.role_family or "General Management"
            seniority = entry.seniority or (_detect_seniority_ladder(t) or "IC")
            return (role, seniority)

    # 2) Heuristic detection
    role = _detect_role_family(t) or "General Management"
    seniority = _detect_seniority_ladder(t) or "IC"

    # If role is Founder but we didn't detect seniority, upgrade to C
    if role == "Founder" and seniority == "IC":
        seniority = "C"

    return (role, seniority)
