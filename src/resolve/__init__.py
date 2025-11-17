# src/resolve/__init__.py
from __future__ import annotations

from .domain import (
    RESOLVER_VERSION,
    Candidate,
    Decision,
    candidates_from_name,
    decide,
    normalize_hint,
    resolve,
)

# R15 — MX resolver re-exports
from .mx import norm_domain, resolve_mx

"""
Resolve package

R08:
  - `domain` resolver selects the official company domain.

R15:
  - `mx` resolver deterministically resolves MX records with caching.

Public re-exports:
  - Domain resolver symbols (R08): RESOLVER_VERSION, Candidate, Decision,
    candidates_from_name, decide, normalize_hint, resolve
  - MX resolver symbols (R15): resolve_mx, norm_domain
"""

__all__ = [
    # R08
    "RESOLVER_VERSION",
    "Candidate",
    "Decision",
    "normalize_hint",
    "candidates_from_name",
    "decide",
    "resolve",
    # R15
    "resolve_mx",
    "norm_domain",
]
