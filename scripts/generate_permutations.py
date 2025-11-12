# scripts/generate_permutations.py
from __future__ import annotations

# Re-export canonical utilities for scripting convenience.
from src.generate.patterns import (  # noqa: F401
    PATTERNS,
    ROLE_ALIASES,
    Inference,
    LPFn,
    apply_pattern,
    infer_domain_pattern,
    norm_name,
)

__all__ = [
    "LPFn",
    "PATTERNS",
    "ROLE_ALIASES",
    "norm_name",
    "apply_pattern",
    "Inference",
    "infer_domain_pattern",
]
