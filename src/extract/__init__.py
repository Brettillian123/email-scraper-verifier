# src/extract/__init__.py
from __future__ import annotations

from .candidates import Candidate, extract_candidates

"""
R11: Candidate extractor slice.

This package exposes the pure HTML → candidate extractor for pulling
(email, first_name, last_name, source_url) tuples from pages saved by R10.

Public API:
- extract_candidates(html: str, source_url: str, official_domain: str | None) -> list[Candidate]
- Candidate: dataclass describing a single extracted candidate.

Implementation lives in .candidates; this __init__ re-exports the stable API.
"""

__all__ = ["Candidate", "extract_candidates"]

__version__ = "0.1.0"
