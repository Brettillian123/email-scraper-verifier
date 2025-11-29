# src/search/__init__.py
"""
Search helpers and indexing utilities for lead search.

R21 notes
---------
This package currently exposes SQLite FTS5-backed search helpers via
`search_people_leads`, along with a small fuzzy company lookup helper.

R22's /leads/search endpoint should call into these helpers (or a thin wrapper)
instead of talking to the database directly, so that the implementation can be
swapped out later (e.g. Postgres tsvector/GIN, Meilisearch/OpenSearch).
"""

from .indexing import (
    LeadSearchParams,
    fuzzy_company_lookup,
    search_people_leads,
    simple_similarity,
)

__all__ = [
    "LeadSearchParams",
    "search_people_leads",
    "simple_similarity",
    "fuzzy_company_lookup",
]
