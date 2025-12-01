from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from .indexing import (
    FacetCounts,
    LeadSearchParams,
    compute_facets,
    search_people_leads,
)


@dataclass
class SearchResult:
    """
    Container for search results returned by a SearchBackend.

    Attributes:
        leads:
            The list of lead dicts, as produced by search_people_leads().
        next_cursor:
            Opaque keyset pagination cursor for fetching the next page, or None
            if there is no next page or pagination is disabled.
        facets:
            Optional facet counts, keyed by facet name, e.g.:
                {
                  "verify_status": [
                    {"value": "valid", "count": 10},
                    {"value": "invalid", "count": 2},
                  ],
                  "icp_bucket": [...]
                }
            May be None if no facets were requested or if the backend chooses
            not to compute them (e.g. non-first pages).
    """

    leads: list[dict[str, Any]]
    next_cursor: str | None
    facets: FacetCounts | None = None


class SearchBackend(Protocol):
    """
    Abstract interface for a lead search backend.

    R21/R22/R23 note
    ----------------
    Higher-level code (e.g. /leads/search in R22+) should depend on this
    protocol instead of talking to SQLite/Postgres/Meilisearch directly.

    For now, we only provide a SQLite FTS5 implementation (SqliteFtsBackend).
    Later, you can add Meilisearch/OpenSearch implementations that satisfy this
    protocol without changing the calling code.
    """

    def search(self, params: LeadSearchParams) -> SearchResult:
        """
        Execute a lead search and return a SearchResult containing:

          * leads: list of dicts
          * next_cursor: opaque cursor string or None
          * facets: optional facet counts
        """
        ...

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        """
        Convenience wrapper that returns only the list of lead dicts.

        This is mainly for backwards-compatibility with older R21/R22 code
        that only cared about the rows and not cursors/facets.
        """
        ...

    def index_batch(self, docs: Iterable[dict[str, Any]]) -> None:
        """
        Index a batch of documents into the backend.

        For SQLite FTS this is effectively a no-op because indexing is handled
        by DB triggers and migrations. For remote search engines
        (Meilisearch/OpenSearch), this will typically perform a bulk index
        operation.
        """
        ...


class SqliteFtsBackend:
    """
    R21/R22/R23 implementation of SearchBackend using SQLite FTS5.

    This is a thin wrapper over search_people_leads() + compute_facets() so
    that the HTTP layer can depend on a SearchBackend instead of directly
    importing the indexing module.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @property
    def conn(self) -> sqlite3.Connection:
        """
        Expose the underlying connection for rare escape hatches (e.g. ad-hoc
        diagnostics). Prefer using the search_* methods in normal code.
        """
        return self._conn

    def _is_first_page(self, params: LeadSearchParams) -> bool:
        """
        Determine whether this is a "first page" search (no keyset cursor).

        Facets are typically only computed on the first page, both for UX and
        performance reasons.
        """
        return (
            params.cursor_icp is None
            and params.cursor_verified_at is None
            and params.cursor_person_id is None
        )

    def search(self, params: LeadSearchParams) -> SearchResult:
        """
        Primary search entrypoint used by /leads/search and the cache layer.

        Delegates to search_people_leads() for the main row retrieval and
        compute_facets() for facet counts (first page only, when requested).
        """
        leads = search_people_leads(self._conn, params)

        # TODO: wire in real keyset cursor encoding/decoding (R22 pagination).
        # For now, we leave next_cursor as None and let higher layers handle
        # any cursor logic they already implement.
        next_cursor: str | None = None

        facets: FacetCounts | None = None
        if params.facets and self._is_first_page(params):
            facets = compute_facets(self._conn, params)

        return SearchResult(leads=leads, next_cursor=next_cursor, facets=facets)

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        """
        Convenience wrapper that returns only the lead list, for callers that
        don't care about cursors/facets.
        """
        result = self.search(params)
        return result.leads

    def index_batch(self, docs: Iterable[dict[str, Any]]) -> None:
        """
        For SQLite FTS, indexing is handled by:
          * migrate_r21_search_indexing.py backfill, and
          * people/companies triggers.

        We keep this method for API compatibility with future backends that
        need explicit indexing; it intentionally does nothing here.
        """
        # No-op for SQLite FTS backend.
        _ = list(docs)
