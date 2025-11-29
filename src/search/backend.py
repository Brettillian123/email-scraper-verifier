from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from typing import Any, Protocol

from .indexing import LeadSearchParams, search_people_leads


class SearchBackend(Protocol):
    """
    Abstract interface for a lead search backend.

    R21/R22 note
    ------------
    The goal is for higher-level code (e.g. /leads/search in R22) to depend on
    this protocol instead of talking to SQLite/Postgres/Meilisearch directly.

    For now, we only provide a SQLite FTS5 implementation (SqliteFtsBackend).
    Later, you can add Meilisearch/OpenSearch implementations that satisfy this
    protocol without changing the calling code.
    """

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        """
        Execute a lead search and return a list of plain dicts.

        The dicts should include at least the keys returned by
        src.search.indexing.search_people_leads.
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

    # Backwards-compat: older code/tests may still call `search()`.
    # We declare it in the protocol so type-checkers know it exists,
    # but SqliteFtsBackend implements it as a thin alias.
    def search(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        """
        Backwards-compatible alias for search_leads().

        New code should prefer search_leads(); older callers can keep using
        search() without changes.
        """
        ...


class SqliteFtsBackend:
    """
    R21 implementation of SearchBackend using SQLite FTS5.

    This is a thin wrapper over search_people_leads() so that R22's HTTP layer
    can depend on a SearchBackend instead of directly importing the indexing
    module.
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

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        """
        Primary search entrypoint used by R22 (/leads/search) and the cache
        layer. Delegates to search_people_leads().
        """
        return search_people_leads(self._conn, params)

    def search(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        """
        Backwards-compatible alias so any existing R21 tests or callers that
        still use backend.search(...) keep working.
        """
        return self.search_leads(params)

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
