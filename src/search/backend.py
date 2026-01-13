# src/search/backend.py
from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from src.verify.labels import choose_primary_index, compute_verify_label_from_row

from .indexing import FacetCounts, LeadSearchParams, compute_facets, search_people_leads


@dataclass
class SearchResult:
    """
    Container for search results returned by a SearchBackend.

    Attributes:
        leads:
            The list of lead dicts.
        next_cursor:
            Opaque keyset pagination cursor for fetching the next page, or None.
        facets:
            Optional facet counts keyed by facet name.
    """

    leads: list[dict[str, Any]]
    next_cursor: str | None
    facets: FacetCounts | None = None


class SearchBackend(Protocol):
    """
    Abstract interface for a lead search backend.
    """

    def search(self, params: LeadSearchParams) -> SearchResult: ...

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]: ...

    def index_batch(self, docs: Iterable[dict[str, Any]]) -> None: ...


def _is_first_page(params: LeadSearchParams) -> bool:
    """
    Facets are typically computed only on the first page (no keyset cursor).
    """
    return (
        getattr(params, "cursor_icp", None) is None
        and getattr(params, "cursor_verified_at", None) is None
        and getattr(params, "cursor_person_id", None) is None
    )


def _annotate_verify_labels(leads: list[dict[str, Any]]) -> None:
    """
    O26 helper: mutate leads in-place to add verify_label and is_primary_for_person
    flags where possible.
    """
    if not leads:
        return

    by_person: dict[Any, list[int]] = defaultdict(list)

    for idx, lead in enumerate(leads):
        person_id = lead.get("person_id")
        if person_id is None:
            lead["verify_label"] = compute_verify_label_from_row(lead, is_primary=None)
        else:
            by_person[person_id].append(idx)

    for _, indices in by_person.items():
        if not indices:
            continue

        group_rows = [leads[i] for i in indices]
        primary_idx = choose_primary_index(group_rows)

        for rel_idx, global_idx in enumerate(indices):
            lead = leads[global_idx]

            if primary_idx is None:
                lead["verify_label"] = compute_verify_label_from_row(lead, is_primary=None)
                continue

            is_primary = rel_idx == primary_idx
            lead["verify_label"] = compute_verify_label_from_row(lead, is_primary=is_primary)
            lead["is_primary_for_person"] = is_primary


class SqliteFtsBackend:
    """
    R21/R22/R23 implementation of SearchBackend using SQLite FTS5.

    This backend delegates to search_people_leads() + compute_facets() from
    src/search/indexing.py and annotates verify labels (O26).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def cache_namespace(self) -> str:
        try:
            row = self._conn.execute("PRAGMA database_list").fetchone()
        except Exception:
            row = None

        file_path = ""
        if row and len(row) >= 3 and isinstance(row[2], str):
            file_path = row[2].strip()

        if file_path:
            try:
                norm = os.path.normcase(os.path.abspath(file_path))
            except Exception:
                norm = file_path
            return f"sqlite:file:{norm}"

        return f"sqlite:mem:{id(self._conn)}"

    def _annotate_verify_labels(self, leads: list[dict[str, Any]]) -> None:
        """
        O26 helper: mutate leads in-place to add verify_label and
        is_primary_for_person flags where possible.

        Strategy:

          1) For rows without a person_id, compute a simple verify_label based
             only on the row fields (no primary/alternate distinction).

          2) For rows grouped by person_id, use choose_primary_index() to
             select a canonical primary among valid addresses, then recompute
             verify_label for each row in the group with is_primary set
             appropriately.

        This keeps all logic derived from a single pass over the already
        fetched search results; no additional SQL is executed here.
        """
        if not leads:
            return

        # First pass: group rows by person_id, and give standalone rows a base
        # verify_label so they still get something even if they are not part
        # of a person group.
        by_person: dict[Any, list[int]] = defaultdict(list)

        for idx, lead in enumerate(leads):
            person_id = lead.get("person_id")
            if person_id is None:
                # No person context; we can still compute a label, but we
                # cannot mark primary vs alternate.
                lead["verify_label"] = compute_verify_label_from_row(
                    lead,
                    is_primary=None,
                )
            else:
                by_person[person_id].append(idx)

        # Second pass: for each person with one or more rows, choose a primary
        # among valid addresses (if any) and assign labels accordingly.
        for _, indices in by_person.items():
            if not indices:
                continue

            group_rows = [leads[i] for i in indices]
            primary_idx = choose_primary_index(group_rows)

            for rel_idx, global_idx in enumerate(indices):
                lead = leads[global_idx]

                # If there were no valid addresses for this person, we still
                # want a coarse verify_label, but there is no concept of
                # primary vs alternate.
                if primary_idx is None:
                    label = compute_verify_label_from_row(lead, is_primary=None)
                    lead["verify_label"] = label
                    # Do not set is_primary_for_person in this case.
                    continue

                is_primary = rel_idx == primary_idx
                label = compute_verify_label_from_row(lead, is_primary=is_primary)
                lead["verify_label"] = label
                lead["is_primary_for_person"] = is_primary

    def search(self, params: LeadSearchParams) -> SearchResult:
        leads = search_people_leads(self._conn, params)

        _annotate_verify_labels(leads)

        next_cursor: str | None = None

        facets: FacetCounts | None = None
        if getattr(params, "facets", None) and _is_first_page(params):
            facets = compute_facets(self._conn, params)

        return SearchResult(leads=leads, next_cursor=next_cursor, facets=facets)

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        return self.search(params).leads

    def index_batch(self, docs: Iterable[dict[str, Any]]) -> None:
        _ = list(docs)  # no-op for SQLite FTS backend


class PostgresSearchBackend:
    """
    Postgres-backed SearchBackend using:
      - lead_search_docs (O14) as the search document table, and
      - v_emails_latest for verification enrichment.

    This backend is tenant-filtered.

    Requirements:
      - DATABASE_URL/DB_URL points at Postgres
      - lead_search_docs exists (recommended); otherwise it falls back to
        a basic ILIKE search on v_emails_latest.
    """

    _VERIFIED_TS_EXPR = (
        "COALESCE(NULLIF(v.verified_at, '')::timestamptz, NULLIF(v.checked_at, '')::timestamptz)"
    )

    def __init__(self, conn: Any, *, default_tenant_id: str | None = None) -> None:
        self._conn = conn
        self._default_tenant_id = (
            default_tenant_id or os.getenv("TENANT_ID") or "dev"
        ).strip() or "dev"
        self._cols_cache: dict[str, set[str]] = {}

    # ----------------------------
    # Introspection / utilities
    # ----------------------------

    def _table_columns(self, table: str) -> set[str]:
        t = (table or "").strip()
        if not t:
            return set()
        if t in self._cols_cache:
            return self._cols_cache[t]

        cols: set[str] = set()
        try:
            cur = self._conn.execute(f"PRAGMA table_info({t})")
            rows = cur.fetchall() or []
            for r in rows:
                # (cid, name, type, notnull, dflt_value, pk)
                if r and len(r) >= 2 and r[1]:
                    cols.add(str(r[1]))
        except Exception:
            cols = set()

        self._cols_cache[t] = cols
        return cols

    def cache_namespace(self) -> str:
        """
        Stable namespace for caching /leads/search results (Postgres).

        We avoid including secrets (passwords). We also include the default tenant
        to prevent accidental cross-tenant cache collisions in early deployments
        where tenant_id is not yet passed through LeadSearchParams.
        """
        url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip()
        if not url:
            return f"postgres:unknown:{self._default_tenant_id}"

        # Avoid importing urllib.parse to keep this file dependency-light.
        # We conservatively redact anything after '@' in the authority.
        safe = url
        if "@" in safe and "://" in safe:
            prefix, rest = safe.split("://", 1)
            if "@" in rest:
                rest = rest.split("@", 1)[1]
            safe = f"{prefix}://{rest}"
        return f"postgres:{safe}:{self._default_tenant_id}"

    def _tenant_id(self, params: LeadSearchParams) -> str:
        # Future-proof: allow params.tenant_id if you later add it.
        t = getattr(params, "tenant_id", None)
        if isinstance(t, str) and t.strip():
            return t.strip()
        return self._default_tenant_id

    @staticmethod
    def _rows_to_dicts(cur: Any) -> list[dict[str, Any]]:
        rows = cur.fetchall() or []
        desc = getattr(cur, "description", None)
        if not desc:
            # Best-effort: if rows are already dict-like, return as-is
            out: list[dict[str, Any]] = []
            for r in rows:
                if isinstance(r, dict):
                    out.append(r)
                else:
                    try:
                        out.append(dict(r))  # type: ignore[arg-type]
                    except Exception:
                        out.append({})
            return out

        names = [str(d[0]) for d in desc]
        out2: list[dict[str, Any]] = []
        for r in rows:
            if isinstance(r, dict):
                out2.append(r)
            elif isinstance(r, tuple):
                lim = min(len(names), len(r))
                out2.append({names[i]: r[i] for i in range(lim)})
            else:
                try:
                    rr = tuple(r)  # type: ignore[arg-type]
                    lim = min(len(names), len(rr))
                    out2.append({names[i]: rr[i] for i in range(lim)})
                except Exception:
                    out2.append({})
        return out2

    @staticmethod
    def _sql_in(values: list[Any]) -> tuple[str, list[Any]]:
        if not values:
            return "(NULL)", []
        return "(" + ", ".join(["?"] * len(values)) + ")", list(values)

    @staticmethod
    def _append_if_present(
        cols: set[str],
        insert_cols: list[str],
        insert_vals: list[Any],
        col: str,
        val: Any,
    ) -> None:
        if col in cols:
            insert_cols.append(col)
            insert_vals.append(val)

    # ----------------------------
    # Query builder helpers
    # ----------------------------

    def _append_tenant_filter(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        tenant_id: str,
        has_lsd: bool,
    ) -> None:
        where.append("lsd.tenant_id = ?" if has_lsd else "v.tenant_id = ?")
        sql_params.append(tenant_id)

    def _append_query_predicate(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        query: str,
        has_lsd: bool,
        lsd_cols: set[str],
    ) -> None:
        if has_lsd and "doc_tsv" in lsd_cols:
            where.append("lsd.doc_tsv @@ websearch_to_tsquery('english', ?)")
            sql_params.append(query)
            return

        # Fallback: very basic ILIKE search over a few fields in v_emails_latest
        like = f"%{query}%"
        where.append(
            "(v.email ILIKE ? OR v.full_name ILIKE ? OR v.company_name ILIKE ? OR v.title ILIKE ?)"
        )
        sql_params.extend([like, like, like, like])

    def _append_verify_status_filter(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        params: LeadSearchParams,
        has_lsd: bool,
        lsd_cols: set[str],
    ) -> None:
        verify_status = list(getattr(params, "verify_status", []) or [])
        if not verify_status:
            return

        col = "v.verify_status"
        if has_lsd and "verify_status" in lsd_cols:
            col = "lsd.verify_status"

        in_sql, in_params = self._sql_in([str(x) for x in verify_status])
        where.append(f"{col} IN {in_sql}")
        sql_params.extend(in_params)

    def _append_icp_filter(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        params: LeadSearchParams,
        has_lsd: bool,
        lsd_cols: set[str],
    ) -> None:
        icp_min = getattr(params, "icp_min", None)
        if icp_min is None:
            return

        col = "v.icp_score"
        if has_lsd and "icp_score" in lsd_cols:
            col = "lsd.icp_score"

        where.append(f"{col} >= ?")
        sql_params.append(int(icp_min))

    def _append_in_filter_if_present(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        values: list[Any],
        col_expr: str,
    ) -> None:
        if not values:
            return
        in_sql, in_params = self._sql_in([str(x) for x in values])
        where.append(f"{col_expr} IN {in_sql}")
        sql_params.extend(in_params)

    def _append_source_filter(self, where: list[str], *, params: LeadSearchParams) -> None:
        raw = getattr(params, "source", []) or []
        source: list[str] = []
        for x in raw:
            s = str(x).strip().lower()
            if s:
                source.append(s)

        if not source:
            return

        want_published = "published" in source
        want_generated = "generated" in source
        if want_published and not want_generated:
            where.append("COALESCE(v.is_published, 0) = 1")
        elif want_generated and not want_published:
            where.append("COALESCE(v.is_published, 0) = 0")

    def _append_recency_filter(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        params: LeadSearchParams,
    ) -> None:
        recency_days = getattr(params, "recency_days", None)
        if recency_days is None:
            return

        where.append(self._VERIFIED_TS_EXPR + " >= NOW() - (? || ' days')::interval")
        sql_params.append(int(recency_days))

    def _append_cursor_constraints(
        self,
        where: list[str],
        sql_params: list[Any],
        *,
        params: LeadSearchParams,
        has_lsd: bool,
        lsd_cols: set[str],
    ) -> None:
        sort = (getattr(params, "sort", "") or "icp_desc").strip()
        cursor_person_id = getattr(params, "cursor_person_id", None)
        if cursor_person_id is None:
            return

        pid_expr = "lsd.person_id" if has_lsd else "v.person_id"

        if sort == "icp_desc":
            cursor_icp = getattr(params, "cursor_icp", None)
            if cursor_icp is None:
                return

            icp_expr = "COALESCE(v.icp_score, -1)"
            if has_lsd and "icp_score" in lsd_cols:
                icp_expr = "COALESCE(lsd.icp_score, -1)"

            where.append(f"({icp_expr} < ? OR ({icp_expr} = ? AND {pid_expr} < ?))")
            sql_params.extend([int(cursor_icp), int(cursor_icp), int(cursor_person_id)])
            return

        cursor_verified_at = getattr(params, "cursor_verified_at", None)
        if cursor_verified_at is None:
            return

        ts_expr = self._VERIFIED_TS_EXPR
        where.append(
            f"({ts_expr} < ?::timestamptz OR ({ts_expr} = ?::timestamptz AND {pid_expr} < ?))"
        )
        sql_params.extend([str(cursor_verified_at), str(cursor_verified_at), int(cursor_person_id)])

    # ----------------------------
    # Query builder
    # ----------------------------

    def _build_base_from_where(
        self,
        params: LeadSearchParams,
        *,
        include_cursor: bool,
    ) -> tuple[str, list[Any]]:
        tenant_id = self._tenant_id(params)

        lsd_cols = self._table_columns("lead_search_docs")
        has_lsd = bool(lsd_cols)

        where: list[str] = []
        sql_params: list[Any] = []

        self._append_tenant_filter(where, sql_params, tenant_id=tenant_id, has_lsd=has_lsd)

        query = (getattr(params, "query", "") or "").strip()
        self._append_query_predicate(
            where,
            sql_params,
            query=query,
            has_lsd=has_lsd,
            lsd_cols=lsd_cols,
        )

        self._append_verify_status_filter(
            where,
            sql_params,
            params=params,
            has_lsd=has_lsd,
            lsd_cols=lsd_cols,
        )
        self._append_icp_filter(
            where,
            sql_params,
            params=params,
            has_lsd=has_lsd,
            lsd_cols=lsd_cols,
        )

        if has_lsd and "role_family" in lsd_cols:
            roles = list(getattr(params, "roles", []) or [])
            self._append_in_filter_if_present(
                where,
                sql_params,
                values=roles,
                col_expr="lsd.role_family",
            )

        if has_lsd and "seniority" in lsd_cols:
            seniority = list(getattr(params, "seniority", []) or [])
            self._append_in_filter_if_present(
                where,
                sql_params,
                values=seniority,
                col_expr="lsd.seniority",
            )

        if has_lsd and "company_industry" in lsd_cols:
            industries = list(getattr(params, "industries", []) or [])
            self._append_in_filter_if_present(
                where,
                sql_params,
                values=industries,
                col_expr="lsd.company_industry",
            )

        if has_lsd and "company_size_bucket" in lsd_cols:
            sizes = list(getattr(params, "sizes", []) or [])
            self._append_in_filter_if_present(
                where,
                sql_params,
                values=sizes,
                col_expr="lsd.company_size_bucket",
            )

        self._append_source_filter(where, params=params)
        self._append_recency_filter(where, sql_params, params=params)

        if include_cursor:
            self._append_cursor_constraints(
                where,
                sql_params,
                params=params,
                has_lsd=has_lsd,
                lsd_cols=lsd_cols,
            )

        base_where_sql = " AND ".join(where) if where else "TRUE"
        return base_where_sql, sql_params

    def _build_search_sql(self, params: LeadSearchParams) -> tuple[str, list[Any]]:
        lsd_cols = self._table_columns("lead_search_docs")
        has_lsd = bool(lsd_cols)

        base_where_sql, sql_params = self._build_base_from_where(params, include_cursor=True)

        sort = (getattr(params, "sort", "") or "icp_desc").strip()
        limit = int(getattr(params, "limit", 50) or 50)

        if has_lsd:
            # Prefer company attrs column if present (common in this codebase)
            c_cols = self._table_columns("companies")
            company_attrs_expr = "c.attrs" if "attrs" in c_cols else "NULL"

            industry_expr = "lsd.company_industry" if "company_industry" in lsd_cols else "NULL"
            size_expr = "lsd.company_size_bucket" if "company_size_bucket" in lsd_cols else "NULL"

            role_expr = "lsd.role_family" if "role_family" in lsd_cols else "NULL"
            seniority_expr = "lsd.seniority" if "seniority" in lsd_cols else "NULL"

            icp_expr = "lsd.icp_score" if "icp_score" in lsd_cols else "NULL"

            if sort == "verified_desc":
                order_sql = f"ORDER BY {self._VERIFIED_TS_EXPR} DESC NULLS LAST, lsd.person_id DESC"
            else:
                order_sql = "ORDER BY COALESCE(lsd.icp_score, -1) DESC, lsd.person_id DESC"

            sql = f"""
                SELECT
                    lsd.email AS email,
                    p.first_name AS first_name,
                    p.last_name AS last_name,
                    p.full_name AS full_name,
                    p.title AS title,

                    {role_expr} AS role_family,
                    {seniority_expr} AS seniority,

                    c.name AS company,
                    c.id AS company_id,
                    LOWER(SPLIT_PART(lsd.email, '@', 2)) AS company_domain,

                    {industry_expr} AS industry,
                    {size_expr} AS company_size,
                    {company_attrs_expr} AS company_attrs,

                    {icp_expr} AS icp_score,

                    COALESCE(v.verify_status, lsd.verify_status) AS verify_status,
                    v.verify_reason AS verify_reason,
                    v.verified_at AS verified_at,

                    CASE
                        WHEN COALESCE(v.is_published, 0) = 1 THEN 'published'
                        ELSE 'generated'
                    END AS source,
                    v.source_url AS source_url,

                    lsd.person_id AS person_id
                FROM lead_search_docs AS lsd
                JOIN people AS p
                  ON p.id = lsd.person_id
                 AND p.tenant_id = lsd.tenant_id
                JOIN companies AS c
                  ON c.id = p.company_id
                 AND c.tenant_id = lsd.tenant_id
                LEFT JOIN v_emails_latest AS v
                  ON v.tenant_id = lsd.tenant_id
                 AND v.person_id = lsd.person_id
                 AND v.email = lsd.email
                WHERE {base_where_sql}
                {order_sql}
                LIMIT ?
            """
            sql_params2 = list(sql_params) + [limit]
            return sql, sql_params2

        # Fallback mode: v_emails_latest-only
        if sort == "verified_desc":
            order_sql2 = f"ORDER BY {self._VERIFIED_TS_EXPR} DESC NULLS LAST, v.person_id DESC"
        else:
            order_sql2 = "ORDER BY COALESCE(v.icp_score, -1) DESC, v.person_id DESC"

        sql2 = f"""
            SELECT
                v.email AS email,
                v.first_name AS first_name,
                v.last_name AS last_name,
                v.full_name AS full_name,
                v.title AS title,

                NULL AS role_family,
                NULL AS seniority,

                v.company_name AS company,
                v.company_id AS company_id,
                v.company_domain AS company_domain,

                NULL AS industry,
                NULL AS company_size,
                NULL AS company_attrs,

                v.icp_score AS icp_score,

                v.verify_status AS verify_status,
                v.verify_reason AS verify_reason,
                v.verified_at AS verified_at,

                CASE
                    WHEN COALESCE(v.is_published, 0) = 1 THEN 'published'
                    ELSE 'generated'
                END AS source,
                v.source_url AS source_url,

                v.person_id AS person_id
            FROM v_emails_latest AS v
            WHERE {base_where_sql}
            {order_sql2}
            LIMIT ?
        """
        return sql2, list(sql_params) + [limit]

    def _compute_facets(self, params: LeadSearchParams) -> FacetCounts:
        requested = [str(x) for x in (getattr(params, "facets", None) or []) if str(x)]
        if not requested:
            return {}

        lsd_cols = self._table_columns("lead_search_docs")
        has_lsd = bool(lsd_cols)

        # We compute facets from the unpaginated (no cursor) filtered set.
        base_where_sql, sql_params = self._build_base_from_where(params, include_cursor=False)

        facets: FacetCounts = {}

        # We only support a subset that maps cleanly to our Postgres query.
        # Unknown facets are ignored (safe forward compatibility).
        for facet in requested:
            facet_key = facet.strip()
            if not facet_key:
                continue

            if facet_key == "verify_status":
                expr = "v.verify_status"
                if has_lsd:
                    expr = "COALESCE(v.verify_status, lsd.verify_status)"
            elif facet_key == "role_family" and has_lsd and "role_family" in lsd_cols:
                expr = "lsd.role_family"
            elif facet_key == "seniority" and has_lsd and "seniority" in lsd_cols:
                expr = "lsd.seniority"
            elif facet_key == "industries" and has_lsd and "company_industry" in lsd_cols:
                expr = "lsd.company_industry"
            elif facet_key == "sizes" and has_lsd and "company_size_bucket" in lsd_cols:
                expr = "lsd.company_size_bucket"
            elif facet_key == "source":
                expr = (
                    "CASE "
                    "WHEN COALESCE(v.is_published, 0) = 1 THEN 'published' "
                    "ELSE 'generated' "
                    "END"
                )
            elif facet_key == "icp_bucket":
                icp_col = "v.icp_score"
                if has_lsd and "icp_score" in lsd_cols:
                    icp_col = "lsd.icp_score"
                expr = (
                    "CASE "
                    f"WHEN {icp_col} IS NULL THEN NULL "
                    f"WHEN {icp_col} < 40 THEN '0-39' "
                    f"WHEN {icp_col} < 60 THEN '40-59' "
                    f"WHEN {icp_col} < 80 THEN '60-79' "
                    "ELSE '80-100' "
                    "END"
                )
            else:
                continue

            if has_lsd:
                facet_sql = f"""
                    SELECT
                        val AS value,
                        COUNT(*) AS count
                    FROM (
                        SELECT {expr} AS val
                        FROM lead_search_docs AS lsd
                        JOIN people AS p
                          ON p.id = lsd.person_id
                         AND p.tenant_id = lsd.tenant_id
                        JOIN companies AS c
                          ON c.id = p.company_id
                         AND c.tenant_id = lsd.tenant_id
                        LEFT JOIN v_emails_latest AS v
                          ON v.tenant_id = lsd.tenant_id
                         AND v.person_id = lsd.person_id
                         AND v.email = lsd.email
                        WHERE {base_where_sql}
                    ) AS t
                    WHERE val IS NOT NULL AND val <> ''
                    GROUP BY val
                    ORDER BY count DESC, value ASC
                    LIMIT 200
                """
            else:
                facet_sql = f"""
                    SELECT
                        val AS value,
                        COUNT(*) AS count
                    FROM (
                        SELECT {expr} AS val
                        FROM v_emails_latest AS v
                        WHERE {base_where_sql}
                    ) AS t
                    WHERE val IS NOT NULL AND val <> ''
                    GROUP BY val
                    ORDER BY count DESC, value ASC
                    LIMIT 200
                """

            cur = self._conn.execute(facet_sql, tuple(sql_params))
            rows = self._rows_to_dicts(cur)
            facets[facet_key] = [
                {"value": r.get("value"), "count": int(r.get("count") or 0)} for r in rows
            ]

        return facets

    # ----------------------------
    # SearchBackend API
    # ----------------------------

    def search(self, params: LeadSearchParams) -> SearchResult:
        sql, sql_params = self._build_search_sql(params)
        cur = self._conn.execute(sql, tuple(sql_params))
        leads = self._rows_to_dicts(cur)

        _annotate_verify_labels(leads)

        facets: FacetCounts | None = None
        if getattr(params, "facets", None) and _is_first_page(params):
            facets = self._compute_facets(params)

        return SearchResult(leads=leads, next_cursor=None, facets=facets)

    def search_leads(self, params: LeadSearchParams) -> list[dict[str, Any]]:
        return self.search(params).leads

    def index_batch(self, docs: Iterable[dict[str, Any]]) -> None:
        """
        Best-effort upsert for lead_search_docs (optional).

        If you already backfill via scripts/backfill_o14_lead_search_docs.py and/or
        maintain docs via triggers, you can ignore this.
        """
        cols = self._table_columns("lead_search_docs")
        if not cols:
            return

        docs_list = list(docs)
        if not docs_list:
            return

        for d in docs_list:
            if not isinstance(d, dict):
                continue
            person_id = d.get("person_id")
            tenant_id = d.get("tenant_id") or self._default_tenant_id
            if person_id is None:
                continue

            insert_cols: list[str] = []
            insert_vals: list[Any] = []

            self._append_if_present(cols, insert_cols, insert_vals, "person_id", int(person_id))
            self._append_if_present(cols, insert_cols, insert_vals, "tenant_id", str(tenant_id))
            self._append_if_present(cols, insert_cols, insert_vals, "email", d.get("email"))
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "verify_status",
                d.get("verify_status"),
            )
            self._append_if_present(cols, insert_cols, insert_vals, "icp_score", d.get("icp_score"))
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "role_family",
                d.get("role_family"),
            )
            self._append_if_present(cols, insert_cols, insert_vals, "seniority", d.get("seniority"))
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "company_size_bucket",
                d.get("company_size_bucket"),
            )
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "company_industry",
                d.get("company_industry"),
            )
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "icp_bucket",
                d.get("icp_bucket"),
            )
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "created_at",
                d.get("created_at"),
            )
            self._append_if_present(
                cols,
                insert_cols,
                insert_vals,
                "updated_at",
                d.get("updated_at"),
            )

            # Optional doc text -> doc_tsv
            doc_text = d.get("doc_text") or d.get("doc") or d.get("text")
            if "doc_tsv" in cols and doc_text:
                # Use to_tsvector on the server
                # We'll include doc_tsv as an expression in SQL, not a value placeholder.
                pass

            if not insert_cols:
                continue

            # Build INSERT ... ON CONFLICT(person_id) DO UPDATE SET ...
            ph = ", ".join(["?"] * len(insert_cols))
            cols_sql = ", ".join(insert_cols)

            updates: list[str] = []
            for c in insert_cols:
                if c == "person_id":
                    continue
                updates.append(f"{c} = EXCLUDED.{c}")

            sql = (
                f"INSERT INTO lead_search_docs ({cols_sql}) VALUES ({ph}) "
                "ON CONFLICT (person_id) DO UPDATE SET "
                + (", ".join(updates) if updates else "person_id = EXCLUDED.person_id")
            )

            self._conn.execute(sql, tuple(insert_vals))


def build_search_backend(
    *,
    sqlite_conn: sqlite3.Connection | None = None,
    sqlite_path: str | None = None,
) -> SearchBackend:
    """
    Convenience factory:

      - If DATABASE_URL/DB_URL points at Postgres, returns PostgresSearchBackend(get_conn()).
      - Otherwise returns SqliteFtsBackend(sqlite_conn or get_connection(sqlite_path)).

    This lets the HTTP layer switch without needing to embed backend selection logic.
    """
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    is_pg = url.startswith("postgres://") or url.startswith("postgresql://")

    if is_pg:
        from src.db import get_conn  # local import to avoid cycles

        return PostgresSearchBackend(get_conn())

    if sqlite_conn is not None:
        return SqliteFtsBackend(sqlite_conn)

    from src.db import get_connection  # local import to avoid cycles

    path = sqlite_path or os.getenv("DB_PATH") or os.getenv("DATABASE_PATH") or "data/dev.db"
    return SqliteFtsBackend(get_connection(path))
