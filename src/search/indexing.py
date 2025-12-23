from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any

from src.config import FACET_USE_MV

FacetCounts = dict[str, list[dict[str, object]]]


@dataclass
class LeadSearchParams:
    """
    Parameters for lead search.

    Attributes:
        query:
            Full-text search query (FTS5 syntax). Must be a non-empty string for
            now; empty / None queries are rejected to avoid full scans.

        verify_status:
            Optional list of verify_status values to include. If provided, only
            leads whose latest verification status is in this list are returned.

        icp_min:
            Optional minimum ICP score threshold. If provided, only leads with
            people.icp_score >= icp_min are returned.

        roles:
            Optional list of canonical role_family values to include, e.g.
            ["sales", "marketing", "revops"].

        seniority:
            Optional list of canonical seniority values to include, e.g.
            ["director", "vp", "cxo"].

        industries:
            Optional list of company industry labels to include, e.g.
            ["B2B SaaS", "Fintech"]. Backed by companies.attrs (JSON) or a
            denormalized column, depending on schema.

        sizes:
            Optional list of company size buckets to include, e.g.
            ["1-10", "11-50", "51-200"]. Backed by companies.attrs.

        tech:
            Optional list of tech keywords to filter on, e.g.
            ["salesforce", "hubspot"]. Backed by companies.attrs.tech_keywords
            (JSON array) or a simple LIKE-based search over attrs.

        source:
            Optional list of lead sources to include, e.g.
            ["published", "generated"]. Backed by the v_emails_latest.source
            (or equivalent) field.

        recency_days:
            Optional integer number of days for a recency filter. If provided,
            only leads whose verified_at is within the last recency_days (based
            on UTC now) are returned.

        sort:
            Sort order for results. Currently supported values:
              - "icp_desc" (default): icp_score DESC, person_id ASC
              - "verified_desc": verified_at DESC, person_id ASC

            Unknown values should be rejected by callers (e.g. API layer) and
            will also raise ValueError here.

        limit:
            Maximum number of rows to return. The caller is responsible for
            clamping this to a reasonable range (e.g. 1–100) if exposed over
            HTTP.

        cursor_icp:
            Keyset pagination cursor component for icp_desc. When sort == "icp_desc"
            and both cursor_icp and cursor_person_id are set, the query continues
            from strictly after that (icp_score, person_id) tuple.

        cursor_verified_at:
            Keyset pagination cursor component for verified_desc. When sort ==
            "verified_desc" and both cursor_verified_at and cursor_person_id
            are set, the query continues from strictly after that
            (verified_at, person_id) tuple.

        cursor_person_id:
            Keyset pagination cursor component common to both sort modes. Used
            as a stable tiebreaker within a given primary sort key.

        facets:
            Optional list of facet names (e.g. "verify_status", "icp_bucket")
            to compute counts for under the current filters.
    """

    query: str | None = None

    verify_status: Sequence[str] | None = None
    icp_min: int | None = None

    roles: Sequence[str] | None = None
    seniority: Sequence[str] | None = None
    industries: Sequence[str] | None = None
    sizes: Sequence[str] | None = None
    tech: Sequence[str] | None = None
    source: Sequence[str] | None = None
    recency_days: int | None = None

    sort: str = "icp_desc"
    limit: int = 50

    # keyset pagination
    cursor_icp: int | None = None
    cursor_verified_at: str | None = None  # ISO timestamp, or None
    cursor_person_id: int | None = None

    # facets requested for this search
    facets: Sequence[str] | None = None


@dataclass
class _FacetSchemaInfo:
    has_company_attrs: bool
    industry_expr: str
    size_expr: str


def _rows_to_dicts(cursor: sqlite3.Cursor, rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row, strict=False)) for row in rows]


def _normalize_fts_query(raw: str) -> str:
    """
    Turn a user query string into a safe FTS5 MATCH expression.

    - If it looks like advanced FTS syntax (boolean ops / column:term), pass
      it through as-is.
    - Otherwise, treat it as a literal phrase by wrapping in double quotes.
      This avoids syntax errors for inputs like domains/emails
      (e.g. "crestwellpartners.com", "user@example.com").
    """
    q = (raw or "").strip()
    if not q:
        return q

    upper = q.upper()
    # Heuristic: if user is clearly using FTS operators or column scoping,
    # respect it and do not wrap.
    if any(op in upper for op in (" AND ", " OR ", " NEAR ")):
        return q
    if ":" in q:
        return q

    # Literal phrase mode: strip embedded double quotes to keep syntax valid.
    q = q.replace('"', " ")
    return f'"{q}"'


def _normalize_query_and_sort(params: LeadSearchParams) -> str:
    """
    Validate query and normalize sort, raising ValueError on invalid input.
    """
    if not params.query or not params.query.strip():
        raise ValueError("LeadSearchParams.query must be a non-empty string")

    sort = params.sort or "icp_desc"
    if sort not in {"icp_desc", "verified_desc"}:
        msg = f"Unsupported sort value for search_people_leads: {sort!r}"
        raise ValueError(msg)
    return sort


def _detect_company_schema(conn: sqlite3.Connection) -> tuple[bool, str, str, str]:
    """
    Detect whether companies.attrs exists and return expressions for industry,
    size_bucket, and attrs.
    """
    has_company_attrs = False
    try:
        cur_meta = conn.execute("PRAGMA table_info(companies)")
        cols = [row[1] for row in cur_meta.fetchall()]
        has_company_attrs = "attrs" in cols
    except sqlite3.Error:
        has_company_attrs = False

    if has_company_attrs:
        industry_expr = "JSON_EXTRACT(c.attrs, '$.industry')"
        size_expr = "JSON_EXTRACT(c.attrs, '$.size_bucket')"
        attrs_expr = "c.attrs"
    else:
        industry_expr = "NULL"
        size_expr = "NULL"
        attrs_expr = "NULL"

    return has_company_attrs, industry_expr, size_expr, attrs_expr


def _detect_ve_schema(conn: sqlite3.Connection) -> tuple[bool, str, str]:
    """
    Detect whether v_emails_latest has source/source_url columns and return
    expressions plus a flag for source availability.
    """
    has_ve_source = False
    has_ve_source_url = False
    try:
        cur_meta = conn.execute("PRAGMA table_info(v_emails_latest)")
        v_cols = [row[1] for row in cur_meta.fetchall()]
        has_ve_source = "source" in v_cols
        has_ve_source_url = "source_url" in v_cols
    except sqlite3.Error:
        has_ve_source = False
        has_ve_source_url = False

    source_expr = "ve.source" if has_ve_source else "NULL"
    source_url_expr = "ve.source_url" if has_ve_source_url else "NULL"
    return has_ve_source, source_expr, source_url_expr


def _build_base_sql(
    industry_expr: str,
    size_expr: str,
    attrs_expr: str,
    source_expr: str,
    source_url_expr: str,
) -> str:
    """
    Build the base SELECT + FROM + JOIN + initial WHERE clause.
    """
    return f"""
        SELECT
          ve.email AS email,
          p.first_name AS first_name,
          p.last_name AS last_name,
          COALESCE(p.full_name, p.first_name || ' ' || p.last_name) AS full_name,
          COALESCE(p.title_norm, p.title) AS title,
          p.role_family AS role_family,
          p.seniority AS seniority,
          p.icp_score AS icp_score,
          p.id AS person_id,
          c.id AS company_id,
          c.name AS company,
          -- Keep old 'domain' key and also expose 'company_domain'
          COALESCE(c.official_domain, c.domain) AS domain,
          COALESCE(c.official_domain, c.domain) AS company_domain,
          -- Company attributes (JSON-backed or denormalized fields)
          {industry_expr} AS industry,
          {size_expr} AS company_size,
          {attrs_expr} AS company_attrs,
          -- Email / verification context
          {source_expr} AS source,
          {source_url_expr} AS source_url,
          ve.verify_status AS verify_status,
          ve.verified_at AS verified_at,
          bm25(people_fts) AS rank
        FROM people_fts
        JOIN people AS p
          ON p.id = people_fts.rowid
        JOIN v_emails_latest AS ve
          ON ve.person_id = p.id
        JOIN companies AS c
          ON c.id = p.company_id
        WHERE people_fts MATCH :query
    """


def _apply_icp_filter(
    params: LeadSearchParams,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if params.icp_min is None:
        return
    conditions.append("p.icp_score IS NOT NULL AND p.icp_score >= :icp_min")
    sql_params["icp_min"] = params.icp_min


def _apply_verify_status_filter(
    params: LeadSearchParams,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not params.verify_status:
        return

    placeholders: list[str] = []
    for idx, status in enumerate(params.verify_status):
        key = f"vs_{idx}"
        placeholders.append(f":{key}")
        sql_params[key] = status
    conditions.append(f"ve.verify_status IN ({', '.join(placeholders)})")


def _apply_roles_filter(
    params: LeadSearchParams,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not params.roles:
        return

    placeholders: list[str] = []
    for idx, role in enumerate(params.roles):
        key = f"role_{idx}"
        placeholders.append(f":{key}")
        sql_params[key] = role
    conditions.append(f"p.role_family IN ({', '.join(placeholders)})")


def _apply_seniority_filter(
    params: LeadSearchParams,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not params.seniority:
        return

    placeholders: list[str] = []
    for idx, s in enumerate(params.seniority):
        key = f"sen_{idx}"
        placeholders.append(f":{key}")
        sql_params[key] = s
    conditions.append(f"p.seniority IN ({', '.join(placeholders)})")


def _apply_industry_filter(
    params: LeadSearchParams,
    has_company_attrs: bool,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not (params.industries and has_company_attrs):
        return

    placeholders: list[str] = []
    for idx, industry in enumerate(params.industries):
        key = f"ind_{idx}"
        placeholders.append(f":{key}")
        sql_params[key] = industry
    conditions.append(
        f"JSON_EXTRACT(c.attrs, '$.industry') IN ({', '.join(placeholders)})",
    )


def _apply_size_filter(
    params: LeadSearchParams,
    has_company_attrs: bool,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not (params.sizes and has_company_attrs):
        return

    placeholders: list[str] = []
    for idx, size in enumerate(params.sizes):
        key = f"size_{idx}"
        placeholders.append(f":{key}")
        sql_params[key] = size
    conditions.append(
        f"JSON_EXTRACT(c.attrs, '$.size_bucket') IN ({', '.join(placeholders)})",
    )


def _apply_tech_filter(
    params: LeadSearchParams,
    has_company_attrs: bool,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not (params.tech and has_company_attrs):
        return

    tech_clauses: list[str] = []
    for idx, tech in enumerate(params.tech):
        key = f"tech_{idx}"
        sql_params[key] = f"%{tech}%"
        tech_clauses.append(f"c.attrs LIKE :{key}")
    if tech_clauses:
        conditions.append("(" + " OR ".join(tech_clauses) + ")")


def _apply_source_filter(
    params: LeadSearchParams,
    has_ve_source: bool,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if not params.source:
        return
    if not has_ve_source:
        # Old schema: can't apply this filter safely; fail loudly so callers
        # (e.g. API) know the DB is missing required columns.
        raise ValueError("source filter requires v_emails_latest.source column")

    placeholders: list[str] = []
    for idx, src in enumerate(params.source):
        key = f"src_{idx}"
        placeholders.append(f":{key}")
        sql_params[key] = src
    conditions.append(f"ve.source IN ({', '.join(placeholders)})")


def _compute_recency_cutoff(recency_days: int) -> str:
    cutoff_dt = datetime.utcnow() - timedelta(days=recency_days)
    return cutoff_dt.isoformat(sep=" ", timespec="seconds")


def _apply_recency_filter(
    params: LeadSearchParams,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if params.recency_days is None:
        return

    cutoff_str = _compute_recency_cutoff(params.recency_days)
    sql_params["recency_cutoff"] = cutoff_str
    conditions.append(
        "ve.verified_at IS NOT NULL AND ve.verified_at >= :recency_cutoff",
    )


def _apply_keyset_pagination(
    sort: str,
    params: LeadSearchParams,
    conditions: list[str],
    sql_params: dict[str, Any],
) -> None:
    if sort == "icp_desc":
        if params.cursor_icp is None or params.cursor_person_id is None:
            return
        sql_params["cursor_icp"] = params.cursor_icp
        sql_params["cursor_person_id"] = params.cursor_person_id
        conditions.append(
            """
            (
              p.icp_score < :cursor_icp
              OR (p.icp_score = :cursor_icp AND p.id > :cursor_person_id)
            )
            """.strip(),
        )
        return

    if sort == "verified_desc":
        if params.cursor_verified_at is None or params.cursor_person_id is None:
            return
        sql_params["cursor_verified_at"] = params.cursor_verified_at
        sql_params["cursor_person_id"] = params.cursor_person_id
        conditions.append(
            """
            (
              ve.verified_at < :cursor_verified_at
              OR (ve.verified_at = :cursor_verified_at AND p.id > :cursor_person_id)
            )
            """.strip(),
        )


def _build_order_by(sort: str) -> str:
    if sort == "icp_desc":
        return """
            ORDER BY
              p.icp_score DESC,
              p.id ASC
        """
    # sort == "verified_desc"
    return """
            ORDER BY
              ve.verified_at DESC,
              p.id ASC
        """


def search_people_leads(conn: sqlite3.Connection, params: LeadSearchParams) -> list[dict[str, Any]]:
    """
    Perform a full-text search over people_fts + joins to people, companies, v_emails_latest.

    This is the main helper R22's /leads/search will call on SQLite. It:

      * Uses FTS5 (people_fts) to match the text query.
      * Joins back to people, companies, v_emails_latest for metadata and filters.
      * Applies filters for verify_status, icp_min, roles, seniority, industry,
        size, tech, source, and recency_days when provided.
      * Applies sort + keyset pagination based on params.sort and cursor_*.
      * Returns a list of plain dicts suitable for JSON serialization.

    Returned dict keys (subject to the underlying schema) include at least:
      - email
      - first_name
      - last_name
      - full_name
      - title
      - role_family
      - seniority
      - company
      - company_id
      - domain           (backwards-compat alias)
      - company_domain   (for R22 API)
      - industry
      - company_size
      - company_attrs   (raw JSON, if available)
      - source
      - source_url
      - verify_status
      - verified_at
      - icp_score
      - person_id
      - rank            (FTS bm25 score; lower is "more relevant")
    """
    sort = _normalize_query_and_sort(params)

    has_company_attrs, industry_expr, size_expr, attrs_expr = _detect_company_schema(conn)
    has_ve_source, source_expr, source_url_expr = _detect_ve_schema(conn)

    base_sql = _build_base_sql(industry_expr, size_expr, attrs_expr, source_expr, source_url_expr)

    match_query = _normalize_fts_query(params.query or "")

    sql_params: dict[str, Any] = {
        "query": match_query,
        "limit": params.limit,
    }
    conditions: list[str] = []

    _apply_icp_filter(params, conditions, sql_params)
    _apply_verify_status_filter(params, conditions, sql_params)
    _apply_roles_filter(params, conditions, sql_params)
    _apply_seniority_filter(params, conditions, sql_params)
    _apply_industry_filter(params, has_company_attrs, conditions, sql_params)
    _apply_size_filter(params, has_company_attrs, conditions, sql_params)
    _apply_tech_filter(params, has_company_attrs, conditions, sql_params)
    _apply_source_filter(params, has_ve_source, conditions, sql_params)
    _apply_recency_filter(params, conditions, sql_params)
    _apply_keyset_pagination(sort, params, conditions, sql_params)

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    order_by = _build_order_by(sort)
    base_sql += f"""
        {order_by}
        LIMIT :limit
    """

    cur = conn.execute(base_sql, sql_params)
    rows = cur.fetchall()
    return _rows_to_dicts(cur, rows)


def _build_join_facet_base_sql(
    conn: sqlite3.Connection,
    params: LeadSearchParams,
) -> tuple[str, dict[str, Any], _FacetSchemaInfo]:
    """
    Build the FROM/JOIN/WHERE clause used for facet counts when we are not
    using the materialized view. This mirrors the filters used by
    search_people_leads, but without keyset pagination or ordering.
    """
    has_company_attrs, industry_expr, size_expr, _attrs_expr = _detect_company_schema(conn)
    has_ve_source, _source_expr, _source_url_expr = _detect_ve_schema(conn)

    base_sql = """
        FROM people_fts
        JOIN people AS p
          ON p.id = people_fts.rowid
        JOIN v_emails_latest AS ve
          ON ve.person_id = p.id
        JOIN companies AS c
          ON c.id = p.company_id
        WHERE 1=1
    """
    sql_params: dict[str, Any] = {}
    conditions: list[str] = []

    if params.query and params.query.strip():
        conditions.append("people_fts MATCH :query")
        sql_params["query"] = _normalize_fts_query(params.query)

    _apply_icp_filter(params, conditions, sql_params)
    _apply_verify_status_filter(params, conditions, sql_params)
    _apply_roles_filter(params, conditions, sql_params)
    _apply_seniority_filter(params, conditions, sql_params)
    _apply_industry_filter(params, has_company_attrs, conditions, sql_params)
    _apply_size_filter(params, has_company_attrs, conditions, sql_params)
    _apply_tech_filter(params, has_company_attrs, conditions, sql_params)
    _apply_source_filter(params, has_ve_source, conditions, sql_params)
    _apply_recency_filter(params, conditions, sql_params)

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    schema_info = _FacetSchemaInfo(
        has_company_attrs=has_company_attrs,
        industry_expr=industry_expr,
        size_expr=size_expr,
    )
    return base_sql, sql_params, schema_info


def _run_join_facet_query(
    conn: sqlite3.Connection,
    facet_name: str,
    base_sql: str,
    base_params: dict[str, Any],
    schema_info: _FacetSchemaInfo,
) -> list[dict[str, object]] | None:
    """
    Execute a facet GROUP BY over the main search joins. Returns a list of
    {value, count} dicts, or None if the facet is unknown.
    """
    if facet_name == "verify_status":
        sql = f"""
            SELECT ve.verify_status AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY ve.verify_status
            ORDER BY count DESC
        """
    elif facet_name == "icp_bucket":
        sql = f"""
            SELECT
              CASE
                WHEN p.icp_score >= 80 THEN '80-100'
                WHEN p.icp_score >= 60 THEN '60-79'
                WHEN p.icp_score >= 40 THEN '40-59'
                ELSE '0-39'
              END AS value,
              COUNT(*) AS count
            {base_sql}
            GROUP BY value
            ORDER BY value
        """
    elif facet_name == "role_family":
        sql = f"""
            SELECT p.role_family AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY p.role_family
            ORDER BY count DESC
        """
    elif facet_name == "seniority":
        sql = f"""
            SELECT p.seniority AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY p.seniority
            ORDER BY count DESC
        """
    elif facet_name == "company_size_bucket":
        if not schema_info.has_company_attrs:
            return []
        sql = f"""
            SELECT {schema_info.size_expr} AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY value
            ORDER BY count DESC
        """
    elif facet_name == "company_industry":
        if not schema_info.has_company_attrs:
            return []
        sql = f"""
            SELECT {schema_info.industry_expr} AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY value
            ORDER BY count DESC
        """
    else:
        # Unknown facet (including tech_keyword for now): ignore.
        return None

    cur = conn.execute(sql, base_params)
    rows = cur.fetchall()
    dict_rows = _rows_to_dicts(cur, rows)
    return [
        {"value": row["value"], "count": row["count"]}
        for row in dict_rows
        if row["value"] is not None
    ]


def _build_mv_base_sql(params: LeadSearchParams) -> tuple[str, dict[str, Any]]:
    """
    Build the FROM/WHERE clause for facet counts over the O14 materialized
    view lead_search_docs. This table is expected to contain the columns needed
    for filtering & faceting; we intentionally do not join back to the full
    search graph here.
    """
    base_sql = """
        FROM lead_search_docs AS d
        WHERE 1=1
    """
    sql_params: dict[str, Any] = {}
    conditions: list[str] = []

    if params.icp_min is not None:
        conditions.append("d.icp_score IS NOT NULL AND d.icp_score >= :icp_min")
        sql_params["icp_min"] = params.icp_min

    if params.verify_status:
        placeholders = []
        for idx, status in enumerate(params.verify_status):
            key = f"vs_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = status
        conditions.append(f"d.verify_status IN ({', '.join(placeholders)})")

    if params.roles:
        placeholders = []
        for idx, role in enumerate(params.roles):
            key = f"role_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = role
        conditions.append(f"d.role_family IN ({', '.join(placeholders)})")

    if params.seniority:
        placeholders = []
        for idx, s in enumerate(params.seniority):
            key = f"sen_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = s
        conditions.append(f"d.seniority IN ({', '.join(placeholders)})")

    if params.industries:
        placeholders = []
        for idx, industry in enumerate(params.industries):
            key = f"ind_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = industry
        conditions.append(f"d.company_industry IN ({', '.join(placeholders)})")

    if params.sizes:
        placeholders = []
        for idx, size in enumerate(params.sizes):
            key = f"size_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = size
        conditions.append(f"d.company_size_bucket IN ({', '.join(placeholders)})")

    if params.source:
        placeholders = []
        for idx, src in enumerate(params.source):
            key = f"src_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = src
        conditions.append(f"d.source IN ({', '.join(placeholders)})")

    if params.recency_days is not None:
        cutoff_str = _compute_recency_cutoff(params.recency_days)
        sql_params["recency_cutoff"] = cutoff_str
        conditions.append(
            "d.verified_at IS NOT NULL AND d.verified_at >= :recency_cutoff",
        )

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    return base_sql, sql_params


def _run_mv_facet_query(
    conn: sqlite3.Connection,
    facet_name: str,
    base_sql: str,
    base_params: dict[str, Any],
) -> list[dict[str, object]] | None:
    """
    Execute a facet GROUP BY over the lead_search_docs materialized view.
    Returns a list of {value, count} dicts, or None if the facet is unknown.
    """
    if facet_name == "verify_status":
        sql = f"""
            SELECT d.verify_status AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY d.verify_status
            ORDER BY count DESC
        """
    elif facet_name == "icp_bucket":
        sql = f"""
            SELECT
              CASE
                WHEN d.icp_score >= 80 THEN '80-100'
                WHEN d.icp_score >= 60 THEN '60-79'
                WHEN d.icp_score >= 40 THEN '40-59'
                ELSE '0-39'
              END AS value,
              COUNT(*) AS count
            {base_sql}
            GROUP BY value
            ORDER BY value
        """
    elif facet_name == "role_family":
        sql = f"""
            SELECT d.role_family AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY d.role_family
            ORDER BY count DESC
        """
    elif facet_name == "seniority":
        sql = f"""
            SELECT d.seniority AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY d.seniority
            ORDER BY count DESC
        """
    elif facet_name == "company_size_bucket":
        sql = f"""
            SELECT d.company_size_bucket AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY d.company_size_bucket
            ORDER BY count DESC
        """
    elif facet_name == "company_industry":
        sql = f"""
            SELECT d.company_industry AS value, COUNT(*) AS count
            {base_sql}
            GROUP BY d.company_industry
            ORDER BY count DESC
        """
    else:
        return None

    cur = conn.execute(sql, base_params)
    rows = cur.fetchall()
    dict_rows = _rows_to_dicts(cur, rows)
    return [
        {"value": row["value"], "count": row["count"]}
        for row in dict_rows
        if row["value"] is not None
    ]


def compute_facets(conn: sqlite3.Connection, params: LeadSearchParams) -> FacetCounts:
    """
    Compute facet counts for the requested dimensions under the current filters.

    By default this groups over the same joins that search_people_leads uses.
    When FACET_USE_MV is true and the lead_search_docs table exists (O14),
    and when there is no full-text query, we instead group over the
    materialized view for better performance.
    """
    if not params.facets:
        return {}

    # Decide whether to use the O14 materialized view.
    use_mv = False
    if FACET_USE_MV and (not params.query or not params.query.strip()):
        try:
            cur = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'lead_search_docs'
                """
            )
            use_mv = cur.fetchone() is not None
        except sqlite3.Error:
            use_mv = False

    # For now, avoid MV when tech filters are present since lead_search_docs
    # does not model tech-keyword details.
    if use_mv and params.tech:
        use_mv = False

    facets: FacetCounts = {}

    if use_mv:
        base_sql, base_params = _build_mv_base_sql(params)
        for facet_name in params.facets:
            rows = _run_mv_facet_query(conn, facet_name, base_sql, base_params)
            if rows is not None:
                facets[facet_name] = rows
        return facets

    base_sql, base_params, schema_info = _build_join_facet_base_sql(conn, params)
    for facet_name in params.facets:
        rows = _run_join_facet_query(conn, facet_name, base_sql, base_params, schema_info)
        if rows is not None:
            facets[facet_name] = rows

    return facets


def simple_similarity(a: str, b: str) -> float:
    """
    Simple string similarity using difflib.SequenceMatcher.

    Returns a float in [0.0, 1.0], where 1.0 is an exact match.
    """
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()


def fuzzy_company_lookup(
    conn: sqlite3.Connection,
    name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """
    Fuzzy lookup for companies, using a combination of FTS and Python similarity.

    Steps:
      1. Try to get candidates via companies_fts MATCH.
      2. If that yields too few candidates, fall back to scanning all companies.
      3. Compute simple_similarity between the query and each candidate's display name.
      4. Sort by similarity descending and return the top `limit` matches.

    Returned dict keys include:
      - id
      - name
      - domain
      - similarity
    """
    query = (name or "").strip()
    if not query:
        return []

    candidates_by_id: dict[int, dict[str, Any]] = {}

    # 1) FTS-based candidates (if available).
    try:
        cur = conn.execute(
            """
            SELECT
              c.id AS id,
              c.name AS name,
              COALESCE(c.official_domain, c.domain) AS domain
            FROM companies_fts
            JOIN companies AS c
              ON c.id = companies_fts.rowid
            WHERE companies_fts MATCH :match
            """,
            {"match": _normalize_fts_query(query)},
        )
        rows = cur.fetchall()
        for row in _rows_to_dicts(cur, rows):
            candidates_by_id[row["id"]] = row
    except sqlite3.OperationalError:
        # If FTS is not available for some reason, just skip to fallback.
        pass

    # 2) Fallback / augmentation: if we have too few candidates, add all companies.
    MIN_CANDIDATES = 3
    if len(candidates_by_id) < MIN_CANDIDATES:
        cur = conn.execute(
            """
            SELECT
              c.id AS id,
              c.name AS name,
              COALESCE(c.official_domain, c.domain) AS domain
            FROM companies AS c
            """
        )
        rows = cur.fetchall()
        for row in _rows_to_dicts(cur, rows):
            # Preserve any FTS-derived rows, but add missing ones.
            candidates_by_id.setdefault(row["id"], row)

    candidates = list(candidates_by_id.values())

    # 3) Attach similarity scores and sort.
    for row in candidates:
        row["similarity"] = simple_similarity(query, row.get("name") or "")

    candidates.sort(key=lambda r: r["similarity"], reverse=True)
    return candidates[:limit]
