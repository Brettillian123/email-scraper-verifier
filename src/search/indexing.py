# src/search/indexing.py
from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any


@dataclass
class LeadSearchParams:
    """
    Parameters for lead search.

    Attributes:
        query: Full-text search query (FTS5 syntax).
        verify_status: Optional list of verify_status values to include.
        icp_min: Optional minimum ICP score threshold.
        limit: Maximum number of rows to return.
    """

    query: str
    verify_status: Sequence[str] | None = None
    icp_min: int | None = None
    limit: int = 50


def _rows_to_dicts(cursor: sqlite3.Cursor, rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row, strict=False)) for row in rows]


def search_people_leads(conn: sqlite3.Connection, params: LeadSearchParams) -> list[dict[str, Any]]:
    """
    Perform a full-text search over people_fts + joins to people, companies, v_emails_latest.

    This is the main helper R22's /leads/search will call on SQLite. It:

      * Uses FTS5 (people_fts) to match the text query.
      * Joins back to people, companies, v_emails_latest for metadata and filters.
      * Applies verify_status and icp_min filters when provided.
      * Returns a list of plain dicts suitable for JSON serialization.

    Returned dict keys (subject to the underlying schema) include:
      - email
      - first_name
      - last_name
      - full_name
      - title
      - company
      - domain
      - source_url
      - verify_status
      - verified_at
      - icp_score
      - rank  (FTS bm25 score; lower is better)
    """
    if not params.query or not params.query.strip():
        raise ValueError("LeadSearchParams.query must be a non-empty string")

    base_sql = """
        SELECT
          ve.email AS email,
          p.first_name AS first_name,
          p.last_name AS last_name,
          COALESCE(p.full_name, p.first_name || ' ' || p.last_name) AS full_name,
          COALESCE(p.title_norm, p.title) AS title,
          c.name AS company,
          COALESCE(c.official_domain, c.domain) AS domain,
          ve.source_url AS source_url,
          ve.verify_status AS verify_status,
          ve.verified_at AS verified_at,
          p.icp_score AS icp_score,
          bm25(people_fts) AS rank
        FROM people_fts
        JOIN people p
          ON p.id = people_fts.rowid
        JOIN v_emails_latest ve
          ON ve.person_id = p.id
        JOIN companies c
          ON c.id = p.company_id
        WHERE people_fts MATCH :query
    """

    sql_params: dict[str, Any] = {
        "query": params.query,
        "limit": params.limit,
    }
    conditions: list[str] = []

    if params.icp_min is not None:
        conditions.append("p.icp_score IS NOT NULL AND p.icp_score >= :icp_min")
        sql_params["icp_min"] = params.icp_min

    if params.verify_status:
        placeholders: list[str] = []
        for idx, status in enumerate(params.verify_status):
            key = f"vs_{idx}"
            placeholders.append(f":{key}")
            sql_params[key] = status
        conditions.append(f"ve.verify_status IN ({', '.join(placeholders)})")

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    base_sql += """
        ORDER BY rank ASC, email ASC
        LIMIT :limit
    """

    cur = conn.execute(base_sql, sql_params)
    rows = cur.fetchall()
    return _rows_to_dicts(cur, rows)


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
            {"match": query},
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
