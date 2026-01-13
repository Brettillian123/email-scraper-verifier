# src/resolve/mx.py
"""
MX resolution and caching module.

This module is PostgreSQL-native and uses src.db.get_conn() for database access.
The CompatConnection layer handles SQL translation automatically.

Tables used:
  - domain_resolutions: MX resolution cache (defined in main schema.sql)
  - mx_probe_stats: Individual probe statistics for behavior analysis

SCHEMA COMPATIBILITY:
  The main schema.sql uses `chosen_domain` column, but this module also supports
  legacy `domain` column for backward compatibility. The _get_domain_column()
  helper detects which column exists and uses it appropriately.
"""

from __future__ import annotations

import json
import socket
import statistics
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.db import get_conn

DEFAULT_TTL_SECONDS = 86400  # 24h

# Exposed for tests to patch
_DNSPY_AVAILABLE = False
try:  # pragma: no cover
    import dns.resolver  # type: ignore

    _DNSPY_AVAILABLE = True
except Exception:  # pragma: no cover
    _DNSPY_AVAILABLE = False


# -----------------------------
# Time & normalization helpers
# -----------------------------


def _now_iso() -> str:
    # UTC, second precision, trailing Z (no microseconds)
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now_epoch() -> int:
    return int(time.time())


def _parse_iso(iso: str) -> int | None:
    try:
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso)
        return int(dt.timestamp())
    except Exception:
        return None


def norm_domain(domain: str | None) -> str | None:
    """
    NFKC → lower → IDNA ASCII if possible, else fallback to raw.
    """
    if not domain:
        return None
    s = unicodedata.normalize("NFKC", str(domain)).strip().lower()
    if not s:
        return None
    try:
        return s.encode("idna").decode("ascii")
    except Exception:
        return s


# -----------------------------
# DNS lookups (patch points)
# -----------------------------


def _mx_lookup_with_dnspython(domain: str) -> list[tuple[int, str]]:
    """
    Return list of (preference, host) for MX records.
    - Preserve the special host "." for Null MX (RFC 7505).
    - Otherwise return hostnames WITHOUT trailing dot.
    """
    assert _DNSPY_AVAILABLE, "dnspython not available"
    resolver = dns.resolver.Resolver()  # type: ignore[name-defined]
    resolver.lifetime = 2.0
    resolver.timeout = 2.0

    answers = resolver.resolve(domain, "MX")
    pairs: list[tuple[int, str]] = []
    for r in answers:
        pref = int(getattr(r, "preference", 0))
        exch = getattr(r, "exchange", None)
        if exch is None:
            host = ""
        else:
            host = str(exch.to_text()).rstrip(".") or ""
            # Special case: Null MX has exchange "."
            if host == "" and str(exch) == ".":
                host = "."
        pairs.append((pref, host))
    return pairs


def _a_or_aaaa_exists(domain: str) -> bool:
    """
    Lightweight A/AAAA presence check via socket.getaddrinfo.
    """
    try:
        socket.getaddrinfo(domain, None, proto=socket.IPPROTO_TCP)
        return True
    except Exception:
        return False


def _a_aaaa_fallback(domain: str) -> list[tuple[int, str]]:
    """
    Try A/AAAA records for the domain itself as an implicit MX (RFC 5321).
    Returns [(0, domain)] if ANY address resolves, else [].
    """
    if _a_or_aaaa_exists(domain):
        return [(0, domain)]
    return []


# Patch points exposed for tests
mx_lookup = _mx_lookup_with_dnspython
a_aaaa_fallback = _a_aaaa_fallback


# -----------------------------
# Schema utilities
# -----------------------------


def _table_columns(conn: Any, table: str) -> set[str]:
    """
    Return column names for a table using PRAGMA table_info(...).
    Works for both SQLite and Postgres via CompatCursor PRAGMA emulation.
    """
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall() or []
    except Exception:
        return set()

    cols: set[str] = set()
    for row in rows:
        try:
            name = row[1] if isinstance(row, tuple) else row.get("name", row[1])
        except Exception:
            continue
        if name:
            cols.add(str(name))
    return cols


# Cache for domain column name detection
_domain_column_cache: dict[int, str] = {}


def _get_domain_column(conn: Any) -> str:
    """
    Detect whether the domain_resolutions table uses 'domain' or 'chosen_domain'.

    The main schema.sql uses 'chosen_domain', but older schemas may use 'domain'.
    Returns the correct column name to use in queries.
    """
    conn_id = id(conn)
    if conn_id in _domain_column_cache:
        return _domain_column_cache[conn_id]

    cols = _table_columns(conn, "domain_resolutions")

    # Prefer chosen_domain (main schema), fall back to domain (legacy/fallback)
    if "chosen_domain" in cols:
        col = "chosen_domain"
    elif "domain" in cols:
        col = "domain"
    else:
        # Table doesn't exist yet or has neither - use legacy name for fallback table
        col = "domain"

    _domain_column_cache[conn_id] = col
    return col


def _clear_domain_column_cache() -> None:
    """Clear the domain column cache (for testing)."""
    _domain_column_cache.clear()


def _ensure_table(conn: Any) -> None:
    """
    Ensure domain_resolutions table exists if running in an empty DB.
    Note: The main schema.sql already defines this table for PostgreSQL.
    This is a safety fallback for standalone usage.
    """
    # Check if table exists first
    cols = _table_columns(conn, "domain_resolutions")
    if cols:
        return  # Table already exists

    # Create minimal table (PostgreSQL-compatible) - uses 'domain' for legacy compat
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS domain_resolutions (
            id BIGSERIAL PRIMARY KEY,
            company_id BIGINT,
            domain TEXT,
            mx_hosts TEXT,
            preference_map TEXT,
            lowest_mx TEXT,
            resolved_at TEXT,
            ttl INTEGER DEFAULT 86400,
            failure TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id
            ON domain_resolutions(company_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_domain_resolutions_domain
            ON domain_resolutions(domain)
        """
    )
    conn.commit()


def _ensure_behavior_schema(conn: Any) -> None:
    """
    Ensure mx_probe_stats table exists for behavior tracking.
    """
    cols = _table_columns(conn, "mx_probe_stats")
    if cols:
        return  # Table already exists

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mx_probe_stats (
            id BIGSERIAL PRIMARY KEY,
            mx_host TEXT NOT NULL,
            ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            code INTEGER,
            category TEXT,
            error_kind TEXT,
            elapsed_ms INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mx_probe_host_ts
            ON mx_probe_stats(mx_host, ts)
        """
    )
    conn.commit()


def _select_row(conn: Any, company_id: int, domain: str) -> dict[str, Any] | None:
    """Select the most recent resolution row for a company/domain pair."""
    domain_col = _get_domain_column(conn)

    cur = conn.execute(
        f"""
        SELECT id, company_id, {domain_col} as domain, mx_hosts, preference_map, lowest_mx,
               resolved_at, ttl, failure
          FROM domain_resolutions
         WHERE company_id = ? AND {domain_col} = ?
         ORDER BY id DESC
         LIMIT 1
        """,
        (int(company_id), domain),
    )
    row = cur.fetchone()
    if row is None:
        return None

    # Convert to dict for consistent access
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        return dict(row)
    # Tuple fallback
    return {
        "id": row[0],
        "company_id": row[1],
        "domain": row[2],
        "mx_hosts": row[3],
        "preference_map": row[4],
        "lowest_mx": row[5],
        "resolved_at": row[6],
        "ttl": row[7],
        "failure": row[8],
    }


def _should_use_cache(row: dict[str, Any], now_epoch: int, force: bool) -> bool:
    if force:
        return False
    if row.get("failure"):
        return False
    ttl = int(row.get("ttl") or DEFAULT_TTL_SECONDS)
    resolved_iso = row.get("resolved_at") or ""
    resolved_epoch = _parse_iso(resolved_iso)
    if resolved_epoch is None:
        return False
    return (resolved_epoch + ttl) > now_epoch


def _serialize_result(
    mx_pairs: list[tuple[int, str]],
) -> tuple[list[str], dict[str, int], str | None]:
    """
    Sort and produce (mx_hosts, preference_map, lowest_mx).
    Sorting: preference ASC, host ASC (lexicographic).
    - Drop any empty host strings defensively.
    """
    cleaned: list[tuple[int, str]] = []
    for p, h in mx_pairs:
        h2 = str(h).rstrip(".").lower()
        if not h2:
            continue
        cleaned.append((int(p), h2))

    cleaned.sort(key=lambda t: (t[0], t[1]))

    mx_hosts = [h for _p, h in cleaned]
    preference_map = {h: p for p, h in cleaned}
    lowest_mx = mx_hosts[0] if mx_hosts else None

    return mx_hosts, preference_map, lowest_mx


def _fetch_company_name(conn: Any, company_id: int) -> str:
    """
    Best-effort lookup of companies.name; returns "" on any failure.
    Used to satisfy NOT NULL constraint on company_name when inserting.
    """
    try:
        cur = conn.execute(
            "SELECT name FROM companies WHERE id = ?",
            (int(company_id),),
        )
        row = cur.fetchone()
        if row:
            return (row[0] if isinstance(row, tuple) else row.get("name", "")) or ""
    except Exception:
        pass
    return ""


def _default_for_type(sql_type: str) -> Any:
    t = (sql_type or "").upper()
    if "INT" in t:
        return 0
    if "REAL" in t or "FLOA" in t or "DOUB" in t or "NUM" in t:
        return 0
    if "BLOB" in t:
        return b""
    # TEXT or unknown → empty string
    return ""


def _build_insert_payload(
    conn: Any,
    company_id: int,
    domain: str,
    *,
    mx_hosts: list[str],
    preference_map: dict[str, int],
    lowest_mx: str | None,
    ttl: int,
    failure: str | None,
) -> tuple[list[str], list[Any]]:
    """
    Build a column/value list that satisfies any extra NOT NULL columns that the
    live table may have (e.g., company_name NOT NULL). We fill:
      - known R15 columns we manage
      - any extra NOT NULL columns without defaults, using sensible fallbacks
        (TEXT → "", INT/REAL → 0, BLOB → b"") and a special case:
        company_name → companies.name (fallback "").
    """
    try:
        cur = conn.execute("PRAGMA table_info(domain_resolutions)")
        info = cur.fetchall() or []
    except Exception:
        info = []

    # Convert info rows to dicts for easier access
    info_dicts: list[dict[str, Any]] = []
    for r in info:
        if isinstance(r, dict):
            info_dicts.append(r)
        elif hasattr(r, "keys"):
            info_dicts.append(dict(r))
        else:
            info_dicts.append(
                {
                    "cid": r[0],
                    "name": r[1],
                    "type": r[2],
                    "notnull": r[3],
                    "dflt_value": r[4],
                    "pk": r[5],
                }
            )

    have = {d["name"] for d in info_dicts}

    # Detect the correct domain column name
    domain_col = _get_domain_column(conn)

    base_values: dict[str, Any] = {
        "company_id": int(company_id),
        domain_col: domain,  # Use detected column name
        "mx_hosts": json.dumps(mx_hosts, ensure_ascii=False),
        "preference_map": json.dumps(preference_map, ensure_ascii=False),
        "lowest_mx": lowest_mx,
        "resolved_at": _now_iso(),
        "ttl": int(ttl),
        "failure": failure or None,
    }

    cols: list[str] = []
    vals: list[Any] = []

    # 1) Include standard columns if present in the live table
    for k in (
        "company_id",
        domain_col,  # Use detected column name
        "mx_hosts",
        "preference_map",
        "lowest_mx",
        "resolved_at",
        "ttl",
        "failure",
    ):
        if k in have:
            cols.append(k)
            vals.append(base_values.get(k))

    # 2) Satisfy any extra NOT NULL columns with no default
    for r in info_dicts:
        name = r["name"]
        if name in cols:
            continue
        notnull = int(r.get("notnull") or 0) == 1
        has_default = r.get("dflt_value") is not None
        if notnull and not has_default:
            if name == "company_name":
                fallback = _fetch_company_name(conn, company_id)
            else:
                fallback = _default_for_type(str(r.get("type", "")))
            cols.append(name)
            vals.append(fallback)

    return cols, vals


def _upsert_row(
    conn: Any,
    company_id: int,
    domain: str,
    *,
    mx_hosts: list[str],
    preference_map: dict[str, int],
    lowest_mx: str | None,
    ttl: int,
    failure: str | None,
) -> int:
    """
    Idempotent upsert by (company_id, domain).
    Returns row_id.
    """
    row = _select_row(conn, company_id, domain)
    domain_col = _get_domain_column(conn)

    if row:
        conn.execute(
            """
            UPDATE domain_resolutions
               SET mx_hosts = ?,
                   preference_map = ?,
                   lowest_mx = ?,
                   resolved_at = ?,
                   ttl = ?,
                   failure = ?
             WHERE id = ?
            """,
            (
                json.dumps(mx_hosts, ensure_ascii=False),
                json.dumps(preference_map, ensure_ascii=False),
                lowest_mx,
                _now_iso(),
                int(ttl),
                failure or None,
                int(row["id"]),
            ),
        )
        conn.commit()
        return int(row["id"])

    # INSERT path — build a payload that satisfies extra NOT NULL columns
    cols, vals = _build_insert_payload(
        conn,
        company_id,
        domain,
        mx_hosts=mx_hosts,
        preference_map=preference_map,
        lowest_mx=lowest_mx,
        ttl=ttl,
        failure=failure,
    )
    placeholders = ",".join("?" for _ in cols)
    sql = f"INSERT INTO domain_resolutions ({','.join(cols)}) VALUES ({placeholders})"
    cur = conn.execute(sql, vals)
    conn.commit()

    # Try to get lastrowid from cursor
    if hasattr(cur, "lastrowid") and cur.lastrowid:
        return int(cur.lastrowid)

    # Fallback: re-select using detected column name
    got = conn.execute(
        f"""
        SELECT id FROM domain_resolutions
        WHERE company_id=? AND {domain_col}=?
        ORDER BY id DESC LIMIT 1
        """,
        (int(company_id), domain),
    ).fetchone()

    if got:
        return int(got[0] if isinstance(got, tuple) else got.get("id", got[0]))
    return 0


# -----------------------------
# Result type
# -----------------------------


@dataclass
class MXResult:
    row_id: int
    company_id: int
    domain: str
    mx_hosts: list[str]
    preference_map: dict[str, int]
    lowest_mx: str | None
    resolved_at: str
    ttl: int
    failure: str | None
    cached: bool


# -----------------------------
# Public API
# -----------------------------


def resolve_mx(
    company_id: int,
    domain: str,
    *,
    force: bool = False,
    db_path: str | None = None,  # Deprecated, kept for API compatibility
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> MXResult:
    """
    Resolve MX for a domain with caching in domain_resolutions.

    Behavior:
      - Try cache if not forced, not failed, and within TTL.
      - Resolve MX via dnspython (if available), sorted by (pref ASC, host ASC).
      - **Null MX** (single record with host ".") → failure="null_mx" and *no* A/AAAA fallback.
      - On MX failure (non-null), try A/AAAA fallback: treat domain as MX with pref 0 if present.
      - On full failure, record failure string and empty hosts.
    """
    d = norm_domain(domain) or ""
    if not d:
        return MXResult(
            row_id=0,
            company_id=company_id,
            domain=domain or "",
            mx_hosts=[],
            preference_map={},
            lowest_mx=None,
            resolved_at=_now_iso(),
            ttl=ttl_seconds,
            failure="invalid_domain",
            cached=False,
        )

    conn = get_conn()
    _ensure_table(conn)

    now_epoch = _utc_now_epoch()
    row = _select_row(conn, company_id, d)

    if row and _should_use_cache(row, now_epoch, force):
        # Deserialize from cached row
        try:
            mx_hosts = json.loads(row.get("mx_hosts") or "[]")
        except Exception:
            mx_hosts = []
        try:
            preference_map = json.loads(row.get("preference_map") or "{}")
        except Exception:
            preference_map = {}
        return MXResult(
            row_id=int(row.get("id") or 0),
            company_id=int(row.get("company_id") or company_id),
            domain=row.get("domain") or d,
            mx_hosts=mx_hosts,
            preference_map=preference_map,
            lowest_mx=row.get("lowest_mx"),
            resolved_at=row.get("resolved_at") or "",
            ttl=int(row.get("ttl") or ttl_seconds),
            failure=row.get("failure"),
            cached=True,
        )

    # Live resolve
    mx_hosts: list[str] = []
    preference_map: dict[str, int] = {}
    lowest_mx: str | None = None
    failure: str | None = None

    if _DNSPY_AVAILABLE:
        try:
            pairs = mx_lookup(d)
            # Check for Null MX (single record with ".")
            if len(pairs) == 1 and pairs[0][1] == ".":
                failure = "null_mx"
            else:
                mx_hosts, preference_map, lowest_mx = _serialize_result(pairs)
        except Exception as e:
            # MX lookup failed — try A/AAAA fallback
            try:
                pairs = a_aaaa_fallback(d)
                if pairs:
                    mx_hosts, preference_map, lowest_mx = _serialize_result(pairs)
                else:
                    failure = f"mx_lookup_failed:{type(e).__name__}"
            except Exception as e2:
                failure = f"mx_lookup_failed:{type(e).__name__};fallback:{type(e2).__name__}"
    else:
        # No dnspython — try A/AAAA fallback only
        try:
            pairs = a_aaaa_fallback(d)
            if pairs:
                mx_hosts, preference_map, lowest_mx = _serialize_result(pairs)
            else:
                failure = "no_dnspython_no_a_aaaa"
        except Exception as e:
            failure = f"fallback_only_failed:{type(e).__name__}"

    row_id = _upsert_row(
        conn,
        company_id,
        d,
        mx_hosts=mx_hosts,
        preference_map=preference_map,
        lowest_mx=lowest_mx,
        ttl=ttl_seconds,
        failure=failure,
    )

    return MXResult(
        row_id=row_id,
        company_id=company_id,
        domain=d,
        mx_hosts=mx_hosts,
        preference_map=preference_map,
        lowest_mx=lowest_mx,
        resolved_at=_now_iso(),
        ttl=ttl_seconds,
        failure=failure,
        cached=False,
    )


def get_cached_mx(company_id: int, domain: str) -> MXResult | None:
    """
    Return cached MX result if available and not expired.
    Does NOT resolve if missing/expired.
    """
    d = norm_domain(domain) or ""
    if not d:
        return None

    conn = get_conn()
    _ensure_table(conn)

    row = _select_row(conn, company_id, d)
    if not row:
        return None

    now_epoch = _utc_now_epoch()
    if not _should_use_cache(row, now_epoch, force=False):
        return None

    try:
        mx_hosts = json.loads(row.get("mx_hosts") or "[]")
    except Exception:
        mx_hosts = []
    try:
        preference_map = json.loads(row.get("preference_map") or "{}")
    except Exception:
        preference_map = {}

    return MXResult(
        row_id=int(row.get("id") or 0),
        company_id=int(row.get("company_id") or company_id),
        domain=row.get("domain") or d,
        mx_hosts=mx_hosts,
        preference_map=preference_map,
        lowest_mx=row.get("lowest_mx"),
        resolved_at=row.get("resolved_at") or "",
        ttl=int(row.get("ttl") or DEFAULT_TTL_SECONDS),
        failure=row.get("failure"),
        cached=True,
    )


# -----------------------------
# MX behavior tracking (O06)
# -----------------------------


def record_mx_probe(
    mx_host: str,
    code: int | None,
    elapsed_s: float,
    *,
    error_kind: str | None = None,
    category: str | None = None,
    db_path: str | None = None,  # Deprecated, kept for API compatibility
) -> None:
    """
    Append a single probe datapoint (called by R16 smtp.probe_rcpt).

    Note: db_path parameter is deprecated. Uses get_conn() for PostgreSQL.
    """
    mx_host = (mx_host or "").strip().lower()
    if not mx_host:
        return

    try:
        conn = get_conn()
        _ensure_behavior_schema(conn)
        conn.execute(
            """
            INSERT INTO mx_probe_stats(
                mx_host,
                code,
                category,
                error_kind,
                elapsed_ms
            )
            VALUES(?,?,?,?,?)
            """,
            (
                mx_host,
                None if code is None else int(code),
                category,
                error_kind,
                int(round(elapsed_s * 1000)),
            ),
        )
        conn.commit()
    except Exception:
        # Best-effort; don't fail the probe on stats errors
        pass


def get_mx_behavior_hint(
    mx_host: str, *, window_days: int = 30, db_path: str | None = None
) -> dict[str, Any] | None:
    """
    Return a compact behavior hint dict derived from recent probes.

    Note: db_path parameter is deprecated. Uses get_conn() for PostgreSQL.
    """
    mx_host = (mx_host or "").strip().lower()
    if not mx_host:
        return None

    try:
        conn = get_conn()
        _ensure_behavior_schema(conn)

        # Calculate cutoff date (works for both SQLite and PostgreSQL)
        cutoff_ts = datetime.now(UTC).timestamp() - (window_days * 86400)
        cutoff_iso = datetime.fromtimestamp(cutoff_ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        rows = conn.execute(
            """
            SELECT code, category, elapsed_ms
            FROM mx_probe_stats
            WHERE mx_host = ? AND ts >= ?
            ORDER BY ts DESC
            LIMIT 5000
            """,
            (mx_host, cutoff_iso),
        ).fetchall()
    except Exception:
        return None

    if not rows:
        return None

    total = len(rows)

    # Handle both tuple and dict row types
    def get_val(row: Any, idx: int, key: str) -> Any:
        if isinstance(row, dict):
            return row.get(key)
        if hasattr(row, "keys"):
            return row.get(key)
        return row[idx]

    temps = sum(1 for r in rows if (get_val(r, 1, "category") or "") == "temp_fail")
    accpt = sum(1 for r in rows if (get_val(r, 1, "category") or "") == "accept")

    elats: list[int] = []
    for r in rows:
        v = get_val(r, 2, "elapsed_ms")
        if v is None:
            continue
        elats.append(int(v or 0))

    p50 = _percentile(elats, 50)
    p95 = _percentile(elats, 95)
    tfr = round(temps / total, 3)
    srate = round(accpt / total, 3)
    tarpit = (p95 > 2000) or (tfr > 0.30)

    return {
        "n": total,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "recent_temp_fail_rate": tfr,
        "success_rate": srate,
        "tarpit": bool(tarpit),
        "window_days": window_days,
    }


def _percentile(vals: list[int], p: float) -> int:
    if not vals:
        return 0
    try:
        # statistics.quantiles requires n>=1; we handle small n via sorted index
        vals = sorted(vals)
        k = max(0, min(len(vals) - 1, int(round((p / 100.0) * (len(vals) - 1)))))
        return int(vals[k])
    except Exception:
        return int(statistics.median(vals)) if vals else 0


def _update_latest_resolution_behavior(
    domain: str, behavior: dict | None, *, db_path: str | None = None
) -> None:
    """
    Write a JSON summary into the most-recent domain_resolutions row for this
    domain, ordered by resolved_at (ties broken by id).

    Note: db_path parameter is deprecated. Uses get_conn() for PostgreSQL.
    """
    if not behavior:
        return

    try:
        payload = json.dumps(behavior, ensure_ascii=False)
        conn = get_conn()

        cols = _table_columns(conn, "domain_resolutions")
        if "mx_behavior" not in cols or "resolved_at" not in cols:
            return

        domain_col = _get_domain_column(conn)

        cur = conn.execute(
            f"""
            SELECT id
              FROM domain_resolutions
             WHERE {domain_col} = ?
             ORDER BY
                  COALESCE(NULLIF(resolved_at, ''), '0000-01-01T00:00:00Z') DESC,
                  id DESC
             LIMIT 1
            """,
            (domain,),
        )
        row = cur.fetchone()
        if row:
            row_id = row[0] if isinstance(row, tuple) else row.get("id", row[0])
            conn.execute(
                "UPDATE domain_resolutions SET mx_behavior = ? WHERE id = ?",
                (payload, int(row_id)),
            )
            conn.commit()
    except Exception:
        # best-effort; do not raise
        pass


# -----------------------------
# R16-visible behavior hook
# -----------------------------


def record_behavior(
    *,
    domain: str,
    mx_host: str,
    elapsed_ms: int,
    category: str,
    code: int | None,
    error_kind: str | None,
) -> None:
    """
    O06/R16 hook: tests monkeypatch this symbol and assert it is called exactly
    once per probe. Default implementation records a datapoint and refreshes
    the summarized behavior hint on the latest domain_resolutions row.
    """
    try:
        # 1) Append a raw probe datapoint
        record_mx_probe(
            mx_host,
            code,
            float(elapsed_ms) / 1000.0,
            error_kind=error_kind,
            category=category,
        )
        # 2) Recompute hint for this MX and store it back on the latest resolution row
        hint = get_mx_behavior_hint(mx_host)
        _update_latest_resolution_behavior(domain, hint)
    except Exception:
        # Best-effort; swallow errors
        pass


# -----------------------------
# Simple dataclass for mx_info helper
# -----------------------------


@dataclass
class MXInfo:
    lowest_mx: str | None
    mx_behavior: dict[str, Any] | None


def get_or_resolve_mx(
    domain: str, *, force: bool = False, db_path: str | None = None
) -> MXInfo:
    """
    Lightweight helper used by R16 to get lowest_mx plus a behavior hint.
    Falls back to bare DNS if your R15 resolver isn't available.
    Also writes the summarized hint into domain_resolutions.mx_behavior (best-effort).

    Note: db_path parameter is deprecated. Uses get_conn() for PostgreSQL.
    """
    d = (domain or "").strip().lower()
    from importlib import import_module

    lowest = None
    try:
        # Prefer your R15 resolver if present
        mod = import_module("src.resolve.mx")
        if hasattr(mod, "resolve_mx"):
            res = mod.resolve_mx(company_id=0, domain=d, force=force)
            lowest = getattr(res, "lowest_mx", None) or d
        else:
            raise ImportError
    except Exception:
        # Bare DNS fallback
        try:
            import dns.resolver as _dr

            answers = _dr.resolve(d, "MX")
            pairs = sorted(
                [(r.exchange.to_text(omit_final_dot=True), r.preference) for r in answers],
                key=lambda x: x[1],
            )
            lowest = pairs[0][0]
        except Exception:
            lowest = d

    hint = get_mx_behavior_hint(lowest or d)
    try:
        _update_latest_resolution_behavior(d, hint)
    except Exception:
        pass
    return MXInfo(lowest_mx=lowest, mx_behavior=hint)
