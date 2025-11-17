# src/resolve/mx.py
from __future__ import annotations

import json
import socket
import sqlite3
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

DEFAULT_DB_PATH = "data/dev.db"
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
            full = exch.to_text()  # may be '.' or 'host.'
            host = "." if full == "." else exch.to_text(omit_final_dot=True)
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


# -----------------------------
# DB helpers
# -----------------------------


def _ensure_table(con: sqlite3.Connection) -> None:
    """
    Ensure a minimal table exists if running in an empty DB. This won't remove
    or alter any extra columns you may already have.
    """
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS domain_resolutions (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            domain TEXT,
            mx_hosts TEXT,
            preference_map TEXT,
            lowest_mx TEXT,
            resolved_at TEXT,
            ttl INTEGER DEFAULT 86400,
            failure TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id
            ON domain_resolutions(company_id);
        CREATE INDEX IF NOT EXISTS idx_domain_resolutions_domain
            ON domain_resolutions(domain);
        """
    )
    con.commit()


def _table_info(con: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    cur = con.execute(f"PRAGMA table_info({table})")
    cur.row_factory = sqlite3.Row
    rows = cur.fetchall()
    return rows  # columns: cid, name, type, notnull, dflt_value, pk


def _select_row(con: sqlite3.Connection, company_id: int, domain: str) -> sqlite3.Row | None:
    cur = con.execute(
        """
        SELECT id, company_id, domain, mx_hosts, preference_map, lowest_mx,
               resolved_at, ttl, failure
          FROM domain_resolutions
         WHERE company_id = ? AND domain = ?
         ORDER BY id DESC
         LIMIT 1
        """,
        (int(company_id), domain),
    )
    return cur.fetchone()


def _should_use_cache(row: sqlite3.Row, now_epoch: int, force: bool) -> bool:
    if force:
        return False
    if row["failure"]:
        return False
    ttl = int(row["ttl"] or DEFAULT_TTL_SECONDS)
    resolved_iso = row["resolved_at"] or ""
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
    hosts = [h for _, h in cleaned]
    prefmap = {h: int(p) for p, h in cleaned}
    lowest = hosts[0] if hosts else None
    return hosts, prefmap, lowest


def _fetch_company_name(con: sqlite3.Connection, company_id: int) -> str:
    try:
        row = con.execute("SELECT name FROM companies WHERE id = ?", (int(company_id),)).fetchone()
        if row and row[0]:
            return str(row[0])
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
    con: sqlite3.Connection,
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
    info = _table_info(con, "domain_resolutions")
    have = {r["name"] for r in info}

    base_values: dict[str, Any] = {
        "company_id": int(company_id),
        "domain": domain,
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
        "domain",
        "mx_hosts",
        "preference_map",
        "lowest_mx",
        "resolved_at",
        "ttl",
        "failure",
    ):
        if k in have:
            cols.append(k)
            vals.append(base_values[k])

    # 2) Satisfy any extra NOT NULL columns with no default
    for r in info:
        name = r["name"]
        if name in cols:
            continue
        notnull = int(r["notnull"] or 0) == 1
        has_default = r["dflt_value"] is not None
        if notnull and not has_default:
            if name == "company_name":
                fallback = _fetch_company_name(con, company_id)
            else:
                fallback = _default_for_type(str(r["type"]))
            cols.append(name)
            vals.append(fallback)

    return cols, vals


def _upsert_row(
    con: sqlite3.Connection,
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
    row = _select_row(con, company_id, domain)

    if row:
        con.execute(
            """
            UPDATE domain_resolutions
               SET mx_hosts = :mx_hosts,
                   preference_map = :preference_map,
                   lowest_mx = :lowest_mx,
                   resolved_at = :resolved_at,
                   ttl = :ttl,
                   failure = :failure
             WHERE id = :id
            """,
            {
                "mx_hosts": json.dumps(mx_hosts, ensure_ascii=False),
                "preference_map": json.dumps(preference_map, ensure_ascii=False),
                "lowest_mx": lowest_mx,
                "resolved_at": _now_iso(),
                "ttl": int(ttl),
                "failure": failure or None,
                "id": int(row["id"]),
            },
        )
        con.commit()
        return int(row["id"])

    # INSERT path — build a payload that satisfies extra NOT NULL columns
    cols, vals = _build_insert_payload(
        con,
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
    con.execute(sql, vals)
    con.commit()

    cur = con.execute(
        (
            "SELECT id FROM domain_resolutions "
            "WHERE company_id=? AND domain=? "
            "ORDER BY id DESC LIMIT 1"
        ),
        (int(company_id), domain),
    )

    got = cur.fetchone()
    return int(got[0]) if got else 0


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
    db_path: str = DEFAULT_DB_PATH,
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
      - Idempotent write/update by (company_id, domain).
      - INSERT path fills any unexpected NOT NULL columns (e.g., company_name).
    """
    canon = norm_domain(domain)
    if not canon:
        raise ValueError("empty domain")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    _ensure_table(con)

    now_epoch = _utc_now_epoch()

    # Cache check
    row = _select_row(con, company_id, canon)
    if row and _should_use_cache(row, now_epoch, force):
        try:
            hosts = json.loads(row["mx_hosts"] or "[]")
        except Exception:
            hosts = []
        try:
            prefmap = json.loads(row["preference_map"] or "{}")
        except Exception:
            prefmap = {}
        return MXResult(
            row_id=int(row["id"]),
            company_id=int(row["company_id"]),
            domain=str(row["domain"]),
            mx_hosts=list(hosts),
            preference_map=dict(prefmap),
            lowest_mx=row["lowest_mx"],
            resolved_at=row["resolved_at"],
            ttl=int(row["ttl"] or ttl_seconds),
            failure=row["failure"],
            cached=True,
        )

    # Fresh resolution path (or refresh)
    failure: str | None = None
    hosts_out: list[str] = []
    prefmap_out: dict[str, int] = {}
    lowest: str | None = None

    try:
        pairs: list[tuple[int, str]] = []
        if _DNSPY_AVAILABLE:
            try:
                pairs = _mx_lookup_with_dnspython(canon)
            except Exception as e:
                pairs = []
                failure = f"mx_lookup_failed:{type(e).__name__}"
        else:
            failure = "mx_lookup_unavailable"

        # ---- Null MX handling (RFC 7505) ----
        # If the MX RRset consists solely of a single record whose exchange is "."
        # → do not fallback to A/AAAA; treat as "no mail accepted".
        if len(pairs) == 1 and (pairs[0][1] in (".", "")):
            hosts_out = []
            prefmap_out = {}
            lowest = None
            failure = "null_mx"
        elif pairs:
            hosts_out, prefmap_out, lowest = _serialize_result(pairs)
            failure = None
        else:
            # No MX records → A/AAAA fallback (RFC 5321), unless null_mx (already handled).
            if _a_or_aaaa_exists(canon):
                hosts_out = [canon]
                prefmap_out = {canon: 0}
                lowest = canon
                failure = None
            else:
                hosts_out = []
                prefmap_out = {}
                lowest = None
                if not failure:
                    failure = "no_mx_and_no_a"

    except Exception as e:
        hosts_out = []
        prefmap_out = {}
        lowest = None
        failure = f"unexpected:{type(e).__name__}:{e}"

    # Persist and return
    row_id = _upsert_row(
        con,
        company_id,
        canon,
        mx_hosts=hosts_out,
        preference_map=prefmap_out,
        lowest_mx=lowest,
        ttl=int(ttl_seconds),
        failure=failure,
    )
    row2 = _select_row(con, company_id, canon)
    resolved_at = row2["resolved_at"] if row2 else _now_iso()
    ttl_written = int(row2["ttl"] or ttl_seconds) if row2 else int(ttl_seconds)

    con.close()

    return MXResult(
        row_id=row_id,
        company_id=int(company_id),
        domain=canon,
        mx_hosts=hosts_out,
        preference_map=prefmap_out,
        lowest_mx=lowest,
        resolved_at=resolved_at,
        ttl=ttl_written,
        failure=failure,
        cached=False,
    )
