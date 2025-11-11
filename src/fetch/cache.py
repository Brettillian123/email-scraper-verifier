# src/fetch/cache.py
from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

# --------------------------------------------------------------------------------------
# Configuration (env-overridable)
# --------------------------------------------------------------------------------------

FETCH_CACHE_DB = os.getenv("FETCH_CACHE_DB", ":memory:")
# Default TTL when no Cache-Control/Expires is provided
FETCH_CACHE_TTL_SEC = float(os.getenv("FETCH_CACHE_TTL_SEC", "900"))  # 15 minutes
# Max body we are willing to persist (bytes)
FETCH_MAX_BODY_BYTES = int(os.getenv("FETCH_MAX_BODY_BYTES", str(2_000_000)))  # ~2 MB
# Which content types are allowed to store bodies (prefix/starts-with checks)
# Keep narrow for MVP: HTML/text (+xhtml+xml, +xml)
FETCH_CACHE_BODY_TYPES = os.getenv(
    "FETCH_CACHE_BODY_TYPES",
    "text/;application/xhtml+xml;application/xml",
).split(";")

# --------------------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------------------


@dataclass
class CacheEntry:
    scheme: str
    host: str
    path: str  # includes query; fragment removed
    etag: str | None
    last_modified: str | None
    status: int
    content_type: str | None
    body: bytes | None
    fetched_at: float
    expires_at: float | None

    @property
    def fresh(self) -> bool:
        if self.expires_at is None:
            return False
        return self.expires_at > _now()


# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------


def _now() -> float:
    # Use wall clock (tests may monkeypatch time.time() to follow monotonic)
    return time.time()


def _normalize_url(url: str) -> tuple[str, str, str]:
    """
    Return (scheme, host, path_with_query) where 'path' includes the query string.
    Fragments are stripped.
    """
    parts = urlsplit(url)
    scheme = parts.scheme or "https"
    host = parts.netloc.lower()
    # Recompose path + query (no fragment)
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    return scheme, host, path


def _parse_http_date(value: str) -> float | None:
    """
    Parse an HTTP-date (RFC 7231) into epoch seconds (UTC).
    Returns None on failure.
    """
    try:
        dt = parsedate_to_datetime(value)
        return dt.timestamp()
    except Exception:
        return None


def _parse_cache_control(headers: Mapping[str, str]) -> dict:
    raw = headers.get("cache-control") or headers.get("Cache-Control")
    out = {}
    if not raw:
        return out
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if "=" in token:
            k, v = token.split("=", 1)
            out[k.strip().lower()] = v.strip().strip('"')
        else:
            out[token.lower()] = True
    return out


def _compute_expiry(headers: Mapping[str, str], now: float | None = None) -> float | None:
    """
    Compute an expiration timestamp (epoch seconds) from Cache-Control/Expires.
    Returns None if resource should not be cached; otherwise a timestamp (>now) or now+default.
    Policy:
      - If 'no-store' present → do not cache (None).
      - If 'max-age=N' → now + N (N<0 → treat as 0).
      - Else if 'Expires' → max(Expires, now).
      - Else → now + FETCH_CACHE_TTL_SEC (default TTL).
    """
    if now is None:
        now = _now()

    cc = _parse_cache_control(headers)
    if "no-store" in cc:
        return None

    if "max-age" in cc:
        try:
            age = float(cc["max-age"])
            if age < 0:
                age = 0.0
            return now + age
        except Exception:
            # fall back to Expires/default
            pass

    expires_hdr = headers.get("expires") or headers.get("Expires")
    if expires_hdr:
        ts = _parse_http_date(expires_hdr)
        if ts is not None:
            # If Expires is in the past, treat as immediate expiry (stale but cacheable)
            return max(ts, now)

    # default TTL
    return now + FETCH_CACHE_TTL_SEC


def _allowed_body(content_type: str | None, body: bytes | None) -> bool:
    if body is None:
        return False
    if len(body) > FETCH_MAX_BODY_BYTES:
        return False
    if not content_type:
        return False
    ct = content_type.split(";", 1)[0].strip().lower()
    # prefix match
    for pref in FETCH_CACHE_BODY_TYPES:
        pref = pref.strip().lower()
        if not pref:
            continue
        if ct.startswith(pref):
            return True
    return False


def _dict_get_any(headers: Mapping[str, str], keys: Iterable[str]) -> str | None:
    for k in keys:
        if k in headers:
            return headers[k]
    # Case-insensitive lookup fallback
    low = {k.lower(): v for k, v in headers.items()}
    for k in keys:
        v = low.get(k.lower())
        if v is not None:
            return v
    return None


# --------------------------------------------------------------------------------------
# Cache
# --------------------------------------------------------------------------------------


class Cache:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or FETCH_CACHE_DB
        self._cx = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        self._cx.execute("PRAGMA journal_mode=WAL;")
        self._cx.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    # ---- schema ----------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        self._cx.execute(
            """
            CREATE TABLE IF NOT EXISTS http_cache (
              scheme TEXT NOT NULL,
              host   TEXT NOT NULL,
              path   TEXT NOT NULL,
              etag TEXT,
              last_modified TEXT,
              status INTEGER NOT NULL,
              content_type TEXT,
              body BLOB,
              fetched_at REAL NOT NULL,
              expires_at REAL,
              PRIMARY KEY (scheme, host, path)
            )
            """
        )
        self._cx.commit()

    # ---- public API ------------------------------------------------------------------

    def get(self, url: str) -> tuple[CacheEntry | None, bool]:
        """
        Return (entry, is_fresh). If no entry, (None, False).
        """
        scheme, host, path = _normalize_url(url)
        row = self._cx.execute(
            (
                "SELECT scheme,host,path,etag,last_modified,status,content_type,body,"
                "fetched_at,expires_at "
                "FROM http_cache "
                "WHERE scheme=? AND host=? AND path=?"
            ),
            (scheme, host, path),
        ).fetchone()

        if not row:
            return None, False
        entry = CacheEntry(
            scheme=row[0],
            host=row[1],
            path=row[2],
            etag=row[3],
            last_modified=row[4],
            status=int(row[5]),
            content_type=row[6],
            body=row[7],
            fetched_at=float(row[8]),
            expires_at=None if row[9] is None else float(row[9]),
        )
        return entry, entry.fresh

    def conditionals(self, url: str) -> Mapping[str, str]:
        """
        Return conditional headers (If-None-Match / If-Modified-Since) if we have them.
        """
        entry, _ = self.get(url)
        if not entry:
            return {}
        headers = {}
        if entry.etag:
            headers["If-None-Match"] = entry.etag
        if entry.last_modified:
            headers["If-Modified-Since"] = entry.last_modified
        return headers

    def store_200(
        self,
        url: str,
        status: int,
        content_type: str | None,
        body: bytes | None,
        headers: Mapping[str, str],
        *,
        now: float | None = None,
    ) -> CacheEntry:
        """
        Store a 200-series response. Respects max-age/Expires and caps body size/types.
        """
        if now is None:
            now = _now()
        scheme, host, path = _normalize_url(url)

        etag = _dict_get_any(headers, ("etag", "ETag"))
        last_mod = _dict_get_any(headers, ("last-modified", "Last-Modified"))
        ct = _dict_get_any(headers, ("content-type", "Content-Type")) or content_type

        expires_at = _compute_expiry(headers, now=now)
        # If 'no-store', expires_at will be None: store metadata only for completeness.
        body_to_store = body if _allowed_body(ct, body) and expires_at is not None else None

        self._cx.execute(
            """
            INSERT INTO http_cache (
                scheme, host, path,
                etag, last_modified, status,
                content_type, body, fetched_at, expires_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (scheme, host, path) DO UPDATE SET
                etag=excluded.etag,
                last_modified=excluded.last_modified,
                status=excluded.status,
                content_type=excluded.content_type,
                body=excluded.body,
                fetched_at=excluded.fetched_at,
                expires_at=excluded.expires_at
            """,
            (
                scheme,
                host,
                path,
                etag,
                last_mod,
                int(status),
                ct,
                body_to_store,
                float(now),
                None if expires_at is None else float(expires_at),
            ),
        )
        self._cx.commit()
        entry, _ = self.get(url)
        assert entry is not None
        return entry

    def store_304(
        self,
        url: str,
        headers: Mapping[str, str],
        *,
        now: float | None = None,
    ) -> CacheEntry | None:
        """
        Refresh metadata on a 304 Not Modified, reusing the existing body/content_type/status.
        """
        if now is None:
            now = _now()
        existing, _ = self.get(url)
        if not existing:
            # No prior entity (shouldn't happen), do nothing.
            return None

        etag = _dict_get_any(headers, ("etag", "ETag")) or existing.etag
        last_mod = (
            _dict_get_any(headers, ("last-modified", "Last-Modified")) or existing.last_modified
        )
        # If server sent new caching headers, honor; else apply default TTL window from 'now'
        expires_at = _compute_expiry(headers, now=now)
        if expires_at is None:
            # If 304 + no-store, keep old expiry (but it's weird). Fallback to default TTL.
            expires_at = now + FETCH_CACHE_TTL_SEC

        self._cx.execute(
            """
            UPDATE http_cache
               SET etag=?,
                   last_modified=?,
                   fetched_at=?,
                   expires_at=?
             WHERE scheme=? AND host=? AND path=?
            """,
            (
                etag,
                last_mod,
                float(now),
                float(expires_at),
                existing.scheme,
                existing.host,
                existing.path,
            ),
        )
        self._cx.commit()
        entry, _ = self.get(url)
        return entry

    def purge(self, url: str) -> None:
        scheme, host, path = _normalize_url(url)
        self._cx.execute(
            "DELETE FROM http_cache WHERE scheme=? AND host=? AND path=?",
            (scheme, host, path),
        )
        self._cx.commit()

    def clear_all(self) -> None:
        self._cx.execute("DELETE FROM http_cache")
        self._cx.commit()

    def close(self) -> None:
        try:
            self._cx.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Module-level convenience (optional singleton)
# --------------------------------------------------------------------------------------

_default_cache: Cache | None = None


def default() -> Cache:
    global _default_cache
    if _default_cache is None:
        _default_cache = Cache()
    return _default_cache


# Shorthand helpers that use the module-level default cache


def get(url: str) -> tuple[CacheEntry | None, bool]:
    return default().get(url)


def conditionals(url: str) -> Mapping[str, str]:
    return default().conditionals(url)


def store_200(
    url: str,
    status: int,
    content_type: str | None,
    body: bytes | None,
    headers: Mapping[str, str],
) -> CacheEntry:
    return default().store_200(url, status, content_type, body, headers)


def store_304(url: str, headers: Mapping[str, str]) -> CacheEntry | None:
    return default().store_304(url, headers)


def purge(url: str) -> None:
    default().purge(url)


def clear_all() -> None:
    default().clear_all()
