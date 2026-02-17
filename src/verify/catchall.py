"""
R17 â€” Domain-level catch-all detection (with caching on domain_resolutions).

Public API:

    check_catchall_for_domain(domain: str, *, force: bool = False) -> CatchallResult

Behavior:
- Normalizes the domain (strip/lower).
- Looks for a fresh cached catch_all_* verdict on domain_resolutions.
- If fresh (and force=False), returns a cached CatchallResult (no SMTP traffic).
- Otherwise:
    * Ensures MX is resolved via src.resolve.mx.get_or_resolve_mx().
    * If no MX/host -> status="no_mx" (no SMTP call).
    * Generates a random local-part (_ca_<hex>).
    * Uses _smtp_probe_random_address(mx_host, domain, localpart) to probe.
    * Classifies the result into:
        - "catch_all"      (2xx)
        - "not_catch_all"  (5xx)
        - "tempfail"       (4xx or timeout / transient errors)
        - "error"          (unexpected / odd cases)
    * Persists catch_all_* columns on the latest domain_resolutions row.

Tests monkeypatch:

- get_connection() â†’ in-memory SQLite DB with a minimal domain_resolutions table.
- get_or_resolve_mx() and _smtp_probe_random_address() for controlled behavior.
"""

from __future__ import annotations

import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from src.config import (
    SMTP_COMMAND_TIMEOUT,
    SMTP_CONNECT_TIMEOUT,
    SMTP_HELO_DOMAIN,
    SMTP_MAIL_FROM,
)
from src.resolve.mx import get_or_resolve_mx
from src.verify import smtp as smtp_mod

try:  # pragma: no cover
    from src.verify.preflight import (
        SmtpProbingDisabledError,
        assert_smtp_probing_allowed,
    )
except Exception:  # pragma: no cover
    SmtpProbingDisabledError = RuntimeError  # type: ignore

    def assert_smtp_probing_allowed() -> None:  # type: ignore
        raise RuntimeError("SMTP preflight module unavailable; refusing to run SMTP probing.")


CatchallStatus = Literal[
    "catch_all",
    "not_catch_all",
    "tempfail",
    "no_mx",
    "error",
]


@dataclass
class CatchallResult:
    domain: str
    status: CatchallStatus
    mx_host: str | None
    rcpt_code: int | None
    cached: bool
    localpart: str | None
    elapsed_ms: float
    error: str | None = None


# 24h caching window; tests may override this constant or _now_utc().
CATCHALL_TTL_SECONDS = 24 * 60 * 60


# ---------------------------------------------------------------------------
# Time / DB helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _utc_iso(dt: datetime | None = None) -> str:
    if dt is None:
        dt = _now_utc()
    # Match project style: "2025-11-17T22:43:15Z"
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    txt = s.strip()
    if not txt:
        return None
    try:
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        return datetime.fromisoformat(txt)
    except Exception:
        return None


def _is_fresh(checked_at: str | None) -> bool:
    dt = _parse_ts(checked_at)
    if not dt:
        return False
    delta = _now_utc() - dt
    return delta.total_seconds() < CATCHALL_TTL_SECONDS


def get_connection():
    """
    R17 helper: return a DB connection (PostgreSQL or SQLite).

    Tests monkeypatch this to point at an in-memory DB.

    - If DATABASE_URL points to PostgreSQL, uses src.db.get_conn()
      (returns CompatConnection which translates ? placeholders to %s).
    - Otherwise falls back to SQLite via DATABASE_PATH or data/dev.db.
    """
    db_url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    if db_url.startswith("postgres://") or db_url.startswith("postgresql://"):
        from src.db import get_conn

        return get_conn()

    # SQLite fallback (dev/test only)
    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    return sqlite3.connect(db_path)


def _load_cached_state(
    con: sqlite3.Connection,
    domain: str,
) -> dict | None:
    """
    Load the latest catch_all_* state for a domain from domain_resolutions.

    Returns a dict with:
        {
          "id",
          "catch_all_status",
          "catch_all_checked_at",
          "catch_all_localpart",
          "catch_all_smtp_code",
        }
    or None if no row exists yet.

    NOTE: PostgreSQL schema uses chosen_domain and user_hint columns.
    There is NO 'domain' column in the production schema.
    """
    cur = con.execute(
        """
        SELECT
            id,
            catch_all_status,
            catch_all_checked_at,
            catch_all_localpart,
            catch_all_smtp_code
        FROM domain_resolutions
        WHERE chosen_domain = ?
           OR user_hint = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (domain, domain),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "catch_all_status": row[1],
        "catch_all_checked_at": row[2],
        "catch_all_localpart": row[3],
        "catch_all_smtp_code": row[4],
    }


def _update_cached_state(
    con: sqlite3.Connection,
    domain: str,
    *,
    status: CatchallStatus,
    localpart: str | None,
    rcpt_code: int | None,
    rcpt_msg: str | None,
) -> None:
    """
    Update catch_all_* columns on the most recent domain_resolutions row
    for this domain. If no row exists, we silently skip.

    NOTE: PostgreSQL schema uses chosen_domain and user_hint columns.
    There is NO 'domain' column in the production schema.
    """
    cur = con.execute(
        """
        SELECT id
        FROM domain_resolutions
        WHERE chosen_domain = ?
           OR user_hint = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (domain, domain),
    )
    row = cur.fetchone()
    if not row:
        return

    row_id = int(row[0])
    checked_at = _utc_iso()
    con.execute(
        """
        UPDATE domain_resolutions
        SET catch_all_status      = ?,
            catch_all_checked_at  = ?,
            catch_all_localpart   = ?,
            catch_all_smtp_code   = ?,
            catch_all_smtp_msg    = ?
        WHERE id = ?
        """,
        (
            status,
            checked_at,
            localpart,
            rcpt_code,
            (rcpt_msg or None),
            row_id,
        ),
    )
    con.commit()


def _classify_from_probe(code: int | None, error: str | None) -> CatchallStatus:
    """
    Map RCPT code / error into one of the R17 statuses.
    """
    if isinstance(code, int):
        if 200 <= code < 300:
            return "catch_all"
        if 500 <= code < 600:
            return "not_catch_all"
        if 400 <= code < 500:
            return "tempfail"
        # unexpected SMTP code â†’ generic error
        return "error"

    if error:
        e = error.lower()
        # Treat timeouts / transient conditions as tempfail
        if "timeout" in e or "tempor" in e or "tempfail" in e or "temp_fail" in e:
            return "tempfail"
        return "error"

    return "error"


# ---------------------------------------------------------------------------
# SMTP helper â€“ tests monkeypatch _smtp_probe_random_address
# ---------------------------------------------------------------------------


def _smtp_probe_random_address(
    mx_host: str,
    domain: str,
    localpart: str,
) -> tuple[int | None, bytes | None, float, str | None]:
    """
    Low-level SMTP probe used by R17.

    HARD GUARDRAIL:
      This is a TCP/25 operation. It must be blocked on non-approved hosts.

    Tests replace this with a fake via monkeypatch.setattr(catchall_mod,
    "_smtp_probe_random_address", fake_probe).
    """
    # Enforce "where can SMTP probing run?" before any preflight/probe.
    assert_smtp_probing_allowed()

    email = f"{localpart}@{domain}"
    started = time.perf_counter()
    try:
        res: dict[str, Any] = smtp_mod.probe_rcpt(
            email,
            mx_host,
            helo_domain=SMTP_HELO_DOMAIN,
            mail_from=SMTP_MAIL_FROM,
            connect_timeout=SMTP_CONNECT_TIMEOUT,
            command_timeout=SMTP_COMMAND_TIMEOUT,
            behavior_hint=None,
        )
        code = res.get("code")
        msg = res.get("message")
        error = res.get("error")
        elapsed_ms = float(res.get("elapsed_ms") or int((time.perf_counter() - started) * 1000))

        if isinstance(msg, (bytes, bytearray)):
            msg_bytes: bytes | None = bytes(msg)
        elif msg is None:
            msg_bytes = None
        else:
            try:
                msg_bytes = str(msg).encode("latin-1", errors="replace")
            except Exception:
                msg_bytes = None

        return (
            int(code) if isinstance(code, int) else None,
            msg_bytes,
            elapsed_ms,
            str(error) if error is not None else None,
        )
    except SmtpProbingDisabledError:
        # Preserve the explicit guardrail error for clear operator visibility.
        elapsed_ms = float(int((time.perf_counter() - started) * 1000))
        raise
    except Exception as exc:
        elapsed_ms = float(int((time.perf_counter() - started) * 1000))
        return None, None, elapsed_ms, f"{type(exc).__name__}:{exc}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_catchall_for_domain(
    domain: str,
    *,
    force: bool = False,
) -> CatchallResult:
    """
    Check whether a domain behaves as catch-all via SMTP RCPT probe.

    This function:
      - Reads / writes domain_resolutions.catch_all_* (cached verdict).
      - Respects a 24h TTL (CATCHALL_TTL_SECONDS), unless force=True.
      - Uses R15/O06 MX resolution and this module's SMTP probe helper.
    """
    dom = (domain or "").strip().lower()
    if not dom or "@" in dom:
        raise ValueError("domain_required")

    con = get_connection()

    # --- Cached path --------------------------------------------------------
    cached_row = _load_cached_state(con, dom)
    if (
        cached_row
        and not force
        and cached_row.get("catch_all_status") is not None
        and _is_fresh(cached_row.get("catch_all_checked_at"))
    ):
        # Use cached verdict, no new SMTP call
        elapsed_ms = 0.0
        return CatchallResult(
            domain=dom,
            status=cached_row["catch_all_status"],
            mx_host=None,  # we don't store MX host in this table yet
            rcpt_code=cached_row["catch_all_smtp_code"],
            cached=True,
            localpart=cached_row["catch_all_localpart"],
            elapsed_ms=elapsed_ms,
            error=None,
        )

    # --- Fresh probe path ---------------------------------------------------
    # Pass db_path only for SQLite; get_or_resolve_mx uses get_conn() on PG.
    _db_url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip().lower()
    _is_pg = _db_url.startswith("postgres://") or _db_url.startswith("postgresql://")
    mx_kwargs: dict = {"force": force}
    if not _is_pg:
        mx_kwargs["db_path"] = os.getenv("DATABASE_PATH") or "data/dev.db"

    try:
        mx_res = get_or_resolve_mx(dom, **mx_kwargs)
    except Exception as exc:
        status: CatchallStatus = "error"
        _update_cached_state(
            con,
            dom,
            status=status,
            localpart=None,
            rcpt_code=None,
            rcpt_msg=str(exc),
        )
        return CatchallResult(
            domain=dom,
            status=status,
            mx_host=None,
            rcpt_code=None,
            cached=False,
            localpart=None,
            elapsed_ms=0.0,
            error=str(exc),
        )

    # Support both dataclass-like and dict-like return types
    if isinstance(mx_res, dict):
        mx_host = mx_res.get("lowest_mx") or None
    else:
        mx_host = getattr(mx_res, "lowest_mx", None) or None

    if not mx_host:
        # No MX / A-only fallback â†’ no_mx, no SMTP call
        status = "no_mx"
        _update_cached_state(
            con,
            dom,
            status=status,
            localpart=None,
            rcpt_code=None,
            rcpt_msg="no_mx",
        )
        return CatchallResult(
            domain=dom,
            status=status,
            mx_host=None,
            rcpt_code=None,
            cached=False,
            localpart=None,
            elapsed_ms=0.0,
            error=None,
        )

    # Generate random local-part and probe via helper (tests monkeypatch this)
    localpart = f"_ca_{secrets.token_hex(8)}"
    code, msg_bytes, probe_elapsed_ms, error = _smtp_probe_random_address(
        mx_host,
        dom,
        localpart,
    )

    status = _classify_from_probe(code, error)

    if isinstance(msg_bytes, (bytes, bytearray)):
        rcpt_msg = msg_bytes.decode("latin-1", errors="replace")
    elif msg_bytes is None:
        rcpt_msg = None
    else:
        rcpt_msg = str(msg_bytes)

    _update_cached_state(
        con,
        dom,
        status=status,
        localpart=localpart,
        rcpt_code=code if isinstance(code, int) else None,
        rcpt_msg=rcpt_msg,
    )

    # For tests, we need error to be non-None for tempfail cases as well.
    error_out: str | None
    if status in ("error", "tempfail"):
        error_out = error or rcpt_msg or "tempfail"
    else:
        error_out = None

    return CatchallResult(
        domain=dom,
        status=status,
        mx_host=mx_host,
        rcpt_code=code if isinstance(code, int) else None,
        cached=False,
        localpart=localpart,
        elapsed_ms=float(probe_elapsed_ms),
        error=error_out,
    )


__all__ = [
    "CatchallStatus",
    "CatchallResult",
    "CATCHALL_TTL_SECONDS",
    "get_connection",
    "_smtp_probe_random_address",
    "check_catchall_for_domain",
]
