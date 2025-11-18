# src/resolve/behavior.py
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

# ----------------------------
# Lightweight DB path helper
# ----------------------------


def _db_path(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    url = os.environ.get("DATABASE_URL")
    if url:
        if not url.startswith("sqlite:///"):
            raise RuntimeError(f"O06 only supports sqlite in dev; got {url!r}")
        return url.removeprefix("sqlite:///")
    path = os.environ.get("DATABASE_PATH")
    if path:
        return path
    return "data/dev.db"


# ----------------------------
# Public datatypes
# ----------------------------


@dataclass(frozen=True)
class BehaviorHint:
    profile: str  # "tarpit" | "normal" | "fast"
    connect_timeout: float  # seconds
    command_timeout: float  # seconds
    max_retries: int  # how many probe retries the caller *may* attempt

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "connect_timeout": float(self.connect_timeout),
            "command_timeout": float(self.command_timeout),
            "max_retries": int(self.max_retries),
        }


# ----------------------------
# Internal helpers
# ----------------------------


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _classify_by_code(code: int | None, error_kind: str | None) -> str:
    """
    Map SMTP code / error to coarse category.
    Mirrors R16 classification: accept | hard_fail | temp_fail | unknown.
    """
    if isinstance(code, int):
        if 200 <= code < 300:
            return "accept"
        if 500 <= code < 600:
            return "hard_fail"
        if 400 <= code < 500:
            return "temp_fail"
    # No/unknown code or exceptions/timeouts
    return "unknown" if error_kind else "unknown"


def _fetch_domain_resolution_row_id(
    con: sqlite3.Connection, *, domain: str | None, mx_host: str
) -> int | None:
    """
    Choose the most relevant domain_resolutions row to attach behavior to.
    Priority:
      1) If a domain is provided, prefer the latest row for that domain.
      2) Otherwise, prefer the latest row whose lowest_mx matches mx_host.
      3) As a last resort, look for rows whose mx_hosts JSON contains the host.
    """
    cur = con.cursor()

    if domain:
        row = cur.execute(
            """
            SELECT id FROM domain_resolutions
             WHERE domain = ?
             ORDER BY id DESC
             LIMIT 1
            """,
            (domain.strip().lower(),),
        ).fetchone()
        if row:
            return int(row[0])

    row = cur.execute(
        """
        SELECT id FROM domain_resolutions
         WHERE lowest_mx = ?
         ORDER BY id DESC
         LIMIT 1
        """,
        (mx_host,),
    ).fetchone()
    if row:
        return int(row[0])

    # Fallback: search JSON of mx_hosts (stored as TEXT)
    row = cur.execute(
        """
        SELECT id FROM domain_resolutions
         WHERE mx_hosts IS NOT NULL
           AND mx_hosts <> ''
           AND mx_hosts LIKE ?
         ORDER BY id DESC
         LIMIT 1
        """,
        (f"%{mx_host}%",),
    ).fetchone()
    return int(row[0]) if row else None


def _load_behavior_json(con: sqlite3.Connection, row_id: int) -> dict[str, Any]:
    cur = con.cursor()
    row = cur.execute(
        "SELECT mx_behavior FROM domain_resolutions WHERE id = ?",
        (row_id,),
    ).fetchone()
    if not row or row[0] in (None, "", "null"):
        return {}
    try:
        data = json.loads(row[0])
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_behavior_json(con: sqlite3.Connection, row_id: int, payload: dict[str, Any]) -> None:
    cur = con.cursor()
    cur.execute(
        "UPDATE domain_resolutions SET mx_behavior = ? WHERE id = ?",
        (json.dumps(payload, ensure_ascii=False, separators=(",", ":")), row_id),
    )


def _ewma(prev: float | None, new: float, alpha: float = 0.3) -> float:
    return (alpha * new) + ((1.0 - alpha) * (prev or new))


# ----------------------------
# Public API (O06)
# ----------------------------


def record_mx_probe(
    mx_host: str,
    code: int | None,
    elapsed: float,
    *,
    error_kind: str | None = None,
    domain: str | None = None,
    db_path: str | None = None,
) -> None:
    """
    Update aggregated MX behavior stats on the latest relevant domain_resolutions row.

    Schema contract:
      - domain_resolutions.mx_behavior is TEXT (JSON). We store:
        {
          "hosts": {
            "<mx_host>": {
              "n": 12,
              "accept": 8, "hard_fail": 2, "temp_fail": 1, "unknown": 1,
              "avg_ms": 180.2,
              "last_code": 250,
              "last_error": null,
              "updated_at": "2025-11-17T23:50:01Z"
            },
            ...
          },
          "updated_at": "...",
          "version": "o06.1"
        }
    """
    mx_host = (mx_host or "").strip()
    if not mx_host:
        return

    db = _db_path(db_path)
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        row_id = _fetch_domain_resolution_row_id(con, domain=domain, mx_host=mx_host)
        if row_id is None:
            # Nothing to attach to yet; O06 is best-effort.
            return

        payload = _load_behavior_json(con, row_id)
        hosts = payload.get("hosts")
        if not isinstance(hosts, dict):
            hosts = {}
            payload["hosts"] = hosts

        stats = hosts.get(mx_host)
        if not isinstance(stats, dict):
            stats = {
                "n": 0,
                "accept": 0,
                "hard_fail": 0,
                "temp_fail": 0,
                "unknown": 0,
                "avg_ms": None,  # EWMA; becomes float after first point
                "last_code": None,
                "last_error": None,
                "updated_at": None,
            }
            hosts[mx_host] = stats

        # Update aggregates
        category = _classify_by_code(code, error_kind)
        stats["n"] = int(stats.get("n", 0) or 0) + 1
        stats[category] = int(stats.get(category, 0) or 0) + 1
        stats["avg_ms"] = float(_ewma(stats.get("avg_ms"), float(elapsed) * 1000.0))
        stats["last_code"] = int(code) if isinstance(code, int) else None
        stats["last_error"] = str(error_kind) if error_kind else None
        stats["updated_at"] = _utc_now_iso()

        payload["updated_at"] = stats["updated_at"]
        payload["version"] = "o06.1"

        _save_behavior_json(con, row_id, payload)
        con.commit()


def get_behavior_hint(
    *,
    mx_host: str,
    domain: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    """
    Compute a simple, explainable behavior hint for a given MX (optionally scoped by domain).

    Heuristics (tunable, conservative by default):
      - If we have >4 samples and (temp_fail+unknown)/n >= 0.5  OR avg_ms >= 800ms => "tarpit"
      - Else if n >= 5 and avg_ms <= 150ms and hard_fail/n < 0.05 => "fast"
      - Else => "normal"

    Returns:
      {
        "profile": "tarpit"|"normal"|"fast",
        "connect_timeout": <float>,
        "command_timeout": <float>,
        "max_retries": <int>
      }
    """
    mx_host = (mx_host or "").strip()
    if not mx_host:
        return BehaviorHint("normal", 10.0, 10.0, 1).as_dict()

    db = _db_path(db_path)
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        row_id = _fetch_domain_resolution_row_id(con, domain=domain, mx_host=mx_host)
        if row_id is None:
            return BehaviorHint("normal", 10.0, 10.0, 1).as_dict()

        payload = _load_behavior_json(con, row_id)
        hosts = payload.get("hosts") if isinstance(payload, dict) else None
        stats = hosts.get(mx_host) if isinstance(hosts, dict) else None

        if not isinstance(stats, dict) or not stats.get("n"):
            return BehaviorHint("normal", 10.0, 10.0, 1).as_dict()

        n = int(stats.get("n", 0) or 0)
        avg_ms = float(stats.get("avg_ms", 0.0) or 0.0)
        temp_unknown = int(stats.get("temp_fail", 0) or 0) + int(stats.get("unknown", 0) or 0)
        hard = int(stats.get("hard_fail", 0) or 0)

        # Heuristics
        if n >= 4 and (temp_unknown / max(n, 1) >= 0.5 or avg_ms >= 800.0):
            hint = BehaviorHint("tarpit", connect_timeout=5.0, command_timeout=5.0, max_retries=0)
        elif n >= 5 and avg_ms <= 150.0 and (hard / max(n, 1) < 0.05):
            hint = BehaviorHint("fast", connect_timeout=8.0, command_timeout=8.0, max_retries=1)
        else:
            hint = BehaviorHint(
                "normal",
                connect_timeout=10.0,
                command_timeout=10.0,
                max_retries=1,
            )

        return hint.as_dict()
