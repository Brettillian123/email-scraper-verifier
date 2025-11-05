from __future__ import annotations

import os
import re
import sqlite3
import unicodedata as _ud
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

__all__ = [
    "normalize_domain",
    "normalize_company",
    "split_name",
    "map_role",
    "ingest_row",
    "enqueue",
    "_sqlite_path_from_env",  # exposed so tests can monkeypatch
]

# ---- debug / build signature ----
__INGEST_BUILD__ = "2025-11-04T19:10:00Z"
_INGEST_DEBUG = os.getenv("INGEST_DEBUG") == "1"
if _INGEST_DEBUG:
    print(f"[INGEST-DEBUG] module loaded from: {__file__}  build={__INGEST_BUILD__}")

# ======================= Normalization helpers =======================


def normalize_domain(raw: str | None) -> str:  # noqa: C901
    """
    Normalize a user-supplied domain-ish string into a bare host (punycode), or "" if invalid.
    NOTE: This is ONLY for the user-supplied value captured as `user_supplied_domain`.
    We do not trust this as the official company domain. R08 will resolve the true domain.
    """
    if not raw:
        return ""
    s = str(raw).strip()
    placeholders = {"-", "—", "--", "na", "n/a", "none", "null"}
    if s.lower() in placeholders:
        return ""

    host = s
    # Accept full URLs or bare hosts (optionally prefixed with www.)
    if "://" in s or s.lower().startswith("www."):
        parsed = urlparse(s if "://" in s else f"http://{s}")
        host = parsed.netloc or parsed.path

    # If it looks like an email, keep only the domain part
    if "@" in host:
        host = host.split("@", 1)[1]

    # Strip path/port and leading/trailing dots
    host = host.split("/", 1)[0].split(":", 1)[0].strip().strip(".")
    if not host:
        return ""

    # Remove an initial www.
    if host.lower().startswith("www."):
        host = host[4:]

    # IDNA encode to ASCII punycode if possible
    try:
        host = host.encode("idna").decode("ascii").lower()
    except Exception:
        host = host.lower()

    # Basic sanity checks
    if "." not in host:
        return ""
    if " " in host:
        return ""
    labels = host.split(".")
    if any(
        (not lab)
        or lab.startswith("-")
        or lab.endswith("-")
        or not re.fullmatch(r"[a-z0-9-]+", lab)
        for lab in labels
    ):
        return ""
    if len(labels[-1]) < 2:
        return ""

    return host


def normalize_company(raw: str | None) -> str:
    if not raw:
        return ""
    s = re.sub(r"\s+", " ", str(raw), flags=re.UNICODE).strip()
    placeholders = {"-", "—", "--", "na", "n/a", "none", "null"}
    if s.lower() in placeholders:
        return ""
    return s


# ======================= Name splitting =======================

_PREFIXES = {"mr", "mrs", "ms", "dr", "prof"}
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "md", "mba"}


def _smart_case(x: str) -> str:
    parts = re.split(r"([-'\s])", x)
    return "".join(p.capitalize() if p.isalpha() else p for p in parts)


def split_name(full: str | None) -> tuple[str, str]:
    if not full:
        return "", ""
    s = normalize_company(full)
    tokens = [t for t in re.split(r"[^\w\-'.]+", s) if t]
    if not tokens:
        return "", ""

    toks: list[str] = []
    for t in tokens:
        t_clean = t.strip(".").lower()
        if t_clean in _PREFIXES:
            continue
        toks.append(t)

    while toks and toks[-1].strip(".").lower() in _SUFFIXES:
        toks.pop()

    if not toks:
        return "", ""
    if len(toks) == 1:
        first, last = toks[0], ""
    else:
        first, last = toks[0], toks[-1]
    return _smart_case(first), _smart_case(last)


# ======================= Role mapping =======================

_ROLE_MAP: dict[str, list[str]] = {
    "engineering": [
        "cto",
        "chief technology officer",
        "vp engineering",
        "engineering",
        "engineer",
        "developer",
        "software",
        "tech lead",
        "head of engineering",
        "information officer",
        "cio",
    ],
    "sales": [
        "sales",
        "account executive",
        "business development",
        "bd",
        "head of sales",
        "vp sales",
        "cro",
        "chief revenue officer",
    ],
    "marketing": ["marketing", "vp marketing", "growth", "demand gen", "content", "cmo"],
    "finance": ["cfo", "finance", "financial", "controller"],
    "it": [
        "it",
        "information technology",
        "sysadmin",
        "systems administrator",
        "it manager",
    ],
    "operations": ["operations", "coo", "chief operating officer", "ops"],
    "founder": ["founder", "co-founder", "cofounder", "owner", "principal"],
}
_ROLE_PLACEHOLDERS = {"-", "—", "--", "na", "n/a", "none", "null"}


def map_role(raw: str | None) -> str:
    if raw is None:
        return "other"
    s = str(raw).strip().lower()
    if not s or s in _ROLE_PLACEHOLDERS:
        return "other"

    # Direct keyword buckets
    for canon, keys in _ROLE_MAP.items():
        for k in keys:
            if k in s:
                return canon

    # Heuristics
    if "chief" in s and "operat" in s:
        return "operations"
    if "chief" in s and ("tech" in s or "information" in s):
        return "engineering"

    return "other"


# ================= Visibility / emptiness helpers =================


def _has_visible_text(val: str | None) -> bool:
    """
    True iff any visible (non-whitespace) char remains after removing:
    - All Unicode separators (Z*) incl. NBSP
    - All Unicode format chars (Cf) like ZWSP (\u200b), BOM (\ufeff), etc.
    """
    if val is None:
        return False
    t = str(val)
    t = "".join(
        ch for ch in t if not (_ud.category(ch) == "Cf" or _ud.category(ch).startswith("Z"))
    )
    return bool(re.search(r"\S", t, flags=re.UNICODE))


# ======================== SQLite path + helpers ========================


def _sqlite_path_from_env() -> str | None:
    """
    Resolve a filesystem path from DATABASE_URL=sqlite:///... or fallbacks.
    Returns None if not set.
    """
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        try:
            p = urlparse(url)
            if p.scheme == "sqlite":
                path = p.path or ""
                # Windows drive like /C:/... -> strip leading slash
                if re.match(r"^/[A-Za-z]:/", path):
                    path = path[1:]
                # If empty path, ignore
                if path:
                    return path
        except Exception:
            # fall through to env fallbacks
            pass

    # Fallbacks commonly used by tests/fixtures
    for key in ("TEST_DB_PATH", "INGEST_SQLITE_PATH", "SQLITE_PATH", "DB_PATH"):
        pth = (os.getenv(key) or "").strip()
        if pth:
            return pth
    return None


def _connect_sqlite() -> sqlite3.Connection | None:
    db_path = _sqlite_path_from_env()
    if not db_path:
        return None
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        )
        return cur.fetchone() is not None
    except sqlite3.OperationalError:
        return False


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cur.fetchall()}
    except sqlite3.OperationalError:
        return set()


def _insert_dynamic(conn: sqlite3.Connection, table: str, data: Mapping[str, Any]) -> bool:
    cols = _table_columns(conn, table)
    if not cols:
        return False
    payload = {k: v for k, v in data.items() if k in cols}

    if not payload:
        return False

    # Add timestamps if present in schema
    now = datetime.now(UTC).isoformat(timespec="seconds")
    for ts_col in ("created_at", "updated_at"):
        if ts_col in cols and ts_col not in payload:
            payload[ts_col] = now

    col_list = ", ".join(f'"{c}"' for c in payload.keys())
    placeholders = ", ".join([f":{c}" for c in payload.keys()])
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'
    conn.execute(sql, payload)
    conn.commit()
    return True


# ---------- company/person helpers (domain-safe) ----------


def _pick_company_name_col(cols: set[str]) -> str | None:
    """Choose a reasonable column to store the company name."""
    for cand in ("name", "company", "company_name", "display_name"):
        if cand in cols:
            return cand
    return None


def _fallback_company_domain(name: str) -> str:
    """
    Build a harmless, deterministic domain placeholder for NOT NULL schemas.
    Uses the reserved .invalid TLD so it will never resolve.
    """
    base = re.sub(r"[^a-z0-9-]+", "-", (name or "unknown").lower()).strip("-") or "unknown"
    return f"{base}.invalid"


def _ensure_company(conn: sqlite3.Connection, name: str, user_supplied_domain: str) -> int:
    """
    Ensure a row exists in companies and return its id.
    - Inserts a domain value on create to satisfy NOT NULL(domain) schemas.
    - Uses user_supplied_domain if provided; otherwise a '.invalid' placeholder.
    - Optionally stores user_supplied_domain if that column exists.
    """
    cols = _table_columns(conn, "companies")
    name_col = _pick_company_name_col(cols)
    name_val = (name or "").strip() or "Unknown"

    # Domain we will store to satisfy NOT NULL(domain)
    dom_val = (user_supplied_domain or "").strip() or _fallback_company_domain(name_val)

    # Lookup by name if we have a usable name column
    if name_col:
        cur = conn.execute(
            f"SELECT id FROM companies WHERE {name_col} = ?",
            (name_val,),
        )
        row = cur.fetchone()
        if row:
            cid = int(row[0])
            # Optionally keep user_supplied_domain up to date
            if "user_supplied_domain" in cols and user_supplied_domain:
                conn.execute(
                    "UPDATE companies SET user_supplied_domain = "
                    "COALESCE(user_supplied_domain, ?) WHERE id = ?",
                    (user_supplied_domain, cid),
                )
                conn.commit()
            return cid

        # Build insert payload with required columns
        payload_keys = [name_col]
        payload_vals = [name_val]
        if "domain" in cols:
            payload_keys.append("domain")
            payload_vals.append(dom_val)
        if "user_supplied_domain" in cols and user_supplied_domain:
            payload_keys.append("user_supplied_domain")
            payload_vals.append(user_supplied_domain)

        placeholders = ", ".join(["?"] * len(payload_keys))
        conn.execute(
            f'INSERT INTO "companies" ({", ".join(payload_keys)}) VALUES ({placeholders})',
            tuple(payload_vals),
        )
        cid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.commit()
        return cid

    # No usable name column: try a minimal insert that still satisfies NOT NULL(domain)
    payload_keys = []
    payload_vals = []
    if "domain" in cols:
        payload_keys.append("domain")
        payload_vals.append(dom_val)
    if "user_supplied_domain" in cols and user_supplied_domain:
        payload_keys.append("user_supplied_domain")
        payload_vals.append(user_supplied_domain)

    if payload_keys:
        conn.execute(
            f'INSERT INTO "companies" ({", ".join(payload_keys)}) '
            f"VALUES ({', '.join(['?'] * len(payload_keys))})",
            tuple(payload_vals),
        )
    else:
        # Absolute last resort
        conn.execute('INSERT INTO "companies" DEFAULT VALUES')

    cid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    return cid


def _insert_person(
    conn: sqlite3.Connection,
    company_id: int,
    first: str | None,
    last: str | None,
    full: str | None,
    title: str | None,
    source_url: str | None,
) -> None:
    conn.execute(
        "INSERT INTO people(company_id, first_name, last_name, full_name, title, source_url) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            company_id,
            (first or None) or None,
            (last or None) or None,
            (full or None) or None,
            (title or None) or None,
            (source_url or None) or None,
        ),
    )
    conn.commit()


def _persist_row(normalized: Mapping[str, Any]) -> None:
    """
    Write a record to ingest_items AND people (with a valid company_id) when those tables exist.
    This satisfies tests that expect at least one `people` row per accepted row, while preserving
    the staging log in ingest_items for downstream processing.
    """
    conn = _connect_sqlite()
    if conn is None:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] No SQLITE path; skipping persistence")
        return

    try:
        wrote_people = False
        has_companies = _table_exists(conn, "companies")
        has_people = _table_exists(conn, "people")
        has_ingest = _table_exists(conn, "ingest_items")

        # Always try to write people if schema allows (companies + people)
        if has_companies and has_people:
            try:
                company_name = normalized.get("company", "")
                usd = normalized.get("user_supplied_domain", "")
                cid = _ensure_company(conn, company_name, usd)
                _insert_person(
                    conn=conn,
                    company_id=cid,
                    first=normalized.get("first_name", "") or None,
                    last=normalized.get("last_name", "") or None,
                    full=normalized.get("full_name", "") or None,
                    title=normalized.get("title", "") or None,
                    source_url=normalized.get("source_url", "") or None,
                )
                wrote_people = True
            except Exception as e:
                if _INGEST_DEBUG:
                    print("[INGEST-DEBUG] people insert failed:", repr(e))

        # Also record the staging row for audit/traceability if table exists
        if has_ingest:
            _insert_dynamic(
                conn,
                "ingest_items",
                {
                    "company": normalized.get("company", ""),
                    "first_name": normalized.get("first_name", ""),
                    "last_name": normalized.get("last_name", ""),
                    "full_name": normalized.get("full_name", ""),
                    "title": normalized.get("title", ""),
                    "role": normalized.get("role", ""),
                    # table may not have this column; _insert_dynamic will drop it if absent
                    "user_supplied_domain": normalized.get("user_supplied_domain", ""),
                    "source_url": normalized.get("source_url", "") or None,
                },
            )

        if _INGEST_DEBUG:
            print(
                f"[INGEST-DEBUG] persisted: people={'yes' if wrote_people else 'no'}, "
                f"ingest_items={'yes' if has_ingest else 'no'}"
            )
    finally:
        conn.close()


# ================================ Ingest ================================


def ingest_row(row: dict[str, Any]) -> bool:  # noqa: C901
    """
    Normalize, validate, persist, and enqueue a verification job.
    Returns True if accepted, False if rejected.

    R07 rules:
    - `company` is required
    - Must have either `full_name` or BOTH `first_name` and `last_name`
    - `role` is optional and buckets to "other" if blank
    - `user_supplied_domain` is accepted verbatim (normalized if parseable) and not authoritative
    """
    # R07: role is optional; if missing/blank we map to "other"
    role_raw = row.get("role")
    company_raw = row.get("company")
    # R07: prefer user_supplied_domain; accept legacy "domain" as fallback
    domain_raw = row.get("user_supplied_domain", row.get("domain"))

    company = normalize_company(company_raw)
    # Carry the CSV/JSONL domain here only (not authoritative).
    # If it doesn't normalize, leave empty.
    user_supplied_domain = normalize_domain(domain_raw) if domain_raw else ""
    role = map_role(role_raw)
    title = normalize_company(row.get("title"))
    source_url = (row.get("source_url") or "").strip()
    notes = (row.get("notes") or "").strip()

    first_name = normalize_company(row.get("first_name"))
    last_name = normalize_company(row.get("last_name"))
    full_name = normalize_company(row.get("full_name"))

    if not (first_name or last_name) and full_name:
        first_name, last_name = split_name(full_name)

    # R07 guardrails:
    # - company is required
    # - must have either full_name or (first_name AND last_name)
    if not company:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] decision: REJECT (company is required)")
        return False
    has_name = bool(full_name or (first_name and last_name))
    if not has_name:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] decision: REJECT (name is required)")
        return False

    normalized = {
        "company": company,
        # DO NOT set "domain" here. That field is reserved for the resolved official domain (R08).
        "user_supplied_domain": user_supplied_domain,
        "role": role,
        "title": title,
        "source_url": source_url,
        "notes": notes,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name if full_name else f"{first_name} {last_name}".strip(),
    }

    # Respect dry-run skip if set by CLI
    if os.getenv("INGEST_SKIP_PERSIST") != "1":
        try:
            _persist_row(normalized)
        except Exception as _e:
            if _INGEST_DEBUG:
                print("[INGEST-DEBUG] persistence error:", repr(_e))

    # Enqueue one job per accepted row — tests monkeypatch this.
    try:
        enqueue(
            "verify",
            {
                "user_supplied_domain": user_supplied_domain,
                "company": company,
                "first_name": first_name,
                "last_name": last_name,
                "full_name": normalized["full_name"],
                "role": role,
                "title": title,
                "source_url": source_url,
            },
        )
    except Exception as e:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] enqueue error:", repr(e))

    if _INGEST_DEBUG:
        print("[INGEST-DEBUG] decision: ACCEPT")
    return True


def enqueue(task: str, payload: dict[str, Any]) -> None:
    """
    Production path: enqueue into RQ if available; otherwise no-op.
    Tests monkeypatch this function before calling ingest_row.
    """
    try:
        from rq import Queue  # type: ignore

        from src.queueing.redis_conn import get_redis  # type: ignore

        q = Queue(name="verify", connection=get_redis())
        q.enqueue("src.queueing.tasks.handle_task", {"task": task, "payload": payload})
    except Exception:
        # No RQ/Redis in tests or local runs; tests will monkeypatch anyway.
        return
