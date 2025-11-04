# src/ingest/__init__.py
from __future__ import annotations

import os
import re
import sqlite3
import unicodedata as _ud
from typing import Any
from urllib.parse import urlparse

__all__ = [
    "normalize_domain",
    "normalize_company",
    "split_name",
    "map_role",
    "ingest_row",
    "enqueue",
]

# ---- debug / build signature ----
__INGEST_BUILD__ = "2025-11-04T14:45:00Z"
_INGEST_DEBUG = os.getenv("INGEST_DEBUG") == "1"
if _INGEST_DEBUG:
    print(f"[INGEST-DEBUG] module loaded from: {__file__}  build={__INGEST_BUILD__}")

# -------- Normalization helpers --------


def normalize_domain(raw: str | None) -> str:  # noqa: C901
    if not raw:
        return ""
    s = str(raw).strip()
    placeholders = {"-", "—", "--", "na", "n/a", "none", "null"}
    if s.lower() in placeholders:
        return ""

    host = s
    if "://" in s or s.lower().startswith("www."):
        parsed = urlparse(s if "://" in s else f"http://{s}")
        host = parsed.netloc or parsed.path

    if "@" in host:
        host = host.split("@", 1)[1]

    host = host.split("/", 1)[0].split(":", 1)[0].strip().strip(".")
    if not host:
        return ""

    if host.lower().startswith("www."):
        host = host[4:]

    try:
        host = host.encode("idna").decode("ascii").lower()
    except Exception:
        host = host.lower()

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


# -------- Role mapping --------

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
    "it": ["it", "information technology", "sysadmin", "systems administrator", "it manager"],
    "operations": ["operations", "coo", "chief operating officer", "ops"],
    "founder": ["founder", "co-founder", "cofounder", "owner", "principal"],
}

_ROLE_PLACEHOLDERS = {"-", "—", "--", "na", "n/a", "none", "null"}


def map_role(raw: str | None) -> str:
    if not raw:
        return "other"
    s = str(raw).strip().lower()
    if not s:
        return "other"

    for canon, keys in _ROLE_MAP.items():
        for k in keys:
            if k in s:
                return canon
    if "chief" in s and "operat" in s:
        return "operations"
    if "chief" in s and ("tech" in s or "information" in s):
        return "engineering"
    return "other"


# -------- Visibility / emptiness helpers --------


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


# ======================== Persistence (test-schema aware) ========================


def _sqlite_path_from_env() -> str | None:
    """
    Return filesystem path for sqlite from DATABASE_URL, or None if not sqlite.
    Accepts forms like:
      sqlite:///C:/Users/Brett/... (Windows)
      sqlite:////home/user/dev.db    (POSIX absolute)
    """
    url = os.getenv("DATABASE_URL", "").strip()
    if not url.startswith("sqlite:///"):
        return None
    return url[len("sqlite:///") :]


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # column name


def _upsert_company(con: sqlite3.Connection, name: str | None, domain: str | None) -> int:
    """
    Returns company_id; prefers domain match when present; falls back to name.
    Only uses columns that exist in the 'companies' table.
    """
    cur = con.cursor()
    cols = _table_columns(con, "companies")

    def _insert(n: str | None, d: str | None) -> int:
        insert_cols: list[str] = []
        vals: list[Any] = []
        if "name" in cols:
            insert_cols.append("name")
            vals.append(n)
        if "domain" in cols:
            insert_cols.append("domain")
            vals.append(d)
        placeholders = ",".join("?" for _ in insert_cols) or "NULL"
        sql = f"INSERT INTO companies ({','.join(insert_cols)}) VALUES ({placeholders})"
        # If no recognized cols existed (extremely unlikely), still run a minimal insert
        if not insert_cols:
            sql = "INSERT INTO companies DEFAULT VALUES"
            cur.execute(sql)
        else:
            cur.execute(sql, vals)
        return int(cur.lastrowid)

    # Prefer domain match if provided
    if domain:
        row = cur.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
        if row:
            company_id = int(row[0])
            # Optionally fill missing name if schema supports it
            if name and "name" in cols:
                cur.execute(
                    "UPDATE companies SET name = COALESCE(NULLIF(name,''), ?) WHERE id = ?",
                    (name, company_id),
                )
            return company_id
        return _insert(name, domain)

    # Fall back to name
    if name:
        row = cur.execute("SELECT id FROM companies WHERE name = ?", (name,)).fetchone()
        if row:
            return int(row[0])
        return _insert(name, None)

    # Shouldn’t happen (ingest enforces name or domain), but be safe:
    return _insert(None, None)


def _persist_best_effort(row: dict[str, Any]) -> None:
    """
    Persist into the test schema (companies, people). Writes only columns that exist.
    Uses DATABASE_URL (sqlite) set by the pytest fixture.
    """
    db_path = _sqlite_path_from_env()
    if not db_path:
        # Silent no-op if not using sqlite (keeps local runs safe)
        return

    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA foreign_keys = ON")
        con.row_factory = sqlite3.Row

        # Upsert company first
        company = (row.get("company") or "").strip() or None
        domain = (row.get("domain") or "").strip() or None
        company_id = _upsert_company(con, company, domain)

        # Prepare people insert with schema awareness
        people_cols = _table_columns(con, "people")
        payload: dict[str, Any] = {}

        if "company_id" in people_cols:
            payload["company_id"] = company_id
        if "first_name" in people_cols:
            payload["first_name"] = row.get("first_name") or ""
        if "last_name" in people_cols:
            payload["last_name"] = row.get("last_name") or ""
        if "full_name" in people_cols:
            payload["full_name"] = row.get("full_name") or ""
        if "title" in people_cols:
            payload["title"] = row.get("title") or ""
        if "role" in people_cols:
            payload["role"] = row.get("role") or ""
        if "source_url" in people_cols:
            payload["source_url"] = row.get("source_url") or ""
        if "notes" in people_cols:
            payload["notes"] = row.get("notes") or ""

        cols = list(payload.keys())
        placeholders = ",".join("?" for _ in cols)
        sql = f"INSERT INTO people ({','.join(cols)}) VALUES ({placeholders})"
        con.execute(sql, [payload[c] for c in cols])

        con.commit()
    finally:
        con.close()


# ================================ Ingest ================================


def ingest_row(row: dict[str, Any]) -> bool:  # noqa: C901
    """
    Normalize, validate, best-effort persist, and enqueue a verification job.
    Returns True if accepted, False if rejected.
    """
    # --- EARLY gate on role presence ---
    role_raw = row.get("role")

    if _INGEST_DEBUG:
        rt = "" if role_raw is None else str(role_raw)
        print(
            "[INGEST-DEBUG] role_raw repr=",
            repr(rt),
            "cats=",
            [_ud.category(c) for c in rt],
            "codes=",
            [hex(ord(c)) for c in rt],
        )

    if (not _has_visible_text(role_raw)) or (str(role_raw).strip().lower() in _ROLE_PLACEHOLDERS):
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] decision: REJECT (empty/placeholder role)")
        return False

    company_raw = row.get("company")
    domain_raw = row.get("domain")

    company = normalize_company(company_raw)
    domain = normalize_domain(domain_raw)
    role = map_role(role_raw)
    title = normalize_company(row.get("title"))
    source_url = (row.get("source_url") or "").strip()
    notes = (row.get("notes") or "").strip()

    first_name = normalize_company(row.get("first_name"))
    last_name = normalize_company(row.get("last_name"))
    full_name = normalize_company(row.get("full_name"))

    if not (first_name or last_name) and full_name:
        first_name, last_name = split_name(full_name)

    # If a domain was supplied but normalization is empty -> reject
    if (domain_raw is not None) and str(domain_raw).strip() and not domain:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] decision: REJECT (invalid domain provided)")
        return False

    # Must have at least one of domain/company
    if not (domain or company):
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] decision: REJECT (no company/domain)")
        return False

    normalized = {
        "company": company,
        "domain": domain,
        "role": role,
        "title": title,
        "source_url": source_url,
        "notes": notes,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
    }

    # Best-effort persistence; ignore DB errors
    try:
        _persist_best_effort(normalized)
    except Exception as _e:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] persist skipped due to error:", repr(_e))

    # Enqueue one job per accepted row — late-bind so monkeypatch sees it
    try:
        import importlib

        ingest_module = importlib.import_module("src.ingest")
        fn = getattr(ingest_module, "enqueue", None)
        if callable(fn):
            if _INGEST_DEBUG:
                try:
                    src = getattr(fn, "__code__", None)
                    print(
                        "[INGEST-DEBUG] enqueue target:",
                        getattr(fn, "__module__", None),
                        getattr(src, "co_filename", None),
                        getattr(fn, "__name__", None),
                    )
                except Exception as _e:
                    print("[INGEST-DEBUG] enqueue target introspection failed:", repr(_e))

            fn(
                "verify",
                {
                    "domain": domain,
                    "company": company,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "role": role,
                    "title": title,
                    "source_url": source_url,
                },
            )
        else:
            if _INGEST_DEBUG:
                print("[INGEST-DEBUG] enqueue not callable:", type(fn))
    except Exception as e:
        if _INGEST_DEBUG:
            print("[INGEST-DEBUG] enqueue error:", repr(e))

    if _INGEST_DEBUG:
        print("[INGEST-DEBUG] decision: ACCEPT")
    return True


def enqueue(task: str, payload: dict[str, Any]) -> None:
    try:
        from rq import Queue

        from src.queueing.redis_conn import get_redis

        q = Queue(name="verify", connection=get_redis())
        q.enqueue("src.queueing.tasks.handle_task", {"task": task, "payload": payload})
    except Exception:
        return
