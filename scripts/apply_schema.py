#!/usr/bin/env python
# scripts/apply_schema.py
from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA_FILE = ROOT / "db" / "schema.sql"
DEFAULT_MIGRATIONS_DIR = ROOT / "db" / "migrations"

# Columns that must NEVER be uniquely indexed (multi-brand rule)
_OFFICIAL_COLS = ("official_domain", "domain_official")

_DOLLAR_TAG_RE = re.compile(r"\$[A-Za-z0-9_]*\$")


def _import_get_conn():
    """
    Import get_conn() in a way that works when running as:
      - `python scripts/apply_schema.py` (repo root is on sys.path), or
      - a direct script call without PYTHONPATH configured.
    """
    try:
        from src.db import get_conn  # type: ignore
    except Exception:
        sys.path.insert(0, str(ROOT))
        from src.db import get_conn  # type: ignore
    return get_conn


def _is_postgres_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


@dataclass
class _SqlSplitState:
    in_single: bool = False
    in_double: bool = False
    in_line_comment: bool = False
    in_block_comment: bool = False
    dollar_tag: str | None = None


def _match_dollar_tag(sql_text: str, pos: int) -> str | None:
    m = _DOLLAR_TAG_RE.match(sql_text, pos)
    return m.group(0) if m else None


def _append_stmt_if_any(out: list[str], buf: list[str]) -> None:
    stmt = "".join(buf).strip()
    if stmt:
        out.append(stmt + ";")


def _consume_sql_char(
    sql_text: str, i: int, n: int, buf: list[str], out: list[str], st: _SqlSplitState
) -> int:
    ch = sql_text[i]
    nxt = sql_text[i + 1] if i + 1 < n else ""

    if st.in_line_comment:
        buf.append(ch)
        if ch == "\n":
            st.in_line_comment = False
        return i + 1

    if st.in_block_comment:
        buf.append(ch)
        if ch == "*" and nxt == "/":
            buf.append(nxt)
            st.in_block_comment = False
            return i + 2
        return i + 1

    if st.dollar_tag is not None:
        tag = st.dollar_tag
        if sql_text.startswith(tag, i):
            buf.append(tag)
            st.dollar_tag = None
            return i + len(tag)
        buf.append(ch)
        return i + 1

    if not st.in_single and not st.in_double:
        if ch == "-" and nxt == "-":
            buf.append(ch)
            buf.append(nxt)
            st.in_line_comment = True
            return i + 2

        if ch == "/" and nxt == "*":
            buf.append(ch)
            buf.append(nxt)
            st.in_block_comment = True
            return i + 2

        if ch == "$":
            tag = _match_dollar_tag(sql_text, i)
            if tag:
                buf.append(tag)
                st.dollar_tag = tag
                return i + len(tag)

    if ch == "'" and not st.in_double:
        buf.append(ch)
        if st.in_single and nxt == "'":
            buf.append(nxt)
            return i + 2
        st.in_single = not st.in_single
        return i + 1

    if ch == '"' and not st.in_single:
        buf.append(ch)
        st.in_double = not st.in_double
        return i + 1

    if ch == ";" and not st.in_single and not st.in_double:
        _append_stmt_if_any(out, buf)
        buf.clear()
        return i + 1

    buf.append(ch)
    return i + 1


def _split_sql_statements(sql_text: str) -> list[str]:  # noqa: C901
    """
    Robust-ish SQL splitter:
      - Splits on semicolons not inside:
          * single quotes
          * double quotes
          * dollar-quoted blocks ($$...$$ or $tag$...$tag$)
          * line comments (-- ...)
          * block comments (/* ... */)

    Returns statements with trailing ';' preserved.
    """
    out: list[str] = []
    buf: list[str] = []
    st = _SqlSplitState()

    i = 0
    n = len(sql_text)
    while i < n:
        i = _consume_sql_char(sql_text, i, n, buf, out, st)

    tail = "".join(buf).strip()
    if tail:
        out.append(tail if tail.endswith(";") else tail + ";")

    return out


def _read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path.read_text(encoding="utf-8")


def _is_official_unique_stmt(stmt: str) -> bool:
    """
    True if the SQL creates a UNIQUE index on companies(official_domain|domain_official).
    Robust to case, whitespace, quotes/brackets.
    """
    s = stmt.strip()
    if not re.match(r"(?is)^CREATE\s+UNIQUE\s+INDEX\b", s):
        return False
    pat = r'(?is)\bON\s+["\[]?companies["\]]?\s*\(\s*["\[]?(official_domain|domain_official)["\]]?\s*\)'
    return re.search(pat, s) is not None


def _drop_unique_official_if_present(conn) -> None:
    """
    If a UNIQUE index exists on companies.(official_domain|domain_official), drop it.
    This enforces "many companies → one domain" regardless of existing state.
    """
    try:
        rows = conn.execute(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'companies'
            """
        ).fetchall()
    except Exception:
        return

    for row in rows or []:
        try:
            indexname = row[0] if isinstance(row, tuple) else row["indexname"]
            indexdef = row[1] if isinstance(row, tuple) else row["indexdef"]
        except Exception:
            continue

        idef = str(indexdef or "")
        if "UNIQUE" not in idef.upper():
            continue

        idef_l = idef.lower()
        if any(c in idef_l for c in _OFFICIAL_COLS):
            try:
                conn.execute(f'DROP INDEX IF EXISTS "{indexname}"')
            except Exception:
                pass


def _ensure_schema_migrations_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version TEXT PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def _applied_migrations(conn) -> set[str]:
    try:
        rows = conn.execute("SELECT version FROM schema_migrations;").fetchall() or []
        out: set[str] = set()
        for r in rows:
            if isinstance(r, tuple):
                out.add(str(r[0]))
            else:
                out.add(str(r["version"]))
        return out
    except Exception:
        return set()


def _apply_sql_text(conn, sql_text: str) -> None:
    for stmt in _split_sql_statements(sql_text):
        # Multi-brand guard: never allow unique index on official_domain / domain_official
        if _is_official_unique_stmt(stmt):
            print(
                "· Skipping UNIQUE index on companies.(official_domain|domain_official) per multi-brand rule"
            )
            continue

        if not stmt.strip("; \n\t\r"):
            continue

        conn.execute(stmt)

    # Enforce multi-brand rule post-apply as well (defensive)
    _drop_unique_official_if_present(conn)


def _apply_migrations(conn, migrations_dir: Path) -> int:
    if not migrations_dir.exists():
        return 0

    files = sorted([p for p in migrations_dir.glob("*.sql") if p.is_file()])
    if not files:
        return 0

    _ensure_schema_migrations_table(conn)
    applied = _applied_migrations(conn)

    applied_now = 0
    for p in files:
        version = p.name
        if version in applied:
            continue

        print(f"→ Applying migration: {p.relative_to(ROOT)}")
        sql_text = _read_text(p)
        _apply_sql_text(conn, sql_text)
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?);", (version,))
        conn.commit()
        applied_now += 1

    return applied_now


def _verify_postgres(conn) -> None:
    # Lightweight sanity check
    conn.execute("SELECT 1;").fetchone()


def _object_exists(conn, name: str, kind: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False

    k = (kind or "").strip().lower()
    try:
        if k == "table":
            return (
                conn.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema='public' AND table_name=? LIMIT 1",
                    (n,),
                ).fetchone()
                is not None
            )
        if k == "view":
            return (
                conn.execute(
                    "SELECT 1 FROM information_schema.views "
                    "WHERE table_schema='public' AND table_name=? LIMIT 1",
                    (n,),
                ).fetchone()
                is not None
            )
        if k == "index":
            return (
                conn.execute(
                    "SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=? LIMIT 1",
                    (n,),
                ).fetchone()
                is not None
            )
    except Exception:
        return False
    return False


def _print_summary(conn) -> None:
    for t in (
        "tenants",
        "users",
        "runs",
        "companies",
        "sources",
        "people",
        "emails",
        "verification_results",
    ):
        exists = "yes" if _object_exists(conn, t, "table") else "no"
        print(f"· {t:24} exists: {exists}")

    v_exists = "yes" if _object_exists(conn, "v_emails_latest", "view") else "no"
    print(f"· v_emails_latest         exists: {v_exists}")

    mig_exists = "yes" if _object_exists(conn, "schema_migrations", "table") else "no"
    print(f"· schema_migrations       exists: {mig_exists}")


def main(argv: Iterable[str] | str | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Apply Postgres schema + SQL migrations (Postgres-only)."
    )
    parser.add_argument(
        "--db",
        dest="db_url",
        help="Postgres URL (overrides DATABASE_URL/DB_URL for this run).",
    )
    parser.add_argument(
        "--schema",
        dest="schema_file",
        default=str(DEFAULT_SCHEMA_FILE),
        help="Path to db/schema.sql (defaults to repo db/schema.sql).",
    )
    parser.add_argument(
        "--migrations",
        dest="migrations_dir",
        default=str(DEFAULT_MIGRATIONS_DIR),
        help="Path to db/migrations directory (defaults to repo db/migrations).",
    )

    # Defensive: if someone calls main(db_url_string), treat it as --db <url>
    if isinstance(argv, str):
        argv_list: list[str] | None = ["--db", argv]
    elif argv is None:
        argv_list = None
    else:
        argv_list = list(argv)

    args = parser.parse_args(argv_list)

    if args.db_url:
        if not _is_postgres_url(args.db_url):
            raise SystemExit(
                "ERROR: --db must be a Postgres URL (postgresql://... or postgres://...)."
            )
        os.environ["DATABASE_URL"] = args.db_url

    get_conn = _import_get_conn()

    schema_path = Path(args.schema_file).expanduser()
    if not schema_path.is_absolute():
        schema_path = (ROOT / schema_path).resolve()

    migrations_dir = Path(args.migrations_dir).expanduser()
    if not migrations_dir.is_absolute():
        migrations_dir = (ROOT / migrations_dir).resolve()

    with get_conn() as conn:
        # Enforce Postgres-only target state.
        if not getattr(conn, "is_postgres", False):
            raise SystemExit(
                "ERROR: This project is Postgres-only. DATABASE_URL/DB_URL must point to Postgres "
                "(postgresql://...)."
            )

        print("→ Using Postgres via src.db.get_conn()")
        _verify_postgres(conn)

        if schema_path.is_relative_to(ROOT):
            schema_disp = str(schema_path.relative_to(ROOT))
        else:
            schema_disp = str(schema_path)

        print(f"→ Applying base schema: {schema_disp}")
        schema_text = _read_text(schema_path)
        _apply_sql_text(conn, schema_text)
        conn.commit()

        applied_now = _apply_migrations(conn, migrations_dir)

        _verify_postgres(conn)
        _print_summary(conn)

    print(f"✔ Schema applied; migrations applied this run: {applied_now}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
