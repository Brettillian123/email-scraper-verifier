# scripts/backfill_domain_audit.py
from __future__ import annotations

import argparse
import os
import sys
from contextlib import closing
from datetime import UTC, datetime
from typing import Any

from src.db import get_conn  # type: ignore[import]


def pick(cols: set[str], *names: str) -> str | None:
    for n in names:
        if n in cols:
            return n
    return None


def iso_utc_now() -> str:
    # RFC3339-ish ending in Z, timezone-aware (no deprecation warning)
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    rows = cur.fetchall()
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, dict):
        return [dict(r) for r in rows]
    cols = [d[0] for d in (cur.description or [])]
    return [dict(zip(cols, r, strict=False)) for r in rows]


def pg_relation_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        row = cur.fetchone()
    return bool(row and row[0] is not None)


def pg_columns(conn, table: str) -> list[dict[str, Any]]:
    """
    Return column metadata for `table` in the current schema.

    Keys:
      - name
      - data_type
      - udt_name
      - is_nullable ('YES'/'NO')
      - column_default (or None)
    """
    q = """
        SELECT
          column_name,
          data_type,
          udt_name,
          is_nullable,
          column_default
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        rows = _fetchall_dict(cur)

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "name": r.get("column_name"),
                "data_type": r.get("data_type") or "",
                "udt_name": r.get("udt_name") or "",
                "is_nullable": r.get("is_nullable") or "YES",
                "column_default": r.get("column_default"),
            }
        )
    return out


def find_audit_table(conn) -> str | None:
    for cand in ("domain_resolutions", "domain_resolution_audit", "domain_resolution_log"):
        if pg_relation_exists(conn, cand):
            return cand
    return None


def detect_company_columns(conn) -> tuple[str, str | None, str, str | None]:
    cols_info = pg_columns(conn, "companies")
    cols = {r["name"] for r in cols_info if r.get("name")}
    off = pick(cols, "domain_official", "official_domain")
    conf = pick(cols, "domain_confidence", "official_domain_confidence")
    name_col = pick(cols, "name") or "name"
    tenant_col = pick(cols, "tenant_id")
    if not off:
        raise SystemExit(
            "FATAL: companies has no official-domain column (domain_official/official_domain)."
        )
    return off, conf, name_col, tenant_col


def detect_audit_columns(conn, table: str) -> dict[str, Any]:
    info = pg_columns(conn, table)
    names = {r["name"] for r in info if r.get("name")}
    need = {"company_id", "method", "confidence"}
    if not need.issubset(names):
        raise SystemExit(
            f"FATAL: {table} missing required columns {sorted(need)}; has {sorted(names)}"
        )

    domain_col = pick(
        names,
        "domain",
        "resolved_domain",
        "official_domain",
        "selected_domain",
        "chosen_domain",
        "result_domain",
        "value",
        "candidate_domain",
    )
    created_at = "created_at" if "created_at" in names else pick(names, "timestamp", "logged_at")
    resolver_version = pick(names, "resolver_version", "version")
    source_col = pick(names, "source", "origin", "decision_source", "method_source")
    company_name_col = "company_name" if "company_name" in names else None
    tenant_col = "tenant_id" if "tenant_id" in names else None

    defaults = {r["name"]: r.get("column_default") for r in info if r.get("name")}

    return {
        "domain_col": domain_col,
        "created_at_col": created_at,
        "resolver_version_col": resolver_version,
        "source_col": source_col,
        "company_name_col": company_name_col,
        "tenant_col": tenant_col,
        "defaults": defaults,
        "info": info,
    }


def companies_needing_backfill(
    conn,
    off: str,
    conf: str | None,
    name_col: str,
    tenant_col: str | None,
    tenant_id: str | None,
) -> list[dict[str, Any]]:
    if not conf:
        # If confidence column doesn't exist, there's nothing to backfill per acceptance rules.
        return []

    select_cols = [
        "c.id",
        f"c.{name_col} AS company_name",
        f"c.{off} AS dom",
        f"c.{conf} AS conf",
    ]
    if tenant_col:
        select_cols.insert(1, f"c.{tenant_col} AS tenant_id")

    q = f"""
        SELECT {", ".join(select_cols)}
        FROM companies AS c
        WHERE c.{off} IS NOT NULL AND c.{conf} IS NOT NULL
    """
    params: list[Any] = []
    if tenant_id and tenant_col:
        q += f" AND c.{tenant_col} = %s"
        params.append(tenant_id)

    with conn.cursor() as cur:
        cur.execute(q, tuple(params))
        return _fetchall_dict(cur)


def has_audit_row(
    conn,
    audit_table: str,
    tenant_col: str | None,
    domain_col: str | None,
    company_id: int,
    tenant_id: str | None,
    dom: str | None,
) -> bool:
    params: list[Any] = []
    where = ["company_id = %s"]
    params.append(company_id)

    if tenant_col and tenant_id:
        where.append(f"{tenant_col} = %s")
        params.append(tenant_id)

    if domain_col:
        where.append(f"{domain_col} = %s")
        params.append(dom)

    q = f"SELECT 1 FROM {audit_table} WHERE {' AND '.join(where)} LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(q, tuple(params))
        row = cur.fetchone()
    return bool(row)


def safe_default_for_type(data_type: str, udt_name: str) -> object:
    dt = (data_type or "").lower()
    udt = (udt_name or "").lower()

    if dt in {"smallint", "integer", "bigint", "numeric", "double precision", "real"}:
        return 0
    if dt == "boolean":
        return False
    if dt in {"json", "jsonb"}:
        return "{}"
    if udt == "uuid":
        return "00000000-0000-0000-0000-000000000000"
    return ""


def load_resolver_version() -> str:
    try:
        from src.resolve.domain import RESOLVER_VERSION  # type: ignore

        return RESOLVER_VERSION  # pragma: no cover
    except Exception:
        return "r08-backfill"


def build_base_insert_columns(audit_meta: dict[str, Any]) -> list[str]:
    cols: list[str] = []
    if audit_meta.get("tenant_col"):
        cols.append(audit_meta["tenant_col"])
    cols.extend(["company_id", "method", "confidence"])

    if audit_meta.get("domain_col"):
        cols.append(audit_meta["domain_col"])
    if audit_meta.get("company_name_col"):
        cols.append(audit_meta["company_name_col"])
    if audit_meta.get("resolver_version_col"):
        cols.append(audit_meta["resolver_version_col"])
    if audit_meta.get("source_col"):
        cols.append(audit_meta["source_col"])
    if audit_meta.get("created_at_col"):
        cols.append(audit_meta["created_at_col"])
    return cols


def build_row_values(
    *,
    tenant_id: str | None,
    cid: int,
    cname: str,
    dom: str,
    conf_val: Any,
    ver: str,
    audit_meta: dict[str, Any],
    now_iso: str,
) -> list[object]:
    vals: list[object] = []
    if audit_meta.get("tenant_col"):
        vals.append(tenant_id or "dev")

    vals.extend([cid, "backfill:r08", int(conf_val)])

    if audit_meta.get("domain_col"):
        vals.append(dom)
    if audit_meta.get("company_name_col"):
        vals.append(cname)
    if audit_meta.get("resolver_version_col"):
        vals.append(ver)
    if audit_meta.get("source_col"):
        vals.append("backfill")
    if audit_meta.get("created_at_col"):
        vals.append(now_iso)
    return vals


def add_missing_notnull_defaults(
    insert_cols: list[str], vals: list[object], audit_meta: dict[str, Any]
) -> tuple[list[str], list[object]]:
    """
    If the audit table has other NOT NULL columns we didn't populate:
      - If the column has a DB default, rely on it.
      - Otherwise add a best-effort safe default.

    This keeps the script tolerant across schema evolutions.
    """
    names = set(insert_cols)
    defaults: dict[str, Any] = audit_meta.get("defaults") or {}
    for rinfo in audit_meta.get("info", []):
        col = rinfo.get("name")
        if not col or col in names:
            continue
        if rinfo.get("is_nullable") != "NO":
            continue

        if defaults.get(col) is not None:
            # Let Postgres fill it.
            continue

        insert_cols.append(col)
        vals.append(
            safe_default_for_type(rinfo.get("data_type") or "", rinfo.get("udt_name") or "")
        )
        names.add(col)

    return insert_cols, vals


def do_backfill(conn, tenant_id: str | None) -> int:
    off, conf, name_col, company_tenant_col = detect_company_columns(conn)
    audit_tbl = find_audit_table(conn)
    if not audit_tbl:
        raise SystemExit("FATAL: no audit table found (domain_resolutions/_audit/_log).")

    audit_meta = detect_audit_columns(conn, audit_tbl)
    rows = companies_needing_backfill(conn, off, conf, name_col, company_tenant_col, tenant_id)
    print(f"Found {len(rows)} company decisions to inspect for backfill…")
    if not rows:
        return 0

    base_cols = build_base_insert_columns(audit_meta)
    ver = load_resolver_version()
    now_iso = iso_utc_now()

    inserted = 0
    with conn:
        for r in rows:
            cid = int(r["id"])
            cname = str(r.get("company_name") or "")
            dom = str(r.get("dom") or "")
            conf_val = r.get("conf")
            row_tenant_id = r.get("tenant_id") if company_tenant_col else None

            if has_audit_row(
                conn,
                audit_tbl,
                audit_meta.get("tenant_col"),
                audit_meta.get("domain_col"),
                cid,
                str(row_tenant_id) if row_tenant_id is not None else None,
                dom,
            ):
                continue

            cols = list(base_cols)
            vals = build_row_values(
                tenant_id=str(row_tenant_id) if row_tenant_id is not None else None,
                cid=cid,
                cname=cname,
                dom=dom,
                conf_val=conf_val,
                ver=ver,
                audit_meta=audit_meta,
                now_iso=now_iso,
            )
            cols, vals = add_missing_notnull_defaults(cols, vals, audit_meta)

            placeholders = ", ".join(["%s"] * len(vals))
            sql = f"INSERT INTO {audit_tbl} ({', '.join(cols)}) VALUES ({placeholders})"

            with conn.cursor() as cur:
                cur.execute(sql, vals)
            inserted += 1

    return inserted


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill R08 domain_resolutions audit rows for companies missing audit history."
    )
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope the backfill to one tenant. Default: all tenants.",
    )
    parser.add_argument(
        "--dsn",
        dest="dsn",
        default=None,
        help="Optional Postgres DSN/URL override. If provided, sets DATABASE_URL for this run.",
    )
    args = parser.parse_args(argv)

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    with closing(get_conn()) as conn:
        inserted = do_backfill(conn, tenant_id=args.tenant_id)

    print(f"Backfill complete: inserted {inserted} audit row(s).")
    if inserted == 0:
        print("Nothing to backfill. (Either already audited or no eligible rows.)")


if __name__ == "__main__":
    main(sys.argv[1:])
