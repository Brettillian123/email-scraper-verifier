# scripts/backfill_domain_audit.py
from __future__ import annotations

import sqlite3
import sys
from datetime import UTC, datetime


def pick(cols: set[str], *names: str) -> str | None:
    for n in names:
        if n in cols:
            return n
    return None


def pragma_table_info(con: sqlite3.Connection, table: str) -> list[dict]:
    con.row_factory = sqlite3.Row
    return [dict(r) for r in con.execute(f"PRAGMA table_info({table})")]


def find_audit_table(con: sqlite3.Connection) -> str | None:
    for cand in ("domain_resolutions", "domain_resolution_audit", "domain_resolution_log"):
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
            (cand,),
        ).fetchone():
            return cand
    return None


def detect_company_columns(con: sqlite3.Connection) -> tuple[str, str | None, str]:
    cols = {r["name"] for r in con.execute("PRAGMA table_info(companies)")}
    off = pick(cols, "domain_official", "official_domain")
    conf = pick(cols, "domain_confidence", "official_domain_confidence")
    name_col = "name"
    if not off:
        raise SystemExit(
            "FATAL: companies has no official-domain column (domain_official/official_domain)."
        )
    return off, conf, name_col


def detect_audit_columns(con: sqlite3.Connection, table: str) -> dict:
    info = pragma_table_info(con, table)
    names = {r["name"] for r in info}
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
    notnull_cols = {r["name"] for r in info if r.get("notnull") == 1}

    return {
        "domain_col": domain_col,
        "created_at_col": created_at,
        "resolver_version_col": resolver_version,
        "source_col": source_col,
        "company_name_col": company_name_col,
        "notnull_cols": notnull_cols,
        "info": info,
    }


def iso_utc_now() -> str:
    # RFC3339-ish ending in Z, timezone-aware (no deprecation warning)
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def companies_needing_backfill(
    con: sqlite3.Connection, off: str, conf: str | None, name_col: str
) -> list[sqlite3.Row]:
    q = f"SELECT c.id, c.{name_col} AS company_name, c.{off} AS dom"
    if conf:
        q += f", c.{conf} AS conf"
    else:
        # If confidence column doesn't exist, there's nothing to backfill per acceptance rules.
        return []
    q += f" FROM companies c WHERE c.{off} IS NOT NULL AND c.{conf} IS NOT NULL"
    con.row_factory = sqlite3.Row
    return list(con.execute(q))


def has_audit_row(
    con: sqlite3.Connection,
    audit_table: str,
    domain_col: str | None,
    company_id: int,
    dom: str | None,
) -> bool:
    if domain_col:
        row = con.execute(
            f"SELECT 1 FROM {audit_table} WHERE company_id=? AND {domain_col}=? LIMIT 1",
            (company_id, dom),
        ).fetchone()
    else:
        row = con.execute(
            f"SELECT 1 FROM {audit_table} WHERE company_id=? LIMIT 1",
            (company_id,),
        ).fetchone()
    return bool(row)


def join_qmarks(n: int) -> str:
    return ", ".join(["?"] * n)


def safe_default_for_type(decl_type: str) -> object:
    dt = (decl_type or "").upper()
    if "INT" in dt or "NUM" in dt or "REAL" in dt:
        return 0
    if "CHAR" in dt or "CLOB" in dt or "TEXT" in dt or dt == "":
        return ""
    return ""


def load_resolver_version() -> str:
    try:
        from src.resolve.domain import RESOLVER_VERSION  # type: ignore

        return RESOLVER_VERSION  # pragma: no cover
    except Exception:
        return "r08-backfill"


def build_base_insert_columns(audit_meta: dict) -> list[str]:
    cols: list[str] = ["company_id", "method", "confidence"]
    if audit_meta["domain_col"]:
        cols.append(audit_meta["domain_col"])
    if audit_meta["company_name_col"]:
        cols.append(audit_meta["company_name_col"])
    if audit_meta["resolver_version_col"]:
        cols.append(audit_meta["resolver_version_col"])
    if audit_meta["source_col"]:
        cols.append(audit_meta["source_col"])
    if audit_meta["created_at_col"]:
        cols.append(audit_meta["created_at_col"])
    return cols


def build_row_values(
    cid: int,
    cname: str,
    dom: str,
    conf_val: int,
    ver: str,
    audit_meta: dict,
    now_iso: str,
) -> list[object]:
    vals: list[object] = [cid, "backfill:r08", int(conf_val)]
    if audit_meta["domain_col"]:
        vals.append(dom)
    if audit_meta["company_name_col"]:
        vals.append(cname)
    if audit_meta["resolver_version_col"]:
        vals.append(ver)
    if audit_meta["source_col"]:
        vals.append("backfill")
    if audit_meta["created_at_col"]:
        vals.append(now_iso)
    return vals


def add_missing_notnull_defaults(
    insert_cols: list[str], vals: list[object], audit_meta: dict
) -> tuple[list[str], list[object]]:
    # If the audit table has other NOT NULL columns we didn't populate, add safe defaults.
    names = set(insert_cols)
    for rinfo in audit_meta["info"]:
        col = rinfo["name"]
        if rinfo.get("notnull") == 1 and col not in names:
            insert_cols.append(col)
            vals.append(safe_default_for_type(rinfo.get("type") or ""))
            names.add(col)
    return insert_cols, vals


def do_backfill(con: sqlite3.Connection) -> int:
    off, conf, name_col = detect_company_columns(con)
    audit_tbl = find_audit_table(con)
    if not audit_tbl:
        raise SystemExit("FATAL: no audit table found (domain_resolutions/_audit/_log).")

    audit_meta = detect_audit_columns(con, audit_tbl)
    rows = companies_needing_backfill(con, off, conf, name_col)
    print(f"Found {len(rows)} company decisions to inspect for backfill…")
    if not rows:
        return 0

    base_cols = build_base_insert_columns(audit_meta)
    ver = load_resolver_version()
    now_iso = iso_utc_now()

    inserted = 0
    with con:
        for r in rows:
            cid = r["id"]
            cname = r["company_name"]
            dom = r["dom"]
            conf_val = r["conf"]
            if has_audit_row(con, audit_tbl, audit_meta["domain_col"], cid, dom):
                continue

            cols = list(base_cols)
            vals = build_row_values(cid, cname, dom, conf_val, ver, audit_meta, now_iso)
            cols, vals = add_missing_notnull_defaults(cols, vals, audit_meta)

            sql = f"INSERT INTO {audit_tbl} ({', '.join(cols)}) VALUES ({join_qmarks(len(vals))})"
            con.execute(sql, vals)
            inserted += 1
    return inserted


def main(db: str) -> None:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    inserted = do_backfill(con)
    print(f"Backfill complete: inserted {inserted} audit row(s).")
    if inserted == 0:
        print("Nothing to backfill. (Either already audited or no eligible rows.)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "dev.db")
