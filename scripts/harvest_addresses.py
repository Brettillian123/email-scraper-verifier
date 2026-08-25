#!/usr/bin/env python
# scripts/harvest_addresses.py
from __future__ import annotations

r"""
Harvest real email addresses out of page HTML we already crawled.

This is the DB driver for `src/extract/address_harvest.py`. It reads the
`sources` table (the page-level HTML cache written by `src/db_pages.py`), runs
the pure harvester over each stored page, and records what it finds in
`emails`. No page is re-fetched and no SMTP probe is issued: every address this
finds is free.

The point is cold domains. Pattern inference cannot guess an address for a
company until it has seen one real address there. Companies with zero confirmed
addresses have nothing for it to learn from -- but their HTML is already on
disk, and a single genuine address seeds the pattern for every colleague.

Usage (PowerShell-friendly)
---------------------------
  # Dry run is the DEFAULT -- reports exactly what --apply would do:
  python .\scripts\harvest_addresses.py --tenant dev

  # Only the companies that have no confirmed-valid address yet:
  python .\scripts\harvest_addresses.py --tenant dev --only-cold

  # One company, then commit:
  python .\scripts\harvest_addresses.py --company-id 1234 --apply

Idempotency
-----------
Safe to re-run. Addresses are keyed on (tenant_id, email) -- the unique index
the schema already enforces -- so a second run inserts nothing new. It will
still fill in `person_id` on rows that were unattributed before, which is how
re-running after a fresh people extraction picks up new links.

Notes
-----
* Role inboxes (info@, careers@, ...) are reported but NOT written unless you
  pass --store-role-addresses. They evidence that the domain accepts mail, but
  they are not a person's address, and writing them would make a cold company
  look covered in the "companies with an address" metric without helping
  anyone reach a human.
* Nothing here enqueues verification. Whether to spend an SMTP probe on a
  published address is a separate decision, made elsewhere.
"""

import argparse
import datetime as _dt
import json
import logging
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Ensure "src/" is importable when running as a script.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.db import get_conn  # noqa: E402
from src.extract.address_harvest import (  # noqa: E402
    HarvestedAddress,
    PersonRef,
    attribute_address,
    build_allowed_domains,
    harvest_page,
)

log = logging.getLogger("harvest_addresses")

DEFAULT_PAGE_BATCH = 50  # ~70 KB of HTML per row; keeps peak memory small


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class Report:
    pages_scanned: int = 0
    pages_without_company: int = 0
    companies_touched: set[int] = field(default_factory=set)
    companies_with_a_find: set[int] = field(default_factory=set)

    addresses_found: int = 0  # distinct personal addresses, deduped tenant-wide
    role_addresses_found: int = 0
    rejected_foreign: int = 0
    rejected_placeholder: int = 0

    emails_inserted: int = 0
    newly_attributed: int = 0
    already_present: int = 0
    role_emails_inserted: int = 0

    by_method: dict[str, int] = field(default_factory=dict)
    by_pattern: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """JSON-safe view; the id sets collapse to counts."""
        out: dict[str, Any] = dict(vars(self))  # field declaration order
        out["companies_touched"] = len(self.companies_touched)
        out["companies_with_a_find"] = len(self.companies_with_a_find)
        out["by_method"] = dict(sorted(self.by_method.items()))
        out["by_pattern"] = dict(sorted(self.by_pattern.items(), key=lambda kv: -kv[1]))
        return out


# ---------------------------------------------------------------------------
# Schema helpers (PRAGMA is emulated for Postgres by src/db.py, same as
# src/db_pages.py relies on)
# ---------------------------------------------------------------------------


def _columns(conn: Any, table: str) -> set[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return {str(row[1]) for row in cur.fetchall()}
    except Exception:
        return set()


def _utc_now() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _row_get(row: Any, idx: int, key: str) -> Any:
    """Read a column from a DictCursor row or a plain tuple."""
    try:
        return row[key]
    except Exception:
        try:
            return row[idx]
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def select_company_ids(
    conn: Any,
    *,
    tenant_id: str,
    domain: str | None,
    only_cold: bool,
) -> list[int] | None:
    """
    Resolve the company id filter, or None for "every company in the tenant".

    Returns None when neither --domain nor --only-cold narrows anything: the
    page walk already filters on sources.tenant_id, so materialising every
    company id would only bloat the query with a thousand-item IN list.
    """
    if not domain and not only_cold:
        return None

    ccols = _columns(conn, "companies")
    where: list[str] = []
    params: list[Any] = []

    if "tenant_id" in ccols:
        where.append("c.tenant_id = ?")
        params.append(tenant_id)

    if domain:
        d = domain.strip().lower()
        domain_cols = [
            c for c in ("domain", "official_domain", "user_supplied_domain") if c in ccols
        ]
        if domain_cols:
            where.append("(" + " OR ".join(f"LOWER(c.{c}) = ?" for c in domain_cols) + ")")
            params.extend([d] * len(domain_cols))

    if only_cold:
        # "Cold" == no address on this company has ever verified valid. These
        # are the companies pattern inference has nothing to learn from.
        where.append(
            """NOT EXISTS (
                 SELECT 1 FROM emails e
                 JOIN verification_results vr ON vr.email_id = e.id
                 WHERE e.company_id = c.id AND vr.verify_status = 'valid'
               )"""
        )

    if not where:
        return None

    sql = "SELECT c.id FROM companies c WHERE " + " AND ".join(where)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [int(_row_get(r, 0, "id")) for r in rows]


def load_company_domains(conn: Any, company_ids: Sequence[int]) -> dict[int, set[str]]:
    """Map company_id -> the hosts that count as that company."""
    if not company_ids:
        return {}
    ccols = _columns(conn, "companies")
    cols = ["id"] + [c for c in ("domain", "official_domain", "user_supplied_domain") if c in ccols]
    ph = ", ".join(["?"] * len(company_ids))
    sql = f"SELECT {', '.join(cols)} FROM companies WHERE id IN ({ph})"
    out: dict[int, set[str]] = {}
    for row in conn.execute(sql, tuple(int(c) for c in company_ids)).fetchall():
        cid = int(_row_get(row, 0, "id"))
        values = [_row_get(row, i, cols[i]) for i in range(1, len(cols))]
        out[cid] = build_allowed_domains(*values)
    return out


def load_people(conn: Any, company_id: int) -> list[PersonRef]:
    pcols = _columns(conn, "people")
    cols = ["id"] + [c for c in ("first_name", "last_name", "full_name") if c in pcols]
    sql = f"SELECT {', '.join(cols)} FROM people WHERE company_id = ?"
    people: list[PersonRef] = []
    for row in conn.execute(sql, (int(company_id),)).fetchall():
        values = {cols[i]: _row_get(row, i, cols[i]) for i in range(len(cols))}
        people.append(
            PersonRef(
                person_id=int(values["id"]),
                first_name=values.get("first_name"),
                last_name=values.get("last_name"),
                full_name=values.get("full_name"),
            )
        )
    return people


def iter_stored_pages(
    conn: Any,
    *,
    tenant_id: str,
    company_ids: Sequence[int] | None,
    limit: int | None,
    batch: int = DEFAULT_PAGE_BATCH,
) -> Iterator[tuple[int, int | None, str, str]]:
    """
    Stream (source_id, company_id, source_url, html) from the `sources` cache.

    Keyset-paginated on sources.id so a 232 MB table never lands in memory at
    once, and so the walk is stable if rows are appended while it runs.
    """
    scols = _columns(conn, "sources")
    if "html" not in scols or "source_url" not in scols:
        raise RuntimeError("sources table has no html/source_url columns; nothing to harvest")

    select_cols = ["id", "source_url", "html"]
    has_company = "company_id" in scols
    if has_company:
        select_cols.insert(1, "company_id")

    where = ["id > ?"]
    base_params: list[Any] = []
    if "tenant_id" in scols:
        where.append("tenant_id = ?")
        base_params.append(tenant_id)
    if company_ids is not None and has_company:
        if not company_ids:
            return
        where.append("company_id IN (" + ", ".join(["?"] * len(company_ids)) + ")")
        base_params.extend(int(c) for c in company_ids)

    sql = (
        f"SELECT {', '.join(select_cols)} FROM sources "
        f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ?"
    )

    last_id = 0
    emitted = 0
    while True:
        take = batch if limit is None else min(batch, limit - emitted)
        if take <= 0:
            return
        rows = conn.execute(sql, tuple([last_id, *base_params, take])).fetchall()
        if not rows:
            return
        for row in rows:
            sid = int(_row_get(row, 0, "id"))
            last_id = max(last_id, sid)
            if has_company:
                raw_cid = _row_get(row, 1, "company_id")
                cid = int(raw_cid) if raw_cid is not None else None
                url = _row_get(row, 2, "source_url") or ""
                html = _row_get(row, 3, "html") or ""
            else:
                cid = None
                url = _row_get(row, 1, "source_url") or ""
                html = _row_get(row, 2, "html") or ""
            emitted += 1
            yield sid, cid, str(url), str(html)
        if len(rows) < take:
            return


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def upsert_harvested_email(
    conn: Any,
    *,
    tenant_id: str,
    company_id: int,
    addr: HarvestedAddress,
    person_id: int | None,
    is_role: bool,
) -> str:
    """
    Record one harvested address. Returns "inserted" | "attributed" | "unchanged".

    Keyed on the schema's existing (tenant_id, email) unique index, so calling
    this twice with the same address is a no-op the second time. A row that
    already exists without a person_id gets one filled in when we now have a
    confident match -- that, and only that, counts as "attributed".
    """
    ecols = _columns(conn, "emails")
    now = _utc_now()

    where = "email = ?"
    params: list[Any] = [addr.email]
    if "tenant_id" in ecols:
        where = "tenant_id = ? AND email = ?"
        params = [tenant_id, addr.email]

    existing = conn.execute(
        f"SELECT id, person_id FROM emails WHERE {where} LIMIT 1", tuple(params)
    ).fetchone()

    if existing is not None:
        email_id = int(_row_get(existing, 0, "id"))
        current_person = _row_get(existing, 1, "person_id")
        if person_id is not None and current_person is None and "person_id" in ecols:
            sets = ["person_id = ?"]
            vals: list[Any] = [int(person_id)]
            if "updated_at" in ecols:
                sets.append("updated_at = ?")
                vals.append(now)
            vals.append(email_id)
            conn.execute(f"UPDATE emails SET {', '.join(sets)} WHERE id = ?", tuple(vals))
            return "attributed"
        return "unchanged"

    cols: list[str] = []
    vals: list[Any] = []

    def add(col: str, value: Any) -> None:
        if col in ecols:
            cols.append(col)
            vals.append(value)

    add("tenant_id", tenant_id)
    add("company_id", int(company_id))
    add("person_id", int(person_id) if person_id is not None else None)
    add("email", addr.email)
    add("domain", addr.domain)
    add("source_url", addr.source_url)
    add("is_published", 1)  # seen on-page, not guessed
    add("source", "harvested")
    add("source_note", f"harvest:{'role' if is_role else addr.method}")
    add("created_at", now)
    add("updated_at", now)

    if not cols:
        cols, vals = ["email"], [addr.email]

    ph = ", ".join(["?"] * len(cols))
    conn.execute(
        f"INSERT OR IGNORE INTO emails ({', '.join(cols)}) VALUES ({ph})",
        tuple(vals),
    )
    return "inserted"


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _record_personal(
    conn: Any,
    addr: HarvestedAddress,
    *,
    tenant_id: str,
    company_id: int,
    people: Sequence[PersonRef],
    apply_changes: bool,
    report: Report,
) -> None:
    report.addresses_found += 1
    report.companies_with_a_find.add(company_id)
    report.by_method[addr.method] = report.by_method.get(addr.method, 0) + 1

    match = attribute_address(addr, people)
    if match is not None:
        report.by_pattern[match.pattern] = report.by_pattern.get(match.pattern, 0) + 1

    if not apply_changes:
        return

    outcome = upsert_harvested_email(
        conn,
        tenant_id=tenant_id,
        company_id=company_id,
        addr=addr,
        person_id=match.person_id if match else None,
        is_role=False,
    )
    if outcome == "inserted":
        report.emails_inserted += 1
        if match is not None:
            report.newly_attributed += 1
    elif outcome == "attributed":
        report.newly_attributed += 1
    else:
        report.already_present += 1


def _record_role(
    conn: Any,
    addr: HarvestedAddress,
    *,
    tenant_id: str,
    company_id: int,
    store: bool,
    report: Report,
) -> None:
    report.role_addresses_found += 1
    if not store:
        return
    outcome = upsert_harvested_email(
        conn,
        tenant_id=tenant_id,
        company_id=company_id,
        addr=addr,
        person_id=None,
        is_role=True,
    )
    if outcome == "inserted":
        report.role_emails_inserted += 1


def run_harvest(
    conn: Any,
    *,
    tenant_id: str,
    company_ids: Sequence[int] | None = None,
    limit_pages: int | None = None,
    apply_changes: bool = False,
    store_role_addresses: bool = False,
    progress_every: int = 250,
) -> Report:
    """
    Harvest every stored page in scope and record what turns up.

    With apply_changes=False (the default) this reads only: the report tells
    you exactly what a run with --apply would write.
    """
    report = Report()
    people_cache: dict[int, list[PersonRef]] = {}
    domain_cache: dict[int, set[str]] = {}
    # Tenant-wide dedup: the same address usually appears on several pages.
    handled: set[str] = set()

    for _sid, company_id, url, html in iter_stored_pages(
        conn,
        tenant_id=tenant_id,
        company_ids=company_ids,
        limit=limit_pages,
    ):
        report.pages_scanned += 1
        if progress_every and report.pages_scanned % progress_every == 0:
            log.info(
                "scanned %d pages, %d addresses so far",
                report.pages_scanned,
                report.addresses_found,
            )

        if company_id is None:
            report.pages_without_company += 1
            continue

        if company_id not in domain_cache:
            domain_cache.update(load_company_domains(conn, [company_id]))
        # The crawler only stores same-host pages, so the page's own host is a
        # legitimate alias for this company.
        allowed = set(domain_cache.get(company_id) or set()) | build_allowed_domains(url)
        if not allowed:
            continue

        report.companies_touched.add(company_id)
        found = harvest_page(html, source_url=url, allowed_domains=allowed)
        report.rejected_foreign += found.rejected_foreign
        report.rejected_placeholder += found.rejected_placeholder
        if not found.personal and not found.role:
            continue

        if company_id not in people_cache:
            people_cache[company_id] = load_people(conn, company_id)

        for addr in found.personal:
            if addr.email in handled:
                continue
            handled.add(addr.email)
            _record_personal(
                conn,
                addr,
                tenant_id=tenant_id,
                company_id=company_id,
                people=people_cache[company_id],
                apply_changes=apply_changes,
                report=report,
            )

        for addr in found.role:
            if addr.email in handled:
                continue
            handled.add(addr.email)
            _record_role(
                conn,
                addr,
                tenant_id=tenant_id,
                company_id=company_id,
                store=apply_changes and store_role_addresses,
                report=report,
            )

        if apply_changes:
            conn.commit()

    if apply_changes:
        conn.commit()
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Harvest real email addresses from already-crawled page HTML.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--tenant", default="dev", help="Tenant id to scan (default: dev)")
    p.add_argument("--company-id", type=int, help="Restrict to a single company id")
    p.add_argument("--domain", help="Restrict to companies matching this domain")
    p.add_argument(
        "--only-cold",
        action="store_true",
        help="Only companies with no confirmed-valid address yet (where this pays off most)",
    )
    p.add_argument("--limit-pages", type=int, help="Stop after N stored pages")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually write to the database (default: dry run, reads only)",
    )
    p.add_argument(
        "--store-role-addresses",
        action="store_true",
        help="Also persist role inboxes (info@, careers@) with person_id NULL",
    )
    p.add_argument("--json", action="store_true", help="Print the report as JSON")
    p.add_argument("-v", "--verbose", action="store_true", help="Log progress while scanning")
    return p.parse_args(argv)


def _print_report(report: Report, *, apply_changes: bool) -> None:
    d = report.as_dict()
    mode = "APPLIED" if apply_changes else "DRY RUN (nothing written; pass --apply to write)"
    print(f"\n=== Address harvest: {mode} ===")
    print(f"  pages scanned              : {d['pages_scanned']}")
    if d["pages_without_company"]:
        print(f"  pages with no company link : {d['pages_without_company']} (skipped)")
    print(f"  companies scanned          : {d['companies_touched']}")
    print(f"  companies with a find      : {d['companies_with_a_find']}")
    print(f"  personal addresses found   : {d['addresses_found']}")
    print(f"  role inboxes found         : {d['role_addresses_found']} (domain accepts mail)")
    print(f"  rejected, foreign domain   : {d['rejected_foreign']}")
    print(f"  rejected, placeholder      : {d['rejected_placeholder']}")
    print("  --")
    print(f"  emails inserted            : {d['emails_inserted']}")
    print(f"  newly attributed to person : {d['newly_attributed']}")
    print(f"  already present            : {d['already_present']}")
    if d["role_emails_inserted"]:
        print(f"  role emails inserted       : {d['role_emails_inserted']}")
    if d["by_method"]:
        print("  by source: " + ", ".join(f"{k}={v}" for k, v in d["by_method"].items()))
    if d["by_pattern"]:
        print("  patterns seeded: " + ", ".join(f"{k}={v}" for k, v in d["by_pattern"].items()))
    print()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    conn = get_conn()
    try:
        if args.company_id is not None:
            company_ids: list[int] | None = [int(args.company_id)]
        else:
            company_ids = select_company_ids(
                conn,
                tenant_id=args.tenant,
                domain=args.domain,
                only_cold=args.only_cold,
            )

        if company_ids is not None and not company_ids:
            print("No companies matched the filter; nothing to do.")
            return 0

        report = run_harvest(
            conn,
            tenant_id=args.tenant,
            company_ids=company_ids,
            limit_pages=args.limit_pages,
            apply_changes=args.apply,
            store_role_addresses=args.store_role_addresses,
            progress_every=250 if args.verbose else 0,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        _print_report(report, apply_changes=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
