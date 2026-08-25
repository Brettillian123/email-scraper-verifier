#!/usr/bin/env python
# scripts/backfill_harvest_stored_pages.py
"""
Seed company_email_pattern from addresses already sitting in `sources.html`.

Why this exists
---------------
Pattern inference cannot guess an address for a company until it has seen one
real address there. 1,004 on-domain addresses are already stored across 365
companies (576 of them personal, across 153 companies) and nothing reads them
into the pattern store. Mining them costs zero network calls and zero SMTP
probes, and is worth roughly +78 companies of NET NEW pattern coverage.

Division of labour (read this before adding anything here)
----------------------------------------------------------
* `src/extract/address_harvest.py` is the pure HTML -> addresses extractor.
* `scripts/harvest_addresses.py` is its DB driver and owns writing rows into
  `emails`. It already reports a pattern histogram but never persists it.
* THIS script is the missing half: it consumes the same harvester and writes
  the resulting evidence into `company_email_pattern`.

So this script deliberately does NOT insert addresses. Run
`harvest_addresses.py --apply` for that. Nothing here is duplicated from it --
the page walk, the domain allow-list and the person loader are imported.

Evidence rules
--------------
* Only `.personal` addresses count. Role inboxes are not a person and must
  never seed a pattern (see `_is_role_local` for the extra automated-sender
  names the shared role set misses).
* An address that matches a known person's name exactly (via
  `attribute_address`) is the strongest signal there is -- confidence 1.0.
* Otherwise the local-part shape is classified by
  `patterns.infer_pattern_from_localpart(local_part, first_names)` against the
  cached `patterns.first_name_dictionary(conn)`, and kept only at or above the
  confidence floor for that company. That floor is conditional: 0.70 where
  stored SMTP evidence shows the domain can REFUTE a wrong pattern, 0.85 where
  it cannot -- and 0.85 where there is no evidence either way, because unknown
  is not the same as decidable. `--min-confidence` forces one value for both.
* Evidence lands in `inferred_count`, NEVER in `confirmed_count`: a scraped
  address proves what a company publishes, not that SMTP accepts it.

Idempotence and resumability
----------------------------
Stored HTML does not change, so a re-run recomputes the same evidence.
`record_inferred_patterns()` SETs `inferred_count` from the whole harvested
local-part set, and this script plans a write only where that count differs
from what is already stored -- so a second run plans nothing and writes
nothing. `confirmed_count` and `failed_count` are never touched.

Each company is committed on its own. A page that will not parse, or a write
that the database rejects, is counted and skipped; it can neither end the walk
nor discard the companies already written, so an interrupted run resumes by
simply being re-run.

Usage
-----
    python scripts/backfill_harvest_stored_pages.py --dry-run
    python scripts/backfill_harvest_stored_pages.py --dry-run --limit 500
    python scripts/backfill_harvest_stored_pages.py            # writes

Requires db/migrations/2026_08_19_company_email_pattern.sql (scripts/apply_schema.py).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Sibling script, imported rather than copied: it owns the keyset-paginated
# walk over `sources`, the company domain allow-list and the person loader.
from harvest_addresses import (  # noqa: E402
    iter_stored_pages,
    load_company_domains,
    load_people,
)

from src.db import get_conn  # noqa: E402
from src.extract.address_harvest import (  # noqa: E402
    attribute_address,
    build_allowed_domains,
    harvest_page,
)
from src.generate.patterns import (  # noqa: E402
    first_name_dictionary,
    infer_pattern_from_localpart,
    is_role_localpart,
    localpart_of,
    normalize_domain,
    record_inferred_patterns,
)

#: An exact name match against a known person is not a shape guess.
ATTRIBUTED_CONFIDENCE = 1.0

#: Floor on a DECIDABLE domain -- one whose MX hard-fails an unknown mailbox.
#: A wrong guess here is probed, rejected, and refuted by the negative-evidence
#: path, so the mistake self-corrects and the 88.6% band is worth its coverage:
#: 126 companies with a pattern (76 net new) against 40 (23 net new) at 0.75.
DECIDABLE_MIN_CONFIDENCE = 0.70

#: Floor everywhere else. A catch-all domain (39% of probed companies) accepts
#: every RCPT, so a wrong pattern is never contradicted and stays wrong forever
#: across all ~6.9 people at the company. A domain with NO catch-all evidence
#: gets this same floor: domain_resolutions is empty in production, and unknown
#: is not decidable.
SAFE_MIN_CONFIDENCE = 0.85

# `harvest_page()` already routes RFC 2142 names and the usual marketing
# aliases into `.role`, but that set is built for humans-vs-inboxes and misses
# automated senders. These are never a person and must never seed a pattern.
_EXTRA_ROLE_TOKENS: frozenset[str] = frozenset(
    {
        "noreply",
        "donotreply",
        "mailerdaemon",
        "daemon",
        "bounce",
        "bounces",
        "newsletter",
        "notification",
        "notifications",
        "unsubscribe",
        "automated",
        "robot",
        "webmaster",
    }
)
_ALPHA_SPLIT_RE = re.compile(r"[^a-z]+")

# Existing evidence, read with the qmark shim so this runs on Postgres and on
# a sqlite dev DB alike. patterns.get_domain_patterns() is Postgres-only.
_SELECT_EXISTING = "SELECT pattern, inferred_count FROM company_email_pattern WHERE company_id = ?"

#: Catch-all evidence for one company, from both places it is recorded: the
#: cached domain-level R17 verdict, and the per-address R18 classification.
#: Read with the qmark shim so this runs on Postgres and on a sqlite dev DB.
_SELECT_CATCH_ALL_EVIDENCE = """
    SELECT catch_all_status, CAST(NULL AS TEXT) FROM domain_resolutions WHERE company_id = ?
    UNION
    SELECT vr.verify_status, vr.verify_reason
      FROM verification_results vr
      JOIN emails e ON e.id = vr.email_id
     WHERE e.company_id = ?
"""

#: Labels that prove the domain accepts mail it should not -- SMTP can never
#: contradict a wrong pattern here.
_CATCH_ALL_MARKERS = frozenset({"catch_all", "risky_catch_all"})

#: Labels that prove an SMTP probe DECIDED something at this domain: the R17
#: probe found it non-catch-all, or an RCPT hard-failed an unknown mailbox.
#: Deliberately reasons, not statuses -- a "valid"/"invalid" that came from the
#: fallback vendor says nothing about whether this MX refutes anything.
_DECIDABLE_MARKERS = frozenset({"not_catch_all", "rcpt_2xx_non_catchall", "rcpt_5xx_user_unknown"})

#: Per-row failures printed in full before the summary just counts them.
_MAX_ERRORS_SHOWN = 10


def _rollback(conn: Any) -> None:
    """Postgres aborts the transaction on a failed statement; clear it.

    Same guard as src/queueing/reaper.py:_has_retry_count. Without it every
    statement after the first failure fails too, which is how one bad row ends
    a whole-tenant walk.
    """
    try:
        conn.rollback()
    except Exception:
        pass


def _is_role_local(local: str) -> bool:
    """True for role inboxes and automated senders (info@, no-reply@, ...)."""
    lp = localpart_of(local)
    if not lp or is_role_localpart(lp):
        return True
    squashed = _ALPHA_SPLIT_RE.sub("", lp)
    if squashed in _EXTRA_ROLE_TOKENS:
        return True
    return any(t in _EXTRA_ROLE_TOKENS for t in _ALPHA_SPLIT_RE.split(lp) if t)


def _existing_patterns(conn: Any, company_id: int) -> dict[str, int]:
    """pattern -> inferred_count already recorded for this company ({} if none)."""
    try:
        rows = conn.execute(_SELECT_EXISTING, (int(company_id),)).fetchall()
    except Exception:
        # Table not migrated yet: treat every company as having no evidence.
        _rollback(conn)
        return {}
    return {str(r[0]): int(r[1] or 0) for r in (rows or [])}


def make_min_confidence(
    conn: Any, *, override: float | None, cache: dict[int, float]
) -> Callable[[int], float]:
    """
    Build ``company_id -> confidence floor``.

    ``override`` (--min-confidence) forces one value everywhere. Otherwise a
    company drops to DECIDABLE_MIN_CONFIDENCE only on positive evidence that
    SMTP can refute a wrong guess there, and never when that evidence is
    absent or contradicted -- an unreadable/unmigrated table reads as no
    evidence, which is the safe answer, not the permissive one.

    ``cache`` is the memo (one query per company) and is filled in as a side
    effect so the caller can report the split.
    """

    def resolve(company_id: int) -> float:
        if override is not None:
            return override
        if company_id not in cache:
            try:
                rows = conn.execute(
                    _SELECT_CATCH_ALL_EVIDENCE, (int(company_id), int(company_id))
                ).fetchall()
                marks = {
                    str(value).strip().lower() for row in (rows or []) for value in row if value
                }
            except Exception:
                _rollback(conn)
                marks = set()
            decidable = bool(marks & _DECIDABLE_MARKERS) and not (marks & _CATCH_ALL_MARKERS)
            cache[company_id] = DECIDABLE_MIN_CONFIDENCE if decidable else SAFE_MIN_CONFIDENCE
        return cache[company_id]

    return resolve


def _classify_address(
    addr: Any,
    *,
    people: list[Any],
    first_names: Any,
    min_confidence: float,
    stats: Counter[str],
) -> tuple[str, str] | None:
    """
    (local_part, pattern) for one harvested address, or None to skip it.

    Every reason to skip is counted, including an address that blows up the
    classifier -- that costs one address, never the run.
    """
    try:
        local = localpart_of(addr.local_part)
        if not local or _is_role_local(local):
            stats["skipped_role"] += 1
            return None

        match = attribute_address(addr, people)
        if match is not None:
            pattern, confidence = match.pattern, ATTRIBUTED_CONFIDENCE
            stats["attributed_to_person"] += 1
        else:
            pattern, confidence = infer_pattern_from_localpart(local, first_names)
            stats["shape_classified"] += 1
    except Exception as exc:
        stats["address_errors"] += 1
        if stats["address_errors"] <= _MAX_ERRORS_SHOWN:
            print(f"  ! address {getattr(addr, 'email', addr)!r}: {type(exc).__name__}: {exc}")
        return None

    if pattern is None:
        # Role mailbox, odd characters, or genuinely ambiguous.
        stats["skipped_undecidable"] += 1
        return None
    if confidence < min_confidence:
        stats["skipped_low_confidence"] += 1
        return None
    return local, pattern


def collect_evidence(
    conn: Any,
    *,
    tenant_id: str,
    limit_pages: int | None,
    min_confidence: Callable[[int], float],
    stats: Counter[str],
) -> dict[int, dict[str, set[str]]]:
    """
    Walk stored pages and return {company_id: {pattern: {local_part, ...}}}.

    Reads only. `stats` accumulates the counters the summary prints.
    """
    # Tenant-wide and cached for the process, per the classifier's own docs --
    # it is called once per harvested address and must not hit the DB each time.
    first_names = first_name_dictionary(conn)
    stats["first_name_dictionary"] = len(first_names)

    evidence: dict[int, dict[str, set[str]]] = {}
    people_cache: dict[int, list[Any]] = {}
    domain_cache: dict[int, set[str]] = {}
    seen: set[str] = set()  # same address recurs on many pages

    for sid, company_id, url, html in iter_stored_pages(
        conn, tenant_id=tenant_id, company_ids=None, limit=limit_pages
    ):
        stats["pages_scanned"] += 1
        if company_id is None:
            stats["pages_without_company"] += 1
            continue

        # One unreadable page -- broken markup, mojibake, a company row that
        # will not load -- is counted and skipped. It never ends the walk.
        try:
            if company_id not in domain_cache:
                domain_cache.update(load_company_domains(conn, [company_id]))
            # The crawler only stores same-host pages, so the page's own host is
            # a legitimate alias for this company.
            allowed = set(domain_cache.get(company_id) or ()) | build_allowed_domains(url)
            if not allowed:
                stats["pages_without_domain"] += 1
                continue

            found = harvest_page(html, source_url=url, allowed_domains=allowed)
            stats["rejected_foreign"] += found.rejected_foreign
            stats["rejected_placeholder"] += found.rejected_placeholder
            stats["role_addresses"] += len(found.role)
            if not found.personal:
                continue

            if company_id not in people_cache:
                people_cache[company_id] = load_people(conn, company_id)
            people = people_cache[company_id]
        except Exception as exc:
            stats["page_errors"] += 1
            _rollback(conn)
            if stats["page_errors"] <= _MAX_ERRORS_SHOWN:
                print(f"  ! page {sid} {url}: {type(exc).__name__}: {exc}")
            continue

        # Conditional floor: cheap after the first page of a company.
        floor = min_confidence(company_id)
        for addr in found.personal:
            if addr.email in seen:
                continue
            seen.add(addr.email)
            stats["addresses_found"] += 1
            kept = _classify_address(
                addr,
                people=people,
                first_names=first_names,
                min_confidence=floor,
                stats=stats,
            )
            if kept is None:
                continue
            local, pattern = kept
            evidence.setdefault(company_id, {}).setdefault(pattern, set()).add(local)
            stats["evidence_kept"] += 1

    return evidence


def _local_parts(company_evidence: dict[str, set[str]]) -> list[str]:
    """Every distinct local-part kept for one company."""
    return sorted({lp for locals_ in company_evidence.values() for lp in locals_})


def _planned_counts(
    local_parts: list[str], first_names: Any, min_confidence: float
) -> dict[str, int]:
    """
    What record_inferred_patterns() will store for these local-parts.

    Mirrors that function's own filter, because the write re-derives patterns
    from the local-parts rather than from the buckets collect_evidence built.
    Without this the dry run would print a different plan than the real run
    executes, and the plan could never be compared with what is already stored.
    """
    counts: Counter[str] = Counter()
    for lp in local_parts:
        pattern, confidence = infer_pattern_from_localpart(lp, first_names)
        if pattern and confidence >= min_confidence:
            counts[pattern] += 1
    return dict(counts)


def plan_writes(
    conn: Any,
    evidence: dict[int, dict[str, set[str]]],
    *,
    first_names: Any,
    min_confidence: Callable[[int], float],
    stats: Counter[str],
) -> tuple[dict[int, dict[str, int]], set[int]]:
    """
    {company_id: {pattern: inferred_count}} for the companies whose stored
    inferred_count would actually change.

    A company whose counts already match is dropped, so a second run plans
    nothing and writes nothing. Also returns the companies that have no
    pattern evidence at all today -- the NET NEW coverage this backfill buys.
    """
    writes: dict[int, dict[str, int]] = {}
    net_new: set[int] = set()
    for company_id in sorted(evidence):
        existing = _existing_patterns(conn, company_id)
        if not existing:
            net_new.add(company_id)
        planned = _planned_counts(
            _local_parts(evidence[company_id]), first_names, min_confidence(company_id)
        )
        changed = {p: c for p, c in planned.items() if c != existing.get(p, 0)}
        if changed:
            writes[company_id] = planned
        stats["rows_unchanged"] += len(planned) - len(changed)
    return writes, net_new


def _domain_for(conn: Any, company_id: int, cache: dict[int, str | None]) -> str | None:
    if company_id not in cache:
        hosts = load_company_domains(conn, [company_id]).get(company_id) or set()
        cache[company_id] = normalize_domain(sorted(hosts)[0]) if hosts else None
    return cache[company_id]


def run(*, tenant: str, dry_run: bool, limit: int | None, override: float | None) -> int:
    conn = get_conn()
    stats: Counter[str] = Counter()
    floors: dict[int, float] = {}
    min_confidence = make_min_confidence(conn, override=override, cache=floors)
    try:
        evidence = collect_evidence(
            conn,
            tenant_id=tenant,
            limit_pages=limit,
            min_confidence=min_confidence,
            stats=stats,
        )
        first_names = first_name_dictionary(conn)  # cached; collect_evidence built it
        writes, net_new = plan_writes(
            conn,
            evidence,
            first_names=first_names,
            min_confidence=min_confidence,
            stats=stats,
        )
        planned_rows = sum(len(v) for v in writes.values())
        pattern_hist = Counter(pattern for counts in writes.values() for pattern in counts)

        if override is not None:
            policy = f"{override:.2f} everywhere (forced by --min-confidence)"
        else:
            decidable = sum(1 for f in floors.values() if f == DECIDABLE_MIN_CONFIDENCE)
            policy = (
                f"{DECIDABLE_MIN_CONFIDENCE:.2f} on {decidable} companies SMTP can refute,"
                f" {SAFE_MIN_CONFIDENCE:.2f} on {len(floors) - decidable} it cannot or"
                " has no evidence for"
            )

        print(f"tenant                        : {tenant}")
        print(f"first-name dictionary         : {stats['first_name_dictionary']} folded names")
        print(f"pages scanned                 : {stats['pages_scanned']}")
        print(f"pages with no company link    : {stats['pages_without_company']} (skipped)")
        print(f"pages with no known domain    : {stats['pages_without_domain']} (skipped)")
        print(f"pages unreadable              : {stats['page_errors']} (skipped)")
        print(f"on-domain addresses found     : {stats['addresses_found']}")
        print(f"addresses unclassifiable      : {stats['address_errors']} (skipped)")
        print(f"role inboxes ignored          : {stats['role_addresses'] + stats['skipped_role']}")
        print(f"rejected, foreign domain      : {stats['rejected_foreign']}")
        print(f"rejected, placeholder         : {stats['rejected_placeholder']}")
        print(f"matched a known person        : {stats['attributed_to_person']}")
        print(f"classified by local-part shape: {stats['shape_classified']}")
        print(f"confidence floor              : {policy}")
        print(f"dropped, below that floor     : {stats['skipped_low_confidence']}")
        print(f"dropped, undecidable shape    : {stats['skipped_undecidable']}")
        print(f"usable evidence addresses     : {stats['evidence_kept']}")
        print(f"companies with evidence       : {len(evidence)}")
        print(f"companies with NO pattern yet : {len(net_new)}  <- net new coverage")
        print(f"rows already up to date       : {stats['rows_unchanged']} (nothing to write)")
        print(f"(company, pattern) rows to write : {planned_rows} across {len(writes)} companies")
        if pattern_hist:
            print("pattern distribution:")
            for pattern, count in pattern_hist.most_common():
                print(f"  {pattern:<12} {count}")

        if dry_run:
            print("\nDRY RUN - nothing written.")
            return 0

        # A scraped address proves what a company PUBLISHES, not that SMTP
        # accepts it, so this lands in inferred_count -- never confirmed_count.
        # Writing it as confirmed would let two harvested local-parts of the
        # same shape grade STATUS_TRUSTED, indistinguishable from two
        # SMTP-verified people. On the 39% of domains that are catch-all
        # nothing can ever contradict that, so the mistake would be permanent
        # and invisible. record_inferred_patterns() SETs counts from the full
        # input, which is what makes a re-run idempotent.
        #
        # One company per transaction: a company the database rejects is rolled
        # back and skipped, and cannot discard the companies already committed.
        domains: dict[int, str | None] = {}
        written = failed = 0
        for company_id in sorted(writes):
            try:
                recorded = record_inferred_patterns(
                    conn,
                    company_id=company_id,
                    domain=_domain_for(conn, company_id, domains),
                    local_parts=_local_parts(evidence[company_id]),
                    first_names=first_names,
                    min_confidence=min_confidence(company_id),
                )
                conn.commit()
                written += len(recorded)
            except Exception as exc:
                _rollback(conn)
                failed += 1
                if failed <= _MAX_ERRORS_SHOWN:
                    print(f"  ! company {company_id}: {type(exc).__name__}: {exc}")

        print(
            f"\nprocessed {len(evidence)} companies with evidence"
            f" | written {written} (company, pattern) rows to inferred_count"
            f" | skipped {stats['rows_unchanged']} already up to date"
            f" | errors {failed} companies + {stats['page_errors']} pages"
            f" + {stats['address_errors']} addresses"
        )
        return 1 if failed else 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Seed company_email_pattern from on-domain addresses already stored "
            "in sources.html. Costs no network calls and no SMTP probes."
        ),
    )
    parser.add_argument("--tenant", default="dev", help="Tenant id to scan (default: dev)")
    parser.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="DB URL (optional; overrides DATABASE_URL for this run).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N stored pages (default: every page in the tenant).",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help=(
            "Force ONE confidence floor for every company, overriding the "
            f"conditional policy (default: {DECIDABLE_MIN_CONFIDENCE} where stored "
            f"SMTP evidence shows the domain can refute a wrong pattern, "
            f"{SAFE_MIN_CONFIDENCE} where it cannot or where there is no evidence)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be written without touching the database.",
    )
    args = parser.parse_args(argv)

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    return run(
        tenant=args.tenant,
        dry_run=args.dry_run,
        limit=args.limit,
        override=args.min_confidence,
    )


if __name__ == "__main__":
    raise SystemExit(main())
