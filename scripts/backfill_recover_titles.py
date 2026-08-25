#!/usr/bin/env python
# scripts/backfill_recover_titles.py
"""
Recover executive titles that extraction dropped, from HTML we already stored.

The problem
-----------
227 companies have no executive identified. 147 of them have exec-role text
("CEO", "Chief Executive", "Founder", "Co-Founder", "President", "Managing
Director/Partner", "Owner") sitting in `sources.html` right now. The role text
was on the page and the person's name was on the page; extraction just failed
to bind the two. This script re-reads the stored HTML and binds them.

No page is re-fetched. No network call is made.

How a binding is recovered, cheapest first
------------------------------------------
1. JSON-LD `Person` objects carrying `name` + `jobTitle`. Highest precision:
   the markup itself asserts the binding. (Nothing in src/extract reads
   jobTitle today -- address_harvest reads JSON-LD `email` only.)
2. `src/extract/people_cards.extract_people_cards()` -- the existing team-card
   / heading / list / image-alt extractor, which already returns Candidates
   carrying a `title`.
3. Proximity: an exec role phrase within _NEAR_WINDOW characters of a person
   we already have a row for, in the page's visible text.

LLMs are OFF by default and stay off
------------------------------------
The owner has explicitly refused pushing raw HTML through an LLM on token
cost. `--use-ai` is opt-in, is capped by `--ai-max-pages`, and only fires on
companies the three deterministic passes above could not solve. It calls
`src/extract/ai_candidates.extract_people_from_html()`, which sends extracted
TEXT (never raw HTML), already truncated to _HTML_EXTRACT_MAX_CHARS = 12,000
chars. Do not add a raw-HTML path here.

Safety
------
A title is only ever filled when the stored one is NULL, empty, or the
placeholder 'Auto-discovered'. That condition is repeated in the UPDATE's
WHERE clause, not just checked in Python, so a real title can never be
clobbered even if another writer lands between the read and the write.
Only titles that actually name an executive role are accepted; a recovered
"Marketing Coordinator" is not what this backfill is for.

Idempotence and resumability
----------------------------
A second run sees the filled titles as real and skips those people entirely,
so it proposes nothing and writes nothing.

Each title is committed on its own. A page that will not parse, a people row
that will not load, or an UPDATE the database rejects is counted and skipped;
none of them can end the walk or undo the titles already committed, so an
interrupted run resumes by simply being re-run.

Usage
-----
    python scripts/backfill_recover_titles.py --dry-run
    python scripts/backfill_recover_titles.py --dry-run --limit 500
    python scripts/backfill_recover_titles.py
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Sibling script, imported rather than copied: it owns the keyset-paginated
# walk over `sources`.
from harvest_addresses import iter_stored_pages  # noqa: E402

from src.db import get_conn  # noqa: E402
from src.extract.people_cards import extract_people_cards  # noqa: E402
from src.generate.patterns import fold_name  # noqa: E402

#: Placeholder the extractor writes when it found a person but no role.
PLACEHOLDER_TITLE = "Auto-discovered"

#: Characters either side of a name that count as "next to" a role phrase.
_NEAR_WINDOW = 120

#: The executive roles this backfill exists to recover. Anything else is left
#: alone -- filling in "Office Manager" does not make a company placeable.
_EXEC_PHRASE = (
    r"(?:Co[-\s]?Founders?|Founders?|Chief\s+Executive(?:\s+Officer)?|CEO"
    r"|President|Managing\s+(?:Director|Partner)|Owner)"
)
#: Matches compound titles as one string: "Co-Founder & CEO", "Founder / Owner".
_EXEC_TITLE_RE = re.compile(
    rf"{_EXEC_PHRASE}(?:\s*(?:&|and|/|,|\|)\s*{_EXEC_PHRASE})*",
    re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")
_JSONLD_MAX_DEPTH = 12
_STRIPPED_TAGS = ("script", "style", "noscript", "template")

# Precedence for competing proposals; lower wins.
_METHOD_RANK = {"jsonld": 0, "people_cards": 1, "proximity": 2, "ai": 3}

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


@dataclass(frozen=True)
class Person:
    person_id: int
    company_id: int
    first_name: str | None
    last_name: str | None
    full_name: str | None
    title: str | None

    @property
    def needs_title(self) -> bool:
        t = (self.title or "").strip()
        return not t or t == PLACEHOLDER_TITLE

    def keys(self) -> set[str]:
        """Folded name forms this person can be recognised by."""
        out: set[str] = set()
        joined = fold_name(f"{self.first_name or ''}{self.last_name or ''}")
        if self.first_name and self.last_name and joined:
            out.add(joined)
        if self.full_name:
            folded = fold_name(self.full_name)
            if folded:
                out.add(folded)
        return out

    def display(self) -> str:
        if self.full_name and self.full_name.strip():
            return self.full_name.strip()
        return f"{self.first_name or ''} {self.last_name or ''}".strip()


def _utc_now() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def exec_title(text: str | None) -> str | None:
    """Return the executive title inside `text`, tidied, or None."""
    if not text:
        return None
    m = _EXEC_TITLE_RE.search(_WS_RE.sub(" ", str(text)))
    if not m:
        return None
    title = m.group(0).strip(" -–—,|/&")
    return _WS_RE.sub(" ", title) or None


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

_SELECT_PEOPLE = """
    SELECT id, company_id, first_name, last_name, full_name, title
    FROM people
    WHERE tenant_id = ?
"""


def load_people(
    conn: Any, *, tenant_id: str, company_id: int | None, stats: Counter[str]
) -> list[Person]:
    """Roster for the tenant. A row that will not read is counted and dropped."""
    sql = _SELECT_PEOPLE
    params: list[Any] = [tenant_id]
    if company_id is not None:
        sql += " AND company_id = ?"
        params.append(int(company_id))
    rows = conn.execute(sql, tuple(params)).fetchall() or []
    people: list[Person] = []
    for r in rows:
        try:
            people.append(
                Person(
                    person_id=int(r[0]),
                    company_id=int(r[1]),
                    first_name=r[2],
                    last_name=r[3],
                    full_name=r[4],
                    title=r[5],
                )
            )
        except Exception:
            stats["people_rows_unreadable"] += 1
    return people


def target_companies(people: list[Person]) -> set[int]:
    """
    Companies worth scanning: at least one person missing a title, and no
    executive identified yet. A company that already has a CEO on file has
    nothing for this backfill to recover.
    """
    missing = {p.company_id for p in people if p.needs_title}
    solved = {p.company_id for p in people if not p.needs_title and exec_title(p.title)}
    return missing - solved


# ---------------------------------------------------------------------------
# Deterministic extraction
# ---------------------------------------------------------------------------


def _walk_jsonld(node: Any, depth: int = 0):
    """Yield (name, jobTitle) from schema.org Person objects."""
    if depth > _JSONLD_MAX_DEPTH:
        return
    if isinstance(node, list):
        for item in node:
            yield from _walk_jsonld(item, depth + 1)
        return
    if not isinstance(node, dict):
        return

    name, job = node.get("name"), node.get("jobTitle")
    if isinstance(job, list):
        job = ", ".join(str(j) for j in job if isinstance(j, str))
    if isinstance(name, str) and isinstance(job, str) and name.strip() and job.strip():
        yield name.strip(), job.strip()

    for value in node.values():
        yield from _walk_jsonld(value, depth + 1)


def from_jsonld(soup: Any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        blob = script.string or script.get_text()
        if not blob or not blob.strip():
            continue
        try:
            data = json.loads(blob)
        except (ValueError, TypeError):
            continue
        out.extend(_walk_jsonld(data))
    return out


def from_people_cards(html: str, source_url: str) -> list[tuple[str, str]]:
    """Reuse the existing team-card / heading / list / alt-text extractor."""
    try:
        cards = extract_people_cards(html, source_url)
    except Exception:
        return []
    out: list[tuple[str, str]] = []
    for c in cards:
        if not c.title:
            continue
        name = c.raw_name or f"{c.first_name or ''} {c.last_name or ''}".strip()
        if name:
            out.append((name, c.title))
    return out


def _name_occurrences(text: str, people: list[Person]) -> list[tuple[int, int, str]]:
    """Every (start, end, display) where a roster name appears in `text`."""
    lowered = text.lower()
    out: list[tuple[int, int, str]] = []
    for p in people:
        display = p.display()
        if len(display) < 5:
            continue
        needle = display.lower()
        at = lowered.find(needle)
        while at >= 0:
            out.append((at, at + len(needle), display))
            at = lowered.find(needle, at + len(needle))
    out.sort()
    return out


def from_proximity(text: str, people: list[Person]) -> list[tuple[str, str]]:
    """
    Bind each exec role phrase to the NEAREST name we already have a row for.

    Walking role phrases (not names) is what stops a title bleeding onto a
    colleague: on a team page reading "Jane Kowalski / CEO / Robert Nguyen /
    CFO", "CEO" is nearer to Jane than to Robert, so Robert cannot inherit it.
    A phrase further than _NEAR_WINDOW from every known name is dropped rather
    than guessed.

    Deliberately narrow: it only matches people already in the database, so it
    can never invent a person -- only a binding.
    """
    occurrences = _name_occurrences(text, people)
    if not occurrences:
        return []

    out: list[tuple[str, str]] = []
    for m in _EXEC_TITLE_RE.finditer(text):
        m_start, m_end = m.span()
        best: str | None = None
        best_gap = _NEAR_WINDOW + 1
        for start, end, display in occurrences:
            if start < m_end and m_start < end:
                gap = 0  # the name and the phrase overlap ("CEO Jane Kowalski")
            else:
                gap = m_start - end if end <= m_start else start - m_end
            if gap < best_gap:
                best, best_gap = display, gap
        title = exec_title(m.group(0))
        if best is not None and title:
            out.append((best, title))
    return out


def visible_text(soup: Any) -> str:
    """Visible page text. Mutates `soup`, so call it after the tag readers."""
    for el in soup.find_all(_STRIPPED_TAGS):
        el.decompose()
    return _WS_RE.sub(" ", soup.get_text(" ", strip=True))


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _propose(
    proposals: dict[int, tuple[int, str, str, str]],
    by_key: dict[str, Person],
    pairs: list[tuple[str, str]],
    *,
    method: str,
    source_url: str,
    stats: Counter[str],
) -> None:
    """Bind (name, title) pairs to person rows, keeping the best-ranked method."""
    rank = _METHOD_RANK[method]
    for name, raw_title in pairs:
        person = by_key.get(fold_name(name))
        if person is None:
            stats["unmatched_name"] += 1
            continue
        title = exec_title(raw_title)
        if not title:
            stats["not_an_exec_title"] += 1
            continue
        current = proposals.get(person.person_id)
        if current is None or rank < current[0]:
            proposals[person.person_id] = (rank, title, method, source_url)


def collect(
    conn: Any,
    *,
    tenant_id: str,
    company_id: int | None,
    limit_pages: int | None,
    use_ai: bool,
    ai_max_pages: int,
    stats: Counter[str],
) -> tuple[dict[int, tuple[int, str, str, str]], dict[int, Person]]:
    """Walk stored pages for the target companies and propose titles. Reads only."""
    people = load_people(conn, tenant_id=tenant_id, company_id=company_id, stats=stats)
    targets = target_companies(people)
    stats["people_total"] = len(people)
    stats["people_missing_title"] = sum(1 for p in people if p.needs_title)
    stats["companies_targeted"] = len(targets)
    if not targets:
        return {}, {}

    by_company: dict[int, list[Person]] = {}
    for p in people:
        if p.company_id in targets:
            by_company.setdefault(p.company_id, []).append(p)

    proposals: dict[int, tuple[int, str, str, str]] = {}
    people_by_id = {p.person_id: p for p in people}
    ai_pages_used = 0

    from bs4 import BeautifulSoup  # local import: matches src/extract convention

    for sid, cid, url, html in iter_stored_pages(
        conn, tenant_id=tenant_id, company_ids=sorted(targets), limit=limit_pages
    ):
        stats["pages_scanned"] += 1
        roster = by_company.get(cid) if cid is not None else None
        if not roster or not html:
            continue
        # Everyone on this company's roster is a legitimate match target; the
        # needs_title filter is applied when the write is planned, so a page
        # that names an already-titled colleague still costs nothing.
        by_key = {k: p for p in roster for k in p.keys()}
        if not by_key:
            continue

        # One unparseable page is counted and skipped. It never ends the walk.
        try:
            soup = BeautifulSoup(html, "html.parser")
            before = len(proposals)
            _propose(
                proposals, by_key, from_jsonld(soup), method="jsonld", source_url=url, stats=stats
            )
            _propose(
                proposals,
                by_key,
                from_people_cards(html, url),
                method="people_cards",
                source_url=url,
                stats=stats,
            )
            _propose(
                proposals,
                by_key,
                from_proximity(visible_text(soup), roster),
                method="proximity",
                source_url=url,
                stats=stats,
            )

            if use_ai and len(proposals) == before and ai_pages_used < ai_max_pages:
                ai_pages_used += 1
                stats["ai_pages_used"] += 1
                _propose(
                    proposals,
                    by_key,
                    _from_ai(html, url),
                    method="ai",
                    source_url=url,
                    stats=stats,
                )
        except Exception as exc:
            stats["page_errors"] += 1
            _rollback(conn)
            if stats["page_errors"] <= _MAX_ERRORS_SHOWN:
                print(f"  ! page {sid} {url}: {type(exc).__name__}: {exc}")
            continue

    return proposals, people_by_id


def _from_ai(html: str, source_url: str) -> list[tuple[str, str]]:
    """
    Text-only, token-capped LLM fallback. OFF unless --use-ai is passed.

    extract_people_from_html() runs _extract_text_for_ai() first, so the model
    never sees raw HTML, and the payload is truncated at 12,000 chars.
    """
    try:
        from src.extract.ai_candidates import extract_people_from_html
    except Exception:
        return []
    try:
        cands = extract_people_from_html(
            html=html,
            source_url=source_url,
            company_name="",
            domain="",
        )
    except Exception:
        return []
    out: list[tuple[str, str]] = []
    for c in cands:
        name = c.raw_name or f"{c.first_name or ''} {c.last_name or ''}".strip()
        if name and c.title:
            out.append((name, c.title))
    return out


_UPDATE_TITLE = """
    UPDATE people SET title = ?, updated_at = ?
    WHERE id = ? AND (title IS NULL OR title = '' OR title = ?)
"""


def run(
    *,
    tenant: str,
    company_id: int | None,
    dry_run: bool,
    limit: int | None,
    use_ai: bool,
    ai_max_pages: int,
    verbose: bool,
) -> int:
    conn = get_conn()
    stats: Counter[str] = Counter()
    try:
        proposals, people_by_id = collect(
            conn,
            tenant_id=tenant,
            company_id=company_id,
            limit_pages=limit,
            use_ai=use_ai,
            ai_max_pages=ai_max_pages,
            stats=stats,
        )

        # Never clobber a real title. Checked here and again in the UPDATE.
        writes = [
            (pid, title, method, url)
            for pid, (_rank, title, method, url) in sorted(proposals.items())
            if people_by_id[pid].needs_title
        ]
        stats["skipped_already_titled"] = len(proposals) - len(writes)
        by_method = Counter(m for _, _, m, _ in writes)
        companies = {people_by_id[pid].company_id for pid, _, _, _ in writes}

        print(f"tenant                        : {tenant}")
        print(f"people in scope               : {stats['people_total']}")
        print(f"people rows unreadable        : {stats['people_rows_unreadable']} (skipped)")
        print(f"people missing a title        : {stats['people_missing_title']}")
        print(f"companies with no exec        : {stats['companies_targeted']}")
        print(f"stored pages scanned          : {stats['pages_scanned']}")
        print(f"pages unparseable             : {stats['page_errors']} (skipped)")
        print(f"names on page, no person row  : {stats['unmatched_name']}")
        print(f"titles found, not executive   : {stats['not_an_exec_title']}")
        print(f"proposals already titled      : {stats['skipped_already_titled']} (left alone)")
        print(f"LLM pages used                : {stats['ai_pages_used']} (--use-ai)")
        print(f"titles to fill                : {len(writes)}")
        print(f"companies gaining an exec     : {len(companies)}")
        if by_method:
            print("by method:")
            for method, count in by_method.most_common():
                print(f"  {method:<14} {count}")
        if verbose or dry_run:
            for pid, title, method, url in writes:
                p = people_by_id[pid]
                print(f"  company={p.company_id} person={pid} {p.display()!r} -> {title!r}")
                print(f"      via {method}  {url}")

        if dry_run:
            print("\nDRY RUN - nothing written.")
            return 0

        # One title per transaction: a row the database rejects is rolled back
        # and skipped, and cannot undo the titles already committed.
        now = _utc_now()
        updated = unchanged = failed = 0
        for pid, title, _method, _url in writes:
            try:
                cur = conn.execute(_UPDATE_TITLE, (title, now, pid, PLACEHOLDER_TITLE))
                changed = cur.rowcount if (cur.rowcount or 0) > 0 else 0
                conn.commit()
                if changed:
                    updated += changed
                else:
                    # Another writer filled in a real title between the read and
                    # the write; the WHERE clause refused to clobber it.
                    unchanged += 1
            except Exception as exc:
                _rollback(conn)
                failed += 1
                if failed <= _MAX_ERRORS_SHOWN:
                    print(f"  ! person {pid}: {type(exc).__name__}: {exc}")

        print(
            f"\nprocessed {len(writes)} proposals"
            f" | written {updated} titles"
            f" | skipped {unchanged} refused by the no-clobber guard"
            f" + {stats['skipped_already_titled']} already titled"
            f" | errors {failed} writes + {stats['page_errors']} pages"
            f" + {stats['people_rows_unreadable']} people rows"
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
            "Recover executive titles from already-stored HTML for companies "
            "that have no executive identified. No page is re-fetched."
        ),
    )
    parser.add_argument("--tenant", default="dev", help="Tenant id to scan (default: dev)")
    parser.add_argument("--company-id", type=int, default=None, help="Restrict to one company id")
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
        help="Stop after N stored pages (default: every page for the target companies).",
    )
    parser.add_argument(
        "--use-ai",
        action="store_true",
        help=(
            "Opt-in LLM fallback for pages the deterministic passes could not solve. "
            "Text-only and token-capped; OFF by default on purpose."
        ),
    )
    parser.add_argument(
        "--ai-max-pages",
        type=int,
        default=50,
        help="Hard cap on LLM calls when --use-ai is passed (default: 50).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be written without touching the database.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print every recovered binding.")
    args = parser.parse_args(argv)

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    return run(
        tenant=args.tenant,
        company_id=args.company_id,
        dry_run=args.dry_run,
        limit=args.limit,
        use_ai=args.use_ai,
        ai_max_pages=args.ai_max_pages,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    raise SystemExit(main())
