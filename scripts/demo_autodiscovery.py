from __future__ import annotations

"""
scripts/demo_autodiscovery.py

End-to-end demo for the email-scraper project:

Given a single company name + domain and a SQLite DB path, this script will:

  1. Ensure the schema is applied (optional flag).
  2. Upsert the company into `companies`.
  3. Resolve its official domain (R08) using resolve_company_domain().
  4. Crawl the site (R10) and persist HTML pages into `sources`.
  5. Extract people/email candidates from those pages (R11 + O05 + optional O27).
  6. Persist people + emails linked to the company.
  7. Generate permutations for people without emails (R12 + O01/O09 + O26),
     restricted to people that came through the AI refiner when AI is enabled.
  8. Verify emails synchronously via task_probe_email (R16–R18 + O07).
  9. Optionally backfill ICP scores using scripts/backfill_r14_icp.py (R14).
 10. Print a small summary from v_emails_latest and, if available, search backend.

The goal is *clarity* rather than maximum throughput: everything runs
synchronously in-process, without depending on RQ workers.
"""

import argparse
import datetime as dt
import inspect
import logging
import os
import re
import socket
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup, Tag  # noqa: F401

import src.extract.candidates as _extract_mod  # noqa: F401
from src.crawl.runner import Page, crawl_domain
from src.db import get_conn, upsert_generated_email  # noqa: F401
from src.db_pages import save_pages as _save_pages
from src.emails.classify import is_role_or_placeholder_email
from src.extract.candidates import ROLE_ALIASES, Candidate, extract_candidates
from src.extract.stopwords import NAME_STOPWORDS
from src.generate.patterns import (
    PATTERNS as CANON_PATTERNS,
)
from src.generate.patterns import (
    generate_candidate_emails_for_person,
    get_company_email_pattern,
    infer_domain_pattern,
)
from src.generate.patterns import (
    infer_pattern_for_company as infer_company_email_pattern,  # noqa: F401
)
from src.generate.permutations import generate_permutations  # noqa: F401
from src.ingest.normalize import normalize_split_parts
from src.queueing.tasks import (
    resolve_company_domain,
)
from src.queueing.tasks import (
    task_probe_email as _task_probe_email,
)
from src.search.indexing import LeadSearchParams, search_people_leads

# Optional AI-assisted people extractor (O27).
try:
    from src.extract.ai_candidates import extract_ai_candidates as _ai_extract_candidates

    _HAS_AI_EXTRACTOR = True
except Exception:  # pragma: no cover - optional / older repos
    _ai_extract_candidates = None  # type: ignore[assignment]
    _HAS_AI_EXTRACTOR = False

LOG = logging.getLogger("demo_autodiscovery")

ROOT_DIR = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT_DIR / "db" / "schema.sql"

# Optional: demo-only skip list so we can avoid probing certain emails
DEMO_SKIP_EMAILS: set[str] = {
    s.strip().lower() for s in os.getenv("DEMO_SKIP_EMAILS", "").split(",") if s.strip()
}

# Common “people / about” pages that we care about for robots + fallback crawling.
_MANUAL_HINT_PATHS: list[str] = [
    "/",
    "/company",
    "/about",
    "/about-us",
    "/team",
    "/leadership",
    "/our-team",
    "/who-we-are",
    "/founders",
    "/management",
    "/executives",
    "/contact",
    "/people",
    "/staff",
]

# URL path hints that usually correspond to people/leadership/team pages
PEOPLE_URL_HINTS: tuple[str, ...] = (
    "team",
    "our-team",
    "our_team",
    "leadership",
    "people",
    "staff",
    "associates",
    "partners",
    "who-we-are",
    "who_we_are",
)

# O26: cap how many *people without emails* we auto-generate permutations for,
# per company, to avoid blowing up the queue/domain while still getting a
# realistic slice of the team.
MAX_PEOPLE_PER_COMPANY: int = 5

# Names that clearly indicate a generic/marketing concept rather than a person.
# This is now anchored on the global NAME_STOPWORDS list (from name_stopwords.txt)
# plus demo-specific extras.
GENERATION_NAME_STOPWORDS: set[str] = set(NAME_STOPWORDS) | {
    # Generic non-person / boilerplate
    "welcome",
    "team",
    "office",
    "info",
    "support",
    "example",
    "contact",
    "admin",
    "marketing",
    "billing",
    "hello",
    "hi",
    # Navigation / site chrome
    "home",
    "about",
    "company",
    "services",
    "service",
    "news",
    "blog",
    "careers",
    "career",
    "jobs",
    "job",
    "resources",
    "resource",
    "library",
    "libraries",
    "documents",
    "document",
    "downloads",
    "download",
    "faq",
    "faqs",
    "help",
    "policies",
    "policy",
    "privacy",
    "terms",
    "conditions",
    "disclaimer",
    "disclaimers",
    # Auth / account words
    "login",
    "log",
    "in",
    "signin",
    "signup",
    "sign",
    "register",
    "account",
    "dashboard",
    "portal",
    "profile",
    "settings",
    "preferences",
    # Payments / billing / CTAs
    "payment",
    "payments",
    "pay",
    "checkout",
    "cart",
    "basket",
    "invoice",
    "invoices",
    "subscribe",
    "subscription",
    "unsubscribe",
    "request",
    "quote",
    "quotes",
    "demo",
    "book",
    "schedule",
    "call",
    "meeting",
    "meet",
    # Generic marketing / corporate terms
    "building",
    "deliver",
    "delivery",
    "growth",
    "strategy",
    "strategic",
    "solution",
    "solutions",
    "partner",
    "partners",
    "partnership",
    "talent",
    "pricing",
    "price",
    "results",
    "insights",
    "impact",
    "innovation",
    "innovative",
    # Section / navigation labels we see a lot
    "our",
    "firm",
    "story",
    "mission",
    "vision",
    "values",
    "history",
    "leadership",
    "management",
    "executive",
    "executives",
    # Crestwell-specific marketing/tagline words we’ve seen
    "ambitious",
    "efficient",
    "dedicated",
    "specialized",
    "expertise",
    "fractional",
    "readiness",
    "quiz",
    "founder",
    "founderled",
    "founder-led",
    "process",
    "how",
    "works",
    "take",
    "option",
    "options",
    # LinkedIn pseudo-person
    "linkedin",
    "view",
    # NEW: kill 'Community Advisor' pseudo-person
    "community",
    "advisor",
    "advisors",
    # Brandt navigation and pseudo-sections
    "tax",
    "business",
    "links",
    "link",
    "useful",
    "testimonials",
    "testimonial",
    "payroll",
    "contractor",
    "contractors",
    # Brandt pseudo-“people” role labels
    "certified",
    "public",
    "accountant",
    "accountants",
    "preparation",
    "preparations",
    "individual",
    "manager",
    "managers",
    "bookkeeping",
    "bookkeeper",
    "bookkeepers",
    # General role/position terms that should not, by themselves, be treated as names
    "president",
    "vice",
    "chair",
    "chairman",
    "chairwoman",
    "director",
    "directors",
    "principal",
    "principals",
    "associate",
    "associates",
    "analyst",
    "analysts",
    "engineer",
    "engineers",
    "developer",
    "developers",
    "designer",
    "designers",
    "consultant",
    "consultants",
    "intern",
    "interns",
    "assistant",
    "assistants",
    "coordinator",
    "coordinators",
    "specialist",
    "specialists",
    "lead",
    "leads",
    "head",
    "heads",
    "administrative",
    "administration",
}

# Full-line phrases that are clearly navigation/section labels, not people.
TEAM_NAV_FULL_NAME_STOPWORDS: set[str] = {
    "log in",
    "login",
    "home",
    "about",
    "our team",
    "our firm",
    "disclaimers & privacy policy",
    "disclaimers and privacy policy",
    "services",
    "individual tax preparation",
    "business services",
    "make a payment",
    "useful links",
    "documents",
    "testimonials",
    "add a testimonial",
    "contact",
    "more",
    "brandt & associates, p.c.",
    "brandt & associates, pc",
}

# Credential suffixes like "CPA", "MBA" that can appear after the real name.
TEAM_CREDENTIAL_SUFFIXES: set[str] = {
    "cpa",
    "mba",
    "esq",
    "phd",
    "md",
    "jd",
    "dmd",
    "dds",
    "do",
    "cfa",
    "cfp",
}

# Simple pattern: 2–4 capitalized tokens, letters/dot/hyphen/apostrophe allowed.
_NAME_RE = re.compile(r"^[A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){1,3}$")

# Strip trailing professional credentials like ", CPA", ", MBA" etc.
_CREDENTIAL_SUFFIX_RE = re.compile(
    r",?\s*(CPA|CFA|MBA|LLM|LL\.M\.|JD|J\.D\.|ESQ\.?|MD|M\.D\.|PhD|Ph\.D\.)\.?$",
    re.IGNORECASE,
)


def _normalize_name_line(text: str) -> str:
    """
    Normalize a potential 'name' line:

      - Trim whitespace.
      - Remove trailing credential suffixes (CPA, MBA, etc.).
      - Strip trailing commas and spaces.
    """
    t = text.strip()
    if not t:
        return ""

    # Drop credential suffixes like ", CPA", ", MBA"
    t = _CREDENTIAL_SUFFIX_RE.sub("", t)

    # Remove trailing commas / extra spaces
    t = re.sub(r"[,\s]+$", "", t)

    return t


# ---------------------------------------------------------------------------
# Generic helpers: schema application + flexible inserts
# ---------------------------------------------------------------------------


def _mx_hosts_for_domain(domain: str, *, lifetime: float = 3.0) -> list[str]:
    """
    Best-effort MX lookup for a domain, sorted by preference ascending.

    Returns a list of hostnames (no trailing dot). If MX lookup fails,
    falls back to [domain].
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return []

    try:
        import dns.resolver  # type: ignore
    except Exception:
        return [dom]

    try:
        answers = dns.resolver.resolve(dom, "MX", lifetime=lifetime)
    except Exception:
        return [dom]

    pairs: list[tuple[int, str]] = []
    for rdata in answers:
        try:
            pref = int(getattr(rdata, "preference", 0))
            exch = str(getattr(rdata, "exchange", "")).rstrip(".").strip().lower()
            if exch:
                pairs.append((pref, exch))
        except Exception:
            continue

    pairs.sort(key=lambda x: x[0])

    # Dedup while preserving order
    out: list[str] = []
    seen: set[str] = set()
    for _pref, host in pairs:
        if host not in seen:
            seen.add(host)
            out.append(host)

    return out or [dom]


def _smtp_tcp25_preflight(
    domain: str,
    *,
    timeout_s: float = 3.0,
    max_mx_to_try: int = 8,
) -> dict[str, object]:
    """
    Quick connectivity check: can we open TCP/25 to ANY MX for this domain?

    This catches the common case where your ISP / network blocks outbound 25,
    which otherwise shows up as long "unknown timeout" probes.
    """
    mx_hosts = _mx_hosts_for_domain(domain, lifetime=timeout_s) or []
    tried: list[str] = []
    errors: list[str] = []

    for host in mx_hosts[:max_mx_to_try]:
        tried.append(host)
        try:
            conn = socket.create_connection((host, 25), timeout=timeout_s)
            try:
                conn.close()
            except Exception:
                pass
            return {
                "ok": True,
                "mx_hosts": mx_hosts,
                "tried": tried,
                "errors": errors,
            }
        except Exception as exc:
            errors.append(f"{host}: {type(exc).__name__}: {exc}")

    return {
        "ok": False,
        "mx_hosts": mx_hosts,
        "tried": tried,
        "errors": errors,
    }


def apply_schema(conn: sqlite3.Connection) -> None:
    """
    Apply db/schema.sql to the given connection.

    Safe to run multiple times thanks to IF NOT EXISTS / DROP VIEW IF EXISTS
    patterns used throughout schema.sql.
    """
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript("PRAGMA foreign_keys = ON;")
    conn.executescript(sql)


def _default_for_column(col_type: str, name: str) -> Any:
    """
    Provide conservative defaults for NOT NULL columns that do not have explicit
    values when inserting synthetic rows.

    This mirrors the logic used in tests/test_r25_qa_acceptance.py so that
    the demo stays robust when the schema gains new columns.
    """
    t = (col_type or "").upper()
    if name in {"attrs", "extra_attrs", "meta"}:
        return "{}"
    if "INT" in t:
        return 0
    if "REAL" in t or "FLOAT" in t or "DOUBLE" in t:
        return 0.0
    if "BOOL" in t:
        return 0
    if "DATE" in t or "TIME" in t:
        return "1970-01-01T00:00:00"
    return "demo-default"


def insert_row(
    conn: sqlite3.Connection,
    table: str,
    values: dict[str, Any],
    *,
    return_id: bool = False,
) -> int | None:
    """
    Generic INSERT helper that:

      * Introspects the table via PRAGMA table_info(table).
      * Populates any NOT NULL columns without defaults with conservative
        synthetic values.
      * Ignores keys that do not correspond to actual columns.
    """
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = cur.fetchall()

    if not cols:
        raise RuntimeError(f"Table {table!r} does not exist")

    insert_cols: list[str] = []
    params: list[Any] = []

    for col in cols:
        name = col["name"]
        col_type = col["type"]
        notnull = bool(col["notnull"])
        has_default = col["dflt_value"] is not None

        if name in values:
            insert_cols.append(name)
            params.append(values[name])
        elif notnull and not has_default:
            insert_cols.append(name)
            params.append(_default_for_column(col_type, name))
        else:
            # Nullable or has default; omit from INSERT.
            continue

    if not insert_cols:
        raise RuntimeError(f"No insertable columns for table {table!r}")

    placeholders = ", ".join(["?"] * len(insert_cols))
    col_list = ", ".join(insert_cols)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    cur = conn.execute(sql, params)
    return int(cur.lastrowid) if return_id else None


# ---------------------------------------------------------------------------
# Company + sources wiring
# ---------------------------------------------------------------------------


def ensure_company(
    conn: sqlite3.Connection,
    *,
    name: str,
    domain: str,
    website_url: str | None = None,
) -> int:
    """
    Upsert a company row for the given name/domain pair and return its id.

    We treat any existing row with matching official_domain/domain/user_supplied_domain
    as the same logical company.
    """
    dom = (domain or "").strip().lower()
    row = conn.execute(
        """
        SELECT id
        FROM companies
        WHERE lower(official_domain) = ?
           OR lower(domain) = ?
           OR lower(coalesce(user_supplied_domain, '')) = ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (dom, dom, dom),
    ).fetchone()
    if row:
        company_id = int(row["id"])
        LOG.info("Using existing company id=%s for domain=%s", company_id, dom)
        return company_id

    company_id = insert_row(
        conn,
        "companies",
        {
            "name": name,
            "domain": dom,
            "website_url": website_url or f"https://{dom}",
            "user_supplied_domain": dom,
        },
        return_id=True,
    )
    if company_id is None:
        raise RuntimeError("Failed to insert company row")
    LOG.info("Inserted company id=%s for domain=%s", company_id, dom)
    conn.commit()
    return company_id


def get_company_domain_info(
    conn: sqlite3.Connection, company_id: int
) -> tuple[str | None, str | None]:
    """
    Return (official_domain, fallback_domain) for a company.

    fallback_domain is the raw domain/user_supplied_domain value.
    """
    row = conn.execute(
        """
        SELECT
          official_domain,
          coalesce(official_domain, domain, user_supplied_domain) AS fallback
        FROM companies
        WHERE id = ?
        """,
        (company_id,),
    ).fetchone()
    if not row:
        return None, None
    return (row["official_domain"], row["fallback"])


def _save_pages_for_company(
    conn: sqlite3.Connection,
    company_id: int,
    pages: list[Page],
) -> int:
    """
    Persist crawled pages into the sources table, associating them to company_id
    when the schema/function supports it.

    Returns the number of pages written.
    """
    if not pages:
        return 0

    # Try to call save_pages(conn, pages, company_id=...) when available; fall back
    # to the original 2-arg form for older versions.
    try:
        sig = inspect.signature(_save_pages)
        if "company_id" in sig.parameters:
            _save_pages(conn, pages, company_id=company_id)  # type: ignore[arg-type]
        else:
            _save_pages(conn, pages)  # type: ignore[arg-type]
    except Exception:
        # As a fallback, call original and then patch company_id if the column exists.
        _save_pages(conn, pages)  # type: ignore[arg-type]

    # Ensure company_id is set on sources rows when the column exists.
    try:
        cols = {
            r["name"]
            for r in conn.execute("PRAGMA table_info(sources)").fetchall()  # type: ignore[attr-defined]
        }
    except Exception:
        cols = set()

    if "company_id" in cols:
        conn.execute(
            """
            UPDATE sources
            SET company_id = ?
            WHERE company_id IS NULL
              AND lower(source_url) LIKE ?
            """,
            (company_id, "%"),
        )
    conn.commit()
    return len(pages)


def _iter_sources_for_company(
    conn: sqlite3.Connection,
    company_id: int,
    official_domain: str | None,
) -> Iterable[tuple[str, bytes]]:
    """
    Yield (source_url, html_bytes) pairs for pages that belong to this company.

    Prefer the company_id column on sources when present; otherwise filter by host.
    """
    dom = (official_domain or "").strip().lower()

    try:
        rows = conn.execute("PRAGMA table_info(sources)").fetchall()  # type: ignore[attr-defined]
        cols = {r["name"] for r in rows}
    except Exception:
        cols = set()

    if "company_id" in cols:
        cur = conn.execute(
            "SELECT source_url, html FROM sources WHERE company_id = ? ORDER BY id ASC",
            (company_id,),
        )
        for row in cur.fetchall():
            yield row["source_url"], row["html"]
        return

    # Fallback: no company_id column; filter by host/domain in Python.
    cur = conn.execute("SELECT source_url, html FROM sources ORDER BY id ASC")
    for row in cur.fetchall():
        url = row["source_url"]
        html = row["html"]
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception:
            host = ""
        if dom and host and (host == dom or host.endswith("." + dom)):
            yield url, html


# ---------------------------------------------------------------------------
# robots.txt helpers + fallback crawling
# ---------------------------------------------------------------------------


def _load_robots_parser(domain: str) -> RobotFileParser | None:
    """
    Best-effort loader for robots.txt for the given domain.

    Tries https://domain/robots.txt and https://www.domain/robots.txt.
    Returns a RobotFileParser or None if fetching/parsing fails.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return None

    candidates = [
        f"https://{dom}/robots.txt",
        f"https://www.{dom}/robots.txt",
    ]

    for robots_url in candidates:
        rp = RobotFileParser()
        try:
            rp.set_url(robots_url)
            rp.read()
        except Exception as exc:
            LOG.debug("Failed to load robots.txt from %s: %s", robots_url, exc)
            continue

        # If we got here without raising, treat it as a usable parser.
        LOG.debug("Loaded robots.txt from %s", robots_url)
        return rp

    return None


def _check_robots_and_suggest_manual_pages(domain: str) -> bool:
    """
    Inspect robots.txt and see if likely 'people' pages appear to be disallowed.

    If so, log a clear message with manual URLs the user can visit and return True
    (meaning: we should *not* crawl automatically).

    If robots.txt is missing or does *not* explicitly disallow these paths,
    return False.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return False

    rp = _load_robots_parser(dom)
    if rp is None:
        LOG.info(
            "robots.txt could not be fetched for %s; treating crawl permissions as unknown.",
            dom,
        )
        return False

    blocked_urls: list[str] = []
    for path in _MANUAL_HINT_PATHS:
        # Check both bare domain and www. for clarity.
        for host in (dom, f"www.{dom}"):
            url = f"https://{host}{path}"
            try:
                if not rp.can_fetch("*", url):
                    blocked_urls.append(url)
            except Exception:
                continue

    # Deduplicate while preserving order.
    seen: set[str] = set()
    blocked_urls = [u for u in blocked_urls if not (u in seen or seen.add(u))]

    if not blocked_urls:
        # robots.txt does not explicitly block our typical "people" pages.
        return False

    LOG.warning(
        "robots.txt for %s appears to disallow crawling of some likely 'people' pages.",
        dom,
    )
    LOG.warning("Out of respect for robots.txt, this demo will not crawl those URLs.")
    LOG.warning(
        "If you still want to research this company, you can open these URLs manually "
        "in your browser and add people to the database yourself:"
    )
    for url in blocked_urls:
        LOG.warning("  - %s", url)

    LOG.warning(
        "Once you have added people records manually, you can run a separate "
        "email-generation / verification process that does not rely on crawling."
    )
    return True


def _build_page(url: str, html_bytes: bytes, *, source: str | None = None) -> Page:
    """
    Build a Page instance from raw HTML bytes in a way that's compatible
    with the src.crawl.runner.Page dataclass/model.
    """
    html = html_bytes.decode("utf-8", errors="replace")

    kwargs: dict[str, Any] = {
        "url": url,
        "html": html,
    }

    if source is not None:
        kwargs["source"] = source

    # Page now requires `fetched_at`, so make sure we provide it.
    kwargs.setdefault(
        "fetched_at",
        dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    )

    return Page(**kwargs)  # type: ignore[arg-type]


def _fallback_crawl_core_pages(domain: str) -> list[Page]:
    """
    Minimal in-script crawler used when the main crawl_domain() returns 0 pages
    but robots.txt does not obviously block us.

    It:
      * Respects robots.txt (best-effort).
      * Tries a handful of common 'about/team/company/contact' URLs.
      * Tracks final URLs after redirects to avoid duplicate fetches.
      * Returns a list of Page objects for any 200 text/html responses.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return []

    rp = _load_robots_parser(dom)

    pages: list[Page] = []
    seen_request_urls: set[str] = set()  # URLs we've attempted to fetch
    seen_final_urls: set[str] = set()  # Final URLs after redirects (dedup key)

    headers = {"User-Agent": "email-scraper-demo/0.1"}
    timeout = httpx.Timeout(10.0, connect=5.0)

    with httpx.Client(follow_redirects=True, headers=headers, timeout=timeout) as client:
        for path in _MANUAL_HINT_PATHS:
            # Try both bare domain and www. variant
            for host in (dom, f"www.{dom}"):
                url = f"https://{host}{path}"

                # Skip if we've already tried this exact request URL
                if url in seen_request_urls:
                    continue
                seen_request_urls.add(url)

                # Check robots.txt before fetching
                if rp is not None:
                    try:
                        if not rp.can_fetch("*", url):
                            LOG.debug("Fallback crawl: robots.txt disallows %s; skipping", url)
                            continue
                    except Exception:
                        pass

                try:
                    resp = client.get(url)
                except Exception as exc:
                    LOG.debug("Fallback crawl: error fetching %s: %s", url, exc)
                    continue

                # Get the FINAL URL after any redirects
                final_url = str(resp.url)

                # Skip if we've already captured this final URL
                if final_url in seen_final_urls:
                    LOG.debug(
                        "Fallback crawl: skipping %s → already captured %s",
                        url,
                        final_url,
                    )
                    continue

                ctype = resp.headers.get("Content-Type", "")
                if resp.status_code != 200 or "text/html" not in ctype.lower():
                    LOG.debug(
                        "Fallback crawl: skipping %s (status=%s, content-type=%r)",
                        url,
                        resp.status_code,
                        ctype,
                    )
                    continue

                html_bytes = resp.content or b""
                if not html_bytes:
                    LOG.debug("Fallback crawl: %s returned empty body; skipping", url)
                    continue

                # Mark this final URL as seen BEFORE adding to pages
                seen_final_urls.add(final_url)

                LOG.info(
                    "Fallback crawl: captured HTML page %s (len=%s)",
                    final_url,
                    len(html_bytes),
                )
                pages.append(_build_page(final_url, html_bytes))

    LOG.info(
        "Fallback crawl complete: %d unique pages from %d URLs attempted",
        len(pages),
        len(seen_request_urls),
    )

    return pages


def _looks_like_title(text: str) -> bool:
    """
    Very loose heuristic for job titles.
    """
    t = text.strip()
    if not t:
        return False
    if "@" in t:
        return False
    # Common leadership words
    keywords = [
        "CEO",
        "CFO",
        "COO",
        "CTO",
        "Chief",
        "President",
        "VP",
        "Vice President",
        "Founder",
        "Co-founder",
        "Head",
        "Director",
        "Manager",
        "Lead",
        "Advisor",
    ]
    return any(k.lower() in t.lower() for k in keywords)


def _strip_html_to_lines(html: str) -> list[str]:
    """
    Crude HTML → text: strip tags, keep some structure as line breaks.
    Meant only for fallback heuristics, not pretty output.
    """
    # Line breaks around common block elements
    html = re.sub(r"(?i)</(h[1-6]|p|div|section|article|li|br)>", "\n", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    # Remove all remaining tags
    html = re.sub(r"<[^>]+>", " ", html)
    text = unescape(html)
    # Normalize whitespace
    text = re.sub(r"[ \t\r]+", " ", text)
    lines = [ln.strip() for ln in text.split("\n")]
    return [ln for ln in lines if ln]


def _split_name_simple(full: str) -> tuple[str | None, str | None]:
    """
    Simple first/last splitter for fallback team-page detection.
    """
    tokens = re.findall(r"[A-Za-z']+", full.strip())
    if len(tokens) < 2:
        return None, None
    return tokens[0], tokens[-1]


def _normalize_team_line_to_name(
    text: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Normalize a team-page line into (first_name, last_name, full_name), or
    (None, None, None) if it is clearly not a person.
    """
    # Collapse whitespace and strip.
    raw = re.sub(r"\s+", " ", text).strip()
    if not raw:
        return None, None, None

    # Kill obvious nav / section labels.
    if raw.lower() in TEAM_NAV_FULL_NAME_STOPWORDS:
        return None, None, None

    # Trim trailing punctuation.
    raw = raw.rstrip(" .:;,-")

    # Chop off credentials or extra text after comma / dash / pipe.
    for sep in [",", " - ", " – ", "|"]:
        if sep in raw:
            left, _right = raw.split(sep, 1)
            left = left.strip()
            if left:
                raw = left
            break

    if not raw:
        return None, None, None

    # If the line is ALL CAPS (common on headings), title-case it.
    if raw.isupper():
        raw = raw.title()

    # Extract word-like tokens.
    tokens = re.findall(r"[A-Za-z][A-Za-z'.-]*", raw)
    if len(tokens) < 2 or len(tokens) > 3:
        # Too short or too long to be a simple person heading.
        return None, None, None

    # Drop trailing credentials like "CPA", "MBA" if they slipped through
    # without a comma.
    if len(tokens) >= 3 and tokens[-1].lower() in TEAM_CREDENTIAL_SUFFIXES:
        tokens = tokens[:-1]
        if len(tokens) < 2:
            return None, None, None

    lowered = [t.lower() for t in tokens]

    # If *all* tokens are in the (global+extra) stopword set, it's a pure role/section.
    if all(t in GENERATION_NAME_STOPWORDS for t in lowered):
        return None, None, None

    first = tokens[0]
    last = tokens[-1]

    if not _should_generate_for_person(first, last):
        return None, None, None

    full_name = f"{first} {last}"
    return first, last, full_name


def _should_generate_for_person(first_name: str, last_name: str) -> bool:
    """
    Decide if we should treat this (first, last) pair as a real person candidate.

    This is used both for fallback team-page detection and for deciding whether
    to generate permutations for a person without emails.
    """
    first = (first_name or "").strip().lower()
    last = (last_name or "").strip().lower()

    # Super short name tokens are almost never real people.
    if len(first) <= 1 or len(last) <= 1:
        return False

    # Drop anything with digits in either token.
    if any(ch.isdigit() for ch in first) or any(ch.isdigit() for ch in last):
        return False

    # Tokens we consider for the stopword ratio.
    tokens = [t for t in (first, last) if t]
    if not tokens:
        return False

    # Count how many tokens are exact stopword matches.
    stop_count = sum(1 for t in tokens if t in GENERATION_NAME_STOPWORDS)

    # If >= half the tokens are stopwords, treat this as non-person.
    if stop_count * 2 >= len(tokens):
        return False

    return True


def _is_people_page_url(url: str) -> bool:
    """
    Heuristic: return True if the URL path suggests this is a people/leadership/team page.
    """
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        return False
    return any(hint in path for hint in PEOPLE_URL_HINTS)


def _extract_team_people_from_html(html: str) -> list[tuple[str, str, str]]:
    """
    Text-based fallback for team pages.

    Returns list of (first_name, last_name, full_name).
    """
    lines = _strip_html_to_lines(html)
    results: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    # Only consider the part of the page after an "Our Team" heading if present.
    start_idx = 0
    for i, raw in enumerate(lines):
        t = raw.strip()
        if re.search(r"\bour team\b", t, flags=re.IGNORECASE):
            start_idx = i + 1
            break

    for line in lines[start_idx:]:
        text = line.strip()
        if not text:
            continue
        if "@" in text:
            continue
        # Ignore any LinkedIn-related lines outright
        if "linkedin" in text.lower():
            continue
        # Hard length limit to avoid paragraphs.
        if len(text) > 80:
            continue

        # Normalize name line (strip credentials / trailing commas).
        cleaned = _normalize_name_line(text)
        if not cleaned:
            continue

        # If the cleaned line looks like a title, skip it.
        if _looks_like_title(cleaned):
            continue

        # Quick "name-like" pattern (2–4 capitalized tokens).
        if not _NAME_RE.match(cleaned):
            continue

        first_name, last_name = _split_name_simple(cleaned)
        if not first_name or not last_name:
            continue

        if not _should_generate_for_person(first_name, last_name):
            continue

        key = (first_name.strip().lower(), last_name.strip().lower())
        if key in seen:
            continue
        seen.add(key)

        full_name = f"{first_name} {last_name}"
        results.append((first_name, last_name, full_name))

    return results


def _html_to_text(html: bytes | str) -> str:
    if isinstance(html, bytes):
        return html.decode("utf-8", "ignore")
    return str(html)


def _leadership_fallback_candidates_from_html(source_url: str, html_str: str) -> list[Candidate]:
    if "Our Leadership Team" not in html_str:
        return []

    lines = _strip_html_to_lines(html_str)
    try:
        start_idx = next(i for i, line in enumerate(lines) if "Our Leadership Team" in line)
    except StopIteration:
        return []

    stop_markers = [
        "Our Board of Directors",
        "Our Investors",
        "Our Journey",
        "Our Story",
    ]
    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        if any(marker in lines[i] for marker in stop_markers):
            end_idx = i
            break

    section = lines[start_idx + 1 : end_idx]
    out: list[Candidate] = []

    i = 0
    while i < len(section) - 1:
        name_line = section[i].strip()
        title_line = section[i + 1].strip()

        if not (_NAME_RE.match(name_line) and _looks_like_title(title_line)):
            i += 1
            continue

        full_name = name_line.strip()
        first_name, last_name = _split_name_simple(full_name)

        if not first_name or not last_name:
            i += 2
            continue

        if not _should_generate_for_person(first_name, last_name):
            i += 2
            continue

        out.append(
            Candidate(
                email=None,
                source_url=source_url,
                first_name=first_name,
                last_name=last_name,
                raw_name=full_name,
                title=title_line or None,
                source_type="leadership_fallback",
                context_snippet=full_name,
                is_role_address_guess=False,
            )
        )
        i += 2

    return out


def _team_fallback_candidates_from_html(source_url: str, html_str: str) -> list[Candidate]:
    if not _is_people_page_url(source_url):
        return []

    people = _extract_team_people_from_html(html_str)
    if not people:
        return []

    LOG.info(
        "Team-page fallback: found %s potential people on %s (for AI candidates)",
        len(people),
        source_url,
    )

    return [
        Candidate(
            email=None,
            source_url=source_url,
            first_name=first_name,
            last_name=last_name,
            raw_name=full_name,
            title=None,
            source_type="team_fallback",
            context_snippet=full_name,
            is_role_address_guess=False,
        )
        for first_name, last_name, full_name in people
    ]


def _dedup_fallback_candidates(candidates: list[Candidate]) -> list[Candidate]:
    out: list[Candidate] = []
    seen: set[tuple[str, str, str]] = set()  # (full_name_lc, url_lc, source_type)

    for cand in candidates:
        full_name = (cand.raw_name or "").strip().lower()
        url = (cand.source_url or "").strip().lower()
        source_type = (getattr(cand, "source_type", "") or "").strip().lower()
        key = (full_name, url, source_type)
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)

    return out


def _build_fallback_candidates_from_team_and_leadership(
    conn: sqlite3.Connection,
    company_id: int,
    official_domain: str | None,
) -> list[Candidate]:
    """
    Build Candidate objects for people inferred from leadership/team pages so
    they can flow through the AI refiner like email-anchored candidates.

    We do NOT write anything to the DB here; we only return a list of
    Candidate(email=None, source_type='team_fallback'/'leadership_fallback', ...).
    """
    collected: list[Candidate] = []

    for source_url, html in _iter_sources_for_company(conn, company_id, official_domain):
        if not html:
            continue
        html_str = _html_to_text(html)

        collected.extend(_leadership_fallback_candidates_from_html(source_url, html_str))
        collected.extend(_team_fallback_candidates_from_html(source_url, html_str))

    return _dedup_fallback_candidates(collected)


# ---------------------------------------------------------------------------
# Extraction + persistence
# ---------------------------------------------------------------------------


@dataclass
class PersistedCandidate:
    email: str
    person_id: int | None
    company_id: int
    source_url: str


def extract_candidates_for_company(
    conn: sqlite3.Connection,
    company_id: int,
    official_domain: str | None,
    *,
    company_name: str,
    crawl_domain: str,
) -> list[Candidate]:
    """
    Run the R11/O05 extractor across all sources rows for this company.

    If the optional AI extractor (O27) is available and supports the
    new company-level API, it will refine the combined candidate set.

    IMPORTANT: this now unifies:
      - email-anchored candidates from R11, and
      - name-only team/leadership fallback candidates

    into a single candidate list that flows through AI.
    """
    heuristic_candidates: list[Candidate] = []

    for source_url, html in _iter_sources_for_company(conn, company_id, official_domain):
        if not html:
            continue
        html_str = _html_to_text(html)

        base_cands = extract_candidates(
            html_str,
            source_url=source_url,
            official_domain=official_domain,
        )
        heuristic_candidates.extend(base_cands)

    # NEW: team/leadership fallback candidates (name-only, email=None)
    fallback_candidates = _build_fallback_candidates_from_team_and_leadership(
        conn,
        company_id,
        official_domain,
    )

    all_candidates: list[Candidate] = heuristic_candidates + fallback_candidates

    LOG.info(
        "Heuristic extractor produced %s email-anchored candidates and %s fallback "
        "candidates for company_id=%s (total=%s)",
        len(heuristic_candidates),
        len(fallback_candidates),
        company_id,
        len(all_candidates),
    )

    if not all_candidates:
        return all_candidates

    # Optional: AI refinement at the *company* level, if available and compatible.
    refined_candidates: list[Candidate] = all_candidates
    if _HAS_AI_EXTRACTOR and _ai_extract_candidates is not None:
        try:
            sig = inspect.signature(_ai_extract_candidates)
            params = sig.parameters
            if "raw_candidates" in params:
                ai_result = _ai_extract_candidates(
                    company_name=company_name,
                    domain=crawl_domain,
                    raw_candidates=all_candidates,
                )
                if ai_result is not None:
                    refined_candidates = list(ai_result)
                LOG.info(
                    "AI extractor refined %s → %s candidates for company_id=%s",
                    len(all_candidates),
                    len(refined_candidates),
                    company_id,
                )
            else:
                LOG.info(
                    "AI extractor is present but does not expose a 'raw_candidates' "
                    "parameter; skipping AI refinement in demo_autodiscovery.",
                )
        except Exception as exc:  # pragma: no cover - defensive
            LOG.warning(
                "AI people extractor failed for company_id=%s domain=%s: %s",
                company_id,
                crawl_domain,
                exc,
            )

    LOG.info(
        "Extracted %s candidates for company_id=%s (after AI refinement)",
        len(refined_candidates),
        company_id,
    )
    return refined_candidates


def ensure_leadership_people_for_company(
    conn: sqlite3.Connection,
    company_id: int,
    official_domain: str | None,
) -> int:
    """
    Best-effort fallback (legacy):

    Look through this company's sources for an 'Our Leadership Team' section
    and insert people rows (no emails) for any (name, title) pairs we can
    confidently identify.

    Returns the number of people inserted.

    NOTE: the unified AI path now also builds Candidate objects from leadership
    sections. This function remains as a conservative extra seeding mechanism
    and will typically skip people that already exist.
    """
    # Build a set of existing names so we do not duplicate work on reruns.
    existing_keys: set[str] = set()
    cur = conn.execute(
        """
        SELECT
          lower(coalesce(full_name, '')) AS full_name,
          lower(coalesce(first_name, '')) AS first_name,
          lower(coalesce(last_name, '')) AS last_name
        FROM people
        WHERE company_id = ?
        """,
        (company_id,),
    )
    for row in cur.fetchall():
        key = "|".join(
            [
                row["full_name"] or "",
                row["first_name"] or "",
                row["last_name"] or "",
            ]
        )
        existing_keys.add(key)

    inserted = 0

    for source_url, html in _iter_sources_for_company(conn, company_id, official_domain):
        if not html:
            continue

        html_str = _html_to_text(html)

        if "Our Leadership Team" not in html_str:
            continue

        lines = _strip_html_to_lines(html_str)

        # Find the line index with the heading
        try:
            start_idx = next(i for i, line in enumerate(lines) if "Our Leadership Team" in line)
        except StopIteration:
            continue

        stop_markers = [
            "Our Board of Directors",
            "Our Investors",
            "Our Journey",
            "Our Story",
        ]
        end_idx = len(lines)
        for i in range(start_idx + 1, len(lines)):
            if any(marker in lines[i] for marker in stop_markers):
                end_idx = i
                break

        section = lines[start_idx + 1 : end_idx]

        i = 0
        while i < len(section) - 1:
            name_line = section[i]
            title_line = section[i + 1]

            if _NAME_RE.match(name_line) and _looks_like_title(title_line):
                full_name = name_line.strip()
                first_name, last_name = _split_name_simple(full_name)

                key = "|".join(
                    [
                        (full_name or "").strip().lower(),
                        (first_name or "").strip().lower() if first_name else "",
                        (last_name or "").strip().lower() if last_name else "",
                    ]
                )
                if not full_name or key in existing_keys:
                    i += 2
                    continue

                insert_row(
                    conn,
                    "people",
                    {
                        "company_id": company_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": full_name,
                        "title": title_line.strip(),
                        "source_url": source_url,
                    },
                    return_id=True,
                )
                existing_keys.add(key)
                inserted += 1
                i += 2
            else:
                i += 1

    if inserted:
        conn.commit()

    LOG.info(
        "Leadership fallback inserted %s people for company_id=%s",
        inserted,
        company_id,
    )
    return inserted


def ensure_team_people_for_company(
    conn: sqlite3.Connection,
    company_id: int,
    official_domain: str | None,
) -> int:
    """
    Additional fallback (legacy):

    Look through this company's sources for obvious team/our-team/people pages
    and insert people rows (no emails) based on text lines that look like
    person names (2–4 capitalized tokens) and are not obviously marketing/tagline
    phrases.

    Returns the number of people inserted.

    NOTE: the unified AI path now also builds Candidate objects from these
    pages. This function remains as a conservative extra seeding mechanism and
    will typically only add people that AI skipped or that the candidate path
    failed to capture.
    """
    existing_keys: set[str] = set()
    cur = conn.execute(
        """
        SELECT
          lower(coalesce(full_name, '')) AS full_name,
          lower(coalesce(first_name, '')) AS first_name,
          lower(coalesce(last_name, '')) AS last_name
        FROM people
        WHERE company_id = ?
        """,
        (company_id,),
    )
    for row in cur.fetchall():
        key = "|".join(
            [
                row["full_name"] or "",
                row["first_name"] or "",
                row["last_name"] or "",
            ]
        )
        existing_keys.add(key)

    inserted = 0

    for source_url, html in _iter_sources_for_company(conn, company_id, official_domain):
        if not html:
            continue

        if not _is_people_page_url(source_url):
            continue

        html_str = _html_to_text(html)

        people = _extract_team_people_from_html(html_str)
        if not people:
            continue

        LOG.info(
            "Team-page fallback (legacy): found %s potential people on %s",
            len(people),
            source_url,
        )

        for first_name, last_name, full_name in people:
            key = "|".join(
                [
                    (full_name or "").strip().lower(),
                    (first_name or "").strip().lower(),
                    (last_name or "").strip().lower(),
                ]
            )
            if not full_name or key in existing_keys:
                continue

            insert_row(
                conn,
                "people",
                {
                    "company_id": company_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "source_url": source_url,
                },
                return_id=True,
            )
            existing_keys.add(key)
            inserted += 1

    if inserted:
        conn.commit()

    LOG.info(
        "Team-page fallback inserted %s people for company_id=%s",
        inserted,
        company_id,
    )
    return inserted


def _load_person_lookup(
    conn: sqlite3.Connection, company_id: int
) -> dict[tuple[str, str, str], int]:
    person_lookup: dict[tuple[str, str, str], int] = {}
    cur = conn.execute(
        """
        SELECT id, first_name, last_name, full_name
        FROM people
        WHERE company_id = ?
        """,
        (company_id,),
    )
    for row in cur.fetchall():
        key = (
            (row["first_name"] or "").strip().lower(),
            (row["last_name"] or "").strip().lower(),
            (row["full_name"] or "").strip().lower(),
        )
        person_lookup[key] = int(row["id"])
    return person_lookup


def _candidate_has_name(cand: Candidate) -> bool:
    if cand.first_name and cand.last_name:
        return True
    if cand.raw_name and len(cand.raw_name.split()) >= 2:
        return True
    return False


def _is_role_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    localpart = email.split("@", 1)[0].lower()
    if localpart in ROLE_ALIASES:
        return True
    return is_role_or_placeholder_email(email)


def _build_full_name(cand: Candidate) -> str:
    return cand.raw_name or " ".join([p for p in [cand.first_name, cand.last_name] if p]).strip()


def _get_or_create_person(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    person_lookup: dict[tuple[str, str, str], int],
    cand: Candidate,
    full_name: str,
) -> int | None:
    fn_local = (cand.first_name or "").strip()
    ln_local = (cand.last_name or "").strip()
    key = (fn_local.lower(), ln_local.lower(), full_name.strip().lower())
    existing = person_lookup.get(key)
    if existing is not None:
        return existing

    person_id = insert_row(
        conn,
        "people",
        {
            "company_id": company_id,
            "first_name": cand.first_name,
            "last_name": cand.last_name,
            "full_name": full_name,
            "title": getattr(cand, "title", None),
            "source_url": cand.source_url,
        },
        return_id=True,
    )
    if person_id is not None:
        person_lookup[key] = person_id
    return person_id


def _dedup_candidates(
    candidates: list[Candidate],
) -> tuple[dict[str, Candidate], dict[tuple[str, str, str], Candidate]]:
    by_email: dict[str, Candidate] = {}
    name_only: dict[tuple[str, str, str], Candidate] = {}

    for cand in candidates:
        email_lc = (cand.email or "").strip().lower()
        fn = (cand.first_name or "").strip().lower()
        ln = (cand.last_name or "").strip().lower()
        rn = (cand.raw_name or "").strip().lower()

        if email_lc:
            by_email.setdefault(email_lc, cand)
        else:
            name_only.setdefault((fn, ln, rn), cand)

    return by_email, name_only


def _persist_existing_email_row(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    email_lc: str,
    cand: Candidate,
    existing_email_id: int,
    existing_person_id: int | None,
    is_role: bool,
    person_lookup: dict[tuple[str, str, str], int],
) -> PersistedCandidate:
    person_id_for_return = existing_person_id

    if existing_person_id is None and _candidate_has_name(cand):
        full_name = _build_full_name(cand)
        if full_name:
            new_person_id = _get_or_create_person(
                conn,
                company_id=company_id,
                person_lookup=person_lookup,
                cand=cand,
                full_name=full_name,
            )
            if new_person_id is not None:
                person_id_for_return = new_person_id

                if not is_role:
                    conn.execute(
                        "UPDATE emails SET person_id = ? WHERE id = ?",
                        (new_person_id, existing_email_id),
                    )
                    LOG.debug(
                        "Linked existing email %s to person_id=%s (%s)",
                        email_lc,
                        new_person_id,
                        full_name,
                    )
                else:
                    LOG.debug(
                        "Created person_id=%s (%s) for role email %s but NOT linking",
                        new_person_id,
                        full_name,
                        email_lc,
                    )

    return PersistedCandidate(
        email=email_lc,
        person_id=person_id_for_return,
        company_id=company_id,
        source_url=cand.source_url,
    )


def _persist_new_email_row(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    email_lc: str,
    cand: Candidate,
    is_role: bool,
    person_lookup: dict[tuple[str, str, str], int],
) -> PersistedCandidate:
    full_name = _build_full_name(cand)
    person_id_for_person: int | None = None
    if full_name:
        person_id_for_person = _get_or_create_person(
            conn,
            company_id=company_id,
            person_lookup=person_lookup,
            cand=cand,
            full_name=full_name,
        )

    person_id_for_email: int | None = None
    if person_id_for_person is not None and not is_role:
        person_id_for_email = person_id_for_person

    email_row = {
        "person_id": person_id_for_email,
        "company_id": company_id,
        "email": email_lc,
        "is_published": 1,
        "source_url": cand.source_url,
    }
    insert_row(conn, "emails", email_row, return_id=False)

    if is_role and person_id_for_person is not None:
        LOG.debug(
            "Created role email %s at company-level; person %s created separately",
            email_lc,
            full_name,
        )

    return PersistedCandidate(
        email=email_lc,
        person_id=person_id_for_person,
        company_id=company_id,
        source_url=cand.source_url,
    )


def _persist_email_candidates(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    by_email: dict[str, Candidate],
    person_lookup: dict[tuple[str, str, str], int],
) -> list[PersistedCandidate]:
    persisted: list[PersistedCandidate] = []

    for email_lc, cand in sorted(by_email.items(), key=lambda kv: kv[0]):
        is_role = _is_role_email(email_lc)

        row = conn.execute(
            "SELECT id, person_id FROM emails WHERE lower(email) = ?",
            (email_lc,),
        ).fetchone()

        if row:
            persisted.append(
                _persist_existing_email_row(
                    conn,
                    company_id=company_id,
                    email_lc=email_lc,
                    cand=cand,
                    existing_email_id=int(row["id"]),
                    existing_person_id=row["person_id"],
                    is_role=is_role,
                    person_lookup=person_lookup,
                )
            )
            continue

        persisted.append(
            _persist_new_email_row(
                conn,
                company_id=company_id,
                email_lc=email_lc,
                cand=cand,
                is_role=is_role,
                person_lookup=person_lookup,
            )
        )

    return persisted


def _persist_name_only_candidates(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    name_only: dict[tuple[str, str, str], Candidate],
    person_lookup: dict[tuple[str, str, str], int],
) -> list[PersistedCandidate]:
    persisted: list[PersistedCandidate] = []

    for (_fn, _ln, _rn), cand in sorted(name_only.items(), key=lambda kv: kv[0]):
        full_name = _build_full_name(cand)
        if not full_name:
            continue

        person_id = _get_or_create_person(
            conn,
            company_id=company_id,
            person_lookup=person_lookup,
            cand=cand,
            full_name=full_name,
        )
        if person_id is None:
            continue

        persisted.append(
            PersistedCandidate(
                email="",
                person_id=person_id,
                company_id=company_id,
                source_url=cand.source_url,
            )
        )

    return persisted


def persist_candidates(
    conn: sqlite3.Connection,
    company_id: int,
    candidates: list[Candidate],
) -> list[PersistedCandidate]:
    """
    Persist people + emails for each candidate.

    Returns a list of PersistedCandidate records that tie back to the *person*
    row (person_id), not just the email row. This allows downstream code to
    know exactly which people came from the AI-approved candidate set.

    BEHAVIOR:
    - Role emails (office@, info@, etc.) get person_id=NULL on the email row,
      but we still CREATE a person if the AI provided name info, and we still
      return that person_id in PersistedCandidate for generation gating.
    - Personal emails get person_id linked to the person row.
    - When an existing email has no person but candidate has name info,
      we create a person but only update the email link if it's not a role email.
    """
    person_lookup = _load_person_lookup(conn, company_id)
    by_email, name_only = _dedup_candidates(candidates)

    persisted: list[PersistedCandidate] = []
    persisted.extend(
        _persist_email_candidates(
            conn,
            company_id=company_id,
            by_email=by_email,
            person_lookup=person_lookup,
        )
    )
    persisted.extend(
        _persist_name_only_candidates(
            conn,
            company_id=company_id,
            name_only=name_only,
            person_lookup=person_lookup,
        )
    )

    conn.commit()

    with_person = sum(1 for p in persisted if p.person_id is not None)
    without_person = len(persisted) - with_person
    role_emails = sum(1 for p in persisted if p.email and _is_role_email(p.email))

    LOG.info(
        "Persisted %s candidates for company_id=%s (%d with person_id, %d without, %d role emails)",
        len(persisted),
        company_id,
        with_person,
        without_person,
        role_emails,
    )

    return persisted


# ---------------------------------------------------------------------------
# Generation + verification (synchronous)
# ---------------------------------------------------------------------------


def _examples_for_domain(conn: sqlite3.Connection, domain: str) -> list[tuple[str, str, str]]:
    """
    Build [(first, last, localpart)] examples for a domain using 'published' emails.
    """
    examples: list[tuple[str, str, str]] = []
    dom = (domain or "").strip().lower()
    if not dom:
        return examples

    try:
        rows = conn.execute(
            """
            SELECT p.first_name, p.last_name, e.email
            FROM emails e
            JOIN people p ON p.id = e.person_id
            WHERE lower(substr(e.email, instr(e.email, '@') + 1)) = ?
              AND e.is_published = 1
            """,
            (dom,),
        ).fetchall()
        for fn, ln, em in rows:
            if not em or "@" not in em or not fn or not ln:
                continue
            local = em.split("@", 1)[0].lower()
            examples.append((str(fn), str(ln), local))
    except Exception:
        pass

    return examples


def _load_cached_pattern(conn: sqlite3.Connection, domain: str) -> str | None:
    """
    Read a cached canonical pattern key for a domain from domain_patterns
    when that table exists.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return None
    try:
        if not conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='domain_patterns'"
        ).fetchone():
            return None
        row = conn.execute(
            "SELECT pattern FROM domain_patterns WHERE domain = ?",
            (dom,),
        ).fetchone()
        if not row:
            return None
        pat = row[0]
        if pat in CANON_PATTERNS:
            return pat
    except Exception:
        return None
    return None


def _save_inferred_pattern(
    conn: sqlite3.Connection,
    domain: str,
    pattern: str,
    confidence: float,
    samples: int,
) -> None:
    dom = (domain or "").strip().lower()
    if not dom:
        return
    try:
        if not conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='domain_patterns'"
        ).fetchone():
            return
        conn.execute(
            """
            INSERT INTO domain_patterns (domain, pattern, confidence, samples)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
              pattern=excluded.pattern,
              confidence=excluded.confidence,
              samples=excluded.samples,
              inferred_at=datetime('now')
            """,
            (dom, pattern, float(confidence), int(samples)),
        )
        conn.commit()
    except Exception:
        LOG.exception(
            "Failed to upsert domain_patterns",
            extra={"domain": dom, "pattern": pattern},
        )


def _load_people_with_emails(
    conn: sqlite3.Connection, company_id: int
) -> dict[int, dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT p.id AS person_id, p.first_name, p.last_name, e.email
        FROM people p
        LEFT JOIN emails e ON e.person_id = p.id
        WHERE p.company_id = ?
        ORDER BY p.id
        """,
        (company_id,),
    )
    rows = cur.fetchall()

    by_person: dict[int, dict[str, Any]] = {}
    for row in rows:
        pid = int(row["person_id"])
        person = by_person.setdefault(
            pid,
            {
                "id": pid,
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "emails": [],
            },
        )
        em = row["email"]
        if em:
            person["emails"].append(str(em))
    return by_person


def _select_missing_people(
    by_person: dict[int, dict[str, Any]],
    *,
    allowed_person_ids: set[int] | None,
) -> list[dict[str, Any]]:
    missing_people: list[dict[str, Any]] = []

    for person in by_person.values():
        pid = int(person["id"])

        if allowed_person_ids is not None and pid not in allowed_person_ids:
            continue

        emails = person["emails"]
        has_non_placeholder = any(not is_role_or_placeholder_email(e) for e in emails)
        if has_non_placeholder:
            continue

        first_raw = (person["first_name"] or "").strip()
        last_raw = (person["last_name"] or "").strip()
        if not _should_generate_for_person(first_raw, last_raw):
            continue

        missing_people.append(person)

    return missing_people


def _infer_patterns_for_generation(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    domain: str,
) -> tuple[str | None, str | None, str | None]:
    dom = (domain or "").strip().lower()
    if not dom:
        return None, None, None

    try:
        company_pattern = get_company_email_pattern(conn, company_id)
    except Exception:
        company_pattern = None

    cached_pattern = _load_cached_pattern(conn, dom)
    examples = _examples_for_domain(conn, dom)

    domain_pattern: str | None = cached_pattern
    if not domain_pattern:
        inf = infer_domain_pattern(examples)
        domain_pattern = inf.pattern
        if domain_pattern:
            _save_inferred_pattern(
                conn, dom, domain_pattern, float(inf.confidence), int(inf.samples)
            )

    preferred_pattern = company_pattern or domain_pattern
    return company_pattern, domain_pattern, preferred_pattern


def _legacy_emails_for_person(nf: str, nl: str, dom: str) -> list[str]:
    from src.generate.permutations import PATTERNS as LEGACY_TEMPLATES  # local import
    from src.generate.permutations import normalize_name_parts as _legacy_norm

    first_n, last_n, f_initial, l_initial = _legacy_norm(nf, nl)
    ctx = {"first": first_n, "last": last_n, "f": f_initial, "l": l_initial}

    out: list[str] = []
    for pattern in LEGACY_TEMPLATES:
        try:
            local = pattern.format(**ctx)
        except Exception:
            continue
        if not local:
            continue
        out.append(f"{local}@{dom}")
    return out


def _ordered_email_candidates_for_person(
    *,
    first_name: str,
    last_name: str,
    domain: str,
    preferred_pattern: str | None,
) -> list[str]:
    canonical_emails: list[str] = generate_candidate_emails_for_person(
        first_name=first_name,
        last_name=last_name,
        domain=domain,
        company_pattern=preferred_pattern,
    )
    legacy_emails = _legacy_emails_for_person(first_name, last_name, domain)

    seen: set[str] = set()
    ordered: list[str] = []
    for email in canonical_emails + legacy_emails:
        local = email.split("@", 1)[0]
        if not local:
            continue
        if local in ROLE_ALIASES:
            continue
        if email in seen:
            continue
        seen.add(email)
        ordered.append(email)

    return ordered


def _generate_for_person(
    conn: sqlite3.Connection,
    *,
    person_id: int,
    first_raw: str,
    last_raw: str,
    domain: str,
    preferred_pattern: str | None,
) -> int:
    nf, nl = normalize_split_parts(first_raw, last_raw)
    if not (nf or nl):
        return 0

    ordered_candidates = _ordered_email_candidates_for_person(
        first_name=nf,
        last_name=nl,
        domain=domain,
        preferred_pattern=preferred_pattern,
    )

    generated = 0
    for email in ordered_candidates:
        upsert_generated_email(conn, person_id, email, domain, source_note="demo_r12")
        generated += 1
    return generated


def generate_for_missing_people(
    conn: sqlite3.Connection,
    company_id: int,
    domain: str,
    *,
    allowed_person_ids: set[int] | None = None,
) -> int:
    """
    For people at this company that have no *personal* emails, generate permutations.

    If allowed_person_ids is provided, we only generate for people whose id is in
    that set. This lets us restrict generation to people that came through the
    AI-approved candidate path.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return 0

    by_person = _load_people_with_emails(conn, company_id)
    if not by_person:
        LOG.info("No people found for company_id=%s", company_id)
        return 0

    missing_people = _select_missing_people(by_person, allowed_person_ids=allowed_person_ids)
    if not missing_people:
        LOG.info(
            "No people without non-placeholder emails for company_id=%s",
            company_id,
        )
        return 0

    if len(missing_people) > MAX_PEOPLE_PER_COMPANY:
        LOG.info(
            "Found %s people without non-placeholder emails for company_id=%s; limiting to first %s",
            len(missing_people),
            company_id,
            MAX_PEOPLE_PER_COMPANY,
        )
        missing_people = missing_people[:MAX_PEOPLE_PER_COMPANY]

    _company_pattern, domain_pattern, preferred_pattern = _infer_patterns_for_generation(
        conn,
        company_id=company_id,
        domain=dom,
    )

    total_generated = 0
    for person in missing_people:
        pid = int(person["id"])
        first_raw = (person["first_name"] or "").strip()
        last_raw = (person["last_name"] or "").strip()

        total_generated += _generate_for_person(
            conn,
            person_id=pid,
            first_raw=first_raw,
            last_raw=last_raw,
            domain=dom,
            preferred_pattern=preferred_pattern,
        )

    conn.commit()
    LOG.info(
        "Generated %s candidate emails for %s people (company_id=%s, domain=%s, preferred_pattern=%s, domain_pattern=%s)",
        total_generated,
        len(missing_people),
        company_id,
        dom,
        preferred_pattern,
        domain_pattern,
    )
    return total_generated


def verify_all_for_company(
    db_path: Path,
    company_id: int,
    domain: str,
    *,
    per_person_budget_seconds: float = 60.0,
    max_probes_per_person: int = 6,
    stop_on_unknown: bool = True,
) -> None:
    """
    Synchronously run task_probe_email for all emails belonging to this company.

    Parameters
    ----------
    db_path : Path
        Path to the SQLite database file.
    company_id : int
        The company ID to verify emails for.
    domain : str
        The company's domain (used for logging/context).
    per_person_budget_seconds : float
        Maximum wall-clock time to spend verifying emails for a single person.
        Default: 60 seconds.
    max_probes_per_person : int
        Hard cap on the number of email probes per person, regardless of time.
        Default: 6 probes.
    stop_on_unknown : bool
        If True, stop probing a person after receiving an unknown/timeout result
        (since subsequent probes are likely to also timeout). Default: True.
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # type: ignore[attr-defined]

    # Pull all emails for this company, including person_id so we can group.
    cur = conn.execute(
        """
        SELECT id, email, person_id
        FROM emails
        WHERE company_id = ?
        ORDER BY person_id, id
        """,
        (company_id,),
    )
    rows = cur.fetchall()

    # Optionally skip specific emails for demo scenarios.
    if DEMO_SKIP_EMAILS:
        filtered_rows: list[sqlite3.Row] = []
        for r in rows:
            em = (r["email"] or "").lower()
            if em in DEMO_SKIP_EMAILS:
                LOG.info(
                    "Skipping email_id=%s email=%s due to DEMO_SKIP_EMAILS",
                    r["id"],
                    em,
                )
                continue
            filtered_rows.append(r)
        rows = filtered_rows

    if not rows:
        LOG.info("No emails to verify for company_id=%s (after filtering)", company_id)
        conn.close()
        return

    # Ensure DATABASE_PATH env is set for task_probe_email internals.
    os.environ.setdefault("DATABASE_PATH", str(db_path))

    # Resolve underlying function (RQ @job wrapper vs plain function).
    probe_func = getattr(_task_probe_email, "__wrapped__", _task_probe_email)

    # Group emails by person_id; use None for rows without a person.
    emails_by_person: dict[int | None, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        emails_by_person[row["person_id"]].append(row)

    total_probes = 0
    total_skipped_budget = 0
    total_skipped_cap = 0
    total_skipped_unknown = 0

    for person_id, email_rows in emails_by_person.items():
        LOG.info(
            "Verifying up to %d emails for person_id=%s (budget=%.1fs, cap=%d)",
            len(email_rows),
            person_id,
            per_person_budget_seconds,
            max_probes_per_person,
        )

        person_start_time = time.time()
        probes_for_person = 0
        stop_reason: str | None = None

        for row in email_rows:
            # Check time budget
            elapsed = time.time() - person_start_time
            if elapsed >= per_person_budget_seconds:
                remaining = len(email_rows) - probes_for_person
                LOG.info(
                    "Time budget exhausted for person_id=%s after %.1fs; "
                    "skipping %d remaining emails",
                    person_id,
                    elapsed,
                    remaining,
                )
                total_skipped_budget += remaining
                stop_reason = "time_budget"
                break

            # Check probe cap
            if probes_for_person >= max_probes_per_person:
                remaining = len(email_rows) - probes_for_person
                LOG.info(
                    "Probe cap reached for person_id=%s (%d probes); skipping %d remaining emails",
                    person_id,
                    max_probes_per_person,
                    remaining,
                )
                total_skipped_cap += remaining
                stop_reason = "probe_cap"
                break

            eid = int(row["id"])
            email = row["email"]
            edom = email.split("@", 1)[1].lower() if ("@" in email) else dom

            LOG.info(
                "Verifying email_id=%s email=%s (probe %d/%d for person_id=%s)",
                eid,
                email,
                probes_for_person + 1,
                max_probes_per_person,
                person_id,
            )

            try:
                _ = probe_func(eid, email, edom, force=False)  # type: ignore[misc]
            except Exception as exc:
                LOG.warning(
                    "Probe failed for email_id=%s: %s",
                    eid,
                    exc,
                )

            probes_for_person += 1
            total_probes += 1

            # Look at the latest verify_status for this email_id.
            cur_vs = conn.execute(
                """
                SELECT verify_status
                FROM verification_results
                WHERE email_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (eid,),
            )
            row_vs = cur_vs.fetchone()
            vs = row_vs["verify_status"] if row_vs else None

            # If we get a definitive positive signal, stop probing this person.
            if vs in ("valid", "risky_catch_all"):
                LOG.info(
                    "Got verify_status=%r for email_id=%s; "
                    "stopping further probes for person_id=%s (success)",
                    vs,
                    eid,
                    person_id,
                )
                stop_reason = "success"
                break

            # If we get unknown/timeout and stop_on_unknown is True, bail early.
            if stop_on_unknown and vs in ("unknown_timeout", "unknown"):
                remaining = len(email_rows) - probes_for_person
                LOG.info(
                    "Got verify_status=%r for email_id=%s; "
                    "stopping further probes for person_id=%s to avoid more timeouts "
                    "(%d emails skipped)",
                    vs,
                    eid,
                    person_id,
                    remaining,
                )
                total_skipped_unknown += remaining
                stop_reason = "unknown_timeout"
                break

        # Log summary for this person
        elapsed = time.time() - person_start_time
        LOG.debug(
            "Finished person_id=%s: %d probes in %.1fs, stop_reason=%s",
            person_id,
            probes_for_person,
            elapsed,
            stop_reason or "exhausted",
        )

    conn.close()

    LOG.info(
        "Verification complete for company_id=%s: %d probes executed, "
        "%d skipped (budget=%d, cap=%d, unknown=%d)",
        company_id,
        total_probes,
        total_skipped_budget + total_skipped_cap + total_skipped_unknown,
        total_skipped_budget,
        total_skipped_cap,
        total_skipped_unknown,
    )


# ---------------------------------------------------------------------------
# ICP scoring + search summary (best-effort)
# ---------------------------------------------------------------------------


def maybe_run_icp_backfill(db_path: Path) -> None:
    """
    Best-effort call to scripts/backfill_r14_icp.py if present.
    """
    script = ROOT_DIR / "scripts" / "backfill_r14_icp.py"
    if not script.exists():
        LOG.warning("ICP backfill script not found at %s; skipping icp scoring", script)
        return

    cmd = [sys.executable, str(script), "--db", str(db_path)]
    LOG.info("Running ICP backfill: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except Exception as exc:  # pragma: no cover - defensive
        LOG.warning("ICP backfill failed: %s", exc)


def _fetch_v_emails_latest_rows(conn: sqlite3.Connection, dom: str) -> list[dict[str, Any]]:
    try:
        cur = conn.execute(
            """
            SELECT
              email,
              first_name,
              last_name,
              title,
              company_name,
              company_domain,
              verify_status,
              icp_score,
              source_url
            FROM v_emails_latest
            WHERE lower(company_domain) = ?
            ORDER BY icp_score DESC, email
            """,
            (dom,),
        )
        return [dict(row) for row in cur.fetchall()]
    except sqlite3.OperationalError as exc:
        if "icp_score" not in str(exc).lower():
            raise
        cur = conn.execute(
            """
            SELECT
              email,
              first_name,
              last_name,
              title,
              company_name,
              company_domain,
              verify_status,
              source_url
            FROM v_emails_latest
            WHERE lower(company_domain) = ?
            ORDER BY email
            """,
            (dom,),
        )
        return [dict(row) for row in cur.fetchall()]


def _print_v_emails_latest_snapshot(dom: str, emails: list[dict[str, Any]]) -> None:
    print("\n=== v_emails_latest snapshot for domain:", dom, "===")

    if not emails:
        print("No leads found in v_emails_latest for domain", dom)
        return

    header = [
        "email",
        "name",
        "title",
        "verify_status",
        "icp_score",
        "source_url",
    ]
    print("\t".join(header))
    for row in emails:
        name = " ".join(p for p in [row.get("first_name"), row.get("last_name")] if p).strip()
        icp_val = row.get("icp_score")
        icp_str = "" if icp_val is None else str(icp_val)
        line = [
            row.get("email", ""),
            name,
            row.get("title", "") or "",
            row.get("verify_status", "") or "",
            icp_str,
            row.get("source_url", "") or "",
        ]
        print("\t".join(line))


def _has_people_fts(conn: sqlite3.Connection) -> bool:
    return bool(
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='people_fts'"
        ).fetchone()
    )


def _search_backend_sanity_check(
    conn: sqlite3.Connection,
    *,
    dom: str,
    search_hint: str | None,
) -> None:
    if not _has_people_fts(conn):
        print("\n(people_fts not present; skipping search backend check)")
        return

    print("\n=== Search backend sanity check ===")

    raw_hint = (search_hint or dom or "").strip()
    if not raw_hint:
        print("(no search hint available; skipping search backend check)")
        return
    search_token = raw_hint.split()[0]

    sig = inspect.signature(LeadSearchParams)
    params = sig.parameters
    kwargs: dict[str, Any] = {}

    if "q" in params:
        kwargs["q"] = search_token
    elif "query" in params:
        kwargs["query"] = search_token
    elif "term" in params:
        kwargs["term"] = search_token

    if "limit" in params:
        kwargs["limit"] = 20
    elif "page_size" in params:
        kwargs["page_size"] = 20

    lp = LeadSearchParams(**kwargs)
    raw_rows = search_people_leads(conn, lp)

    if isinstance(raw_rows, tuple) and raw_rows:
        rows = raw_rows[0]
    else:
        rows = raw_rows

    rows_list: list[dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict):
            rows_list.append(r)
        else:
            rows_list.append(dict(r))

    if not rows_list:
        print(
            "No rows returned from local FTS search for search token",
            repr(search_token),
        )
        return

    print(f"Search returned {len(rows_list)} rows; first few:")
    for row in rows_list[:5]:
        print(
            "-",
            row.get("email"),
            "|",
            (row.get("company") or row.get("company_name") or ""),
            "| verify_status=",
            row.get("verify_status"),
        )


def print_summary(
    conn: sqlite3.Connection,
    domain: str,
    search_hint: str | None = None,
) -> None:
    """
    Print a small table of leads for this domain from v_emails_latest, plus
    a short search-based check when FTS is available.
    """
    dom = (domain or "").strip().lower()
    emails = _fetch_v_emails_latest_rows(conn, dom)
    _print_v_emails_latest_snapshot(dom, emails)
    _search_backend_sanity_check(conn, dom=dom, search_hint=search_hint)


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Demo: crawl + extract + generate + verify + score for a single company/domain",
    )
    p.add_argument(
        "--db",
        dest="db",
        default="data/dev.db",
        help="Path to SQLite database (default: data/dev.db)",
    )
    p.add_argument(
        "--company",
        dest="company",
        required=True,
        help="Company name (for display/normalization)",
    )
    p.add_argument(
        "--domain",
        dest="domain",
        required=True,
        help="Primary domain to resolve/crawl (e.g. 'example.com')",
    )
    p.add_argument(
        "--website-url",
        dest="website_url",
        default=None,
        help="Optional website URL override for companies.website_url",
    )
    p.add_argument(
        "--init-schema",
        dest="init_schema",
        action="store_true",
        help="Apply db/schema.sql before running (safe to run multiple times).",
    )
    p.add_argument(
        "--run-icp",
        dest="run_icp",
        action="store_true",
        help="Run scripts/backfill_r14_icp.py after verification to compute ICP scores.",
    )
    p.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        help="Python logging level (DEBUG, INFO, WARNING, ...).",
    )

    # NEW: keep verification from blowing up when port 25 is blocked / hostile
    p.add_argument(
        "--verify-budget-seconds",
        dest="verify_budget_seconds",
        type=float,
        default=float(os.getenv("DEMO_VERIFY_BUDGET_SECONDS", "60")),
        help="Max seconds spent verifying per person_id (default: 60 or DEMO_VERIFY_BUDGET_SECONDS).",
    )
    p.add_argument(
        "--max-probes-per-person",
        dest="max_probes_per_person",
        type=int,
        default=int(os.getenv("DEMO_MAX_PROBES_PER_PERSON", "6")),
        help="Hard cap probes per person_id (default: 6 or DEMO_MAX_PROBES_PER_PERSON).",
    )
    p.add_argument(
        "--tcp25-probe-timeout",
        dest="tcp25_probe_timeout",
        type=float,
        default=float(os.getenv("DEMO_TCP25_PROBE_TIMEOUT_SECONDS", "3")),
        help="Seconds for the TCP/25 preflight probe (default: 3 or DEMO_TCP25_PROBE_TIMEOUT_SECONDS).",
    )
    p.add_argument(
        "--force-verify",
        dest="force_verify",
        action="store_true",
        help="Attempt SMTP verification even if TCP/25 preflight fails (not recommended on blocked networks).",
    )
    p.add_argument(
        "--continue-on-unknown",
        dest="continue_on_unknown",
        action="store_true",
        help="Do NOT stop a person's probes after unknown/timeout; keep trying until budget/cap.",
    )

    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Keep DB_URL / DATABASE_PATH aligned so src.db.get_conn() and R16/R15 helpers
    # operate on the same SQLite file as this script.
    os.environ.setdefault("DB_URL", f"sqlite:///{db_path.as_posix()}")
    os.environ.setdefault("DATABASE_PATH", str(db_path))

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # type: ignore[attr-defined]

    try:
        if args.init_schema or not db_path.exists():
            LOG.info("Applying schema from %s", SCHEMA_PATH)
            apply_schema(conn)

        # 1) Ensure company row
        company_id = ensure_company(
            conn,
            name=args.company,
            domain=args.domain,
            website_url=args.website_url,
        )

        # 2) Resolve official domain (R08)
        LOG.info("Resolving official domain for company_id=%s", company_id)
        _ = resolve_company_domain(
            company_id=company_id,
            company_name=args.company,
            user_supplied_domain=args.domain,
        )

        official_domain, fallback_domain = get_company_domain_info(conn, company_id)
        crawl_domain_str = official_domain or fallback_domain
        if not crawl_domain_str:
            raise RuntimeError("No domain available for crawling after resolution")

        LOG.info(
            "Resolved domain: official=%r fallback=%r; using %s for crawl",
            official_domain,
            fallback_domain,
            crawl_domain_str,
        )

        # 3) Crawl the domain (R10)
        pages = crawl_domain(crawl_domain_str)
        LOG.info("Crawled %s pages for domain=%s", len(pages), crawl_domain_str)

        if not pages:
            # First check if robots.txt is the reason.
            robots_blocked = _check_robots_and_suggest_manual_pages(crawl_domain_str)
            if robots_blocked:
                LOG.info(
                    "Stopping automatic crawl for %s out of respect for robots.txt. "
                    "You can manually add people later and run email generation / "
                    "verification as a separate process.",
                    crawl_domain_str,
                )
                return

            # robots.txt does not obviously block us; try a small, local fallback crawl.
            LOG.info(
                "Core crawler returned 0 pages for %s and robots.txt does not block "
                "common 'people' URLs; running fallback crawler.",
                crawl_domain_str,
            )
            pages = _fallback_crawl_core_pages(crawl_domain_str)
            LOG.info(
                "Fallback crawler captured %s pages for domain=%s",
                len(pages),
                crawl_domain_str,
            )

        # 4) Persist pages into sources
        written = _save_pages_for_company(conn, company_id, pages)
        LOG.info("Persisted %s pages into sources for company_id=%s", written, company_id)

        # 5) Extract candidates (heuristics + fallback) and persist people/emails
        candidates = extract_candidates_for_company(
            conn,
            company_id,
            official_domain or crawl_domain_str,
            company_name=args.company,
            crawl_domain=crawl_domain_str,
        )
        persisted = persist_candidates(conn, company_id, candidates)
        LOG.info(
            "Persisted %s extracted candidates as people/emails for company_id=%s",
            len(persisted),
            company_id,
        )

        # Build the set of person_ids that came through the AI/refined candidate path.
        allowed_person_ids: set[int] = {
            pc.person_id for pc in persisted if pc.person_id is not None
        }
        LOG.info(
            "Allowed person_ids for generation (from AI/refined candidates): %s",
            sorted(allowed_person_ids),
        )

        # 6) Leadership fallback: seed people rows from 'Our Leadership Team'
        inserted_leaders = ensure_leadership_people_for_company(
            conn,
            company_id,
            official_domain or crawl_domain_str,
        )
        LOG.info(
            "Leadership fallback inserted %s people for company_id=%s",
            inserted_leaders,
            company_id,
        )

        # 6b) Team-page fallback: seed people rows from obvious team/our-team pages.
        inserted_team = ensure_team_people_for_company(
            conn,
            company_id,
            official_domain or crawl_domain_str,
        )
        LOG.info(
            "Team-page fallback inserted %s people for company_id=%s",
            inserted_team,
            company_id,
        )

        # 7) Generate permutations for people without direct personal emails,
        # restricted to AI-approved person_ids when that set is non-empty.
        generate_kwargs: dict[str, Any] = {}
        if allowed_person_ids:
            generate_kwargs["allowed_person_ids"] = allowed_person_ids

        generated_count = generate_for_missing_people(
            conn,
            company_id,
            crawl_domain_str,
            **generate_kwargs,
        )
        LOG.info(
            "Generated %s candidate emails for company_id=%s",
            generated_count,
            company_id,
        )

        # 8) Verify all emails synchronously (R16–R18 + O26 learning)
        pre = _smtp_tcp25_preflight(
            crawl_domain_str,
            timeout_s=float(args.tcp25_probe_timeout),
        )
        if not bool(pre.get("ok")) and not bool(args.force_verify):
            LOG.error(
                "TCP/25 preflight FAILED for domain=%s. This usually means outbound port 25 is blocked "
                "(ISP/firewall/VPN) — which will manifest as long 'unknown_timeout' probes.",
                crawl_domain_str,
            )
            LOG.error(
                "MX tried: %s",
                ", ".join([str(x) for x in (pre.get("tried") or [])]),
            )
            LOG.error(
                "Skipping SMTP verification step to avoid long timeouts. "
                "Run from a network/VPS with outbound 25 allowed, or use your O07/O26 fallbacks. "
                "Use --force-verify to try anyway."
            )
        else:
            verify_all_for_company(
                db_path,
                company_id,
                crawl_domain_str,
                per_person_budget_seconds=float(args.verify_budget_seconds),
                max_probes_per_person=int(args.max_probes_per_person),
                stop_on_unknown=(not bool(args.continue_on_unknown)),
            )

        # 9) Optional ICP scoring
        if args.run_icp:
            maybe_run_icp_backfill(db_path)

        # 10) Print summary
        print_summary(conn, crawl_domain_str, search_hint=args.company)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
