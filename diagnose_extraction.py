#!/usr/bin/env python3
"""
Diagnostic script to trace why candidates from team pages aren't being extracted.

Usage:
    python diagnose_extraction.py brandtcpa.com
    python diagnose_extraction.py brandtcpa.com --url "https://brandtcpa.com/our-team"

This will:
1. Show what pages were crawled for the domain
2. For each page, show classifier scores and why extraction might be skipped
3. Show what people_cards extracts from each page
4. Show what candidates pass quality gates
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any
from urllib.parse import urlparse

# Configure logging to see debug output
logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s | %(name)s | %(message)s",
)

# Reduce noise from libraries
for noisy in ["urllib3", "requests", "httpx", "httpcore"]:
    logging.getLogger(noisy).setLevel(logging.WARNING)

log = logging.getLogger("diagnose")


def get_db_connection():
    """Get database connection."""
    try:
        from src.db import get_conn

        return get_conn()
    except Exception as e:
        log.error("Failed to get DB connection: %s", e)
        sys.exit(1)


def get_pages_for_domain(con, domain: str) -> list[tuple[str, str]]:
    """Fetch pages from sources table for a domain."""
    # Try with company_id first
    try:
        cur = con.execute(
            """
            SELECT s.source_url, s.html
            FROM sources s
            JOIN companies c ON s.company_id = c.id
            WHERE c.official_domain = ? OR c.domain = ?
            """,
            (domain, domain),
        )
        rows = cur.fetchall()
        if rows:
            return [(r[0], r[1]) for r in rows]
    except Exception:
        pass

    # Fallback to URL matching
    cur = con.execute("SELECT source_url, html FROM sources")
    rows = cur.fetchall()
    pages: list[tuple[str, str]] = []
    for url, html in rows:
        host = urlparse(url).netloc.lower()
        if host == domain or host.endswith(f".{domain}"):
            pages.append((url, html))
    return pages


def diagnose_page_classification(url: str, html: str) -> dict[str, Any]:
    """Run the page classifier and return detailed results."""
    result: dict[str, Any] = {
        "url": url,
        "is_blocked": None,
        "block_reason": None,
        "is_employee_url": None,
        "classifier_ok": None,
        "classifier_score": None,
        "classifier_reasons": [],
    }

    try:
        from src.extract.source_filters import (
            classify_page_for_people_extraction,
            is_blocked_source_url,
            is_employee_page_url,
        )

        blocked, reason = is_blocked_source_url(url)
        result["is_blocked"] = blocked
        result["block_reason"] = reason

        result["is_employee_url"] = is_employee_page_url(url)

        classification = classify_page_for_people_extraction(url, html, min_score=8)
        result["classifier_ok"] = classification.ok
        result["classifier_score"] = classification.score
        result["classifier_reasons"] = list(classification.reasons)

    except Exception as e:
        result["error"] = str(e)

    return result


def diagnose_should_run_people_cards(url: str, html: str, domain: str) -> dict[str, Any]:
    """Check if people_cards extraction would run for this page."""
    result: dict[str, Any] = {
        "would_run": False,
        "reason": "unknown",
    }

    try:
        from src.extract.candidates import _load_optional_helpers, _should_run_people_cards_page

        (
            is_blocked_source_url,
            is_employee_page_url,
            classify_page_for_people_extraction,
            extract_people_cards,
        ) = _load_optional_helpers()

        run_cards, reason = _should_run_people_cards_page(
            source_url=url,
            html=html,
            effective_domain=domain,
            extract_people_cards=extract_people_cards,
            is_blocked_source_url=is_blocked_source_url,
            classify_page_for_people_extraction=classify_page_for_people_extraction,
            is_employee_page_url=is_employee_page_url,
        )
        result["would_run"] = run_cards
        result["reason"] = reason

    except Exception as e:
        result["error"] = str(e)

    return result


def extract_people_cards_direct(url: str, html: str, domain: str) -> list[dict[str, Any]]:
    """Run people_cards extraction directly (bypassing gating)."""
    try:
        from src.extract.people_cards import extract_people_cards

        candidates = extract_people_cards(
            html=html,
            source_url=url,
            official_domain=domain,
        )

        return [
            {
                "raw_name": getattr(c, "raw_name", None),
                "first_name": getattr(c, "first_name", None),
                "last_name": getattr(c, "last_name", None),
                "title": getattr(c, "title", None),
                "source_type": getattr(c, "source_type", None),
                "email": getattr(c, "email", None),
            }
            for c in candidates
        ]
    except Exception as e:
        return [{"error": str(e)}]


def extract_all_candidates(url: str, html: str, domain: str) -> list[dict[str, Any]]:
    """Run full candidate extraction (emails + people_cards)."""
    try:
        from src.extract.candidates import extract_candidates

        candidates = extract_candidates(
            html=html,
            company_domain=domain,
            source_url=url,
            official_domain=domain,
        )

        return [
            {
                "email": getattr(c, "email", None),
                "raw_name": getattr(c, "raw_name", None),
                "first_name": getattr(c, "first_name", None),
                "last_name": getattr(c, "last_name", None),
                "title": getattr(c, "title", None),
                "source_type": getattr(c, "source_type", None),
                "is_role_address_guess": getattr(c, "is_role_address_guess", None),
            }
            for c in candidates
        ]
    except Exception as e:
        return [{"error": str(e)}]


def check_people_cards_internal_classifier(url: str, html: str) -> dict[str, Any]:
    """Check if people_cards internal classifier allows extraction."""
    result: dict[str, Any] = {"allows": None, "reason": "unknown"}

    try:
        from src.extract.people_cards import _classifier_allows_people_cards

        allows = _classifier_allows_people_cards(html, url)
        result["allows"] = allows
        result["reason"] = "classifier check"
    except Exception as e:
        result["error"] = str(e)

    return result


def find_leadership_sections(html: str) -> list[str]:
    """Find leadership section headings in HTML."""
    try:
        from bs4 import BeautifulSoup

        from src.extract.people_cards import _find_leadership_sections

        soup = BeautifulSoup(html, "html.parser")
        sections = _find_leadership_sections(soup)

        headings: list[str] = []
        for section in sections:
            for h in section.find_all(["h1", "h2", "h3", "h4"]):
                text = h.get_text(strip=True)
                if text:
                    headings.append(text)
        return headings[:10]  # Limit output
    except Exception as e:
        return [f"error: {e}"]


def _decode_html(html_raw: Any) -> str:
    if isinstance(html_raw, bytes):
        return html_raw.decode("utf-8", "ignore")
    return str(html_raw or "")


def _display_name(c: dict[str, Any]) -> str:
    raw = c.get("raw_name")
    if raw:
        return str(raw)

    first = str(c.get("first_name") or "").strip()
    last = str(c.get("last_name") or "").strip()
    return f"{first} {last}".strip()


def _print_page_classification(classification: dict[str, Any]) -> None:
    blocked = classification.get("is_blocked")
    block_reason = classification.get("block_reason")
    employee_url = classification.get("is_employee_url")
    ok = classification.get("classifier_ok")
    score = classification.get("classifier_score")
    reasons = classification.get("classifier_reasons")

    print("\n   🏷️  PAGE CLASSIFICATION:")
    print(f"      is_blocked: {blocked} ({block_reason})")
    print(f"      is_employee_url: {employee_url}")
    print(f"      classifier_ok: {ok}")
    print(f"      classifier_score: {score}")
    print(f"      reasons: {reasons}")


def _print_people_cards(cards: list[dict[str, Any]]) -> None:
    print("\n   👥 PEOPLE_CARDS EXTRACTION (bypassing gates):")
    if cards and "error" not in cards[0]:
        print(f"      Found {len(cards)} candidates:")
        for c in cards[:5]:
            name = _display_name(c)
            title = c.get("title") or "(no title)"
            source = c.get("source_type") or "unknown"
            print(f"        • {name} | {title} | via {source}")
        if len(cards) > 5:
            print(f"        ... and {len(cards) - 5} more")
        return

    if cards and "error" in cards[0]:
        print(f"      ❌ Error: {cards[0]['error']}")
        return

    print("      (no candidates extracted)")


def _print_full_extraction(all_cands: list[dict[str, Any]]) -> None:
    print("\n   🎯 FULL EXTRACTION (actual pipeline):")
    if not all_cands:
        print("      ⚠️  NO CANDIDATES - this is the problem!")
        return

    if "error" in all_cands[0]:
        print(f"      ❌ Error: {all_cands[0]['error']}")
        return

    email_cands = [c for c in all_cands if c.get("email")]
    no_email_cands = [c for c in all_cands if not c.get("email")]

    print(f"      With email: {len(email_cands)}")
    for c in email_cands[:3]:
        print(f"        • {c.get('email')} | {c.get('raw_name')} | {c.get('title')}")

    print(f"      No email (people cards): {len(no_email_cands)}")
    for c in no_email_cands[:3]:
        name = _display_name(c)
        print(f"        • {name} | {c.get('title')}")

    if len(email_cands) + len(no_email_cands) == 0:
        print("      ⚠️  NO CANDIDATES - this is the problem!")


def _diagnose_single_page(url: str, html: str, domain: str) -> None:
    print(f"\n{'─' * 70}")
    print(f"📍 URL: {url}")
    print(f"   HTML size: {len(html):,} bytes")

    classification = diagnose_page_classification(url, html)
    _print_page_classification(classification)

    print("\n   🚦 PEOPLE_CARDS GATING:")
    gating = diagnose_should_run_people_cards(url, html, domain)
    print(f"      would_run: {gating.get('would_run')}")
    print(f"      reason: {gating.get('reason')}")

    print("\n   🔍 PEOPLE_CARDS INTERNAL CLASSIFIER:")
    internal = check_people_cards_internal_classifier(url, html)
    print(f"      allows: {internal.get('allows')}")
    if "error" in internal:
        print(f"      error: {internal['error']}")

    print("\n   📋 LEADERSHIP SECTIONS FOUND:")
    sections = find_leadership_sections(html)
    if sections:
        for s in sections:
            print(f"      • {s[:60]}...")
    else:
        print("      (none found)")

    cards = extract_people_cards_direct(url, html, domain)
    _print_people_cards(cards)

    all_cands = extract_all_candidates(url, html, domain)
    _print_full_extraction(all_cands)


def print_diagnosis(domain: str, url_filter: str | None = None) -> None:
    """Main diagnostic function."""
    print(f"\n{'=' * 70}")
    print(f"DIAGNOSTIC REPORT FOR: {domain}")
    print(f"{'=' * 70}\n")

    con = get_db_connection()
    pages = get_pages_for_domain(con, domain)

    if not pages:
        print(f"❌ No pages found in database for domain: {domain}")
        print("   → Run crawl_company_site first to populate the sources table")
        return

    print(f"📄 Found {len(pages)} pages in database\n")

    for url, html_raw in pages:
        if url_filter and url_filter not in url:
            continue
        html = _decode_html(html_raw)
        _diagnose_single_page(url, html, domain)

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(
        """
If candidates appear in "PEOPLE_CARDS EXTRACTION (bypassing gates)" but NOT in
"FULL EXTRACTION", the issue is the page classifier rejecting the page.

COMMON FIXES:
1. Lower the min_score threshold in people_cards.py:
   _PEOPLE_CARDS_CLASSIFY_MIN_SCORE = 4  (default is 8)

2. Add HTML signals to the page that boost the score:
   - Add headings like "Our Team" or "Leadership Team"
   - Add JSON-LD markup with "@type": "Person"

3. Check if the URL pattern needs to be added to _ALLOWED_URL_PATTERNS in
   source_filters.py (e.g., /our-professionals, /firm/team, etc.)

4. If the internal classifier in people_cards.py is blocking, check
   _is_people_page_url() for the legacy URL pattern check.
"""
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose candidate extraction issues")
    parser.add_argument("domain", help="Domain to diagnose (e.g., brandtcpa.com)")
    parser.add_argument("--url", help="Filter to specific URL containing this string")
    args = parser.parse_args()

    print_diagnosis(args.domain, args.url)


if __name__ == "__main__":
    main()
