#!/usr/bin/env python3
"""
Analyze HTML structure of a team page to understand why people aren't being extracted.
"""

from __future__ import annotations

import re
import sys

from bs4 import BeautifulSoup

sys.path.insert(0, "/opt/email-scraper")

from src.db import get_conn


def get_page_html(domain: str, url_contains: str) -> tuple[str | None, str | None]:
    """Fetch HTML from database."""
    con = get_conn()
    cur = con.execute(
        "SELECT source_url, html FROM sources WHERE source_url LIKE ?",
        (f"%{domain}%{url_contains}%",),
    )
    row = cur.fetchone()
    if not row:
        return None, None

    url, html = row
    if isinstance(html, bytes):
        html = html.decode("utf-8", "ignore")
    return url, html


def find_person_like_patterns(html: str) -> None:
    """Look for patterns that might contain person names."""
    soup = BeautifulSoup(html, "html.parser")

    print("=" * 70)
    print("LOOKING FOR PERSON-LIKE PATTERNS IN TEXT")
    print("=" * 70)

    text = soup.get_text()

    # Find CPA-style names
    cpa_pattern = (
        r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)"
        r"(?:,?\s*(?:CPA|MBA|CFP|EA))"
    )
    cpa_names = re.findall(cpa_pattern, text)
    if cpa_names:
        print("\n📋 Names with credentials (CPA, MBA, etc.):")
        for name in set(cpa_names):
            print(f"   • {name}")

    # Find potential names near title keywords
    title_pattern = (
        r"(.{0,50}(?:Partner|Principal|Director|Manager|Senior|Staff|Tax|Audit).{0,50})"
    )
    title_context = re.findall(title_pattern, text)
    if title_context:
        print("\n📋 Text near title keywords (first 10):")
        for ctx in title_context[:10]:
            clean = " ".join(ctx.split())
            if len(clean) > 20:
                print(f"   • {clean[:80]}...")


def analyze_card_structures(html: str) -> None:
    """Look for div structures that might be person cards."""
    soup = BeautifulSoup(html, "html.parser")

    print("\n" + "=" * 70)
    print("ANALYZING POTENTIAL CARD STRUCTURES")
    print("=" * 70)

    # Look for repeated sibling divs with images
    for container in soup.find_all(["section", "div"], class_=True):
        classes = " ".join(container.get("class", []))

        # Skip obvious non-team containers
        if any(skip in classes.lower() for skip in ["nav", "footer", "header", "menu", "sidebar"]):
            continue

        # Look for containers with multiple similar children containing images
        children = container.find_all(["div", "article"], recursive=False)
        if len(children) < 3:
            continue

        children_with_img = [c for c in children if c.find("img")]
        if len(children_with_img) < 3:
            continue

        print(f"\n🎯 POTENTIAL TEAM GRID (class='{classes[:50]}'):")
        print(f"   {len(children_with_img)} children with images")

        # Sample first few cards
        for i, child in enumerate(children_with_img[:3]):
            child_text = " ".join(child.get_text().split())[:100]
            img = child.find("img")
            img_alt = img.get("alt", "") if img else ""
            print(f"\n   Card {i + 1}:")
            print(f"      img alt: {img_alt[:50]}")
            print(f"      text: {child_text[:80]}...")


def find_team_section(html: str) -> None:
    """Find the section containing 'Our Team' or similar headings."""
    soup = BeautifulSoup(html, "html.parser")

    print("\n" + "=" * 70)
    print("FINDING TEAM SECTION CONTENT")
    print("=" * 70)

    keywords = ["team", "people", "staff", "leadership", "partner", "professional"]

    # Find headings with team-related text
    for heading in soup.find_all(["h1", "h2", "h3"]):
        heading_text = heading.get_text(strip=True)
        heading_text_lower = heading_text.lower()

        if not any(kw in heading_text_lower for kw in keywords):
            continue

        print(f"\n📍 Found heading: <{heading.name}>{heading_text}</{heading.name}>")

        # Get the parent container
        parent = heading.find_parent(["section", "div"])
        if not parent:
            continue

        section_text = parent.get_text()

        # Pattern: Two capitalized words
        name_candidates = re.findall(r"\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b", section_text)

        # Filter out obvious non-names
        skip_words = {
            "Our",
            "The",
            "For",
            "Tax",
            "Business",
            "Individual",
            "About",
            "Contact",
            "Useful",
            "Links",
            "Learn",
            "More",
            "Read",
            "View",
            "Click",
            "Here",
            "Home",
            "Services",
            "News",
            "Blog",
            "Privacy",
            "Policy",
            "Terms",
        }

        real_names = [
            (first, last)
            for first, last in name_candidates
            if first not in skip_words
            and last not in skip_words
            and len(first) > 1
            and len(last) > 1
        ]

        if real_names:
            print("\n   Potential names found in this section:")
            seen: set[str] = set()
            for first, last in real_names[:15]:
                full_name = f"{first} {last}"
                if full_name in seen:
                    continue
                seen.add(full_name)
                print(f"      • {full_name}")


def dump_team_section_html(html: str) -> None:
    """Dump the raw HTML of the team section for manual inspection."""
    soup = BeautifulSoup(html, "html.parser")

    print("\n" + "=" * 70)
    print("RAW HTML AROUND TEAM HEADING (first 3000 chars)")
    print("=" * 70)

    for heading in soup.find_all(["h1", "h2", "h3"]):
        heading_text = heading.get_text(strip=True).lower()
        if "team" not in heading_text:
            continue

        parent = heading.find_parent(["section", "div"])
        if parent:
            raw = str(parent)[:3000]
            raw = re.sub(r"\s+", " ", raw)
            print(raw)
            return

    print("(No team section found)")


def check_quality_gates(names: list[str]) -> None:
    """Test which names pass/fail quality gates."""
    print("\n" + "=" * 70)
    print("QUALITY GATE TEST")
    print("=" * 70)

    try:
        from src.extract.quality_gates import validate_person_name
    except ImportError:
        print("Could not import quality_gates")
        return

    test_names = names + [
        "Our Firm",
        "Individual Tax Preparation",
        "Useful Links",
        "John Smith",
        "Abbey Shenberg",
        "Tax Services",
    ]

    for name in test_names:
        result = validate_person_name(name)
        if hasattr(result, "is_valid"):
            valid = result.is_valid
            reason = getattr(result, "rejection_reason", None)
        elif isinstance(result, tuple):
            valid = result[0]
            reason = result[1] if len(result) > 1 else None
        else:
            valid = bool(result)
            reason = None

        status = "✅ PASS" if valid else "❌ FAIL"
        reason_str = f" ({reason})" if reason else ""
        print(f"   {status}: '{name}'{reason_str}")


def main() -> None:
    url, html = get_page_html("brandtcpa.com", "our-team")
    if not html:
        print("Could not find the our-team page in database")
        return

    print(f"Analyzing: {url}")
    print(f"HTML size: {len(html):,} bytes\n")

    find_person_like_patterns(html)
    find_team_section(html)
    analyze_card_structures(html)

    # Test quality gates
    check_quality_gates(
        [
            "Our Firm",
            "Individual Tax Preparation",
            "Useful Links",
        ]
    )

    # Uncomment to see raw HTML
    # dump_team_section_html(html)


if __name__ == "__main__":
    main()
