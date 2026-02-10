#!/usr/bin/env python3
"""Find all potential person names on the brandtcpa.com team page."""

import re
import sys

sys.path.insert(0, "/opt/email-scraper")

from bs4 import BeautifulSoup

from src.db import get_conn


def _get_team_page_html() -> str:
    con = get_conn()
    cur = con.execute(
        "SELECT html FROM sources WHERE source_url LIKE %s",
        ("%brandtcpa%our-team%",),
    )
    row = cur.fetchone()
    if not row:
        print("Team page not found")
        sys.exit(1)

    raw = row[0]
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "ignore")
    return raw


def _print_header() -> None:
    print("=" * 70)
    print("ANALYZING brandtcpa.com/our-team FOR PERSON NAMES")
    print("=" * 70)


def _print_names_with_credentials(soup: BeautifulSoup) -> None:
    print("\n📋 NAMES WITH CREDENTIALS (CPA, MBA, etc.):")
    text = soup.get_text()
    cpa_pattern = (
        r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)"
        r"(?:,?\s*(?:CPA|MBA|CFP|EA|JD|PhD|CFA))"
    )
    matches = re.findall(cpa_pattern, text)
    for name in sorted(set(matches)):
        print(f"   • {name}")


def _print_heading_names(soup: BeautifulSoup) -> None:
    print("\n📋 HEADINGS THAT LOOK LIKE NAMES:")
    skip_words = {
        "our",
        "the",
        "team",
        "about",
        "contact",
        "certified",
        "public",
        "accountant",
        "manager",
        "director",
        "administrative",
        "tax",
        "services",
        "planning",
        "business",
        "individual",
    }

    for heading in soup.find_all(["h2", "h3", "h4", "h5", "h6"]):
        text = heading.get_text(strip=True)
        words = text.split()

        # Check if it looks like a name (2-4 words, each capitalized, no common words)
        if 2 <= len(words) <= 4:
            if not any(w.lower() in skip_words for w in words):
                if all(w[0].isupper() for w in words if w):
                    print(f"   • {text} (tag: {heading.name})")


def _print_image_alt_names(soup: BeautifulSoup) -> None:
    print("\n📋 IMAGE ALT TEXT THAT LOOKS LIKE NAMES:")
    skip_words = {"logo", "icon", "image", "photo", "picture", "banner", "header"}

    for img in soup.find_all("img"):
        alt = img.get("alt", "").strip()
        if not alt:
            continue

        words = alt.split()
        if 2 <= len(words) <= 4:
            if not any(w.lower() in skip_words for w in words):
                if all(w[0].isupper() for w in words if len(w) > 1):
                    print(f"   • {alt}")


def _find_team_heading(soup: BeautifulSoup):
    heading_tags = ["h1", "h2", "h3"]

    def _matcher(tag) -> bool:
        return tag.name in heading_tags and "team" in tag.get_text().lower()

    return soup.find(_matcher)


def _print_text_near_team_heading(soup: BeautifulSoup) -> None:
    print("\n📋 TEXT NEAR 'TEAM' HEADING (first 2000 chars):")
    team_heading = _find_team_heading(soup)
    if not team_heading:
        return

    parent = team_heading.find_parent(["section", "div", "article"])
    if not parent:
        return

    section_text = parent.get_text()[:2000]
    name_pattern = r"\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b"
    found = re.findall(name_pattern, section_text)

    skip_first = {
        "Our",
        "The",
        "Tax",
        "For",
        "About",
        "Contact",
        "Business",
        "Individual",
        "Certified",
        "Public",
        "Administrative",
    }
    skip_last = {
        "Team",
        "Services",
        "Planning",
        "Accountant",
        "Manager",
        "Director",
        "Preparation",
        "Advisory",
    }

    seen: set[str] = set()
    for first, last in found:
        if first in skip_first or last in skip_last:
            continue
        name = f"{first} {last}"
        if name in seen:
            continue
        seen.add(name)
        print(f"   • {name}")


def _print_team_section_structure(soup: BeautifulSoup) -> None:
    print("\n📋 TEAM SECTION HTML STRUCTURE (simplified):")
    team_heading = _find_team_heading(soup)
    if not team_heading:
        return

    parent = team_heading.find_parent(["section", "div"])
    if not parent:
        return

    for div in parent.find_all("div", recursive=True)[:20]:
        children = [c for c in div.children if getattr(c, "name", None)]
        text = div.get_text(strip=True)[:80]
        classes = " ".join(div.get("class", []))[:40]
        if len(children) >= 2 and len(text) > 20:
            print(f"   div.{classes}: {len(children)} children, text: {text[:50]}...")


def main() -> None:
    html = _get_team_page_html()
    soup = BeautifulSoup(html, "html.parser")

    _print_header()
    _print_names_with_credentials(soup)
    _print_heading_names(soup)
    _print_image_alt_names(soup)
    _print_text_near_team_heading(soup)
    _print_team_section_structure(soup)


if __name__ == "__main__":
    main()
