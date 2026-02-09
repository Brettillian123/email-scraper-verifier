#!/usr/bin/env python3
"""Find all potential person names on the openspace.ai about page."""

import re
import sys

sys.path.insert(0, "/opt/email-scraper")

from bs4 import BeautifulSoup

from src.db import get_conn

# Get the about page HTML
con = get_conn()
cur = con.execute(
    "SELECT source_url, html FROM sources WHERE source_url LIKE %s",
    ("%openspace%about%",)
)
row = cur.fetchone()
if not row:
    print("About page not found, checking what pages exist...")
    cur = con.execute(
        "SELECT source_url FROM sources WHERE source_url LIKE %s",
        ("%openspace%",)
    )
    for r in cur.fetchall():
        print(f"  Found: {r[0]}")
    sys.exit(1)

url, html = row
html = html.decode("utf-8", "ignore") if isinstance(html, bytes) else html
soup = BeautifulSoup(html, "html.parser")

print("=" * 70)
print(f"ANALYZING: {url}")
print(f"HTML size: {len(html):,} bytes")
print("=" * 70)

# 1. Look for headings that might be names
print("\n📋 ALL H2/H3/H4 HEADINGS:")
for heading in soup.find_all(["h2", "h3", "h4"])[:30]:
    text = heading.get_text(strip=True)[:60]
    if text:
        print(f"   <{heading.name}> {text}")

# 2. Check what the _looks_like_person_name function returns
print("\n📋 TESTING _looks_like_person_name() on headings:")
try:
    from src.extract.people_cards import _looks_like_person_name
    for heading in soup.find_all(["h2", "h3", "h4"])[:30]:
        text = heading.get_text(strip=True)
        if text and len(text) < 50:
            result = _looks_like_person_name(text)
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"   {status}: '{text}'")
except Exception as e:
    print(f"   Error: {e}")

# 3. Look for names with typical patterns
print("\n📋 CAPITALIZED WORD PAIRS (potential names):")
text = soup.get_text()
name_pattern = r'\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b'
found = re.findall(name_pattern, text)
skip_first = {'Our', 'The', 'We', 'For', 'About', 'Contact', 'Learn', 'Read', 'View', 
              'Get', 'See', 'Join', 'Meet', 'Work', 'Sign', 'Log', 'Start', 'Book',
              'San', 'New', 'Los', 'Las'}
skip_last = {'Team', 'Us', 'More', 'Now', 'Here', 'Today', 'Free', 'Demo',
             'Francisco', 'York', 'Angeles', 'Vegas'}
seen = set()
count = 0
for first, last in found:
    if first not in skip_first and last not in skip_last:
        name = f"{first} {last}"
        if name not in seen and count < 30:
            seen.add(name)
            print(f"   • {name}")
            count += 1

# 4. Look for LinkedIn links (strong person signal)
print("\n📋 LINKEDIN LINKS:")
for a in soup.find_all("a", href=True):
    href = a.get("href", "")
    if "linkedin.com" in href.lower():
        text = a.get_text(strip=True)[:50]
        print(f"   • {text} -> {href[:60]}...")

# 5. Look for images with alt text that might be names
print("\n📋 IMAGES WITH POTENTIAL NAME ALT TEXT:")
for img in soup.find_all("img"):
    alt = img.get("alt", "").strip()
    if alt and 2 <= len(alt.split()) <= 4:
        skip = {'logo', 'icon', 'image', 'photo', 'banner', 'header', 'background'}
        if not any(s in alt.lower() for s in skip):
            print(f"   • {alt}")
