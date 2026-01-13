# tests/test_people_cards.py
"""
Tests for the people cards extractor (Paddle-style pages).

These tests validate extraction of leadership/team members from:
  - LinkedIn anchor elements
  - Structured person card divs
  - Adjacent name/title pairs
"""

import pytest

# Skip all tests if BeautifulSoup is not available
pytest.importorskip("bs4")

from src.extract.people_cards import (
    _is_linkedin_url,
    _looks_like_person_name,
    _looks_like_title,
    extract_people_cards,
)


class TestNameValidation:
    """Tests for person name validation."""

    def test_valid_two_word_names(self):
        assert _looks_like_person_name("John Smith") is True
        assert _looks_like_person_name("Jane Doe") is True
        assert _looks_like_person_name("Mary Jane") is True

    def test_valid_three_word_names(self):
        assert _looks_like_person_name("Mary Jane Watson") is True
        assert _looks_like_person_name("Jean-Pierre Dupont") is True

    def test_single_word_rejected(self):
        assert _looks_like_person_name("John") is False
        assert _looks_like_person_name("CEO") is False

    def test_non_capitalized_rejected(self):
        assert _looks_like_person_name("john smith") is False
        assert _looks_like_person_name("john Smith") is False

    def test_navigation_text_rejected(self):
        assert _looks_like_person_name("Learn More") is False
        assert _looks_like_person_name("Contact Us") is False
        assert _looks_like_person_name("About Us") is False
        assert _looks_like_person_name("Our Team") is False


class TestTitleValidation:
    """Tests for job title validation."""

    def test_valid_titles(self):
        assert _looks_like_title("CEO") is True
        assert _looks_like_title("Chief Executive Officer") is True
        assert _looks_like_title("VP of Sales") is True
        assert _looks_like_title("Co-Founder") is True
        assert _looks_like_title("Head of Engineering") is True

    def test_geography_rejected(self):
        assert _looks_like_title("San Francisco") is False
        assert _looks_like_title("New York") is False

    def test_long_text_rejected(self):
        long_text = "This is a very long text that is definitely not a job title " * 3
        assert _looks_like_title(long_text) is False


class TestLinkedInDetection:
    """Tests for LinkedIn URL detection."""

    def test_linkedin_urls(self):
        assert _is_linkedin_url("https://linkedin.com/in/johnsmith") is True
        assert _is_linkedin_url("https://www.linkedin.com/in/johnsmith") is True
        assert _is_linkedin_url("http://linkedin.com/in/johnsmith") is True

    def test_non_linkedin_urls(self):
        assert _is_linkedin_url("https://twitter.com/johnsmith") is False
        assert _is_linkedin_url("https://example.com") is False
        assert _is_linkedin_url("") is False
        assert _is_linkedin_url(None) is False


class TestExtractPeopleCards:
    """Tests for the main extraction function."""

    def test_extracts_from_linkedin_anchors(self):
        """Test extraction from LinkedIn-linked names like Paddle uses."""
        html = """
        <html>
        <body>
            <section>
                <h2>Meet the leadership team</h2>
                <div class="person">
                    <a href="https://linkedin.com/in/jimmyfitz">Jimmy Fitzgerald</a>
                    <p>Chief Executive Officer</p>
                </div>
                <div class="person">
                    <a href="https://linkedin.com/in/janesmith">Jane Smith</a>
                    <p>Chief Technology Officer</p>
                </div>
            </section>
        </body>
        </html>
        """

        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/about",
            official_domain="example.com",
        )

        assert len(candidates) == 2

        names = {c.raw_name for c in candidates}
        assert "Jimmy Fitzgerald" in names
        assert "Jane Smith" in names

        # Check that emails are None
        for c in candidates:
            assert c.email is None

    def test_extracts_titles_adjacent_to_names(self):
        """Test that titles are extracted from adjacent elements."""
        html = """
        <html>
        <body>
            <div class="team">
                <h2>Our Leadership</h2>
                <div class="card">
                    <a href="https://linkedin.com/in/bob">Bob Johnson</a>
                    <span>VP of Engineering</span>
                </div>
            </div>
        </body>
        </html>
        """

        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/team",
            official_domain="example.com",
        )

        assert len(candidates) >= 1
        bob = next((c for c in candidates if "Bob" in (c.raw_name or "")), None)
        assert bob is not None
        assert bob.title == "VP of Engineering"

    def test_extracts_from_card_structures(self):
        """Test extraction from structured person cards without LinkedIn."""
        html = """
        <html>
        <body>
            <section class="leadership">
                <h2>Executive Team</h2>
                <div class="team-member">
                    <h3>Alice Brown</h3>
                    <p class="title">Chief Financial Officer</p>
                </div>
                <div class="team-member">
                    <h3>Charlie Wilson</h3>
                    <p class="title">Chief Operating Officer</p>
                </div>
            </section>
        </body>
        </html>
        """

        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/leadership",
            official_domain="example.com",
        )

        assert len(candidates) >= 2
        names = {c.raw_name for c in candidates}
        assert "Alice Brown" in names
        assert "Charlie Wilson" in names

    def test_skips_non_people_pages(self):
        """Test that non-team pages don't produce candidates."""
        html = """
        <html>
        <body>
            <a href="https://linkedin.com/in/someone">Someone Name</a>
        </body>
        </html>
        """

        # Blog page - should not extract
        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/blog/post",
            official_domain="example.com",
        )

        assert len(candidates) == 0

    def test_deduplicates_by_name(self):
        """Test that duplicate names are removed."""
        html = """
        <html>
        <body>
            <div class="about">
                <h2>Meet the Team</h2>
                <a href="https://linkedin.com/in/john1">John Smith</a>
                <a href="https://linkedin.com/in/john2">John Smith</a>
            </div>
        </body>
        </html>
        """

        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/about",
            official_domain="example.com",
        )

        # Should only have one John Smith
        johns = [c for c in candidates if c.raw_name == "John Smith"]
        assert len(johns) == 1

    def test_paddle_style_page(self):
        """Test a realistic Paddle-style About page structure."""
        html = """
        <!DOCTYPE html>
        <html>
        <head><title>About Paddle</title></head>
        <body>
            <header>
                <nav><a href="/products">Products</a></nav>
            </header>
            <main>
                <section class="hero">
                    <h1>About Paddle</h1>
                    <p>We help software companies grow.</p>
                </section>

                <section class="leadership">
                    <h2>Meet the leadership team</h2>
                    <p>Our experienced leaders guide our mission.</p>

                    <div class="leader-grid">
                        <article class="leader-card">
                            <a href="https://linkedin.com/in/christianowner">Christian Owens</a>
                            <span class="role">Chief Executive Officer</span>
                            <span class="location">London</span>
                        </article>

                        <article class="leader-card">
                            <a href="https://linkedin.com/in/harrisonrose">Harrison Rose</a>
                            <span class="role">Co-Founder</span>
                            <span class="location">New York</span>
                        </article>

                        <article class="leader-card">
                            <a href="https://linkedin.com/in/sarahcfo">Sarah Johnson</a>
                            <span class="role">Chief Financial Officer</span>
                        </article>
                    </div>
                </section>
            </main>
        </body>
        </html>
        """

        candidates = extract_people_cards(
            html=html,
            source_url="https://paddle.com/about",
            official_domain="paddle.com",
        )

        assert len(candidates) >= 3

        names = {c.raw_name for c in candidates}
        assert "Christian Owens" in names
        assert "Harrison Rose" in names
        assert "Sarah Johnson" in names

        # Check source_type
        for c in candidates:
            assert c.source_type in ("people_card_linkedin", "people_card_structure")
            assert c.email is None
            assert c.source_url == "https://paddle.com/about"


class TestEdgeCases:
    """Edge case and error handling tests."""

    def test_empty_html(self):
        candidates = extract_people_cards(
            html="",
            source_url="https://example.com/about",
            official_domain="example.com",
        )
        assert candidates == []

    def test_malformed_html(self):
        html = "<html><body><div><a href='linkedin.com'>Name<</div></body>"
        # Should not crash
        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/team",
            official_domain="example.com",
        )
        # May or may not extract anything, but should not raise
        assert isinstance(candidates, list)

    def test_no_leadership_section(self):
        """Test page with LinkedIn links but no leadership section."""
        html = """
        <html>
        <body>
            <footer>
                <a href="https://linkedin.com/company/example">Follow us on LinkedIn</a>
            </footer>
        </body>
        </html>
        """

        candidates = extract_people_cards(
            html=html,
            source_url="https://example.com/about",
            official_domain="example.com",
        )

        # Should not extract company page links as people
        assert len(candidates) == 0
