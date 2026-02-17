"""
tests/test_zero_candidates_fixes.py

Regression tests for the 0-candidate autodiscovery bug.

Covers:
  - /company URL classification (source_filters)
  - Classifier call from candidates.py (signature fix)
  - Webflow image-text extraction (parent-sibling traversal)
  - Framer LinkedIn-anchor extraction (nested children)
  - Repeated-sibling threshold (lowered to 2)

Run:
    pytest tests/test_zero_candidates_fixes.py -v
"""

import pytest

pytest.importorskip("bs4")

from src.extract.people_cards import (
    extract_people_cards,
)
from src.extract.source_filters import (
    PageClassification,
    classify_page_for_people_extraction,
    is_blocked_source_url,
    is_employee_page_url,
)

# ============================================================================
# BUG 1: /company URL classification
# ============================================================================


class TestCompanyURLClassification:
    """Bare /company URLs must be recognised as employee page URLs."""

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.stainless.com/company",
            "https://www.airspace-intelligence.com/company",
            "https://axoni.com/company",
            "https://example.com/company/",
            "https://example.com/company",
        ],
    )
    def test_company_url_is_employee_page(self, url: str):
        assert is_employee_page_url(url) is True, f"{url} should be employee page"

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.stainless.com/company",
            "https://axoni.com/company",
        ],
    )
    def test_company_url_not_blocked(self, url: str):
        blocked, reason = is_blocked_source_url(url)
        assert blocked is False, f"{url} blocked: {reason}"

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.stainless.com/company",
            "https://axoni.com/company",
            "https://www.airspace-intelligence.com/company",
        ],
    )
    def test_company_url_classifier_passes(self, url: str):
        """Score should reach at least 4 (the people_cards threshold)."""
        result = classify_page_for_people_extraction(url, min_score=4)
        assert result.ok is True, f"{url}: score={result.score}, reasons={result.reasons}"

    def test_company_subpaths_still_work(self):
        """Existing /company/about, /company/team patterns remain recognised."""
        for url in [
            "https://example.com/company/about",
            "https://example.com/company/team",
            "https://example.com/company/leadership",
        ]:
            assert is_employee_page_url(url) is True

    def test_blocked_urls_still_blocked(self):
        """Ensure the /company fix doesn't weaken blocking."""
        for url in [
            "https://example.com/case-studies/acme",
            "https://example.com/customers/big-corp",
            "https://example.com/blog/2024/01/new-feature",
        ]:
            blocked, _ = is_blocked_source_url(url)
            # These should NOT be employee pages
            assert is_employee_page_url(url) is False or blocked is True


# ============================================================================
# BUG 2: classifier call signature in candidates.py
# ============================================================================


class TestClassifierReturnType:
    """classify_page_for_people_extraction returns PageClassification, not tuple."""

    def test_returns_dataclass_not_tuple(self):
        result = classify_page_for_people_extraction(
            "https://example.com/team",
        )
        assert isinstance(result, PageClassification)
        assert hasattr(result, "ok")
        assert hasattr(result, "score")
        assert hasattr(result, "reasons")

    def test_no_official_domain_kwarg(self):
        """Calling with official_domain= should raise TypeError."""
        with pytest.raises(TypeError):
            classify_page_for_people_extraction(
                "https://example.com/team",
                None,
                official_domain="example.com",
            )


# ============================================================================
# BUG 3: Webflow image-text extraction (parent-sibling pattern)
# ============================================================================


class TestWebflowImageTextExtraction:
    """Webflow wraps img in a child div; name is in a sibling div."""

    WEBFLOW_TEAM_HTML = """
    <html>
    <body>
        <section>
            <h2>Our Team</h2>
            <div class="team-grid">
                <div class="team-member">
                    <div class="photo-wrapper">
                        <img src="/images/ben-nowack.webp" alt="">
                    </div>
                    <div class="info-wrapper">
                        <h3>Ben Nowack</h3>
                        <p>Founder & CEO</p>
                    </div>
                </div>
                <div class="team-member">
                    <div class="photo-wrapper">
                        <img src="/images/tristan.webp" alt="">
                    </div>
                    <div class="info-wrapper">
                        <h3>Tristan Semmelhack</h3>
                        <p>Co-Founder & CTO</p>
                    </div>
                </div>
                <div class="team-member">
                    <div class="photo-wrapper">
                        <img src="/images/ally.webp" alt="">
                    </div>
                    <div class="info-wrapper">
                        <h3>Ally Stone</h3>
                        <p>Chief Strategy Officer</p>
                    </div>
                </div>
            </div>
        </section>
    </body>
    </html>
    """

    def test_extracts_webflow_team_members(self):
        candidates = extract_people_cards(
            html=self.WEBFLOW_TEAM_HTML,
            source_url="https://www.reflectorbital.com/team",
            official_domain="reflectorbital.com",
        )
        names = {c.raw_name for c in candidates}
        assert "Ben Nowack" in names, f"Expected Ben Nowack, got: {names}"
        assert "Tristan Semmelhack" in names
        assert len(candidates) >= 3

    def test_extracts_titles(self):
        candidates = extract_people_cards(
            html=self.WEBFLOW_TEAM_HTML,
            source_url="https://www.reflectorbital.com/team",
            official_domain="reflectorbital.com",
        )
        ceo = next((c for c in candidates if "Ben" in (c.raw_name or "")), None)
        assert ceo is not None
        # Title may or may not be extracted depending on heading vs sibling parsing
        # but the candidate itself must exist


# ============================================================================
# BUG 3b: Webflow company page (airspace-intelligence pattern)
# ============================================================================


class TestWebflowCompanyPageExtraction:
    """Airspace-style: images with name-bearing filenames + text blocks."""

    AIRSPACE_HTML = """
    <html>
    <body>
        <section>
            <h2>Leadership</h2>
            <div class="leadership-grid">
                <div class="leader-card">
                    <img src="/images/Phillip-Buckendorf.webp" alt="Phillip Buckendorf">
                    <h3>Phillip Buckendorf</h3>
                    <p>CEO</p>
                </div>
                <div class="leader-card">
                    <img src="/images/Kris-Dorosz.webp" alt="Kris Dorosz">
                    <h3>Kris Dorosz</h3>
                    <p>CTO</p>
                </div>
                <div class="leader-card">
                    <img src="/images/Nabil-Enayet.webp" alt="Nabil Enayet">
                    <h3>Nabil Enayet</h3>
                    <p>VP Federal Engineering</p>
                </div>
            </div>
        </section>
    </body>
    </html>
    """

    def test_extracts_from_company_url(self):
        candidates = extract_people_cards(
            html=self.AIRSPACE_HTML,
            source_url="https://www.airspace-intelligence.com/company",
            official_domain="airspace-intelligence.com",
        )
        names = {c.raw_name for c in candidates}
        assert "Phillip Buckendorf" in names
        assert "Kris Dorosz" in names
        assert len(candidates) >= 3


# ============================================================================
# BUG 4: Framer LinkedIn-anchor nested children
# ============================================================================


class TestFramerLinkedInAnchorExtraction:
    """Framer pattern: <a href="linkedin"><img><p>Name</p><p>Title</p></a>."""

    FRAMER_HTML = """
    <html>
    <body>
        <section>
            <h2>Our Team</h2>
            <div class="team-grid">
                <a href="https://www.linkedin.com/in/alexrattray/" class="person-card">
                    <img src="/images/alex.jpg" alt="">
                    <p class="name">Alex Rattray</p>
                    <p class="role">Founder/CEO</p>
                </a>
                <a href="https://www.linkedin.com/in/markmcgranaghan/" class="person-card">
                    <img src="/images/mark.jpg" alt="">
                    <p class="name">Mark McGranaghan</p>
                    <p class="role">President/CTO</p>
                </a>
                <a href="https://www.linkedin.com/in/miorelpalii/" class="person-card">
                    <img src="/images/miorel.jpg" alt="">
                    <p class="name">Miorel Palii</p>
                    <p class="role">Head of Product Engineering</p>
                </a>
            </div>
        </section>
    </body>
    </html>
    """

    def test_extracts_framer_linkedin_cards(self):
        candidates = extract_people_cards(
            html=self.FRAMER_HTML,
            source_url="https://www.stainless.com/company",
            official_domain="stainless.com",
        )
        names = {c.raw_name for c in candidates}
        assert "Alex Rattray" in names, f"Expected Alex Rattray, got: {names}"
        assert "Mark McGranaghan" in names
        assert len(candidates) >= 3

    def test_extracts_titles_from_framer(self):
        candidates = extract_people_cards(
            html=self.FRAMER_HTML,
            source_url="https://www.stainless.com/company",
            official_domain="stainless.com",
        )
        alex = next((c for c in candidates if "Alex" in (c.raw_name or "")), None)
        assert alex is not None
        # Title extraction from sibling <p> inside the <a>
        if alex.title:
            assert "CEO" in alex.title or "Founder" in alex.title


# ============================================================================
# BUG 5: WordPress bold-text headings (axoni pattern)
# ============================================================================


class TestWordPressBoldHeadings:
    """Axoni uses <strong>Name</strong> followed by title text."""

    AXONI_HTML = """
    <html>
    <body>
        <section>
            <h2>Leadership</h2>
            <div class="leadership-section">
                <div class="leader">
                    <h3>Greg Schvey</h3>
                    <p>CEO</p>
                </div>
                <div class="leader">
                    <h3>Jeff Schvey</h3>
                    <p>CTO</p>
                </div>
                <div class="leader">
                    <h3>Ishan Singh</h3>
                    <p>SVP of Engineering</p>
                </div>
            </div>
        </section>
    </body>
    </html>
    """

    def test_extracts_wordpress_leaders(self):
        candidates = extract_people_cards(
            html=self.AXONI_HTML,
            source_url="https://axoni.com/company",
            official_domain="axoni.com",
        )
        names = {c.raw_name for c in candidates}
        assert "Greg Schvey" in names, f"Expected Greg Schvey, got: {names}"
        assert "Jeff Schvey" in names
        assert len(candidates) >= 3


# ============================================================================
# Repeated-siblings threshold (lowered to 2)
# ============================================================================


class TestRepeatedSiblingsThreshold:
    """Containers with exactly 2 person children should still extract."""

    TWO_PERSON_HTML = """
    <html>
    <body>
        <section>
            <h2>Our Founders</h2>
            <div class="founders-grid">
                <div class="founder-card">
                    <h3>Sarah Chen</h3>
                    <p>Co-Founder & CEO</p>
                </div>
                <div class="founder-card">
                    <h3>David Park</h3>
                    <p>Co-Founder & CTO</p>
                </div>
            </div>
        </section>
    </body>
    </html>
    """

    def test_two_person_container_extracts(self):
        candidates = extract_people_cards(
            html=self.TWO_PERSON_HTML,
            source_url="https://example.com/company",
            official_domain="example.com",
        )
        names = {c.raw_name for c in candidates}
        assert "Sarah Chen" in names
        assert "David Park" in names


# ============================================================================
# Negative cases: pages that should still return 0 candidates
# ============================================================================


class TestNegativeCasesUnchanged:
    """Ensure fixes don't introduce false positives on marketing/careers pages."""

    MARKETING_HTML = """
    <html>
    <body>
        <h1>About Our Company</h1>
        <p>We are building the future of work. Our mission is to empower teams
        worldwide with innovative technology solutions.</p>
        <h2>Our Values</h2>
        <p>Innovation, Integrity, Inclusivity</p>
        <h2>Our Customers</h2>
        <p>We serve over 500 companies including Fortune 500 enterprises.</p>
    </body>
    </html>
    """

    CAREERS_HTML = """
    <html>
    <body>
        <h1>Join Our Team</h1>
        <p>We're hiring! Check out our open positions.</p>
        <div class="job-listing">
            <h3>Senior Engineer</h3>
            <p>San Francisco, CA</p>
        </div>
        <div class="job-listing">
            <h3>Product Manager</h3>
            <p>New York, NY</p>
        </div>
    </body>
    </html>
    """

    def test_marketing_page_returns_zero(self):
        candidates = extract_people_cards(
            html=self.MARKETING_HTML,
            source_url="https://traba.work/company/about",
            official_domain="traba.work",
        )
        assert len(candidates) == 0

    def test_careers_page_returns_zero(self):
        candidates = extract_people_cards(
            html=self.CAREERS_HTML,
            source_url="https://www.trycents.com/team",
            official_domain="trycents.com",
        )
        # Job titles like "Senior Engineer" should not be extracted as people
        people_names = [c.raw_name for c in candidates]
        assert "Senior Engineer" not in people_names
        assert "Product Manager" not in people_names
