# tests/test_url_filters.py
"""
Tests for URL filtering logic.

These tests verify that the URL filter correctly:
1. Blocks product pages, blog posts, and other non-team content
2. Allows legitimate team/leadership/about pages
3. Handles edge cases and ambiguous URLs
"""

import pytest

from src.extract.url_filters import (
    classify_url,
    is_allowed_url,
    is_blocked_url,
    is_people_page_url,
)


class TestBlockedUrls:
    """Test that non-team URLs are correctly blocked."""

    # =========================================================================
    # Product pages with misleading keywords
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            # Microsoft Teams / team products
            "https://www.summit7.us/teams-phone-system-and-audio-conferencing",
            "https://example.com/teams-integration",
            "https://example.com/microsoft-teams-setup",
            "https://example.com/products/teams-collaboration",
            # Thought leadership content (not leadership team)
            "https://www.coherehealth.com/thought-leadership",
            "https://example.com/thought-leadership/article-name",
            "https://example.com/leadership-insights/2024",
            "https://example.com/leadership-blog",
        ],
    )
    def test_blocks_product_pages_with_misleading_keywords(self, url):
        """Product pages containing 'team' or 'leadership' should be blocked."""
        is_blocked, reason = is_blocked_url(url)
        assert is_blocked, f"Expected {url} to be blocked, reason: {reason}"
        assert not is_people_page_url(url)

    # =========================================================================
    # Blog / Content pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/blog/some-article",
            "https://example.com/blog/2024/01/article",
            "https://example.com/blog/author/john-smith",
            "https://example.com/blog/tag/about-roadie",
            "https://example.com/blog/category/leadership",
            "https://www.summit7.us/blog/author/summit-7-leadership",
            "https://example.com/news/2024/announcement",
            "https://example.com/press-release/2024-funding",
            "https://example.com/podcast/episode-5",
            "https://example.com/webinar/leadership-tips",
        ],
    )
    def test_blocks_blog_and_content_pages(self, url):
        """Blog posts, articles, and content pages should be blocked."""
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked
        assert not is_people_page_url(url)

    # =========================================================================
    # Customer / Case study pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/customer-stories/acme-corp",
            "https://example.com/case-study/enterprise-deployment",
            "https://example.com/success-stories",
            "https://example.com/testimonials",
            "https://example.com/reviews/g2-crowd",
        ],
    )
    def test_blocks_customer_content_pages(self, url):
        """Customer stories and case studies contain third-party names."""
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked
        assert not is_people_page_url(url)

    # =========================================================================
    # Job / Career pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/careers",
            "https://example.com/jobs/software-engineer",
            "https://example.com/openings",
            "https://example.com/join-us",
            "https://example.com/work-with-us",
        ],
    )
    def test_blocks_career_pages(self, url):
        """Career pages list job openings, not team members."""
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked
        assert not is_people_page_url(url)

    # =========================================================================
    # Product / Feature pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/pricing",
            "https://example.com/product/enterprise",
            "https://example.com/features/analytics",
            "https://example.com/solutions/healthcare",
            "https://example.com/platform/overview",
        ],
    )
    def test_blocks_product_pages(self, url):
        """Product and feature pages should be blocked."""
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked
        assert not is_people_page_url(url)

    # =========================================================================
    # Legal / Support pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/privacy",
            "https://example.com/terms-of-service",
            "https://example.com/legal/privacy-policy",
            "https://example.com/support/contact",
            "https://example.com/help/getting-started",
            "https://example.com/docs/api-reference",
        ],
    )
    def test_blocks_legal_and_support_pages(self, url):
        """Legal and support pages should be blocked."""
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked
        assert not is_people_page_url(url)

    # =========================================================================
    # Localized/regional duplicates
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.shipbob.com/au/about/",
            "https://www.shipbob.com/uk/about/",
            "https://example.com/en-us/about",
            "https://example.com/de/team",
        ],
    )
    def test_blocks_localized_pages(self, url):
        """Localized pages are duplicates and should be blocked."""
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked
        assert not is_people_page_url(url)


class TestAllowedUrls:
    """Test that legitimate team pages are correctly allowed."""

    # =========================================================================
    # About pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/about",
            "https://example.com/about/",
            "https://example.com/about-us",
            "https://www.alloy.com/about",
            "https://www.middesk.com/about",
            "https://www.unit21.ai/company/about",
            "https://pando.ai/about-us",
        ],
    )
    def test_allows_about_pages(self, url):
        """About pages are primary sources for leadership info."""
        assert is_allowed_url(url)
        assert is_people_page_url(url)

    # =========================================================================
    # Team pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/team",
            "https://example.com/team/",
            "https://example.com/our-team",
            "https://example.com/the-team",
            "https://example.com/meet-the-team",
            "https://vorihealth.com/care-team",
        ],
    )
    def test_allows_team_pages(self, url):
        """Dedicated team pages should be allowed."""
        assert is_allowed_url(url)
        assert is_people_page_url(url)

    # =========================================================================
    # Leadership pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/leadership",
            "https://example.com/leadership/",
            "https://leantaas.com/company/leadership/",
            "https://example.com/executive-team",
            "https://example.com/executives",
            "https://example.com/our-leadership",
        ],
    )
    def test_allows_leadership_pages(self, url):
        """Leadership team pages should be allowed."""
        assert is_allowed_url(url)
        assert is_people_page_url(url)

    # =========================================================================
    # Company pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/company",
            "https://example.com/company/about",
            "https://example.com/company/team",
            "https://moov.io/company/",
        ],
    )
    def test_allows_company_pages(self, url):
        """Company overview pages often have leadership sections."""
        assert is_allowed_url(url)
        assert is_people_page_url(url)

    # =========================================================================
    # People / Staff pages
    # =========================================================================

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/people",
            "https://example.com/our-people",
            "https://example.com/staff",
            "https://example.com/founders",
        ],
    )
    def test_allows_people_pages(self, url):
        """Dedicated people pages should be allowed."""
        assert is_allowed_url(url)
        assert is_people_page_url(url)


class TestEdgeCases:
    """Test edge cases and ambiguous URLs."""

    def test_empty_url(self):
        """Empty URLs should be handled gracefully."""
        assert not is_people_page_url("")
        assert not is_people_page_url(None)

    def test_homepage(self):
        """Homepage should not be considered a people page."""
        # Homepages don't typically have team info in extractable format
        assert not is_people_page_url("https://example.com/")
        assert not is_people_page_url("https://example.com")

    def test_unknown_paths(self):
        """Unknown paths should be blocked by default (conservative)."""
        assert not is_people_page_url("https://example.com/random-path")
        assert not is_people_page_url("https://example.com/something-else")

    def test_blocklist_priority(self):
        """Blocklist should take priority over allowlist."""
        # This URL contains both "about" (allowed) and "blog" (blocked)
        url = "https://example.com/blog/about-us"
        assert not is_people_page_url(url)

        # This contains "team" but in product context
        url = "https://example.com/teams-phone/about"
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked


class TestClassifyUrl:
    """Test the classify_url diagnostic function."""

    def test_classify_blocked_url(self):
        """Blocked URLs should have correct classification."""
        result = classify_url("https://example.com/blog/article")
        assert result["is_blocked"]
        assert result["block_reason"] is not None
        assert not result["is_allowed"]
        assert not result["is_people_page"]

    def test_classify_allowed_url(self):
        """Allowed URLs should have correct classification."""
        result = classify_url("https://example.com/about")
        assert not result["is_blocked"]
        assert result["is_allowed"]
        assert result["is_people_page"]

    def test_classify_unknown_url(self):
        """Unknown URLs should be blocked by default."""
        result = classify_url("https://example.com/random")
        assert not result["is_blocked"]
        assert not result["is_allowed"]
        assert not result["is_people_page"]


class TestBatchTestCompanies:
    """Test URLs from the actual batch test that were problematic."""

    def test_summit7_product_page(self):
        """Summit 7's Teams phone page should be blocked."""
        url = "https://www.summit7.us/teams-phone-system-and-audio-conferencing"
        assert not is_people_page_url(url)
        is_blocked, reason = is_blocked_url(url)
        assert is_blocked
        assert "teams-phone" in reason

    def test_summit7_blog_author(self):
        """Summit 7's blog author page should be blocked."""
        url = "https://www.summit7.us/blog/author/summit-7-leadership"
        assert not is_people_page_url(url)
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked

    def test_summit7_about_page(self):
        """Summit 7's about page should be allowed."""
        url = "https://www.summit7.us/about"
        assert is_people_page_url(url)

    def test_cohere_thought_leadership(self):
        """Cohere Health's thought leadership page should be blocked."""
        url = "https://www.coherehealth.com/thought-leadership"
        assert not is_people_page_url(url)
        is_blocked, _ = is_blocked_url(url)
        assert is_blocked

    def test_shipbob_regional_pages(self):
        """ShipBob's regional about pages should be blocked (duplicates)."""
        assert not is_people_page_url("https://www.shipbob.com/au/about/")
        assert not is_people_page_url("https://www.shipbob.com/uk/about/")

    def test_shipbob_main_about(self):
        """ShipBob's main about page should be allowed."""
        assert is_people_page_url("https://www.shipbob.com/about/")

    def test_roadie_blog_tag(self):
        """Roadie's blog tag page should be blocked."""
        url = "https://www.roadie.com/blog/tag/about-roadie"
        assert not is_people_page_url(url)

    def test_leantaas_leadership(self):
        """LeanTaaS's leadership page should be allowed."""
        url = "https://leantaas.com/company/leadership/"
        assert is_people_page_url(url)

    def test_unit21_company_about(self):
        """Unit21's company about page should be allowed."""
        url = "https://www.unit21.ai/company/about"
        assert is_people_page_url(url)

    def test_moov_company(self):
        """Moov's company page should be allowed."""
        url = "https://moov.io/company/"
        assert is_people_page_url(url)

    def test_finboa_about(self):
        """FINBOA's about page should be allowed."""
        url = "https://www.finboa.com/about-finboa"
        # Note: "about-finboa" contains "about" so should be allowed
        assert is_people_page_url(url)

    def test_pando_about_us(self):
        """Pando's about-us page should be allowed."""
        url = "https://pando.ai/about-us"
        assert is_people_page_url(url)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
