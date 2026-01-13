# tests/test_source_url_filtering.py
"""
Tests for source URL filtering in quality_gates.py

These tests verify that candidates from third-party sources
(customer stories, case studies, testimonials) are correctly blocked.
"""

import pytest

from src.extract.quality_gates import (
    is_blog_source_url,
    is_third_party_source_url,
    should_persist_as_person,
)


class TestIsThirdPartySourceUrl:
    """Tests for is_third_party_source_url()."""

    @pytest.mark.parametrize(
        "url",
        [
            # Customer stories
            "https://aircall.io/customer-stories/how-vuori-scaled/",
            "https://example.com/customer-story/acme-corp",
            # Case studies
            "https://example.com/case-studies/enterprise-win",
            "https://example.com/case-study/big-client",
            # Success stories
            "https://example.com/success-stories/awesome-client",
            "https://example.com/success-story/great-result",
            # Testimonials
            "https://example.com/testimonials/",
            "https://example.com/testimonial/john-smith",
            # Client stories
            "https://example.com/client-stories/",
            # Customer spotlight
            "https://example.com/customer-spotlight/featured",
            # Reviews
            "https://example.com/review/g2-crowd",
            "https://example.com/reviews/",
        ],
    )
    def test_blocks_third_party_urls(self, url: str):
        """Third-party content URLs should be blocked."""
        assert is_third_party_source_url(url) is True

    @pytest.mark.parametrize(
        "url",
        [
            # About pages
            "https://example.com/about",
            "https://example.com/about-us/",
            "https://example.com/company/about",
            # Team pages
            "https://example.com/team",
            "https://example.com/our-team",
            "https://example.com/leadership",
            # Press
            "https://example.com/press",
            "https://example.com/newsroom",
            # Contact
            "https://example.com/contact",
            # Homepage
            "https://example.com/",
            # Empty/None
            "",
            None,
        ],
    )
    def test_allows_employee_pages(self, url):
        """Employee-related pages should be allowed."""
        assert is_third_party_source_url(url) is False


class TestIsBlogSourceUrl:
    """Tests for is_blog_source_url()."""

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/blog/some-post",
            "https://example.com/blog/2024/01/post-title",
            "https://example.com/article/something",
            "https://example.com/articles/list",
            "https://example.com/post/my-post",
            "https://example.com/posts/",
        ],
    )
    def test_detects_blog_urls(self, url: str):
        """Blog-related URLs should be detected."""
        assert is_blog_source_url(url) is True

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/about",
            "https://example.com/team",
            "https://example.com/leadership",
            "https://example.com/press",
            "",
            None,
        ],
    )
    def test_non_blog_urls(self, url):
        """Non-blog URLs should not be flagged."""
        assert is_blog_source_url(url) is False


class TestShouldPersistAsPersonWithSourceUrl:
    """Tests for should_persist_as_person() with source_url parameter."""

    def test_blocks_customer_story_candidate_even_if_ai_approved(self):
        """
        The Chad Warren bug: AI approved a customer from a case study.
        Source URL filtering should block this.
        """
        result = should_persist_as_person(
            name="Chad Warren",
            email=None,
            title="Senior Manager of Customer Service",  # Vuori title
            ai_approved=True,  # AI incorrectly approved this
            source_url="https://aircall.io/customer-stories/how-vuori-scaled/",
        )
        assert result is False

    def test_blocks_case_study_candidate(self):
        """Candidates from case studies should be blocked."""
        result = should_persist_as_person(
            name="Jane Customer",
            email="jane@acme.com",
            title="VP Engineering",
            ai_approved=True,
            source_url="https://vendor.com/case-study/acme-corp",
        )
        assert result is False

    def test_allows_about_page_candidate(self):
        """Candidates from about pages should be allowed."""
        result = should_persist_as_person(
            name="Steve Cox",
            email=None,
            title="Chief Executive Officer",
            ai_approved=True,
            source_url="https://clari.com/about/",
        )
        assert result is True

    def test_allows_team_page_candidate(self):
        """Candidates from team pages should be allowed."""
        result = should_persist_as_person(
            name="John Smith",
            email=None,
            title="CTO",
            ai_approved=True,
            source_url="https://example.com/team/",
        )
        assert result is True

    def test_blocks_blog_author_without_leadership_title(self):
        """Blog authors without leadership titles should be blocked."""
        result = should_persist_as_person(
            name="Brittany Wolfe",
            email=None,
            title="Content Writer",  # Not a leadership title
            ai_approved=True,
            source_url="https://lokalise.com/blog/localization-tips",
        )
        assert result is False

    def test_allows_blog_author_with_leadership_title(self):
        """Blog authors WITH leadership titles should be allowed."""
        result = should_persist_as_person(
            name="John CEO",
            email=None,
            title="Chief Executive Officer",  # Leadership title
            ai_approved=True,
            source_url="https://example.com/blog/founder-message",
        )
        assert result is True

    def test_allows_press_page_candidate(self):
        """Press/newsroom pages are legitimate sources."""
        result = should_persist_as_person(
            name="Media Contact",
            email=None,
            title="VP Communications",
            ai_approved=True,
            source_url="https://example.com/press-room/",
        )
        assert result is True

    def test_no_source_url_falls_through_to_ai_check(self):
        """Without source_url, should fall through to AI/validation check."""
        # AI approved, no source URL - should pass
        result = should_persist_as_person(
            name="Valid Person",
            email=None,
            title="Engineer",
            ai_approved=True,
            source_url=None,
        )
        assert result is True

    def test_no_source_url_non_ai_applies_validation(self):
        """Without source_url and not AI approved, apply strict validation."""
        # Not AI approved, no source URL - validation applies
        result = should_persist_as_person(
            name="San Francisco",  # Invalid name
            email=None,
            title=None,
            ai_approved=False,
            source_url=None,
        )
        assert result is False


class TestRealWorldScenarios:
    """Integration tests based on real batch test failures."""

    def test_aircall_vuori_customer(self):
        """
        Real bug: Aircall's customer story page mentioned Chad Warren
        from Vuori, who was incorrectly extracted as Aircall employee.
        """
        result = should_persist_as_person(
            name="Chad Warren",
            email=None,
            title="Senior Manager of Customer Service",
            ai_approved=True,  # AI didn't catch this
            source_url="https://aircall.io/customer-stories/how-vuori-scaled-its-customer-service-team-with-aircall/",
        )
        assert result is False, "Should block customer story candidates"

    def test_lokalise_blog_author(self):
        """
        Real bug: Lokalise blog authors were extracted as leadership.
        """
        result = should_persist_as_person(
            name="Mia Comic",
            email="mia.comic@lokalise.com",
            title=None,  # No title or non-leadership title
            ai_approved=True,
            source_url="https://lokalise.com/blog/",
        )
        assert result is False, "Should block blog author without leadership title"

    def test_gong_testimonial_speaker(self):
        """
        Gong extracted testimonial speakers from product pages.
        These should be blocked if from testimonial-style pages.
        """
        # This would need the source URL to contain testimonial pattern
        # If it's from /platform/ or /solutions/ without explicit testimonial,
        # it's harder to catch. But if the URL contains testimonial, block it.
        result = should_persist_as_person(
            name="Jorge Bestard",
            email=None,
            title=None,
            ai_approved=True,
            source_url="https://gong.io/testimonials/customer-1",
        )
        assert result is False

    def test_clari_legitimate_leadership(self):
        """
        Clari's about page has legitimate leadership that should pass.
        """
        result = should_persist_as_person(
            name="Steve Cox",
            email=None,
            title="Chief Executive Officer",
            ai_approved=True,
            source_url="https://www.clari.com/about/",
        )
        assert result is True, "Legitimate leadership should pass"

    def test_sendoso_legitimate_leadership(self):
        """
        Sendoso's about-us page has legitimate leadership.
        """
        result = should_persist_as_person(
            name="Kris Rudeegraap",
            email=None,
            title="Co-Founder and Co-Chief Executive Officer",
            ai_approved=True,
            source_url="https://www.sendoso.com/about-us",
        )
        assert result is True, "Legitimate leadership should pass"


class TestBatchTestEdgeCases:
    """Tests for edge cases discovered in the 30-company batch test."""

    def test_roadie_blog_tag_about(self):
        """
        Roadie's /blog/tag/about-roadie page contains team info.
        This is a blog URL but tag pages about the company are different.
        """
        # The blog URL will be detected
        assert is_blog_source_url("https://www.roadie.com/blog/tag/about-roadie") is True

        # But with a leadership title, it should still pass
        result = should_persist_as_person(
            name="Marc Gorlin",
            email=None,
            title="Founder & CEO",
            ai_approved=True,
            source_url="https://www.roadie.com/blog/tag/about-roadie",
        )
        assert result is True, "Leadership from blog tag page should pass"

    def test_summit7_blog_author_leadership(self):
        """
        Summit7's /blog/author/summit-7-leadership page has leadership info.
        """
        assert is_blog_source_url("https://www.summit7.us/blog/author/summit-7-leadership") is True

        # With leadership title, should pass
        result = should_persist_as_person(
            name="Scott Edwards",
            email=None,
            title="CEO",
            ai_approved=True,
            source_url="https://www.summit7.us/blog/author/summit-7-leadership",
        )
        assert result is True

    def test_alloy_leadership_from_about(self):
        """
        Alloy's about page has valid leadership that should pass.
        """
        result = should_persist_as_person(
            name="Tommy Nicholas",
            email=None,
            title="Co-Founder & CEO",
            ai_approved=True,
            source_url="https://www.alloy.com/about",
        )
        assert result is True

    def test_emerge_company_about(self):
        """
        Emerge's /company/about page has leadership.
        """
        result = should_persist_as_person(
            name="Andrew Leto",
            email=None,
            title="Co-Founder & CEO",
            ai_approved=True,
            source_url="https://www.emergemarket.com/company/about",
        )
        assert result is True

    def test_shipbob_about_founders(self):
        """
        ShipBob's about page has founders info.
        """
        result = should_persist_as_person(
            name="Dhruv Saxena",
            email=None,
            title="Co-Founder & CEO",
            ai_approved=True,
            source_url="https://www.shipbob.com/about/",
        )
        assert result is True

    def test_security_page_compliance_info(self):
        """
        Security/compliance pages often list leadership for compliance reasons.
        """
        assert is_third_party_source_url("https://example.com/security") is False
        assert is_third_party_source_url("https://example.com/about/security") is False


class TestSmartFallbackIntegration:
    """Tests for the smart fallback mechanism in ai_candidates_wrapper."""

    def test_leadership_title_detection(self):
        """Test that leadership titles are correctly detected."""
        try:
            from src.extract.ai_candidates_wrapper import _has_leadership_title

            # C-suite
            assert _has_leadership_title("CEO") is True
            assert _has_leadership_title("Chief Executive Officer") is True
            assert _has_leadership_title("CTO") is True
            assert _has_leadership_title("CFO") is True

            # Founders
            assert _has_leadership_title("Founder") is True
            assert _has_leadership_title("Co-Founder & CEO") is True

            # VPs
            assert _has_leadership_title("VP of Engineering") is True
            assert _has_leadership_title("Vice President, Sales") is True

            # Directors
            assert _has_leadership_title("Director of Product") is True
            assert _has_leadership_title("Head of Marketing") is True

            # Non-leadership
            assert _has_leadership_title("Software Engineer") is False
            assert _has_leadership_title("Content Writer") is False
            assert _has_leadership_title(None) is False
        except ImportError:
            pytest.skip("ai_candidates_wrapper not available")

    def test_valid_name_structure(self):
        """Test that name validation works correctly."""
        try:
            from src.extract.ai_candidates_wrapper import _is_valid_name_structure

            # Valid names
            assert _is_valid_name_structure("John Smith") is True
            assert _is_valid_name_structure("Mary Jane Watson") is True
            assert _is_valid_name_structure("Jean-Pierre Dupont") is True
            assert _is_valid_name_structure("O'Brien Murphy") is True

            # Invalid names
            assert _is_valid_name_structure("CEO") is False  # Single word
            assert _is_valid_name_structure("Learn More") is False  # CTA
            assert _is_valid_name_structure("") is False
            assert _is_valid_name_structure(None) is False
        except ImportError:
            pytest.skip("ai_candidates_wrapper not available")
