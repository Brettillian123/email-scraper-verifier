# tests/test_quality_gates.py
"""
Tests for quality gates that prevent garbage data from being persisted.

These tests validate that:
  - Geographic locations are not treated as person names
  - Compliance acronyms are not treated as person names
  - Placeholder emails are filtered
  - Valid person names pass through
  
NOTE: Title validation has been tightened to require specific role keywords
(chief, ceo, director, manager, vp, etc.) to prevent marketing blurbs.
"""

from src.extract.quality_gates import (
    clean_title_if_invalid,
    is_compliance_term,
    is_geography_term,
    is_nav_boilerplate,
    is_placeholder_localpart,
    should_persist_as_person,
    validate_candidate_for_persistence,
    validate_person_name,
    validate_title,
)


class TestIsPlaceholderLocalpart:
    """Tests for placeholder email detection."""

    def test_classic_placeholders(self):
        assert is_placeholder_localpart("jdoe") is True
        assert is_placeholder_localpart("johndoe") is True
        assert is_placeholder_localpart("john.doe") is True
        assert is_placeholder_localpart("test") is True
        assert is_placeholder_localpart("example") is True
        assert is_placeholder_localpart("demo") is True
        assert is_placeholder_localpart("admin") is True

    def test_real_names_pass(self):
        assert is_placeholder_localpart("john.smith") is False
        assert is_placeholder_localpart("jane.williams") is False
        assert is_placeholder_localpart("bob") is False
        assert is_placeholder_localpart("alice.jones") is False

    def test_case_insensitive(self):
        assert is_placeholder_localpart("JDOE") is True
        assert is_placeholder_localpart("JohnDoe") is True
        assert is_placeholder_localpart("TEST") is True


class TestIsComplianceTerm:
    """Tests for compliance/standard acronym detection."""

    def test_common_compliance_terms(self):
        assert is_compliance_term("PCI DSS") is True
        assert is_compliance_term("pci dss") is True
        assert is_compliance_term("SOC 2") is True
        assert is_compliance_term("SOC2") is True
        assert is_compliance_term("HIPAA") is True
        assert is_compliance_term("GDPR") is True
        assert is_compliance_term("ISO 27001") is True

    def test_real_names_pass(self):
        assert is_compliance_term("John Smith") is False
        assert is_compliance_term("Jane Doe") is False
        assert is_compliance_term("Robert Johnson") is False


class TestIsGeographyTerm:
    """Tests for geographic location detection."""

    def test_us_cities(self):
        assert is_geography_term("San Francisco") is True
        assert is_geography_term("New York") is True
        assert is_geography_term("Los Angeles") is True
        assert is_geography_term("Chicago") is True
        assert is_geography_term("Austin") is True
        assert is_geography_term("Seattle") is True
        assert is_geography_term("Boston") is True

    def test_international_cities(self):
        assert is_geography_term("Buenos Aires") is True
        assert is_geography_term("London") is True
        assert is_geography_term("Paris") is True
        assert is_geography_term("Tokyo") is True
        assert is_geography_term("Sydney") is True
        assert is_geography_term("Singapore") is True

    def test_countries(self):
        assert is_geography_term("USA") is True
        assert is_geography_term("United States") is True
        assert is_geography_term("Canada") is True
        assert is_geography_term("Germany") is True

    def test_regions(self):
        assert is_geography_term("EMEA") is True
        assert is_geography_term("APAC") is True
        assert is_geography_term("North America") is True

    def test_real_names_pass(self):
        assert is_geography_term("John Smith") is False
        assert is_geography_term("Paris Hilton") is False  # Ambiguous but we allow
        # Actually Paris alone should be caught - let's be conservative
        assert is_geography_term("Paris") is True
        assert is_geography_term("Austin Powers") is False  # Full name should pass

    def test_case_insensitive(self):
        assert is_geography_term("SAN FRANCISCO") is True
        assert is_geography_term("buenos aires") is True
        assert is_geography_term("NEW YORK") is True


class TestIsNavBoilerplate:
    """Tests for navigation/boilerplate term detection."""

    def test_nav_terms(self):
        assert is_nav_boilerplate("Home") is True
        assert is_nav_boilerplate("About") is True
        assert is_nav_boilerplate("About Us") is True
        assert is_nav_boilerplate("Contact") is True
        assert is_nav_boilerplate("Team") is True
        assert is_nav_boilerplate("Careers") is True

    def test_real_names_pass(self):
        assert is_nav_boilerplate("John Smith") is False
        assert is_nav_boilerplate("Jane Doe") is False


class TestValidatePersonName:
    """Tests for full person name validation."""

    def test_valid_names(self):
        result = validate_person_name("John Smith")
        assert result.is_valid is True
        assert result.rejection_reason is None

        result = validate_person_name("Jane Doe")
        assert result.is_valid is True

        result = validate_person_name("Robert Johnson III")
        assert result.is_valid is True

    def test_geography_rejected(self):
        result = validate_person_name("San Francisco")
        assert result.is_valid is False
        assert result.rejection_reason == "geography_term"

        result = validate_person_name("Buenos Aires")
        assert result.is_valid is False
        assert result.rejection_reason == "geography_term"

    def test_compliance_rejected(self):
        result = validate_person_name("PCI DSS")
        assert result.is_valid is False
        assert result.rejection_reason == "compliance_term"

        result = validate_person_name("SOC 2")
        assert result.is_valid is False
        assert result.rejection_reason == "compliance_term"

    def test_boilerplate_rejected(self):
        result = validate_person_name("About Us")
        assert result.is_valid is False
        assert result.rejection_reason == "nav_boilerplate"

    def test_empty_rejected(self):
        result = validate_person_name("")
        assert result.is_valid is False
        assert result.rejection_reason == "empty_name"

        result = validate_person_name("   ")
        assert result.is_valid is False

    def test_acronym_detection(self):
        result = validate_person_name("ABC DEF")
        assert result.is_valid is False
        assert result.rejection_reason == "likely_acronym"


class TestValidateTitle:
    """
    Tests for job title validation.
    
    NOTE: Title validation now requires at least one role keyword
    (chief, ceo, director, manager, vp, head, partner, etc.) to prevent
    marketing blurbs from being treated as titles.
    """

    def test_valid_titles_with_role_keywords(self):
        """Titles with role keywords should pass."""
        result = validate_title("CEO")
        assert result.is_valid is True

        result = validate_title("Chief Executive Officer")
        assert result.is_valid is True

        result = validate_title("VP of Sales")
        assert result.is_valid is True
        
        result = validate_title("Engineering Manager")
        assert result.is_valid is True
        
        result = validate_title("Director of Operations")
        assert result.is_valid is True
        
        result = validate_title("Head of Marketing")
        assert result.is_valid is True

    def test_titles_without_role_keywords_rejected(self):
        """Titles without role keywords are now rejected to prevent marketing blurbs."""
        result = validate_title("Software Engineer")
        assert result.is_valid is False
        assert result.rejection_reason == "missing_role_keyword"
        
        result = validate_title("Data Scientist")
        assert result.is_valid is False
        assert result.rejection_reason == "missing_role_keyword"

    def test_geography_as_title_rejected(self):
        result = validate_title("San Francisco")
        assert result.is_valid is False
        assert result.rejection_reason == "geography_as_title"

        result = validate_title("Buenos Aires")
        assert result.is_valid is False
        assert result.rejection_reason == "geography_as_title"

    def test_empty_title_ok(self):
        result = validate_title("")
        assert result.is_valid is True

        result = validate_title(None)  # type: ignore
        assert result.is_valid is True


class TestValidateCandidateForPersistence:
    """Tests for full candidate validation."""

    def test_valid_candidate_with_name_and_email(self):
        result = validate_candidate_for_persistence(
            name="John Smith",
            email="john.smith@acme.com",
            title="CEO",
        )
        assert result.is_valid is True

    def test_valid_candidate_email_only(self):
        result = validate_candidate_for_persistence(
            name=None,
            email="john.smith@acme.com",
        )
        assert result.is_valid is True

    def test_geography_name_rejected(self):
        result = validate_candidate_for_persistence(
            name="San Francisco",
            email="marketplace@aircall.io",
        )
        assert result.is_valid is False
        assert "geography_term" in (result.rejection_reason or "")

    def test_placeholder_email_rejected(self):
        result = validate_candidate_for_persistence(
            name="John Doe",
            email="jdoe@acme.com",
        )
        assert result.is_valid is False
        assert "placeholder_email" in (result.rejection_reason or "")

    def test_no_name_or_email_rejected(self):
        result = validate_candidate_for_persistence(
            name=None,
            email=None,
        )
        assert result.is_valid is False
        assert result.rejection_reason == "no_name_or_email"


class TestShouldPersistAsPerson:
    """Tests for the main persistence decision function."""

    def test_ai_approved_with_valid_name_passes(self):
        """AI approval with valid name should pass (source URL checks still apply)."""
        assert should_persist_as_person(
            name="John Smith",
            email="test@acme.com",
            ai_approved=True,
        ) is True

    def test_valid_data_passes_without_ai(self):
        assert should_persist_as_person(
            name="John Smith",
            email="john.smith@acme.com",
            ai_approved=False,
        ) is True

    def test_geography_rejected_without_ai(self):
        assert should_persist_as_person(
            name="San Francisco",
            email="marketplace@aircall.io",
            ai_approved=False,
        ) is False

    def test_placeholder_email_with_invalid_name_rejected(self):
        """Placeholder email + invalid name should be rejected."""
        # Note: should_persist_as_person doesn't check placeholder emails directly
        # It relies on validate_candidate_for_persistence for that
        # This test checks that invalid names are rejected
        assert should_persist_as_person(
            name="San Francisco",  # Invalid geography name
            email="jdoe@acme.com",
            ai_approved=False,
        ) is False


class TestCleanTitleIfInvalid:
    """Tests for title cleaning."""

    def test_valid_title_with_role_keyword_unchanged(self):
        assert clean_title_if_invalid("CEO") == "CEO"
        assert clean_title_if_invalid("Engineering Manager") == "Engineering Manager"

    def test_title_without_role_keyword_cleared(self):
        """Titles without role keywords are cleared under the new rules."""
        assert clean_title_if_invalid("Software Engineer") is None

    def test_geography_title_cleared(self):
        assert clean_title_if_invalid("San Francisco") is None
        assert clean_title_if_invalid("Buenos Aires") is None

    def test_empty_title_returns_none(self):
        assert clean_title_if_invalid("") is None
        assert clean_title_if_invalid(None) is None  # type: ignore


class TestRealWorldCases:
    """Tests based on actual issues seen in production."""

    def test_aircall_san_francisco_case(self):
        """
        From batch run: marketplace@aircall.io was persisted with
        name "San Francisco" - this should be rejected.
        """
        result = validate_candidate_for_persistence(
            name="San Francisco",
            email="marketplace@aircall.io",
        )
        assert result.is_valid is False

    def test_lokalise_buenos_aires_case(self):
        """
        From batch run: real emails were persisted with
        name "Buenos Aires" - this should be rejected.
        """
        result = validate_candidate_for_persistence(
            name="Buenos Aires",
            email="hello@lokalise.com",
        )
        assert result.is_valid is False

    def test_crestwell_jdoe_case(self):
        """
        From batch run: jdoe@crestwellpartners.com was persisted
        as a real person - placeholder should be rejected.
        """
        result = validate_candidate_for_persistence(
            name="Jdoe",  # Even with "name" derived from localpart
            email="jdoe@crestwellpartners.com",
        )
        assert result.is_valid is False

    def test_aircall_pci_dss_case(self):
        """
        From batch run: marketplace@aircall.io was persisted with
        name "PCI DSS" - this should be rejected.
        """
        result = validate_candidate_for_persistence(
            name="PCI DSS",
            email="marketplace@aircall.io",
        )
        assert result.is_valid is False
