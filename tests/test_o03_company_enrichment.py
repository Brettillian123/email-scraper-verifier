# tests/test_o03_company_enrichment.py
import pytest

from src.ingest.company_enrich import enrich_company_from_text


def test_enrich_size_industry_tech_from_html():
    html = """
    <html>
      <body>
        <h1>About Us</h1>
        <p>We are a B2B SaaS platform powering healthcare workflows for clinics.</p>
        <p>We're a team of 51–200 employees distributed across the US.</p>
        <p>Our stack includes Salesforce and AWS, and we also use HubSpot for
        marketing automation.</p>
      </body>
    </html>
    """
    got = enrich_company_from_text(html)
    assert got.get("size_bucket") == "51-200"
    # Industry labels can include both categories
    assert "B2B SaaS" in got.get("industry", [])
    assert "Healthcare" in got.get("industry", [])
    # Tech signals
    tech = got.get("tech", [])
    assert "Salesforce" in tech
    assert "AWS" in tech
    assert "HubSpot" in tech


@pytest.mark.parametrize(
    "text,expected_bucket",
    [
        ("We have 11-50 employees worldwide.", "11-50"),
        ("Company size: 200+ employees and growing!", "201-1000"),
    ],
)
def test_param_buckets_range_and_plus(text, expected_bucket):
    got = enrich_company_from_text(text)
    assert got.get("size_bucket") == expected_bucket
