# tests/test_o02_title_normalization.py
import pytest

from src.ingest.title_norm import canonicalize


@pytest.mark.parametrize(
    "title_norm,exp_role,exp_seniority",
    [
        # Executives
        ("Chief Revenue Officer", "Sales", "C"),
        ("CEO", "Executive", "C"),
        ("CTO", "Engineering", "C"),
        ("General Counsel", "Legal", "C"),  # from docs/title_map.yaml
        # VP / Director / Manager ladders
        ("VP, Sales", "Sales", "VP"),
        ("SVP, Sales", "Sales", "VP"),
        ("Head of Sales", "Sales", "Director"),
        ("Director of Marketing", "Marketing", "Director"),
        ("Sales Manager", "Sales", "Manager"),
        # Functional families + IC default
        ("Product Manager", "Product", "IC"),
        ("Principal Data Scientist", "Data", "IC"),
        ("Customer Success Manager", "Customer Success", "Manager"),
        ("IT Systems Administrator", "IT", "IC"),
        ("Security Architect", "Security", "IC"),
        # Fallbacks / edge cases
        (None, "General Management", "IC"),
        ("", "General Management", "IC"),
        ("Unclear Thing", "General Management", "IC"),
    ],
)
def test_canonicalize_title(title_norm, exp_role, exp_seniority):
    role, seniority = canonicalize(title_norm)
    assert (role, seniority) == (exp_role, exp_seniority)
