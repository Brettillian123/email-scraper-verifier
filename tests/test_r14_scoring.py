# tests/test_r14_scoring.py
from __future__ import annotations

from src.scoring.icp import compute_icp

CFG = {
    "min_required": ["domain", "role_family"],
    "weights": {
        "role_family": {"Sales": 30, "Marketing": 25},
        "seniority": {"VP": 25, "Director": 15},
        "company_size": {"201-1000": 15},
        "industry_bonus": {"b2b_saas": 10},
        "tech_keywords": {"salesforce": 6, "hubspot": 4},
    },
    "cap": 100,
}


def test_missing_min_required_zero_score() -> None:
    res = compute_icp({"domain": "x.com"}, None, CFG)
    assert res.score == 0
    assert "missing_min_required" in res.reasons


def test_additive_score_and_reasons() -> None:
    person = {"domain": "acme.com", "role_family": "Sales", "seniority": "VP"}
    company = {
        "size": "201-1000",
        "industry": "b2b_saas",
        "attrs": {"tech_keywords": ["salesforce"]},
    }
    res = compute_icp(person, company, CFG)

    # 30 (Sales) + 25 (VP) + 15 (201-1000) + 10 (b2b_saas) + 6 (salesforce)
    assert res.score == 30 + 25 + 15 + 10 + 6

    joined = "|".join(res.reasons).lower()
    assert "role_family:sales+30" in joined
    assert "seniority:vp+25" in joined
    assert "company_size:201-1000+15" in joined
    assert "industry:b2b_saas+10" in joined
    assert "tech:salesforce+6" in joined


def test_clamp_cap() -> None:
    cfg = {**CFG, "cap": 50}
    person = {"domain": "acme.com", "role_family": "Sales", "seniority": "VP"}
    res = compute_icp(person, None, cfg)
    assert res.score == 50
