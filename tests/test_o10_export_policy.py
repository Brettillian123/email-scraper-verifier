from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.export.policy import ExportPolicy, load_policy


def _default_cfg() -> dict[str, Any]:
    """
    Mirror the O10 example in docs/icp-schema.yaml → export_policies.default.
    """
    return {
        "export_policies": {
            "default": {
                "allowed_statuses": ["valid", "risky_catch_all"],
                "min_icp_score_valid": 70,
                "min_icp_score_catch_all": 80,
                "exclude_roles": ["student", "intern"],
                "exclude_seniority": ["junior"],
                "exclude_industries": ["education", "government"],
            }
        }
    }


def _make_lead(
    *,
    verify_status: str,
    icp_score: float | int | None,
    role_family: str | None = None,
    seniority: str | None = None,
    industry: str | None = None,
) -> Mapping[str, Any]:
    return {
        "verify_status": verify_status,
        "icp_score": icp_score,
        "role_family": role_family,
        "seniority": seniority,
        "industry": industry,
    }


def test_load_policy_from_config() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    assert isinstance(policy, ExportPolicy)
    assert policy.allowed_statuses == {"valid", "risky_catch_all"}
    assert policy.min_icp_score_valid == 70
    assert policy.min_icp_score_catch_all == 80
    assert "student" in policy.exclude_roles
    assert "junior" in policy.exclude_seniority
    assert "education" in policy.exclude_industries


def test_valid_above_threshold_passes() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    lead = _make_lead(
        verify_status="valid",
        icp_score=85,
        role_family="Sales",
        seniority="Manager",
        industry="SaaS",
    )

    ok, reason = policy.should_export(lead)
    assert ok is True
    assert reason == "ok"


def test_valid_below_threshold_fails_icp() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    # valid but icp_score below 70
    lead = _make_lead(
        verify_status="valid",
        icp_score=65,
        role_family="Sales",
        seniority="Manager",
        industry="SaaS",
    )

    ok, reason = policy.should_export(lead)
    assert ok is False
    assert reason == "icp_below_threshold"


def test_risky_catch_all_requires_higher_threshold() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    # risky_catch_all with icp_score just below 80 → fail
    low = _make_lead(
        verify_status="risky_catch_all",
        icp_score=79,
        role_family="Engineering",
        seniority="Director",
        industry="SaaS",
    )
    ok_low, reason_low = policy.should_export(low)
    assert ok_low is False
    assert reason_low == "icp_below_threshold"

    # risky_catch_all with icp_score >= 80 → pass
    high = _make_lead(
        verify_status="risky_catch_all",
        icp_score=82,
        role_family="Engineering",
        seniority="Director",
        industry="SaaS",
    )
    ok_high, reason_high = policy.should_export(high)
    assert ok_high is True
    assert reason_high == "ok"


def test_invalid_or_unknown_status_always_fails() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    for status in ("invalid", "unknown_timeout", "", "garbage"):
        lead = _make_lead(
            verify_status=status,
            icp_score=99,
            role_family="Sales",
            seniority="C",
            industry="SaaS",
        )
        ok, reason = policy.should_export(lead)
        assert ok is False
        assert reason == "status_not_allowed"


def test_role_excluded() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    lead = _make_lead(
        verify_status="valid",
        icp_score=90,
        role_family="student",  # in exclude_roles
        seniority="IC",
        industry="SaaS",
    )

    ok, reason = policy.should_export(lead)
    assert ok is False
    assert reason == "role_excluded"


def test_seniority_excluded() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    lead = _make_lead(
        verify_status="valid",
        icp_score=90,
        role_family="Engineering",
        seniority="junior",  # in exclude_seniority
        industry="SaaS",
    )

    ok, reason = policy.should_export(lead)
    assert ok is False
    assert reason == "seniority_excluded"


def test_industry_excluded() -> None:
    cfg = _default_cfg()
    policy = load_policy(cfg, "default")

    lead = _make_lead(
        verify_status="valid",
        icp_score=90,
        role_family="Sales",
        seniority="Manager",
        industry="education",  # in exclude_industries
    )

    ok, reason = policy.should_export(lead)
    assert ok is False
    assert reason == "industry_excluded"
