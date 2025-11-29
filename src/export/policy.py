"""
O10 — Export policies based on R18 verify_status + ICP score.

This module provides a small helper that decides whether a lead
(from v_emails_latest + joins) should be eligible for export.

Typical usage:

    from src.export.policy import ExportPolicy

    cfg = icp_cfg["export_policies"]["default"]
    policy = ExportPolicy.from_config("default", cfg)

    ok, reason = policy.should_export(lead_row)
    if ok:
        ... include in export ...
    else:
        ... log/drop with reason ...

Expected lead dict keys (all lower_snake_case):

    verify_status   # R18 canonical status ("valid", "risky_catch_all", "invalid",
                    # "unknown_timeout")
    icp_score       # numeric ICP score (0..100)
    role_family     # normalized role family (e.g. "Sales", "Engineering", "student")
    seniority       # normalized seniority (e.g. "C", "VP", "Manager", "junior")
    industry        # normalized industry label (e.g. "SaaS", "education")

Config shape (from docs/icp-schema.yaml → export_policies.default):

    export_policies:
      default:
        allowed_statuses:
          - valid
          - risky_catch_all
        min_icp_score_valid: 70
        min_icp_score_catch_all: 80
        exclude_roles:
          - student
          - intern
        exclude_seniority:
          - junior
        exclude_industries:
          - education
          - government

Reasons returned by should_export():

    "ok"
    "status_not_allowed"
    "icp_below_threshold"
    "role_excluded"
    "seniority_excluded"
    "industry_excluded"
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


def _normalize_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _to_lower_set(values: Iterable[str] | None) -> set[str]:
    if not values:
        return set()
    return {str(v).strip().lower() for v in values if str(v).strip()}


@dataclass
class ExportPolicy:
    """
    Export decision helper constructed from ICP config.

    You usually want to build this via .from_config(name, cfg).
    """

    name: str
    allowed_statuses: set[str]
    min_icp_score_valid: float | None
    min_icp_score_catch_all: float | None
    exclude_roles: set[str]
    exclude_seniority: set[str]
    exclude_industries: set[str]

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    @classmethod
    def from_config(cls, name: str, cfg: Mapping[str, Any]) -> ExportPolicy:
        """
        Build an ExportPolicy from a dict that looks like:

            {
              "allowed_statuses": ["valid", "risky_catch_all"],
              "min_icp_score_valid": 70,
              "min_icp_score_catch_all": 80,
              "exclude_roles": ["student", "intern"],
              "exclude_seniority": ["junior"],
              "exclude_industries": ["education", "government"],
            }
        """
        allowed_statuses = _to_lower_set(cfg.get("allowed_statuses") or [])

        def _num_or_none(key: str) -> float | None:
            val = cfg.get(key)
            if val is None:
                return None
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        return cls(
            name=name,
            allowed_statuses=allowed_statuses,
            min_icp_score_valid=_num_or_none("min_icp_score_valid"),
            min_icp_score_catch_all=_num_or_none("min_icp_score_catch_all"),
            exclude_roles=_to_lower_set(cfg.get("exclude_roles")),
            exclude_seniority=_to_lower_set(cfg.get("exclude_seniority")),
            exclude_industries=_to_lower_set(cfg.get("exclude_industries")),
        )

    # ------------------------------------------------------------------
    # Core decision logic
    # ------------------------------------------------------------------
    def should_export(self, lead: Mapping[str, Any]) -> tuple[bool, str]:
        """
        Decide whether a lead passes this policy.

        lead: row-like mapping from v_emails_latest join, expected keys:
          - verify_status
          - icp_score
          - role_family
          - seniority
          - industry

        Returns:
          (ok, reason)
            ok     : bool (True = include, False = exclude)
            reason : short machine-readable string as documented above.
        """
        status = _normalize_str(lead.get("verify_status"))
        status_l = (status or "").lower()

        # 1) Verify status must be in allowed_statuses.
        if not status_l or status_l not in self.allowed_statuses:
            return False, "status_not_allowed"

        # 2) ICP score thresholds, per-status.
        icp_raw = lead.get("icp_score")
        icp_score: float | None
        try:
            icp_score = float(icp_raw) if icp_raw is not None else None
        except (TypeError, ValueError):
            icp_score = None

        threshold: float | None = None
        if status_l == "valid":
            threshold = self.min_icp_score_valid
        elif status_l == "risky_catch_all":
            threshold = self.min_icp_score_catch_all

        if threshold is not None:
            # If score missing or below threshold, we exclude.
            if icp_score is None or icp_score < threshold:
                return False, "icp_below_threshold"

        # 3) Role-based exclusions.
        role_family = _normalize_str(lead.get("role_family"))
        if role_family and role_family.lower() in self.exclude_roles:
            return False, "role_excluded"

        # 4) Seniority exclusions.
        seniority = _normalize_str(lead.get("seniority"))
        if seniority and seniority.lower() in self.exclude_seniority:
            return False, "seniority_excluded"

        # 5) Industry exclusions.
        industry = _normalize_str(lead.get("industry"))
        if industry and industry.lower() in self.exclude_industries:
            return False, "industry_excluded"

        # 6) Passed all checks.
        return True, "ok"

    def is_exportable_row(
        self,
        *,
        email: str,
        verify_status: str | None,
        icp_score: float | None,
        extra: Mapping[str, Any],
    ) -> tuple[bool, str]:
        """
        R20-compatible export decision API.

        This is a thin wrapper around should_export(lead), which expects a
        row-like mapping. iter_exportable_leads/export_leads call this with
        the fields broken out plus the full row as `extra`.

        Parameters
        ----------
        email:
            Email address from v_emails_latest.email (not used by the current
            policy rules, but included for completeness / future use).
        verify_status:
            Canonical verification status from R18 (e.g. "valid",
            "risky_catch_all", "invalid", "unknown_timeout").
        icp_score:
            ICP score from v_emails_latest.icp_score (0..100).
        extra:
            The full row from v_emails_latest (sqlite3.Row or dict-like). May
            contain additional fields (role_family, seniority, industry, etc.)
            that should_export uses.

        Returns
        -------
        (ok, reason):
            ok is True if the row should be exported, False otherwise.
            reason is one of the short strings documented in should_export().
        """
        # Start from the underlying row mapping.
        lead = dict(extra)

        # Ensure core fields are present/overridden explicitly.
        # This keeps the policy logic centralized in should_export().
        lead["email"] = email
        lead["verify_status"] = verify_status
        lead["icp_score"] = icp_score

        return self.should_export(lead)


def load_policy(config: Mapping[str, Any], name: str = "default") -> ExportPolicy:
    """
    Convenience wrapper to load a named policy directly from the ICP config dict.

    Example:

        from src.config import load_icp_config
        from src.export.policy import load_policy

        icp_cfg = load_icp_config()
        policy = load_policy(icp_cfg, "default")

    Expects:

        config["export_policies"][name]  -> dict for ExportPolicy.from_config()
    """
    export_cfg = config.get("export_policies") or {}
    policy_cfg = export_cfg.get(name)
    if not isinstance(policy_cfg, dict):
        raise KeyError(f"export_policies.{name!r} not found or not a mapping")
    return ExportPolicy.from_config(name, policy_cfg)
