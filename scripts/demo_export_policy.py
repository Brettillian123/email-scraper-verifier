from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from src.db_suppression import is_email_suppressed
from src.export.policy import ExportPolicy
from src.export.roles import is_role_address


def build_demo_policy() -> ExportPolicy:
    """
    Build a simple demo ExportPolicy.

    This uses hard-coded thresholds that roughly match the examples
    from docs/icp-schema.yaml. In production you would usually load
    this from your ICP config instead.
    """
    cfg: dict[str, object] = {
        "allowed_statuses": ["valid", "risky_catch_all"],
        "min_icp_score_valid": 70,
        "min_icp_score_catch_all": 80,
        # These role/seniority/industry exclusions are separate from the
        # generic role-address classification done by is_role_address().
        "exclude_roles": ["student", "intern"],
        "exclude_seniority": ["junior"],
        "exclude_industries": ["education", "government"],
    }
    return ExportPolicy.from_config("demo", cfg)


def main(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    policy = build_demo_policy()

    samples = [
        {
            "label": "CEO high ICP (should pass)",
            "email": "ceo@crestwellpartners.com",
            "verify_status": "valid",
            "icp_score": 90,
            "role_family": "Executive",
            "seniority": "C",
            "industry": "saas",
        },
        {
            "label": "Role address (blocked by role_address)",
            "email": "info@crestwellpartners.com",
            "verify_status": "valid",
            "icp_score": 95,
            "role_family": "Operations",
            "seniority": "staff",
            "industry": "saas",
        },
        {
            "label": "Suppressed email (blocked by suppression)",
            "email": "blocked@example.com",
            "verify_status": "valid",
            "icp_score": 99,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "label": "Suppressed domain (blocked by suppression)",
            "email": "someone@suppressed-domain.test",
            "verify_status": "valid",
            "icp_score": 99,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "label": "Invalid verify_status (status_not_allowed)",
            "email": "lead@somewhere.com",
            "verify_status": "invalid",
            "icp_score": 95,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
        {
            "label": "Low ICP score (icp_below_threshold)",
            "email": "icp-low@somewhere.com",
            "verify_status": "valid",
            "icp_score": 30,
            "role_family": "Sales",
            "seniority": "Manager",
            "industry": "saas",
        },
    ]

    print(f"Using DB: {Path(db_path).resolve()}")
    print(f"Policy name: {policy.name}")
    print()

    for sample in samples:
        email = sample["email"]
        suppressed = is_email_suppressed(conn, email)
        role_addr = is_role_address(email)

        if suppressed:
            ok = False
            reason = "suppressed"
        elif role_addr:
            ok = False
            reason = "role_address"
        else:
            ok, reason = policy.should_export(sample)

        print(f"{email:35} -> {str(ok):5} ({reason})  # {sample['label']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo R19/O10 export policy decisions.")
    parser.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite database (default: data/dev.db)",
    )
    args = parser.parse_args()
    main(args.db)
