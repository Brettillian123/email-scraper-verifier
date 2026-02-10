#!/usr/bin/env python3
"""
User management CLI for Email Scraper.

Usage:
    # Create a user
    python scripts/manage_users.py create --email admin@example.com --password "SecurePass123!" --tenant default

    # Create a superuser
    python scripts/manage_users.py create --email admin@example.com --password "SecurePass123!" --tenant default --superuser

    # List users
    python scripts/manage_users.py list

    # Grant superuser
    python scripts/manage_users.py grant-superuser --email admin@example.com

    # Revoke superuser
    python scripts/manage_users.py revoke-superuser --email admin@example.com

    # Set user limits
    python scripts/manage_users.py set-limits --email user@example.com --max-runs-per-day 100

    # Disable a user
    python scripts/manage_users.py disable --email user@example.com

    # Enable a user
    python scripts/manage_users.py enable --email user@example.com
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def cmd_create(args):
    """Create a new user."""
    from src.auth.core import create_user
    from src.db import get_conn

    # Auto-approve if superuser or explicitly approved
    is_approved = args.approved or args.superuser

    user, error = create_user(
        email=args.email,
        password=args.password,
        tenant_id=args.tenant,
        display_name=args.name,
        is_verified=True,
        is_approved=is_approved,
    )

    if not user:
        logger.error(f"Failed to create user: {error}")
        return 1

    status = "approved" if is_approved else "pending approval"
    logger.info(
        f"Created user: {user.email} (ID: {user.id}, tenant: {user.tenant_id}, status: {status})"
    )

    if args.superuser:
        conn = get_conn()
        try:
            conn.execute(
                "UPDATE users SET is_superuser = TRUE WHERE id = %s",
                (user.id,),
            )
            conn.commit()
            logger.info("Granted superuser privileges")
        finally:
            conn.close()

    return 0


def cmd_list(args):
    """List all users."""
    from src.db import get_conn

    conn = get_conn()
    try:
        # Filter by pending if requested
        if hasattr(args, "pending") and args.pending:
            cur = conn.execute(
                """
                SELECT id, email, tenant_id, display_name, is_active, is_superuser, 
                       is_approved, created_at, last_login_at
                FROM users
                WHERE is_approved = FALSE
                ORDER BY created_at DESC
                """
            )
        else:
            cur = conn.execute(
                """
                SELECT id, email, tenant_id, display_name, is_active, is_superuser, 
                       is_approved, created_at, last_login_at
                FROM users
                ORDER BY created_at DESC
                """
            )
        rows = cur.fetchall()

        if not rows:
            if hasattr(args, "pending") and args.pending:
                print("No pending users found.")
            else:
                print("No users found.")
            return 0

        print(f"\n{'ID':<20} {'Email':<30} {'Approved':<10} {'Active':<8} {'Super':<8} {'Created'}")
        print("-" * 110)

        for row in rows:
            user_id = row["id"][:18] + ".." if len(row["id"]) > 20 else row["id"]
            email = (
                row["email"][:28] + ".." if len(row["email"] or "") > 30 else (row["email"] or "")
            )
            approved = "Yes" if row.get("is_approved") else "PENDING"
            active = "Yes" if row["is_active"] else "No"
            superuser = "Yes" if row["is_superuser"] else "No"
            created = (row["created_at"] or "")[:10]

            # Highlight pending users
            if not row.get("is_approved"):
                print(
                    f"{user_id:<20} {email:<30} \033[93m{approved:<10}\033[0m {active:<8} {superuser:<8} {created}"
                )
            else:
                print(
                    f"{user_id:<20} {email:<30} {approved:<10} {active:<8} {superuser:<8} {created}"
                )

        print(f"\nTotal: {len(rows)} users")
        return 0

    finally:
        conn.close()


def cmd_grant_superuser(args):
    """Grant superuser privileges to a user."""
    from src.auth.core import get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_superuser = TRUE WHERE id = %s",
            (user.id,),
        )
        conn.commit()
        logger.info(f"Granted superuser privileges to {user.email}")
        return 0
    finally:
        conn.close()


def cmd_revoke_superuser(args):
    """Revoke superuser privileges from a user."""
    from src.auth.core import get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_superuser = FALSE WHERE id = %s",
            (user.id,),
        )
        conn.commit()
        logger.info(f"Revoked superuser privileges from {user.email}")
        return 0
    finally:
        conn.close()


def cmd_approve(args):
    """Approve a pending user."""
    from src.auth.core import get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    if user.is_approved:
        logger.info(f"User {user.email} is already approved")
        return 0

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_approved = TRUE WHERE id = %s",
            (user.id,),
        )
        conn.commit()
        logger.info(f"Approved user: {user.email}")
        return 0
    finally:
        conn.close()


def cmd_reject(args):
    """Reject and delete a pending user."""
    from src.auth.core import delete_user_sessions, get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    if user.is_approved and not args.force:
        logger.error(f"User {user.email} is already approved. Use --force to delete anyway.")
        return 1

    conn = get_conn()
    try:
        # Delete sessions first
        delete_user_sessions(user.id)

        # Delete user
        conn.execute("DELETE FROM user_limits WHERE user_id = %s", (user.id,))
        conn.execute("DELETE FROM users WHERE id = %s", (user.id,))
        conn.commit()
        logger.info(f"Rejected and deleted user: {user.email}")
        return 0
    finally:
        conn.close()


def cmd_set_limits(args):
    """Set user limits."""
    from src.auth.core import get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    # Build UPDATE statement dynamically based on provided args
    updates = []
    values = []

    limit_fields = [
        ("max_runs_per_day", args.max_runs_per_day),
        ("max_domains_per_run", args.max_domains_per_run),
        ("max_concurrent_runs", args.max_concurrent_runs),
        ("max_verifications_per_day", args.max_verifications_per_day),
        ("max_verifications_per_month", args.max_verifications_per_month),
        ("max_exports_per_day", args.max_exports_per_day),
        ("max_export_rows", args.max_export_rows),
    ]

    for field, value in limit_fields:
        if value is not None:
            updates.append(f"{field} = %s")
            values.append(value)

    if not updates:
        logger.error("No limits specified. Use --help for available options.")
        return 1

    values.append(user.id)

    conn = get_conn()
    try:
        # Ensure user_limits row exists
        conn.execute(
            """
            INSERT INTO user_limits (user_id, tenant_id)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user.id, user.tenant_id),
        )

        # Update limits
        conn.execute(
            f"UPDATE user_limits SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE user_id = %s",
            values,
        )
        conn.commit()

        logger.info(f"Updated limits for {user.email}:")
        for field, value in limit_fields:
            if value is not None:
                logger.info(f"  {field}: {value}")

        return 0
    finally:
        conn.close()


def cmd_disable(args):
    """Disable a user account."""
    from src.auth.core import delete_user_sessions, get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_active = FALSE WHERE id = %s",
            (user.id,),
        )
        conn.commit()

        # Also clear their sessions
        delete_user_sessions(user.id)

        logger.info(f"Disabled user: {user.email}")
        return 0
    finally:
        conn.close()


def cmd_enable(args):
    """Enable a user account."""
    from src.db import get_conn

    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, email FROM users WHERE LOWER(email) = LOWER(%s)",
            (args.email,),
        )
        row = cur.fetchone()

        if not row:
            logger.error(f"User not found: {args.email}")
            return 1

        conn.execute(
            "UPDATE users SET is_active = TRUE WHERE id = %s",
            (row["id"],),
        )
        conn.commit()

        logger.info(f"Enabled user: {row['email']}")
        return 0
    finally:
        conn.close()


def cmd_reset_password(args):
    """Reset a user's password."""
    from src.auth.core import delete_user_sessions, get_user_by_email, hash_password
    from src.db import get_conn

    user = get_user_by_email(args.email)
    if not user:
        logger.error(f"User not found: {args.email}")
        return 1

    password_hash = hash_password(args.password)

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET password_hash = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
            (password_hash, user.id),
        )
        conn.commit()

        # Clear existing sessions
        delete_user_sessions(user.id)

        logger.info(f"Password reset for: {user.email}")
        logger.info("All existing sessions have been invalidated.")
        return 0
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="User management CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Create user
    create_parser = subparsers.add_parser("create", help="Create a new user")
    create_parser.add_argument("--email", required=True, help="User email")
    create_parser.add_argument("--password", required=True, help="User password")
    create_parser.add_argument("--tenant", default="default", help="Tenant ID (default: default)")
    create_parser.add_argument("--name", help="Display name")
    create_parser.add_argument(
        "--superuser", action="store_true", help="Grant superuser privileges (auto-approves)"
    )
    create_parser.add_argument("--approved", action="store_true", help="Pre-approve the user")
    create_parser.set_defaults(func=cmd_create)

    # List users
    list_parser = subparsers.add_parser("list", help="List all users")
    list_parser.add_argument("--pending", action="store_true", help="Show only pending users")
    list_parser.set_defaults(func=cmd_list)

    # Approve user
    approve_parser = subparsers.add_parser("approve", help="Approve a pending user")
    approve_parser.add_argument("--email", required=True, help="User email")
    approve_parser.set_defaults(func=cmd_approve)

    # Reject user
    reject_parser = subparsers.add_parser("reject", help="Reject and delete a pending user")
    reject_parser.add_argument("--email", required=True, help="User email")
    reject_parser.add_argument("--force", action="store_true", help="Force delete even if approved")
    reject_parser.set_defaults(func=cmd_reject)

    # Grant superuser
    grant_parser = subparsers.add_parser("grant-superuser", help="Grant superuser privileges")
    grant_parser.add_argument("--email", required=True, help="User email")
    grant_parser.set_defaults(func=cmd_grant_superuser)

    # Revoke superuser
    revoke_parser = subparsers.add_parser("revoke-superuser", help="Revoke superuser privileges")
    revoke_parser.add_argument("--email", required=True, help="User email")
    revoke_parser.set_defaults(func=cmd_revoke_superuser)

    # Set limits
    limits_parser = subparsers.add_parser("set-limits", help="Set user limits")
    limits_parser.add_argument("--email", required=True, help="User email")
    limits_parser.add_argument("--max-runs-per-day", type=int, help="Max runs per day")
    limits_parser.add_argument("--max-domains-per-run", type=int, help="Max domains per run")
    limits_parser.add_argument("--max-concurrent-runs", type=int, help="Max concurrent runs")
    limits_parser.add_argument(
        "--max-verifications-per-day", type=int, help="Max verifications per day"
    )
    limits_parser.add_argument(
        "--max-verifications-per-month", type=int, help="Max verifications per month"
    )
    limits_parser.add_argument("--max-exports-per-day", type=int, help="Max exports per day")
    limits_parser.add_argument("--max-export-rows", type=int, help="Max rows per export")
    limits_parser.set_defaults(func=cmd_set_limits)

    # Disable user
    disable_parser = subparsers.add_parser("disable", help="Disable a user account")
    disable_parser.add_argument("--email", required=True, help="User email")
    disable_parser.set_defaults(func=cmd_disable)

    # Enable user
    enable_parser = subparsers.add_parser("enable", help="Enable a user account")
    enable_parser.add_argument("--email", required=True, help="User email")
    enable_parser.set_defaults(func=cmd_enable)

    # Reset password
    reset_parser = subparsers.add_parser("reset-password", help="Reset a user's password")
    reset_parser.add_argument("--email", required=True, help="User email")
    reset_parser.add_argument("--password", required=True, help="New password")
    reset_parser.set_defaults(func=cmd_reset_password)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
