# src/auth/ses.py
"""
AWS SES email sender for authentication flows.

Sends verification codes and password reset emails via Amazon SES.
Requires:
  - boto3 (already in requirements.txt)
  - SES domain verified for the FROM address
  - SES production access approved (out of sandbox)

Configuration (env vars):
  SES_FROM_EMAIL      - Sender address (default: noreply@account.crestwelliq.com)
  SES_FROM_NAME       - Sender display name (default: CrestwellIQ)
  SES_AWS_REGION      - AWS region for SES (default: us-east-1)
  AWS_ACCESS_KEY_ID   - (standard boto3 credential)
  AWS_SECRET_ACCESS_KEY
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SES_FROM_EMAIL = os.getenv("SES_FROM_EMAIL", "noreply@account.crestwelliq.com")
SES_FROM_NAME = os.getenv("SES_FROM_NAME", "CrestwellIQ")
SES_AWS_REGION = os.getenv("SES_AWS_REGION", "us-east-1")
APP_NAME = os.getenv("APP_NAME", "CrestwellIQ")

# Lazy-initialized SES client (avoids import-time boto3 calls)
_ses_client: Any = None


def _get_ses_client() -> Any:
    """Get or create the boto3 SES client (lazy singleton)."""
    global _ses_client
    if _ses_client is None:
        import boto3
        _ses_client = boto3.client("ses", region_name=SES_AWS_REGION)
    return _ses_client


# ---------------------------------------------------------------------------
# Email Templates
# ---------------------------------------------------------------------------

def _verification_code_html(code: str, expiry_minutes: int = 15) -> str:
    """Build HTML body for the verification code email."""
    body_style = (
        "margin:0; padding:0; background-color:#f4f5f7;"
        " font-family:-apple-system,BlinkMacSystemFont,"
        "'Segoe UI',Roboto,sans-serif;"
    )
    table_bg = "background-color:#f4f5f7; padding:40px 0;"
    card_style = (
        "background:#ffffff; border-radius:12px;"
        " box-shadow:0 2px 8px rgba(0,0,0,0.08);"
        " overflow:hidden;"
    )
    header_style = (
        "background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);"
        " padding:32px 40px; text-align:center;"
    )
    code_box_style = (
        "background:#f0f0ff; border:2px dashed #667eea;"
        " border-radius:8px; padding:20px;"
        " text-align:center; margin:0 0 24px;"
    )
    code_text_style = (
        "font-size:36px; font-weight:700; letter-spacing:8px;"
        " color:#1a1a2e; font-family:'Courier New',monospace;"
    )
    footer_style = (
        "padding:20px 40px; background:#fafafa;"
        " border-top:1px solid #eee; text-align:center;"
    )
    return f"""\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="{body_style}">
  <table width="100%" cellpadding="0" cellspacing="0" style="{table_bg}">
    <tr>
      <td align="center">
        <table width="480" cellpadding="0" cellspacing="0" style="{card_style}">
          <!-- Header -->
          <tr>
            <td style="{header_style}">
              <h1 style="margin:0; color:#ffffff; font-size:24px; font-weight:700;">{APP_NAME}</h1>
            </td>
          </tr>
          <!-- Body -->
          <tr>
            <td style="padding:40px;">
              <h2 style="margin:0 0 8px; color:#1a1a2e; font-size:20px;">Verify your email</h2>
              <p style="margin:0 0 24px; color:#555; font-size:15px; line-height:1.5;">
                Enter this code to complete your registration:
              </p>
              <div style="{code_box_style}">
                <span style="{code_text_style}">{code}</span>
              </div>
              <p style="margin:0 0 8px; color:#888; font-size:13px;">
                This code expires in <strong>{expiry_minutes} minutes</strong>.
              </p>
              <p style="margin:0; color:#888; font-size:13px;">
                If you didn't create an account, you can safely ignore this email.
              </p>
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="{footer_style}">
              <p style="margin:0; color:#aaa; font-size:12px;">
                &copy; {APP_NAME} &middot; This is an automated message, please do not reply.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _verification_code_text(code: str, expiry_minutes: int = 15) -> str:
    """Build plain-text body for the verification code email."""
    return (
        f"{APP_NAME} - Email Verification\n"
        f"{'=' * 40}\n\n"
        f"Your verification code is: {code}\n\n"
        f"This code expires in {expiry_minutes} minutes.\n\n"
        f"If you didn't create an account, you can safely ignore this email.\n"
    )


def _password_reset_html(reset_url: str, expiry_hours: int = 1) -> str:
    """Build HTML body for the password reset email."""
    body_style = (
        "margin:0; padding:0; background-color:#f4f5f7;"
        " font-family:-apple-system,BlinkMacSystemFont,"
        "'Segoe UI',Roboto,sans-serif;"
    )
    table_bg = "background-color:#f4f5f7; padding:40px 0;"
    card_style = (
        "background:#ffffff; border-radius:12px;"
        " box-shadow:0 2px 8px rgba(0,0,0,0.08);"
        " overflow:hidden;"
    )
    header_style = (
        "background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);"
        " padding:32px 40px; text-align:center;"
    )
    btn_style = (
        "display:inline-block; background:#667eea; color:#fff;"
        " text-decoration:none; padding:14px 32px;"
        " border-radius:8px; font-weight:600; font-size:15px;"
    )
    footer_style = (
        "padding:20px 40px; background:#fafafa;"
        " border-top:1px solid #eee; text-align:center;"
    )
    exp_label = f'{expiry_hours} hour{"s" if expiry_hours != 1 else ""}'
    return f"""\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="{body_style}">
  <table width="100%" cellpadding="0" cellspacing="0" style="{table_bg}">
    <tr>
      <td align="center">
        <table width="480" cellpadding="0" cellspacing="0" style="{card_style}">
          <tr>
            <td style="{header_style}">
              <h1 style="margin:0; color:#ffffff; font-size:24px; font-weight:700;">{APP_NAME}</h1>
            </td>
          </tr>
          <tr>
            <td style="padding:40px;">
              <h2 style="margin:0 0 8px; color:#1a1a2e; font-size:20px;">Reset your password</h2>
              <p style="margin:0 0 24px; color:#555; font-size:15px; line-height:1.5;">
                Click the button below to set a new password:
              </p>
              <div style="text-align:center; margin:0 0 24px;">
                <a href="{reset_url}" style="{btn_style}">
                  Reset Password
                </a>
              </div>
              <p style="margin:0 0 8px; color:#888; font-size:13px;">
                This link expires in <strong>{exp_label}</strong>.
              </p>
              <p style="margin:0; color:#888; font-size:13px;">
                If you didn't request this, you can safely ignore this email.
              </p>
            </td>
          </tr>
          <tr>
            <td style="{footer_style}">
              <p style="margin:0; color:#aaa; font-size:12px;">
                &copy; {APP_NAME} &middot; This is an automated message, please do not reply.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _password_reset_text(reset_url: str, expiry_hours: int = 1) -> str:
    """Build plain-text body for the password reset email."""
    return (
        f"{APP_NAME} - Password Reset\n"
        f"{'=' * 40}\n\n"
        f"Click the link below to reset your password:\n\n"
        f"{reset_url}\n\n"
        f"This link expires in {expiry_hours} hour{'s' if expiry_hours != 1 else ''}.\n\n"
        f"If you didn't request this, you can safely ignore this email.\n"
    )


# ---------------------------------------------------------------------------
# Send Functions
# ---------------------------------------------------------------------------

def send_verification_code(to_email: str, code: str, expiry_minutes: int = 15) -> bool:
    """
    Send a verification code email via AWS SES.

    Returns True on success, False on failure (logs the error).
    """
    subject = f"{code} is your {APP_NAME} verification code"
    html_body = _verification_code_html(code, expiry_minutes)
    text_body = _verification_code_text(code, expiry_minutes)
    return _send_email(to_email, subject, html_body, text_body)


def send_password_reset(to_email: str, reset_url: str, expiry_hours: int = 1) -> bool:
    """
    Send a password reset email via AWS SES.

    Returns True on success, False on failure (logs the error).
    """
    subject = f"{APP_NAME} - Reset your password"
    html_body = _password_reset_html(reset_url, expiry_hours)
    text_body = _password_reset_text(reset_url, expiry_hours)
    return _send_email(to_email, subject, html_body, text_body)


def _send_email(to_email: str, subject: str, html_body: str, text_body: str) -> bool:
    """
    Low-level SES send wrapper with error handling.

    Returns True on success, False on failure.
    """
    from_addr = f"{SES_FROM_NAME} <{SES_FROM_EMAIL}>"

    try:
        client = _get_ses_client()
        response = client.send_email(
            Source=from_addr,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                },
            },
        )
        message_id = response.get("MessageId", "unknown")
        logger.info(
            "SES email sent",
            extra={"to": to_email, "subject": subject, "message_id": message_id},
        )
        return True

    except Exception:
        logger.exception(
            "Failed to send SES email",
            extra={"to": to_email, "subject": subject},
        )
        return False
