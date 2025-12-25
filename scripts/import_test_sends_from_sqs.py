from __future__ import annotations

"""
O26 — Import SES test-send bounces from SQS and apply to verification_results.

This script:
  - Connects to the SQLite database.
  - Polls an AWS SQS queue that receives SES bounce notifications (via SNS).
  - For each "Bounce" notification:
      * Tries to extract the token from mail.tags['iq_test_token'].
      * If that fails, tries bounce+TOKEN@... return-path fields.
      * If that fails, tries Subject "(token=...)".
      * As a last resort, scans the raw body for bounce+TOKEN@domain.
      * If we *still* don't have a token, falls back to mapping the
        bouncedRecipients[0].emailAddress to the latest test-send row
        in the DB to recover the token.
      * Classifies the bounce as hard/soft.
      * Calls apply_bounce(...) from src.verify.test_send.
      * On hard bounces, asks O26 for the next-best permutation for the same
        person+domain and schedules another test-send via the queue.
      * Deletes the SQS message on success.

Configuration is loaded from .env via src.config:

  # AWS / SES
  AWS_REGION=us-east-2
  AWS_ACCESS_KEY_ID=YOUR_KEY_ID           # optional if using default AWS creds
  AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY   # optional if using default AWS creds

  # Test-send config (bounce-related fields)
  TEST_SEND_BOUNCE_PREFIX=bounce
  TEST_SEND_BOUNCES_SQS_URL=https://sqs.us-east-2.amazonaws.com/123/ses-bounces-iqverifier

Usage (PowerShell):

  .venv\\Scripts\\Activate.ps1
  python .\\scripts\\import_test_sends_from_sqs.py `
    --db data\\dev.db `
    --max-messages 20
"""

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.config import (
    AwsSesConfig,
    TestSendEmailConfig,
    load_aws_ses_config,
    load_test_send_email_config,
)
from src.queueing.tasks import _enqueue_test_send_email
from src.verify.test_send import (
    apply_bounce,
    choose_next_test_send_candidate,
    request_test_send,
)


@dataclass(frozen=True)
class SqsBounceSettings:
    aws_cfg: AwsSesConfig
    queue_url: str
    bounce_prefix: str


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {p}")


def _load_sqs_settings_from_config() -> SqsBounceSettings:
    """
    Load AWS and SQS/bounce settings from src.config / .env.
    """
    aws_cfg = load_aws_ses_config()
    email_cfg: TestSendEmailConfig = load_test_send_email_config()

    if not email_cfg.bounces_sqs_url:
        raise ValueError(
            "TEST_SEND_BOUNCES_SQS_URL must be set in the environment/.env "
            "to consume SES bounce notifications from SQS.",
        )

    return SqsBounceSettings(
        aws_cfg=aws_cfg,
        queue_url=email_cfg.bounces_sqs_url,
        bounce_prefix=email_cfg.bounce_prefix,
    )


def _create_sqs_client(aws_cfg: AwsSesConfig) -> Any:
    kwargs: dict[str, Any] = {"region_name": aws_cfg.region}
    if aws_cfg.access_key_id and aws_cfg.secret_access_key:
        kwargs["aws_access_key_id"] = aws_cfg.access_key_id
        kwargs["aws_secret_access_key"] = aws_cfg.secret_access_key
    return boto3.client("sqs", **kwargs)


def _extract_token_from_tags(mail: dict[str, Any]) -> str | None:
    """
    Primary path: extract token from SES message tags.

    send_test_sends_ses.py sets:
      Tags=[{"Name": "iq_test_token", "Value": token}, ...]

    In the SES event, this surfaces as:
      mail.tags: { "iq_test_token": ["token-value"], ... }
    """
    tags = mail.get("tags") or {}
    if not isinstance(tags, dict):
        return None

    vals = tags.get("iq_test_token")
    if isinstance(vals, list) and vals:
        token = str(vals[0]).strip()
        return token or None
    if isinstance(vals, str):
        token = vals.strip()
        return token or None
    return None


def _extract_token_from_return_path(
    return_path: str | None,
    expected_prefix: str,
) -> str | None:
    if not return_path:
        return None

    local_part = return_path.split("@", 1)[0]
    if "+" not in local_part:
        return None

    prefix, token = local_part.split("+", 1)
    if prefix != expected_prefix:
        # Not one of our test-send bounce addresses.
        return None

    token = token.strip()
    return token or None


def _try_extract_token_from_mail_fields(
    mail: dict[str, Any],
    expected_prefix: str,
) -> str | None:
    """
    Try multiple mail fields that might contain the bounce+TOKEN@... address:
      - commonHeaders.returnPath
      - source
      - headers[name == 'Return-Path']
    """
    candidates: list[str] = []

    common_headers = mail.get("commonHeaders") or {}
    rp_ch = common_headers.get("returnPath")
    if isinstance(rp_ch, str) and rp_ch:
        candidates.append(rp_ch)

    source = mail.get("source")
    if isinstance(source, str) and source:
        candidates.append(source)

    headers = mail.get("headers") or []
    for h in headers:
        try:
            name = str(h.get("name", "")).lower()
            if name == "return-path":
                value = h.get("value")
                if isinstance(value, str) and value:
                    candidates.append(value.strip("<> "))
                    break
        except Exception:  # pragma: no cover - defensive
            continue

    for rp in candidates:
        token = _extract_token_from_return_path(rp, expected_prefix)
        if token:
            return token

    return None


def _try_extract_token_from_subject(mail: dict[str, Any]) -> str | None:
    """
    Fallback: try to extract token from the Subject line, which we format as:
      "... Email verification (token=vr123-ABC...)"

    We do NOT assume any particular token pattern here; we just grab whatever
    appears after 'token=' up to the next ')' or whitespace.
    """
    common_headers = mail.get("commonHeaders") or {}
    subject = common_headers.get("subject")
    if not isinstance(subject, str) or not subject:
        return None

    m = re.search(r"token=([^\s)]+)", subject)
    if not m:
        return None
    return m.group(1).strip()


def _try_extract_token_from_body_text(
    body: str,
    expected_prefix: str,
) -> str | None:
    """
    Last-resort fallback: scan the raw JSON body for 'bounce+TOKEN@domain'.
    """
    pattern = rf"{re.escape(expected_prefix)}\+([A-Za-z0-9_\-~=]+)@([^\s\"'>]+)"
    m = re.search(pattern, body)
    if not m:
        return None

    addr = f"{expected_prefix}+{m.group(1)}@{m.group(2)}"
    return _extract_token_from_return_path(addr, expected_prefix)


def _parse_ses_bounce(
    body: str,
    expected_prefix: str,
) -> tuple[str | None, str | None, bool, str | None, str | None] | None:
    """
    Parse an SES SNS bounce payload carried in an SQS message.

    Returns (recipient_email, token, is_hard, status_code, reason) or None
    if the message is not a bounce.
    """
    try:
        outer = json.loads(body)
    except json.JSONDecodeError:
        print("WARN: Unable to decode SQS message body as JSON; skipping.")
        return None

    # Typical SES -> SNS -> SQS flow:
    #   SQS body is an SNS notification object with "Message" containing
    #   the SES event JSON as a string.
    ses_event: dict[str, Any]
    message_field = outer.get("Message")
    if isinstance(message_field, str):
        try:
            ses_event = json.loads(message_field)
        except json.JSONDecodeError:
            print("WARN: Unable to decode SNS 'Message' field; skipping.")
            return None
    else:
        if not isinstance(outer, dict):
            return None
        ses_event = outer

    if ses_event.get("notificationType") != "Bounce":
        return None

    mail = ses_event.get("mail") or {}
    bounce = ses_event.get("bounce") or {}

    # 0) Always capture the primary bounced recipient email.
    bounced_recipients = bounce.get("bouncedRecipients") or []
    recipient_info = bounced_recipients[0] if bounced_recipients else {}
    recipient_email = recipient_info.get("emailAddress")

    # 1) primary path — SES tags.
    token = _extract_token_from_tags(mail)

    # 2) structured return-path-like fields.
    if not token:
        token = _try_extract_token_from_mail_fields(mail, expected_prefix)

    # 3) Subject "(token=...)".
    if not token:
        token = _try_extract_token_from_subject(mail)

    # 4) scan body for bounce+TOKEN@domain.
    if not token:
        token = _try_extract_token_from_body_text(body, expected_prefix)

    bounce_type = (bounce.get("bounceType") or "").lower()
    is_hard = bounce_type == "permanent"

    status_code = recipient_info.get("status")
    reason = recipient_info.get("diagnosticCode") or bounce.get("bounceSubType")

    if not token:
        # We return recipient_email + other details; the caller can attempt
        # DB-based resolution of the token using the bounced email address.
        common_headers = mail.get("commonHeaders") or {}
        print(
            "WARN: Bounce without direct token; "
            f"recipient_email={recipient_email!r}, "
            f"mail.tags={mail.get('tags')!r}, "
            f"mail.commonHeaders.returnPath={common_headers.get('returnPath')!r}, "
            f"mail.commonHeaders.subject={common_headers.get('subject')!r}, "
            f"mail.source={mail.get('source')!r}",
        )

    return recipient_email, token, is_hard, status_code, reason


def _lookup_token_for_recipient(
    conn: sqlite3.Connection,
    recipient_email: str,
) -> str | None:
    """
    Fallback resolver: given a bounced recipient email address, try to map
    it to the most recent active test-send row and return its token.

    We look for verification_results rows where:
      - emails.email = recipient_email
      - test_send_token IS NOT NULL
      - test_send_status IN ('sent', 'pending')
    and choose the most recent by test_send_at / id.
    """
    cur = conn.execute(
        """
        SELECT vr.test_send_token
        FROM verification_results AS vr
        JOIN emails AS e ON e.id = vr.email_id
        WHERE e.email = ?
          AND vr.test_send_token IS NOT NULL
          AND vr.test_send_status IN ('sent', 'pending')
        ORDER BY
          CASE WHEN vr.test_send_at IS NULL THEN 1 ELSE 0 END,
          vr.test_send_at DESC,
          vr.id DESC
        LIMIT 1
        """,
        (recipient_email,),
    )
    row = cur.fetchone()
    if not row:
        return None
    token = row[0]
    return str(token) if token is not None else None


def _lookup_email_id_for_token(
    conn: sqlite3.Connection,
    token: str,
) -> int | None:
    """
    Given a test-send token, return the associated emails.id, if any.
    """
    cur = conn.execute(
        """
        SELECT email_id
        FROM verification_results
        WHERE test_send_token = ?
        """,
        (token,),
    )
    row = cur.fetchone()
    if not row:
        return None
    email_id = row[0]
    return int(email_id) if email_id is not None else None


def _handle_sqs_message(
    conn: sqlite3.Connection,
    settings: SqsBounceSettings,
    sqs_client: Any,
    message: dict[str, Any],
    *,
    dry_run: bool = False,
) -> bool:
    body = message.get("Body", "")
    parsed = _parse_ses_bounce(body, settings.bounce_prefix)
    if parsed is None:
        # Not a bounce; delete to avoid clogging the queue.
        receipt_handle = message.get("ReceiptHandle")
        if receipt_handle and not dry_run:
            sqs_client.delete_message(
                QueueUrl=settings.queue_url,
                ReceiptHandle=receipt_handle,
            )
        return False

    recipient_email, token, is_hard, status_code, reason = parsed

    # If we still don't have a token but we know which mailbox bounced,
    # try to resolve the token from the DB by that email.
    if token is None and recipient_email:
        token = _lookup_token_for_recipient(conn, recipient_email)

    if not token:
        # We cannot map this bounce to a test-send row; drop it.
        print(
            "WARN: Unable to resolve test-send token for bounce; "
            f"recipient_email={recipient_email!r}, status={status_code!r}, reason={reason!r}",
        )
        receipt_handle = message.get("ReceiptHandle")
        if receipt_handle and not dry_run:
            sqs_client.delete_message(
                QueueUrl=settings.queue_url,
                ReceiptHandle=receipt_handle,
            )
        return False

    print(
        f"Processing SES bounce: token={token}, "
        f"recipient={recipient_email!r}, is_hard={is_hard}, "
        f"status={status_code}, reason={reason}",
    )

    if not dry_run:
        # Apply the bounce outcome to the originating verification_result.
        apply_bounce(
            conn,
            token=token,
            status_code=status_code,
            reason=reason,
            is_hard=bool(is_hard),
        )

        # For hard bounces, automatically walk to the next-best permutation
        # for the same person+domain and queue a new test-send.
        if is_hard:
            email_id = _lookup_email_id_for_token(conn, token)
            if email_id is not None:
                next_cand = choose_next_test_send_candidate(
                    conn,
                    email_id=email_id,
                )
                if next_cand is not None:
                    new_token = request_test_send(
                        conn,
                        verification_result_id=next_cand.verification_result_id,
                    )
                    try:
                        _enqueue_test_send_email(
                            next_cand.verification_result_id,
                            next_cand.email,
                            new_token,
                        )
                        print(
                            "Enqueued follow-up test-send for "
                            f"email={next_cand.email!r}, "
                            f"vr_id={next_cand.verification_result_id}, "
                            f"token={new_token}",
                        )
                    except Exception as exc:  # pragma: no cover - defensive
                        # Do not fail bounce handling if enqueue fails;
                        # callers can inspect logs and retry separately.
                        print(
                            "ERROR: Failed to enqueue follow-up test-send "
                            f"for email_id={next_cand.email_id}: {exc}",
                        )

        receipt_handle = message.get("ReceiptHandle")
        if receipt_handle:
            sqs_client.delete_message(
                QueueUrl=settings.queue_url,
                ReceiptHandle=receipt_handle,
            )

    return True


def _poll_sqs_once(
    conn: sqlite3.Connection,
    settings: SqsBounceSettings,
    sqs_client: Any,
    *,
    max_messages: int,
    wait_seconds: int,
    dry_run: bool,
) -> int:
    """
    Poll SQS up to max_messages and process any bounce notifications.

    Returns the number of messages that resulted in apply_bounce().
    """
    processed = 0

    while processed < max_messages:
        to_request = min(max_messages - processed, 10)
        try:
            resp = sqs_client.receive_message(
                QueueUrl=settings.queue_url,
                MaxNumberOfMessages=to_request,
                WaitTimeSeconds=wait_seconds,
            )
        except (BotoCoreError, ClientError, Exception) as exc:  # pragma: no cover
            print(f"ERROR: Failed to receive messages from SQS: {exc}")
            break

        messages = resp.get("Messages") or []
        if not messages:
            break

        for msg in messages:
            if _handle_sqs_message(
                conn,
                settings,
                sqs_client,
                msg,
                dry_run=dry_run,
            ):
                processed += 1
                if processed >= max_messages:
                    break

    return processed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Import SES test-send bounces from SQS (O26).",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite database, e.g. data/dev.db",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=20,
        help="Maximum number of SQS messages to process in this run.",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=10,
        help="WaitTimeSeconds for SQS long polling (0-20).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log bounces but do not update the DB or delete messages.",
    )
    args = parser.parse_args(argv)

    _ensure_db_exists(args.db)

    try:
        settings = _load_sqs_settings_from_config()
    except ValueError as exc:
        raise SystemExit(f"Configuration error for SES/SQS bounces: {exc}") from exc

    sqs_client = _create_sqs_client(settings.aws_cfg)

    conn = sqlite3.connect(args.db)
    try:
        updated = _poll_sqs_once(
            conn,
            settings,
            sqs_client,
            max_messages=args.max_messages,
            wait_seconds=args.wait_seconds,
            dry_run=args.dry_run,
        )
        print(f"Completed. Applied {updated} bounce(s).")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
