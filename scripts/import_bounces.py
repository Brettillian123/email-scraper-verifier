from __future__ import annotations

"""
O12 — Seed mailbox & bounce/complaint monitoring.
O26 — Bounce-based verification integration.

Minimal IMAP-based importer that:
  - Connects to a mailbox (typically a "seed" or bounce address),
  - Scans messages for bounced/complaint notifications,
  - Extracts bounced addresses from the message content,
  - Upserts entries into the local suppression table via src.db_suppression,
  - Detects test-send bounce tokens of the form bounce+{token}@yourdomain.com
    and applies bounce outcomes to verification_results via O26 helpers.

Example usage:

    python scripts/import_bounces.py \
        --host imap.example.com \
        --user bounce@example.com \
        --password-env IMAP_PASSWORD \
        --folder INBOX.Bounces \
        --seed-address bounce@example.com

The pure function `parse_bounce_addresses` is unit-testable and can be
covered separately in tests. Additional pure helpers:

  - parse_test_send_tokens(raw_email)
  - parse_bounce_status(raw_email)
"""

import argparse
import imaplib
import os
import re
import sys
from email import message_from_bytes
from email.message import Message

from src.db import get_conn
from src.db_suppression import upsert_suppression
from src.verify.test_send import apply_bounce

EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
)

# Test-send / bounce alias helpers.
BOUNCE_ALIAS_PREFIX = "bounce+"
BOUNCE_TOKEN_RE = re.compile(r"bounce\+([^@\s]+)@", re.IGNORECASE)
STATUS_CODE_RE = re.compile(r"\b([245]\.\d\.\d)\b")


def parse_bounce_addresses(
    raw_email: str,
    ignore: set[str] | None = None,
) -> list[str]:
    """
    Extract likely bounced addresses from a raw RFC 822 message.

    Strategy:
      - Parse the message with the stdlib email parser.
      - Look at common DSN / bounce headers:
          Final-Recipient, Original-Recipient, X-Orig-Recipient, X-Original-To
        (but *not* To:, which is usually the seed/postmaster mailbox).
      - Scan text parts for mail delivery reports and any email-looking tokens.
      - Optionally drop any addresses that appear in the `ignore` set
        (e.g., the seed mailbox itself).
      - Drop any addresses that look like our bounce alias
        (bounce+{token}@...), since those are not actual recipients.

    This is heuristic by design; it's better to slightly over-capture and let
    higher-level suppression policies decide what to do with ambiguous cases.
    """
    ignore_lower: set[str] = {a.lower() for a in (ignore or set())}

    msg: Message = message_from_bytes(raw_email.encode("utf-8", errors="ignore"))
    candidates: set[str] = set()

    # Headers commonly used in DSN / bounce notifications.
    # We intentionally do NOT inspect the generic "To" header, since that is
    # usually the seed mailbox or postmaster, not the failed recipient.
    header_keys = [
        "Final-Recipient",
        "Original-Recipient",
        "X-Orig-Recipient",
        "X-Original-To",
        "Delivered-To",
    ]

    for key in header_keys:
        value = msg.get(key)
        if not value:
            continue
        candidates.update(_extract_emails(value))

    # Walk text/* parts for additional hints.
    for part in msg.walk():
        content_type = part.get_content_type()
        if not content_type.startswith("text/"):
            continue

        try:
            payload_bytes = part.get_payload(decode=True)
            if payload_bytes is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            payload = payload_bytes.decode(charset, errors="ignore")
        except Exception:
            continue

        candidates.update(_extract_emails(payload))

    # Filter and normalize.
    filtered = {addr.lower() for addr in candidates if addr.lower() not in ignore_lower}

    # Drop our own bounce alias addresses (bounce+{token}@...), since they are
    # not the failing recipient and should not be suppressed.
    filtered = {
        addr for addr in filtered if not addr.split("@", 1)[0].startswith(BOUNCE_ALIAS_PREFIX)
    }

    # Return deterministic order for easier testing/debugging.
    return sorted(filtered)


def _extract_emails(text: str) -> set[str]:
    """Return a set of email-like strings from arbitrary text."""
    return set(EMAIL_RE.findall(text or ""))


def parse_test_send_tokens(raw_email: str) -> set[str]:
    """
    Extract test-send tokens from any bounce addresses of the form:

        bounce+{token}@yourdomain.com

    Returns a set of token strings. These are suitable for passing to
    src.verify.test_send.apply_bounce().
    """
    return {m.group(1) for m in BOUNCE_TOKEN_RE.finditer(raw_email)}


def parse_bounce_status(
    raw_email: str,
) -> tuple[str | None, bool | None, str | None]:
    """
    Heuristically parse a DSN / bounce message to derive:

        (status_code, is_hard, normalized_reason)

    - status_code is a RFC 3463-style code that we find (e.g. "5.1.1",
      "4.2.0"), or None if not found.

    - is_hard is True for permanent failures, False for temporary failures,
      or None if we cannot decide.

    - normalized_reason is a short label like "user_unknown",
      "mailbox_full", "policy_block", or None if we cannot infer one.

    We deliberately err on the side of *not* classifying when the message
    does not look like a real DSN to avoid false hard-bounce updates.
    """
    text_lower = raw_email.lower()

    m = STATUS_CODE_RE.search(raw_email)
    status_code = m.group(1) if m else None

    is_hard: bool | None = None
    reason: str | None = None

    if status_code:
        if status_code.startswith("5."):
            is_hard = True
        elif status_code.startswith("4."):
            is_hard = False

    # Heuristic reason mapping based on common DSN text.
    if any(
        kw in text_lower
        for kw in (
            "user unknown",
            "unknown user",
            "no such user",
            "unknown recipient",
            "recipient unknown",
            "address not found",
        )
    ):
        reason = "user_unknown"
        if is_hard is None:
            is_hard = True
    elif any(
        kw in text_lower
        for kw in (
            "mailbox full",
            "over quota",
            "quota exceeded",
            "storage limit",
            "disk quota",
        )
    ):
        reason = "mailbox_full"
        if is_hard is None:
            is_hard = False
    elif any(
        kw in text_lower
        for kw in (
            "spam",
            "blocked",
            "blacklist",
            "blacklisted",
            "policy violation",
            "rejected by policy",
        )
    ):
        reason = "policy_block"
        if is_hard is None:
            is_hard = True

    # If we have neither a code nor a reason, treat this as not reliably
    # classifiable; callers should skip bounce-based verification updates.
    if status_code is None and reason is None:
        return (None, None, None)

    # If still undecided, default to hard; DSNs that reach a bounce alias
    # are typically real failures.
    if is_hard is None:
        is_hard = True

    return (status_code, is_hard, reason)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import bounced/complaint addresses into suppression from an IMAP mailbox.",
    )
    parser.add_argument(
        "--host",
        required=True,
        help="IMAP server hostname, e.g. imap.example.com.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=993,
        help="IMAP port (default: 993 for IMAPS).",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="IMAP username / login for the seed mailbox.",
    )
    parser.add_argument(
        "--password-env",
        dest="password_env",
        default="IMAP_PASSWORD",
        help=("Environment variable name containing the IMAP password (default: IMAP_PASSWORD)."),
    )
    parser.add_argument(
        "--folder",
        default="INBOX",
        help="IMAP folder/mailbox to scan (default: INBOX).",
    )
    parser.add_argument(
        "--seed-address",
        action="append",
        default=[],
        help="Seed address to ignore when parsing bounces. May be passed multiple times.",
    )
    parser.add_argument(
        "--mark-seen",
        action="store_true",
        help="Mark processed messages as \\Seen (default: leave flags unchanged).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    password = os.environ.get(args.password_env)
    if not password:
        print(
            f"IMAP password environment variable '{args.password_env}' is not set.",
            file=sys.stderr,
        )
        sys.exit(1)

    seed_ignore: set[str] = {addr.lower() for addr in (args.seed_address or [])}

    conn = get_conn()
    try:
        processed = import_bounces(
            conn,
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            folder=args.folder,
            seed_ignore=seed_ignore,
            mark_seen=args.mark_seen,
        )
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    print(f"Imported suppression entries for {processed} bounced addresses.")


def _imap_select_folder(imap: imaplib.IMAP4_SSL, folder: str) -> None:
    typ, _ = imap.select(folder)
    if typ != "OK":
        raise RuntimeError(f"Failed to select folder {folder!r}: {typ}")


def _imap_search_all(imap: imaplib.IMAP4_SSL, folder: str) -> list[bytes]:
    typ, data = imap.search(None, "ALL")
    if typ != "OK":
        raise RuntimeError(f"Failed to search folder {folder!r}: {typ}")
    if not data or not data[0]:
        return []
    return data[0].split()


def _imap_fetch_rfc822(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> bytes | None:
    typ, msg_data = imap.fetch(msg_id, "(RFC822)")
    if typ != "OK" or not msg_data:
        return None

    for part in msg_data:
        if isinstance(part, tuple) and part[1]:
            return part[1]
    return None


def _decode_rfc822(raw_bytes: bytes) -> str | None:
    try:
        return raw_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return None


def _apply_test_send_bounces(conn, raw_str: str) -> None:
    tokens = parse_test_send_tokens(raw_str)
    if not tokens:
        return

    status_code, is_hard, norm_reason = parse_bounce_status(raw_str)
    if is_hard is None:
        return

    for token in tokens:
        apply_bounce(
            conn,
            token=token,
            status_code=status_code,
            reason=norm_reason,
            is_hard=is_hard,
        )


def _upsert_suppressions_for_addresses(conn, addresses: set[str]) -> None:
    for addr in addresses:
        upsert_suppression(
            conn,
            email=addr,
            reason="seed_bounce",
            source="o12_bounce_monitor",
        )


def _mark_seen(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> None:
    imap.store(msg_id, "+FLAGS", "\\Seen")


def _commit_if_supported(conn) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def import_bounces(
    conn,
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    folder: str = "INBOX",
    seed_ignore: set[str] | None = None,
    mark_seen: bool = False,
) -> int:
    """
    Connect to the IMAP server, scan the given folder, and upsert suppressions
    for any bounced addresses that can be extracted.

    Additionally, detect test-send bounce tokens (bounce+{token}@...) and
    apply bounce outcomes to verification_results via O26 helpers.

    Returns the number of unique bounced addresses written to suppression.
    """
    if seed_ignore is None:
        seed_ignore = set()

    imap = imaplib.IMAP4_SSL(host, port)
    try:
        imap.login(user, password)
        _imap_select_folder(imap, folder)

        msg_ids = _imap_search_all(imap, folder)
        all_addresses: set[str] = set()

        for msg_id in msg_ids:
            raw_bytes = _imap_fetch_rfc822(imap, msg_id)
            if raw_bytes is None:
                continue

            raw_str = _decode_rfc822(raw_bytes)
            if raw_str is None:
                continue

            _apply_test_send_bounces(conn, raw_str)

            bounced = set(parse_bounce_addresses(raw_str, ignore=seed_ignore))
            if bounced:
                all_addresses.update(bounced)
                _upsert_suppressions_for_addresses(conn, bounced)

            if mark_seen:
                _mark_seen(imap, msg_id)

        _commit_if_supported(conn)
        return len(all_addresses)
    finally:
        try:
            imap.close()
        except Exception:
            pass
        try:
            imap.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()
