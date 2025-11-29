# scripts/import_bounces.py
from __future__ import annotations

"""
O12 – Seed mailbox & bounce/complaint monitoring.

Minimal IMAP-based importer that:
  - Connects to a mailbox (typically a "seed" address used in campaigns),
  - Scans messages for bounced/complaint notifications,
  - Extracts bounced addresses from the message content,
  - Upserts entries into the local suppression table via src.db_suppression.

Example usage:

    $PyExe scripts/import_bounces.py `
        --db data/dev.db `
        --host imap.example.com `
        --user bounce@example.com `
        --password-env IMAP_PASSWORD `
        --folder INBOX.Bounces `
        --seed-address bounce@example.com

The pure function `parse_bounce_addresses` is unit-testable and can be
covered separately in tests.
"""

import argparse
import imaplib
import os
import re
import sys
from email import message_from_bytes
from email.message import Message

from src.db import get_connection
from src.db_suppression import upsert_suppression

EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
)


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

        # Many bounce messages include sections like:
        # "Final-Recipient: rfc822; user@example.com"
        # or original message headers with the bad address.
        if "Final-Recipient" in payload or "Diagnostic-Code" in payload:
            candidates.update(_extract_emails(payload))
        else:
            # As a fallback, scan the whole text for email-ish tokens.
            candidates.update(_extract_emails(payload))

    # Filter and normalize.
    filtered = {addr.lower() for addr in candidates if addr.lower() not in ignore_lower}

    # Return deterministic order for easier testing/debugging.
    return sorted(filtered)


def _extract_emails(text: str) -> set[str]:
    """Return a set of email-like strings from arbitrary text."""
    return set(EMAIL_RE.findall(text or ""))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import bounced/complaint addresses into suppression from an IMAP mailbox.",
    )
    parser.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite DB file (default: data/dev.db).",
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
        help="Environment variable name containing the IMAP password (default: IMAP_PASSWORD).",
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

    conn = get_connection(args.db)
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

    Returns the number of unique bounced addresses written to suppression.
    """
    if seed_ignore is None:
        seed_ignore = set()

    imap = imaplib.IMAP4_SSL(host, port)
    try:
        imap.login(user, password)
        typ, _ = imap.select(folder)
        if typ != "OK":
            raise RuntimeError(f"Failed to select folder {folder!r}: {typ}")

        # We start simple: fetch all messages in the folder. If this becomes
        # large, callers can move older messages elsewhere.
        typ, data = imap.search(None, "ALL")
        if typ != "OK":
            raise RuntimeError(f"Failed to search folder {folder!r}: {typ}")

        msg_ids = data[0].split()
        all_addresses: set[str] = set()

        for msg_id in msg_ids:
            typ, msg_data = imap.fetch(msg_id, "(RFC822)")
            if typ != "OK" or not msg_data:
                continue

            # msg_data is a list of (bytes_header, bytes_body) tuples
            # and/or other responses; grab the first RFC822 payload.
            raw_bytes = None
            for part in msg_data:
                if isinstance(part, tuple) and part[1]:
                    raw_bytes = part[1]
                    break

            if raw_bytes is None:
                continue

            try:
                raw_str = raw_bytes.decode("utf-8", errors="ignore")
            except Exception:
                continue

            bounced = parse_bounce_addresses(raw_str, ignore=seed_ignore)
            if not bounced:
                continue

            for addr in bounced:
                all_addresses.add(addr)
                upsert_suppression(
                    conn,
                    email=addr,
                    reason="seed_bounce",
                    source="o12_bounce_monitor",
                )

            if mark_seen:
                imap.store(msg_id, "+FLAGS", "\\Seen")

        conn.commit()
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
