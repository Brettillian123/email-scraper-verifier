# src/verify/__init__.py
"""
R16 — SMTP verification package

Exposes the core RCPT TO probe used by the queue task and CLI.

Public API:
    probe_rcpt(email, mx_host, *, helo_domain, mail_from, connect_timeout=10.0,
               command_timeout=10.0, behavior_hint=None) -> dict

See: src/verify/smtp.py
"""

from __future__ import annotations

from .smtp import probe_rcpt

__all__ = ["probe_rcpt"]
