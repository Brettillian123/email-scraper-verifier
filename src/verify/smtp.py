# src/verify/smtp.py
"""
R16 — Core SMTP RCPT probe (+ O06 behavior-aware timeouts)

Public function:

    probe_rcpt(
        email: str,
        mx_host: str,
        *,
        helo_domain: str,
        mail_from: str,
        connect_timeout: float = 10.0,
        command_timeout: float = 10.0,
        behavior_hint: dict | None = None,
    ) -> dict

Responsibilities:
- Light email normalization (trim, split, IDNA for domain; preserve local-part case).
- Require a non-empty MX host.
- Open an SMTP connection with sane (and O06-tuned) timeouts.
- EHLO; opportunistic STARTTLS if offered; EHLO again afterward.
- Run MAIL FROM / RCPT TO and capture the RCPT reply code/message.
- Classify the response into: accept | hard_fail | temp_fail | unknown.
- Always record behavior stats (O06) via a behavior hook.
- Return a structured dict suitable for logging and later persistence (R18).

Notes:
- Local-part is case-preserving; domain-part is lowercased and IDNA-encoded.
- On exceptions (timeouts, TLS/protocol errors), return ok=False, category="unknown".
- If no behavior_hint is supplied, we can look one up (preferring MX module).
"""

from __future__ import annotations

import os
import smtplib
import socket
import sys
import time
from typing import Any

# --- Optional deps / helpers -------------------------------------------------

try:  # pragma: no cover - exercised in integration
    from src.ingest.normalize import norm_domain as _shared_norm_domain  # type: ignore
except Exception:  # pragma: no cover
    _shared_norm_domain = None  # type: ignore


def _idna_domain(d: str) -> str:
    d = (d or "").strip().lower()
    try:
        return d.encode("idna").decode("ascii")
    except Exception:
        return d


def _normalize_email(email: str) -> tuple[str, str, str]:
    s = (email or "").strip()
    if not s or "@" not in s:
        raise ValueError("invalid_email")
    local, domain = s.split("@", 1)
    if _shared_norm_domain:
        try:
            nd = _shared_norm_domain(domain) or domain
        except Exception:
            nd = _idna_domain(domain)
    else:
        nd = _idna_domain(domain)
    return local, nd, f"{local}@{nd}"


# --- O06 integrations (prefer src.resolve.mx; fallback to legacy) ------------

_mx_record = None
_mx_get_hint = None
_beh_record = None
_beh_get_hint = None

try:
    # (mx_host, *, window_days=30, db_path=None) -> dict | None
    from src.resolve.mx import get_mx_behavior_hint as _mx_get_hint

    # (domain, mx_host, elapsed_ms, category, code, error_kind)
    from src.resolve.mx import record_behavior as _mx_record_behavior

    # (mx_host, code, elapsed_s, *, error_kind, category?, db_path?)
    from src.resolve.mx import record_mx_probe as _mx_record  # type: ignore
except Exception:
    _mx_record = None
    _mx_get_hint = None
    _mx_record_behavior = None  # type: ignore

try:
    from src.resolve.behavior import get_behavior_hint as _beh_get_hint
    from src.resolve.behavior import (  # type: ignore
        record_mx_probe as _beh_record,
    )
except Exception:
    _beh_record = None
    _beh_get_hint = None


def _record_probe_raw(
    mx_host: str,
    code: int | None,
    elapsed: float,
    *,
    error_kind: str | None,
    category: str | None,
    domain: str | None,
) -> None:
    """Store a raw datapoint via whichever backend is available."""
    db_path = os.getenv("DATABASE_PATH")

    if _mx_record:
        try:
            _mx_record(
                mx_host,
                code,
                elapsed,
                error_kind=error_kind,
                category=category,
                db_path=db_path,
            )  # type: ignore[arg-type]
            return
        except TypeError:
            try:
                _mx_record(
                    mx_host,
                    code,
                    elapsed,
                    error_kind=error_kind,
                )  # type: ignore[misc]
                return
            except Exception:
                pass
        except Exception:
            pass

    if _beh_record:
        try:
            _beh_record(
                mx_host,
                code,
                elapsed,
                error_kind=error_kind,
                domain=domain,
                db_path=db_path,
            )  # type: ignore[arg-type]
            return
        except TypeError:
            try:
                _beh_record(
                    mx_host,
                    code,
                    elapsed,
                    error_kind=error_kind,
                    domain=domain,
                )  # type: ignore[misc]
                return
            except Exception:
                pass
        except Exception:
            pass
    # no-op fallback


def _get_hint(mx_host: str, domain: str | None) -> dict[str, Any] | None:
    db_path = os.getenv("DATABASE_PATH")

    if _mx_get_hint:
        try:
            return _mx_get_hint(mx_host, db_path=db_path)  # type: ignore[call-arg]
        except TypeError:
            try:
                return _mx_get_hint(mx_host)  # type: ignore[misc]
            except Exception:
                pass
        except Exception:
            pass

    if _beh_get_hint:
        try:
            return _beh_get_hint(
                mx_host=mx_host,
                domain=domain,
                db_path=db_path,
            )  # type: ignore[arg-type]
        except TypeError:
            try:
                return _beh_get_hint(
                    mx_host=mx_host,
                    domain=domain,
                )  # type: ignore[misc]
            except Exception:
                pass
        except Exception:
            pass

    return None


# --- Behavior-driven timeout tuning (O06 consumer) ---------------------------


def _apply_hint_timeouts(
    connect_timeout: float,
    command_timeout: float,
    behavior_hint: dict | None,
) -> tuple[float, float]:
    if not isinstance(behavior_hint, dict):
        return connect_timeout, command_timeout
    ct = behavior_hint.get("connect_timeout", connect_timeout)
    mt = behavior_hint.get("command_timeout", command_timeout)
    try:
        return float(ct), float(mt)
    except Exception:
        return connect_timeout, command_timeout


# --- Classification -----------------------------------------------------------


def _classify(code: int | None) -> str:
    if code is None:
        return "unknown"
    if 200 <= code < 300:
        return "accept"
    if 500 <= code < 600:
        return "hard_fail"
    if 400 <= code < 500:
        return "temp_fail"
    return "unknown"


def _decode_msg(msg: bytes | str | None) -> str:
    if msg is None:
        return ""
    if isinstance(msg, bytes):
        try:
            return msg.decode("latin-1", errors="replace").strip()
        except Exception:
            return repr(msg)
    return str(msg).strip()


# --- R16-visible behavior hook (tests monkeypatch this) ----------------------


def record_behavior(
    *,
    domain: str,
    mx_host: str,
    elapsed_ms: int,
    category: str,
    code: int | None,
    error_kind: str | None,
) -> None:
    """
    Tests patch smtp.record_behavior and expect exactly one call per probe.
    Default: if src.resolve.mx.record_behavior exists, delegate to it.
    Otherwise, record a raw datapoint so O06 stats still work.
    """
    try:
        if _mx_record_behavior:
            _mx_record_behavior(
                domain=domain,
                mx_host=mx_host,
                elapsed_ms=elapsed_ms,
                category=category,
                code=code,
                error_kind=error_kind,
            )
            return
    except Exception:
        pass

    # Fallback: write a raw probe event
    try:
        _record_probe_raw(
            mx_host,
            code,
            elapsed_ms / 1000.0,
            error_kind=error_kind,
            category=category,
            domain=domain,
        )
    except Exception:
        pass


# --- Public API ---------------------------------------------------------------


def probe_rcpt(  # noqa: C901
    email: str,
    mx_host: str,
    *,
    helo_domain: str,
    mail_from: str,
    connect_timeout: float = 10.0,
    command_timeout: float = 10.0,
    behavior_hint: dict | None = None,
) -> dict:
    """
    Probe deliverability by issuing SMTP MAIL FROM / RCPT TO against an MX host.
    """
    started = time.monotonic()

    if not (mx_host or "").strip():
        raise ValueError("mx_host_required")

    # Normalize email (preserve local case; normalize domain with IDNA)
    try:
        _local, _domain, email_norm = _normalize_email(email)
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        # SINGLE hook invocation expected by tests — resolve from live module
        hook = sys.modules[__name__].record_behavior
        hook(
            domain="",
            mx_host=mx_host,
            elapsed_ms=elapsed_ms,
            category="unknown",
            code=None,
            error_kind="invalid_email",
        )
        return {
            "ok": False,
            "category": "unknown",
            "code": None,
            "message": "",
            "mx_host": mx_host,
            "helo_domain": helo_domain,
            "elapsed_ms": elapsed_ms,
            "error": f"invalid_email:{exc}",
        }

    if behavior_hint is None:
        behavior_hint = _get_hint(mx_host, _domain)

    c_to, cmd_to = _apply_hint_timeouts(connect_timeout, command_timeout, behavior_hint)

    smtp: smtplib.SMTP | None = None
    rcpt_code: int | None = None
    rcpt_msg: str = ""
    error_str: str | None = None
    category: str = "unknown"

    try:
        smtp = smtplib.SMTP(host=mx_host, port=25, local_hostname=helo_domain, timeout=c_to)
        try:
            if smtp.sock is not None:
                smtp.sock.settimeout(cmd_to)
        except Exception:
            pass

        try:
            smtp.ehlo()
        except smtplib.SMTPHeloError:
            smtp.helo()

        try:
            if hasattr(smtp, "has_extn") and smtp.has_extn("starttls"):
                smtp.starttls()
                try:
                    if smtp.sock is not None:
                        smtp.sock.settimeout(cmd_to)
                except Exception:
                    pass
                smtp.ehlo()
        except (OSError, smtplib.SMTPException):
            pass

        mail_code, mail_resp = smtp.mail(mail_from)
        _ = (mail_code, mail_resp)

        code, resp = smtp.rcpt(email_norm)
        rcpt_code = int(code) if isinstance(code, int) else None
        rcpt_msg = _decode_msg(resp)
        category = _classify(rcpt_code)
        error_str = None

    except TimeoutError as exc:
        error_str = f"timeout:{exc}"
        category = "unknown"
        rcpt_code = None
        rcpt_msg = ""
    except smtplib.SMTPResponseException as exc:
        rcpt_code = int(getattr(exc, "smtp_code", None) or 0) or None
        rcpt_msg = _decode_msg(getattr(exc, "smtp_error", b""))
        category = _classify(rcpt_code)
        error_str = f"smtp_response:{rcpt_code}"
    except smtplib.SMTPServerDisconnected as exc:
        error_str = f"disconnected:{exc}"
        category = "unknown"
        rcpt_code = None
        rcpt_msg = ""
    except smtplib.SMTPException as exc:
        error_str = f"smtp_error:{exc}"
        category = "unknown"
        rcpt_code = None
        rcpt_msg = ""
    except Exception as exc:
        error_str = f"error:{type(exc).__name__}:{exc}"
        category = "unknown"
        rcpt_code = None
        rcpt_msg = ""
    finally:
        try:
            if smtp is not None:
                with socket_timeout_guard(1.0):
                    smtp.quit()
        except Exception:
            pass

    elapsed_ms = int((time.monotonic() - started) * 1000)

    # SINGLE behavior-hook invocation per probe — resolve from live module
    err_kind = None if error_str is None else (error_str.split(":", 1)[0] or "error")
    hook = sys.modules[__name__].record_behavior
    hook(
        domain=_domain,
        mx_host=mx_host,
        elapsed_ms=elapsed_ms,
        category=category,
        code=rcpt_code,
        error_kind=err_kind,
    )

    return {
        "ok": rcpt_code is not None and error_str is None,
        "category": category,
        "code": rcpt_code,
        "message": rcpt_msg,
        "mx_host": mx_host,
        "helo_domain": helo_domain,
        "elapsed_ms": elapsed_ms,
        "error": error_str,
    }


# --- Small context helper -----------------------------------------------------


class socket_timeout_guard:
    """
    Context manager to temporarily set a shorter default socket timeout for
    a narrow operation (e.g., SMTP QUIT), without disturbing global state.
    """

    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s
        self._prev: float | None = None

    def __enter__(self):
        self._prev = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self.timeout_s)

    def __exit__(self, exc_type, exc, tb):
        socket.setdefaulttimeout(self._prev)
        return False


__all__ = ["probe_rcpt", "record_behavior"]
