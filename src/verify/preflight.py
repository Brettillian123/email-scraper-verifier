# src/verify/preflight.py
from __future__ import annotations

import socket
import time
from dataclasses import dataclass

from src.config import SMTP_PROBES_ALLOWED_HOSTS, SMTP_PROBES_ENABLED


class SmtpProbingDisabledError(RuntimeError):
    """
    Raised when a TCP/25 SMTP probe is attempted on a host where probing is disabled.

    This is a deliberate safety guardrail to prevent accidental port-25 probing
    from a local/dev machine (or any non-approved host).
    """


def _current_hostnames() -> tuple[str, str]:
    """
    Returns (hostname, fqdn) in lowercase, best-effort.
    """
    try:
        hostname = (socket.gethostname() or "").strip().lower()
    except Exception:
        hostname = ""
    try:
        fqdn = (socket.getfqdn() or "").strip().lower()
    except Exception:
        fqdn = ""

    # Normalize: if fqdn is empty but hostname exists, derive best-effort.
    if not fqdn and hostname:
        fqdn = hostname
    return hostname, fqdn


def _host_allowed(allowed: list[str], hostname: str, fqdn: str) -> bool:
    """
    Host allowlist matching.

    - Exact match against hostname or fqdn.
    - Also allow a bare hostname token to match fqdn's first label.
    """
    hn = (hostname or "").strip().lower()
    fq = (fqdn or "").strip().lower()
    fq_first = fq.split(".", 1)[0] if fq else ""

    for raw in allowed:
        tok = (raw or "").strip().lower()
        if not tok:
            continue

        if tok == hn or tok == fq:
            return True
        if fq_first and tok == fq_first:
            return True

    return False


def assert_smtp_probing_allowed() -> None:
    """
    Central gate: determines whether this process is allowed to perform TCP/25 probes.

    Rules:
      1) SMTP_PROBES_ENABLED must be true.
      2) If SMTP_PROBES_ALLOWED_HOSTS is set (non-empty), this host must match.

    This makes "where can SMTP run?" centralized and auditable.
    """
    if not SMTP_PROBES_ENABLED:
        hostname, fqdn = _current_hostnames()
        raise SmtpProbingDisabledError(
            "SMTP probing is disabled on this host. "
            "Set SMTP_PROBES_ENABLED=1 on the verifier host only. "
            f"(hostname={hostname!r}, fqdn={fqdn!r})"
        )

    allowed = [t.strip() for t in (SMTP_PROBES_ALLOWED_HOSTS or []) if t.strip()]
    if allowed:
        hostname, fqdn = _current_hostnames()
        if not _host_allowed(allowed, hostname, fqdn):
            raise SmtpProbingDisabledError(
                "SMTP probing is enabled, but this host is not in SMTP_PROBES_ALLOWED_HOSTS. "
                f"(allowed={allowed!r}, hostname={hostname!r}, fqdn={fqdn!r})"
            )


@dataclass(frozen=True)
class Port25Preflight:
    ok: bool
    mx_host: str
    ip: str | None
    elapsed_ms: int
    error: str | None


def _now_ms() -> int:
    return int(time.time() * 1000)


def check_port25(
    mx_host: str,
    timeout_s: float = 2.0,
    max_addrs: int = 2,
) -> Port25Preflight:
    """
    Fast determinism check: can we establish TCP/25 to the MX at all?

    HARD GUARDRAIL:
      This function performs TCP/25 socket connects, so it must only run on
      an approved verifier host. If called elsewhere, it fails immediately.

    - Prefer IPv4 first (often more reliable on consumer networks).
    - Try at most `max_addrs` resolved addresses to avoid multi-IP timeout blowups.
    """
    assert_smtp_probing_allowed()

    start = _now_ms()

    try:
        addrs = socket.getaddrinfo(mx_host, 25, type=socket.SOCK_STREAM)
    except OSError as e:
        return Port25Preflight(
            ok=False,
            mx_host=mx_host,
            ip=None,
            elapsed_ms=_now_ms() - start,
            error=f"getaddrinfo:{e}",
        )

    # Prefer IPv4 first.
    addrs = sorted(addrs, key=lambda a: 0 if a[0] == socket.AF_INET else 1)

    last_err: str | None = None
    tried = 0

    for _family, _socktype, _proto, _canon, sockaddr in addrs:
        if tried >= max_addrs:
            break
        tried += 1
        ip = sockaddr[0]

        try:
            with socket.create_connection((ip, 25), timeout=timeout_s):
                return Port25Preflight(
                    ok=True,
                    mx_host=mx_host,
                    ip=ip,
                    elapsed_ms=_now_ms() - start,
                    error=None,
                )
        except OSError as e:
            last_err = f"{ip}:{e}"

    return Port25Preflight(
        ok=False,
        mx_host=mx_host,
        ip=None,
        elapsed_ms=_now_ms() - start,
        error=last_err or "connect_failed",
    )


__all__ = [
    "SmtpProbingDisabledError",
    "assert_smtp_probing_allowed",
    "Port25Preflight",
    "check_port25",
]
