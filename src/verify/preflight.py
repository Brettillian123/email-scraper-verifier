from __future__ import annotations

import socket
import time
from dataclasses import dataclass


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

    - Prefer IPv4 first (often more reliable on consumer networks).
    - Try at most `max_addrs` resolved addresses to avoid multi-IP timeout blowups.
    """
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
