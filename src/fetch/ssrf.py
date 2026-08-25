# src/fetch/ssrf.py
"""
SSRF guard for every outbound HTTP fetch in the crawl/fetch layer.

WHY THIS EXISTS
---------------
The domains that reach the crawler are attacker-controlled: they come out of a
scraped company database and off the POST /api/browser/runs endpoint. Without a
guard, a caller can submit "localhost", "crestwell-db", "169.254.169.254", or a
domain they own that 302s to http://crestwell-admin:3000/ -- and the crawler
will fetch it from *inside* the private Docker network (admin UI, Postgres,
Redis, LiteLLM, n8n) or from the cloud metadata service. So every outbound
request in this codebase must go through validate_url()/safe_get() rather than
handing a caller-supplied host to httpx with follow_redirects=True.

Rules (ported from crestwell-autonomy/worker-api/ai-outreach.mjs:120-182):
  - real DNS hostnames only, via a strict public-FQDN shape; IP literals and
    single-label names ("localhost", "crestwell-admin") are rejected outright
  - the host must resolve, and EVERY address it resolves to must be public
  - https only
  - redirects are followed MANUALLY, re-validating the destination on every hop,
    at most MAX_REDIRECT_HOPS hops

This module is deliberately stdlib-only (it does not import httpx) so it can be
unit tested and imported from anywhere without pulling in the HTTP stack.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import socket
from typing import Any
from urllib.parse import urljoin, urlsplit

log = logging.getLogger(__name__)

__all__ = [
    "MAX_REDIRECT_HOPS",
    "SSRFBlocked",
    "is_public_address",
    "is_public_host",
    "normalize_host",
    "safe_get",
    "validate_url",
]

MAX_REDIRECT_HOPS = 3

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

# Public FQDN shape: two or more labels, alphabetic TLD, 4..253 chars.
# Rejects IP literals ("127.0.0.1", "[::1]" -> "::1"), single-label names
# ("localhost", "crestwell-admin", "redis"), and anything with a port or
# userinfo left in it. Input is lowercased by normalize_host() before matching.
_PUBLIC_HOST_RE = re.compile(r"^(?=.{4,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$")

# Everything an outbound fetch must never be able to reach.
_BLOCKED_V4 = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "0.0.0.0/8",  # "this network"
        "10.0.0.0/8",  # RFC1918
        "100.64.0.0/10",  # CGNAT
        "127.0.0.0/8",  # loopback
        "169.254.0.0/16",  # link-local, incl. 169.254.169.254 cloud metadata
        "172.16.0.0/12",  # RFC1918 (172.16 - 172.31)
        "192.168.0.0/16",  # RFC1918
        "198.18.0.0/15",  # benchmarking (198.18 - 198.19)
        "224.0.0.0/3",  # multicast + reserved (everything >= 224)
    )
)

_BLOCKED_V6 = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "::/128",  # unspecified
        "::1/128",  # loopback
        "::ffff:0:0/96",  # v4-mapped; never needed to reach a public site
        "fc00::/7",  # unique-local
        "fe80::/10",  # link-local
        "ff00::/8",  # multicast
    )
)


class SSRFBlocked(ValueError):
    """Raised when a URL or host is refused because it may not be public."""


# --------------------------------------------------------------------------------------
# Host / address validation
# --------------------------------------------------------------------------------------


def normalize_host(value: str) -> str:
    """
    Extract a bare, lowercased hostname from a domain or URL-ish string.

    Drops scheme, userinfo, port, path, and any trailing dot. Returns "" when the
    input cannot be parsed at all. This does NOT decide whether the host is safe
    -- use is_public_host() for that.
    """
    raw = (value or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "//" + raw
    try:
        host = urlsplit(raw).hostname or ""
    except ValueError:
        return ""
    host = host.strip().rstrip(".").lower()
    if host and not host.isascii():
        # IDN: use the punycode form so the shape check below runs on ASCII.
        try:
            host = host.encode("idna").decode("ascii").lower()
        except (UnicodeError, ValueError):
            return ""
    return host


def is_public_address(addr: str) -> bool:
    """True only if `addr` is a routable public IPv4/IPv6 address."""
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        return False  # scoped or malformed address -> fail closed
    if ip.version == 4:
        return not any(ip in net for net in _BLOCKED_V4)
    return not any(ip in net for net in _BLOCKED_V6)


def _resolve_host_addresses(host: str) -> list[str]:
    """
    Resolve `host` to every address the resolver returns (A and AAAA).

    Split out as a module-level function so tests can substitute a deterministic
    resolver instead of hitting the network.
    """
    infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return [info[4][0] for info in infos]


def is_public_host(host: str) -> bool:
    """
    True only if `host` is a real public DNS name that resolves exclusively to
    public addresses. Fails closed on unresolvable hosts and resolver errors.
    """
    h = normalize_host(host)
    if not h or not _PUBLIC_HOST_RE.match(h):
        return False
    try:
        addrs = _resolve_host_addresses(h)
    except OSError:
        return False
    return bool(addrs) and all(is_public_address(a) for a in addrs)


def validate_url(url: str) -> str:
    """
    Return `url` unchanged if it is safe to fetch, else raise SSRFBlocked.

    Safe means: https, no embedded credentials, and a hostname that is a public
    FQDN resolving only to public addresses.
    """
    raw = (url or "").strip()
    try:
        parts = urlsplit(raw)
    except ValueError as exc:
        raise SSRFBlocked(f"unparseable URL: {raw!r}") from exc

    if parts.scheme.lower() != "https":
        raise SSRFBlocked(f"non-https URL refused: {raw!r}")
    if parts.username or parts.password:
        raise SSRFBlocked(f"credentials in URL refused: {raw!r}")

    host = parts.hostname or ""
    if not is_public_host(host):
        raise SSRFBlocked(f"host is not a public DNS name: {host!r}")
    return raw


# --------------------------------------------------------------------------------------
# Redirect-safe GET
# --------------------------------------------------------------------------------------


def _get_bounded(client: Any, url: str, *, max_bytes: int | None = None, **kwargs: Any) -> Any:
    """GET `url`, reading at most `max_bytes` of the DECOMPRESSED body.

    Why this is not just a post-hoc slice: the caller's size cap used to be
    applied to `resp.content`, which httpx has already read off the socket and
    run through its decompressors. A crawled site chooses the encoding, so a
    101,941-byte gzip response inflated to 104,857,600 bytes resident in 0.09s
    -- a 1028:1 ratio, measured. A 2 MB wire response, well inside every
    expectation, becomes ~2 GB in an RQ worker. No timeout helps: the allocation
    kills the process long before one fires, and on a single VPS the OOM killer
    may take Postgres or Redis rather than the worker.

    Streaming means we stop reading once the cap is hit, so the decompressor
    never produces more than that. Clients without `.stream` (the fakes in the
    tests) fall back to a plain GET -- they serve fixed bodies, so there is
    nothing to bomb.
    """
    if max_bytes is None or not hasattr(client, "stream"):
        return client.get(url, follow_redirects=False, **kwargs)

    with client.stream("GET", url, follow_redirects=False, **kwargs) as resp:
        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_bytes():
            remaining = max_bytes - total
            if len(chunk) >= remaining:
                chunks.append(chunk[:remaining])
                total = max_bytes
                break
            chunks.append(chunk)
            total += len(chunk)
        body = b"".join(chunks)

    # Hand back a response whose .content is the bounded body. httpx reads
    # .content from _content, and marks the stream consumed so it will not try
    # to read again from a socket we have already closed.
    resp._content = body
    resp.is_stream_consumed = True
    return resp


def safe_get(
    client: Any,
    url: str,
    *,
    max_hops: int = MAX_REDIRECT_HOPS,
    max_bytes: int | None = None,
    **kwargs: Any,
) -> Any:
    """
    GET `url` through `client`, following redirects MANUALLY so that every hop is
    re-validated. `client` is any httpx-like object exposing .get(url, ...).

    Raises SSRFBlocked if the initial URL, or any redirect target, is not safe,
    or if the redirect chain exceeds `max_hops`.

    Timeouts and other per-request options pass straight through, so the caller's
    existing timeout / size-cap behavior is preserved.
    """
    current = validate_url(url)
    for _hop in range(max_hops + 1):
        resp = _get_bounded(client, current, max_bytes=max_bytes, **kwargs)
        if resp.status_code not in _REDIRECT_STATUSES:
            return resp
        location = resp.headers.get("location") or resp.headers.get("Location")
        if not location:
            return resp
        # Re-validate the destination. This is the hop that would otherwise walk
        # straight into http://crestwell-admin:3000/ or the metadata service.
        current = validate_url(urljoin(current, location))
    raise SSRFBlocked(f"too many redirects (> {max_hops}) starting at {url!r}")
