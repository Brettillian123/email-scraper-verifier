# tests/test_ssrf_guard.py
"""
SSRF guard tests for src/fetch/ssrf.py.

DNS is stubbed out, so these are deterministic and never touch the network.
"""

from __future__ import annotations

from typing import Any

import pytest

ssrf = pytest.importorskip("src.fetch.ssrf")


# Fake DNS: everything not listed here is treated as unresolvable (fail closed).
_FAKE_DNS = {
    "example.com": ["93.184.216.34"],
    "www.example.com": ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"],
    "crestwell-admin.internal.example.net": ["10.0.3.7"],  # internal Docker service
    "metadata.evil.example.com": ["169.254.169.254"],  # cloud metadata via public name
    "split.example.com": ["93.184.216.34", "127.0.0.1"],  # one bad address is enough
    "attacker.example.com": ["93.184.216.34"],
}


@pytest.fixture(autouse=True)
def _fake_dns(monkeypatch, _ssrf_allow_reserved_test_tlds):
    # Depends on the conftest fixture purely for ordering: that one also patches
    # the resolver, and this stub must be the one that wins.
    def _resolver(host: str) -> list[str]:
        try:
            return _FAKE_DNS[host]
        except KeyError:
            raise OSError(f"no such host: {host}") from None

    monkeypatch.setattr(ssrf, "_resolve_host_addresses", _resolver)


# --------------------------------------------------------------------------------------
# Host shape / resolution
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host",
    [
        "localhost",  # single label
        "127.0.0.1",  # IPv4 literal loopback
        "169.254.169.254",  # cloud metadata IP literal
        "10.0.3.7",  # RFC1918 literal
        "crestwell-admin",  # internal Docker hostname
        "crestwell-db",
        "redis",
        "[::1]",  # IPv6 literal loopback
        "",
        "crestwell-admin.internal.example.net",  # public shape, private address
        "metadata.evil.example.com",  # public shape, metadata address
        "split.example.com",  # only one address is private
        "does-not-resolve.example.org",  # unresolvable -> fail closed
    ],
)
def test_unsafe_hosts_are_rejected(host: str) -> None:
    assert ssrf.is_public_host(host) is False


@pytest.mark.parametrize("host", ["example.com", "www.example.com", "EXAMPLE.com."])
def test_ordinary_public_hosts_are_accepted(host: str) -> None:
    assert ssrf.is_public_host(host) is True


def test_normal_public_https_url_is_accepted() -> None:
    assert ssrf.validate_url("https://www.example.com/about-us") == (
        "https://www.example.com/about-us"
    )


# --------------------------------------------------------------------------------------
# URL scheme / credentials
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/",  # plain http is never allowed
        "http://crestwell-admin:3000/",
        "file:///etc/passwd",
        "gopher://example.com/",
        "https://user:pass@example.com/",  # embedded credentials
        "https://localhost/",
        "https://127.0.0.1/",
        "https://169.254.169.254/latest/meta-data/",
        "https://[::1]/",
    ],
)
def test_unsafe_urls_are_rejected(url: str) -> None:
    with pytest.raises(ssrf.SSRFBlocked):
        ssrf.validate_url(url)


# --------------------------------------------------------------------------------------
# Redirect handling
# --------------------------------------------------------------------------------------


class _Resp:
    def __init__(self, status_code: int, location: str | None = None) -> None:
        self.status_code = status_code
        self.headers = {"location": location} if location else {}


class _Client:
    """Minimal httpx-like stub recording the requests it was asked to make."""

    def __init__(self, script: dict[str, _Resp]) -> None:
        self.script = script
        self.calls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> _Resp:
        assert kwargs.get("follow_redirects") is False, "redirects must not be auto-followed"
        self.calls.append(url)
        return self.script.get(url, _Resp(200))


def test_redirect_into_internal_service_is_blocked() -> None:
    client = _Client(
        {"https://attacker.example.com/": _Resp(302, "http://crestwell-admin:3000/")},
    )
    with pytest.raises(ssrf.SSRFBlocked):
        ssrf.safe_get(client, "https://attacker.example.com/")
    # The internal host was never requested.
    assert client.calls == ["https://attacker.example.com/"]


def test_redirect_to_public_host_is_followed() -> None:
    client = _Client(
        {"https://attacker.example.com/": _Resp(301, "https://www.example.com/final")},
    )
    resp = ssrf.safe_get(client, "https://attacker.example.com/")
    assert resp.status_code == 200
    assert client.calls == ["https://attacker.example.com/", "https://www.example.com/final"]


def test_redirect_loop_is_capped() -> None:
    client = _Client({"https://example.com/": _Resp(302, "https://example.com/")})
    with pytest.raises(ssrf.SSRFBlocked):
        ssrf.safe_get(client, "https://example.com/")
    assert len(client.calls) == ssrf.MAX_REDIRECT_HOPS + 1


# --------------------------------------------------------------------------------------
# Address classification
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "addr",
    [
        "0.0.0.0",
        "10.1.2.3",
        "100.64.0.1",  # CGNAT
        "127.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "172.31.255.254",
        "192.168.1.1",
        "198.18.0.1",  # benchmarking
        "224.0.0.1",  # multicast
        "255.255.255.255",
        "::",
        "::1",
        "::ffff:127.0.0.1",  # v4-mapped loopback
        "::ffff:8.8.8.8",  # v4-mapped public: still refused
        "fc00::1",
        "fd12::1",
        "fe80::1",
        "ff02::1",
    ],
)
def test_private_addresses_are_not_public(addr: str) -> None:
    assert ssrf.is_public_address(addr) is False


@pytest.mark.parametrize("addr", ["8.8.8.8", "93.184.216.34", "172.32.0.1", "2606:4700::1111"])
def test_public_addresses_are_public(addr: str) -> None:
    assert ssrf.is_public_address(addr) is True
