# tests/test_o07_fallback.py
from __future__ import annotations

import types
from typing import Any

import pytest

import src.queueing.tasks as tasks
import src.verify.fallback as fb_mod

# -----------------------------
# Unit tests for fallback module
# -----------------------------


def _bypass_tcp25_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch TCP25 preflight to always return success."""

    def _fake_preflight(mx_host, *, timeout_s=1.5, redis=None, ttl_s=300):
        return {"ok": True, "mx_host": mx_host, "cached": False, "error": None}

    monkeypatch.setattr(tasks, "_smtp_tcp25_preflight_mx", _fake_preflight)


def test_verify_with_fallback_disabled_skips_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    When THIRD_PARTY_VERIFY_ENABLED is False, verify_with_fallback should
    immediately return status="unknown" with a 'skipped' reason and *not*
    attempt any HTTP calls.
    """

    # Ensure config flags cause short-circuit
    monkeypatch.setattr(fb_mod, "THIRD_PARTY_VERIFY_ENABLED", False, raising=False)
    monkeypatch.setattr(
        fb_mod,
        "THIRD_PARTY_VERIFY_URL",
        "https://api.example.test/verify",
        raising=False,
    )
    monkeypatch.setattr(fb_mod, "THIRD_PARTY_VERIFY_API_KEY", "secret", raising=False)

    # Make any accidental HTTP usage blow up
    class _BoomClient:
        def __init__(self, *a: Any, **k: Any) -> None:  # pragma: no cover
            raise AssertionError("HTTP client should not be constructed when disabled")

    fake_httpx = types.SimpleNamespace(Client=_BoomClient)
    monkeypatch.setattr(fb_mod, "httpx", fake_httpx, raising=False)

    res = fb_mod.verify_with_fallback("user@example.com")
    assert res.email == "user@example.com"
    assert res.status == "unknown"
    assert isinstance(res.raw, dict)
    assert res.raw.get("skipped") is True
    assert res.raw.get("reason") == "fallback_disabled_or_unconfigured"


@pytest.mark.parametrize(
    "provider_status, expected",
    [
        ("valid", "valid"),
        ("deliverable", "valid"),
        ("invalid", "invalid"),
        ("undeliverable", "invalid"),
        ("catch_all", "catch_all"),
        ("catchall", "catch_all"),
        ("weird_status", "unknown"),
        (None, "unknown"),
    ],
)
def test_verify_with_fallback_maps_provider_status(
    monkeypatch: pytest.MonkeyPatch,
    provider_status: str | None,
    expected: str,
) -> None:
    """
    Provider-specific status strings should be mapped into our FallbackStatus.
    """

    # Enable fallback and provide dummy URL/API key
    monkeypatch.setattr(fb_mod, "THIRD_PARTY_VERIFY_ENABLED", True, raising=False)
    monkeypatch.setattr(
        fb_mod, "THIRD_PARTY_VERIFY_URL", "https://api.example.test/verify", raising=False
    )
    monkeypatch.setattr(fb_mod, "THIRD_PARTY_VERIFY_API_KEY", "secret", raising=False)

    # Fake httpx client that returns a canned JSON payload
    class _FakeResp:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return self._data

    class _FakeClient:
        def __init__(self, *a: Any, **k: Any) -> None:
            self.last_url: str | None = None
            self.last_json: dict[str, Any] | None = None
            self.last_headers: dict[str, Any] | None = None

        def __enter__(self) -> _FakeClient:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, json: dict[str, Any], headers: dict[str, Any]) -> _FakeResp:
            self.last_url = url
            self.last_json = json
            self.last_headers = headers
            payload: dict[str, Any] = {}
            if provider_status is not None:
                payload["status"] = provider_status
            return _FakeResp(payload)

    fake_httpx = types.SimpleNamespace(Client=_FakeClient)
    monkeypatch.setattr(fb_mod, "httpx", fake_httpx, raising=False)

    email = "user@example.com"
    res = fb_mod.verify_with_fallback(email)

    assert res.email == email
    assert res.status == expected
    assert isinstance(res.raw, dict)


def test_verify_with_fallback_handles_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Any HTTP/JSON errors should be swallowed and surfaced as status="unknown"
    with a small error note in raw.
    """

    monkeypatch.setattr(fb_mod, "THIRD_PARTY_VERIFY_ENABLED", True, raising=False)
    monkeypatch.setattr(
        fb_mod, "THIRD_PARTY_VERIFY_URL", "https://api.example.test/verify", raising=False
    )
    monkeypatch.setattr(fb_mod, "THIRD_PARTY_VERIFY_API_KEY", "secret", raising=False)

    class _BoomClient:
        def __init__(self, *a: Any, **k: Any) -> None:
            pass

        def __enter__(self) -> _BoomClient:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, json: dict[str, Any], headers: dict[str, Any]) -> Any:
            raise RuntimeError("network down")

    fake_httpx = types.SimpleNamespace(Client=_BoomClient)
    monkeypatch.setattr(fb_mod, "httpx", fake_httpx, raising=False)

    res = fb_mod.verify_with_fallback("user@example.com")
    assert res.status == "unknown"
    assert "error" in res.raw


# -----------------------------------
# Integration tests: R16 + O07 wiring
# -----------------------------------


def _patch_task_env_for_o07(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Common patches to make task_probe_email deterministic and self-contained.

    - Avoid Redis by making get_redis raise (redis_ok=False).
    - Avoid MX resolver by patching _mx_info.
    - Avoid catchall enqueue by patching _maybe_enqueue_catchall.
    """

    # No Redis in tests → redis_ok stays False, throttling paths are skipped.
    def _no_redis() -> None:
        raise RuntimeError("no redis in tests")

    monkeypatch.setattr(tasks, "get_redis", _no_redis, raising=False)

    # Deterministic MX info
    monkeypatch.setattr(
        tasks,
        "_mx_info",
        lambda domain, force, db_path: ("mx.test.local", None),
        raising=False,
    )

    # R17 catch-all enqueue is best-effort; skip in unit tests.
    monkeypatch.setattr(tasks, "_maybe_enqueue_catchall", lambda d: None, raising=False)

    _bypass_tcp25_preflight(monkeypatch)


def test_task_probe_email_calls_fallback_for_ambiguous(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    When the SMTP classification is ambiguous (category 'unknown' or 'temp_fail'),
    task_probe_email should invoke verify_with_fallback() and surface
    fallback_status / fallback_raw in the returned dict.
    """
    _patch_task_env_for_o07(monkeypatch)

    # Fake SMTP probe → ambiguous classification
    def fake_probe_rcpt(email: str, mx_host: str, **kwargs: Any) -> dict[str, Any]:
        return {
            "ok": False,
            "category": "unknown",
            "code": 450,
            "mx_host": mx_host,
            "elapsed_ms": 7,
            "error": "temp",
        }

    monkeypatch.setattr(tasks, "probe_rcpt", fake_probe_rcpt, raising=False)

    # Spy/fake fallback verifier
    calls: list[str] = []

    class _FakeFallbackResult:
        def __init__(self, email: str) -> None:
            self.email = email
            self.status = "catch_all"
            self.raw = {"status": "catch_all", "via": "fake"}

    def fake_verify_with_fallback(email: str) -> _FakeFallbackResult:
        calls.append(email)
        return _FakeFallbackResult(email)

    monkeypatch.setattr(tasks, "verify_with_fallback", fake_verify_with_fallback, raising=False)

    res = tasks.task_probe_email(1, "user@example.com", "example.com", force=False)

    assert res["email"] == "user@example.com"
    assert res["category"] == "unknown"
    assert res["code"] == 450
    # O07 surfaces fallback info
    assert res["fallback_status"] == "catch_all"
    assert res["fallback_raw"] == {"status": "catch_all", "via": "fake"}
    assert calls == ["user@example.com"]


def test_task_probe_email_skips_fallback_for_non_ambiguous(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    For clearly classified results (e.g., category 'accept' / 'hard_fail'),
    task_probe_email should *not* call verify_with_fallback().
    """
    _patch_task_env_for_o07(monkeypatch)

    # Fake SMTP probe → clear success
    def fake_probe_rcpt(email: str, mx_host: str, **kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "category": "accept",
            "code": 250,
            "mx_host": mx_host,
            "elapsed_ms": 5,
            "error": None,
        }

    monkeypatch.setattr(tasks, "probe_rcpt", fake_probe_rcpt, raising=False)

    calls: list[str] = []

    def fake_verify_with_fallback(email: str) -> fb_mod.FallbackResult:  # pragma: no cover
        calls.append(email)
        return fb_mod.FallbackResult(email=email, status="unknown", raw={})

    monkeypatch.setattr(tasks, "verify_with_fallback", fake_verify_with_fallback, raising=False)

    res = tasks.task_probe_email(2, "user@example.com", "example.com", force=False)

    assert res["email"] == "user@example.com"
    assert res["category"] == "accept"
    assert res["code"] == 250
    # No fallback info attached
    assert "fallback_status" not in res
    assert "fallback_raw" not in res
    assert calls == []
