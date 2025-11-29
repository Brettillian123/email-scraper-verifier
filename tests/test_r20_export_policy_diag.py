from __future__ import annotations

import inspect
from typing import Any

import pytest

from src.export import exporter as exporter_mod
from src.export.policy import ExportPolicy


def _introspect_from_config(cls: type) -> tuple[str, inspect.Signature]:
    """
    Introspect whatever .from_config the given class exposes.
    Works for the real ExportPolicy and for test stubs.
    """
    raw_attr = inspect.getattr_static(cls, "from_config")

    if isinstance(raw_attr, classmethod):
        kind = "classmethod"
        func = raw_attr.__func__
    elif isinstance(raw_attr, staticmethod):
        kind = "staticmethod"
        func = raw_attr.__func__
    else:
        kind = type(raw_attr).__name__
        func = raw_attr  # type: ignore[assignment]

    sig = inspect.signature(func)  # type: ignore[arg-type]
    return kind, sig


def test_r20_real_export_policy_from_config_supported_by_loader() -> None:
    """
    Diagnostic/regression for the *real* ExportPolicy:

    - from_config must have either (cls, cfg) or (cls, name, cfg) shape.
    - exporter._load_export_policy('default') must be able to call it
      without raising a TypeError.
    """
    kind, sig = _introspect_from_config(ExportPolicy)
    params = list(sig.parameters.values())
    num_params = len(params)

    assert num_params in (2, 3), (
        "ExportPolicy.from_config is expected to look like either "
        "(cls, cfg) or (cls, name, cfg).\n\n"
        f"Descriptor kind: {kind}\n"
        f"Signature: {sig}"
    )
    assert params[-1].name == "cfg", (
        f"The last parameter of ExportPolicy.from_config should be 'cfg'.\n\nSignature: {sig}"
    )

    # This exercises exporter._load_export_policy against the real class.
    policy = exporter_mod._load_export_policy("default")
    assert isinstance(policy, ExportPolicy)


def test_r20_loader_works_with_legacy_cfg_only_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Diagnostic for the test stub scenario:

    Simulate the R20 stub_export_policy fixture by monkeypatching
    exporter.ExportPolicy to a FakePolicy where from_config has the
    legacy (cls, cfg) signature, and ensure _load_export_policy still
    works and returns a FakePolicy instance.
    """

    class FakePolicy:
        def __init__(self, cfg: dict[str, Any]) -> None:
            self.cfg = cfg

        @classmethod
        def from_config(cls, cfg: dict[str, Any]) -> FakePolicy:
            # In the real stub, this would inspect cfg, but we don't care here.
            return cls(cfg)

        def is_exportable_row(
            self,
            email: str,
            verify_status: str,
            icp_score: float,
            extra: Any,
        ) -> tuple[bool, str]:
            return True, "always_export"

    # Monkeypatch the exporter module's ExportPolicy reference to our stub.
    monkeypatch.setattr(exporter_mod, "ExportPolicy", FakePolicy)

    kind, sig = _introspect_from_config(FakePolicy)
    params = list(sig.parameters.values())
    assert len(params) == 2 and params[-1].name == "cfg", (
        "Sanity check: FakePolicy.from_config should have (cls, cfg) signature.\n\n"
        f"Descriptor kind: {kind}\n"
        f"Signature: {sig}"
    )

    # Now call the loader; it should introspect and call FakePolicy.from_config(cfg),
    # not FakePolicy.from_config(name, cfg).
    policy = exporter_mod._load_export_policy("default")

    assert isinstance(policy, FakePolicy), (
        "_load_export_policy('default') should return a FakePolicy when "
        "exporter.ExportPolicy is monkeypatched to the stub."
    )
    assert isinstance(policy.cfg, dict)
