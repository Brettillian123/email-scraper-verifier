from __future__ import annotations

import inspect
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.export import exporter as exporter_mod
from src.export.policy import ExportPolicy

# --- Helpers -----------------------------------------------------------------


def _introspect_method(
    cls: type,
    name: str,
) -> tuple[bool, str, inspect.Signature | None]:
    """
    Introspect a method on a class.

    Returns:
        (present, descriptor_kind, signature_or_None)
    """
    try:
        raw_attr = inspect.getattr_static(cls, name)
    except AttributeError:
        return False, "missing", None

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
    return True, kind, sig


def _load_icp_yaml() -> tuple[Path, Mapping[str, Any]]:
    """
    Load docs/icp-schema.yaml using the SAME root logic as exporter._load_export_policy.
    """
    # Mirror src/export/exporter.py:
    #   root = Path(__file__).resolve().parents[2]
    # but use exporter_mod.__file__ so we get the project root, not tests/.
    root = Path(exporter_mod.__file__).resolve().parents[2]
    cfg_path = root / "docs" / "icp-schema.yaml"
    if not cfg_path.exists():
        pytest.fail(
            f"Expected export policy config at {cfg_path}, but the file does not exist. "
            "src.export.exporter._load_export_policy() and scripts/export_leads.py "
            "both rely on this file being present.",
        )

    try:
        text = cfg_path.read_text(encoding="utf-8")
    except OSError as exc:
        pytest.fail(
            f"Unable to read export policy config at {cfg_path}: {exc!r}",
        )

    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        pytest.fail(
            f"Expected YAML mapping in {cfg_path}, got {type(data)!r}. "
            "exporter._load_export_policy expects a top-level mapping.",
        )

    return cfg_path, data


# --- Tests -------------------------------------------------------------------


def test_r20_real_export_policy_class_has_expected_interface() -> None:
    """
    Deep diagnostic of the *real* ExportPolicy class (not the test stub).

    This test does NOT monkeypatch anything. It checks:

      - that we are looking at src.export.policy.ExportPolicy (not a FakePolicy)
      - whether .is_exportable_row is defined on the class
      - what its descriptor kind and signature are
      - whether .should_export exists and what signature it has

    If is_exportable_row is missing, the failure message explains exactly
    how to implement it in terms of should_export, which is the current
    export-decision API, and connects that directly to the crash in
    scripts/export_leads.py.
    """
    cls = ExportPolicy
    cls_qualname = getattr(cls, "__qualname__", "")
    cls_module = getattr(cls, "__module__", "")

    assert "FakePolicy" not in cls_qualname, (
        "This diagnostic test must run against the real ExportPolicy class, "
        "but the current class appears to be a stub FakePolicy from tests.\n\n"
        f"ExportPolicy.__qualname__ = {cls_qualname!r}\n"
        f"ExportPolicy.__module__   = {cls_module!r}"
    )

    # 1) Check for is_exportable_row
    present, kind, sig = _introspect_method(cls, "is_exportable_row")

    # 2) Also introspect should_export, which is the legacy / O10 API.
    se_present, se_kind, se_sig = _introspect_method(cls, "should_export")

    if not present:
        pytest.fail(
            "Real ExportPolicy class does not define an 'is_exportable_row' method.\n\n"
            "R20 iter_exportable_leads() and scripts/export_leads.py call:\n"
            "    ok, reason = policy.is_exportable_row(...)\n\n"
            "The R20 unit tests pass because they monkeypatch ExportPolicy with a "
            "FakePolicy stub that *does* implement is_exportable_row, but the "
            "real ExportPolicy in src/export/policy.py is missing this method. "
            "That mismatch is the direct cause of:\n\n"
            "    AttributeError: 'ExportPolicy' object has no attribute 'is_exportable_row'\n"
            "    (raised from src/export/exporter.py when running export_leads.py)\n\n"
            "Observed export-related methods on ExportPolicy:\n"
            f"  - should_export present: {se_present}, kind: {se_kind}, signature: {se_sig}\n\n"
            "This strongly suggests that O10 implemented a method like:\n"
            "    def should_export(self, email: str, verify_status: str | None,\n"
            "                      icp_score: float | None, extra: Mapping[str, Any]) -> bool:\n"
            "        ...\n\n"
            "and R20 later introduced a more descriptive API:\n"
            "    def is_exportable_row(\n"
            "        self,\n"
            "        *,\n"
            "        email: str,\n"
            "        verify_status: str | None,\n"
            "        icp_score: float | None,\n"
            "        extra: Mapping[str, Any],\n"
            "    ) -> tuple[bool, str]:\n"
            "        ...\n\n"
            "The tests for R20 stub this newer API, but the real ExportPolicy was\n"
            "never updated to add is_exportable_row. To fix the crash, implement\n"
            "is_exportable_row on ExportPolicy as a thin wrapper around should_export,\n"
            "for example:\n\n"
            "    def is_exportable_row(\n"
            "        self,\n"
            "        *,\n"
            "        email: str,\n"
            "        verify_status: str | None,\n"
            "        icp_score: float | None,\n"
            "        extra: Mapping[str, Any],\n"
            "    ) -> tuple[bool, str]:\n"
            "        ok = self.should_export(\n"
            "            email=email,\n"
            "            verify_status=verify_status,\n"
            "            icp_score=icp_score,\n"
            "            extra=extra,\n"
            "        )\n"
            "        reason = 'policy_allow' if ok else 'policy_block'\n"
            "        return ok, reason\n\n"
            "Once that method exists, both iter_exportable_leads(...) and "
            "scripts/export_leads.py will be able to call it, and the AttributeError "
            "will disappear.",
        )

    # If is_exportable_row *is* present, validate its basic shape.
    assert sig is not None  # for type checkers
    params = list(sig.parameters.values())
    param_names = [p.name for p in params]

    required = {"email", "verify_status", "icp_score", "extra"}
    missing = required - set(param_names)

    if missing:
        pytest.fail(
            "ExportPolicy.is_exportable_row exists but does not expose the expected "
            "parameters that iter_exportable_leads/export_leads rely on.\n\n"
            f"Expected parameters (besides 'self'/'cls'): {sorted(required)!r}\n"
            f"Actual parameters: {param_names!r}\n"
            f"Missing parameters: {sorted(missing)!r}\n\n"
            "iter_exportable_leads calls is_exportable_row with:\n"
            "    email=email,\n"
            "    verify_status=row['verify_status'],\n"
            "    icp_score=row['icp_score'],\n"
            "    extra=row,\n"
            "so the method must accept these names (either positionally or via kwargs).",
        )

    assert params[0].name in {"self", "cls"}, (
        "First parameter of ExportPolicy.is_exportable_row should be 'self' or 'cls', "
        f"but got {params[0].name!r}.\n\nSignature: {sig}"
    )


def test_r20_real_export_policy_can_be_loaded_and_called() -> None:
    """
    End-to-end diagnostic of the real ExportPolicy:

      1. Load docs/icp-schema.yaml using the same root logic as exporter._load_export_policy.
      2. Verify export_policies.default exists.
      3. Use exporter._load_export_policy('default') to construct a real policy.
      4. Assert that the resulting object has is_exportable_row, and if not, explain
         why export_leads.py will crash.
    """
    cfg_path, data = _load_icp_yaml()

    policies = data.get("export_policies") or {}  # type: ignore[assignment]
    assert isinstance(policies, dict), (
        f"'export_policies' in {cfg_path} must be a mapping. Got {type(policies)!r}."
    )

    assert "default" in policies, (
        f"{cfg_path} must define 'export_policies.default' because "
        "scripts/export_leads.py and exporter._load_export_policy() rely on it."
    )

    try:
        policy = exporter_mod._load_export_policy("default")
    except Exception as exc:  # noqa: BLE001
        pytest.fail(
            "exporter._load_export_policy('default') raised an exception when "
            "trying to construct a real ExportPolicy instance.\n\n"
            f"Exception type: {type(exc).__name__}\n"
            f"Exception: {exc!r}",
        )

    assert isinstance(policy, ExportPolicy), (
        "exporter._load_export_policy('default') should return an instance of "
        "src.export.policy.ExportPolicy when not running under the R20 stub "
        "fixtures.\n\n"
        f"Got instance of type: {type(policy)!r}"
    )

    if not hasattr(policy, "is_exportable_row"):
        attrs = sorted(
            name
            for name in dir(policy)
            if "export" in name.lower() or "policy" in name.lower() or "icp" in name.lower()
        )
        pytest.fail(
            "Real ExportPolicy instance returned by exporter._load_export_policy('default') "
            "does not have an 'is_exportable_row' method.\n\n"
            "This is the *exact* object that scripts/export_leads.py uses, which is why "
            "you see:\n\n"
            "    AttributeError: 'ExportPolicy' object has no attribute 'is_exportable_row'\n"
            "    (from src/export/exporter.py: ok, _reason = policy.is_exportable_row(...))\n\n"
            "Implementing is_exportable_row on ExportPolicy, with the same semantics as the "
            "FakePolicy stub used in tests/test_r20_export_pipeline.py, will resolve this.\n\n"
            f"Export-related attributes found on the policy instance: {attrs!r}",
        )

    # If it *is* present, we could also test call/return type here, but the
    # integration test below will exercise that in a more realistic context.


def test_r20_real_export_policy_can_be_used_in_iter_exportable_leads() -> None:
    """
    Integration-style diagnostic:

      - Build a tiny in-memory SQLite DB with v_emails_latest and suppression.
      - Insert a single 'good' row that should be eligible for export.
      - Run exporter.iter_exportable_leads(...) WITHOUT any test stub.
      - If an AttributeError or TypeError is raised from policy.is_exportable_row,
        fail with a detailed explanation.

    This approximates the same call path as scripts/export_leads.py, but with
    a minimal disposable DB so you can reproduce the error under pytest.
    """

    # 1) Build minimal in-memory DB schema expected by iter_candidate_rows + suppression.
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE suppression (
            email TEXT PRIMARY KEY,
            reason TEXT,
            source TEXT,
            created_at TEXT
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE v_emails_latest (
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            title_norm TEXT,
            title_raw TEXT,
            company_name TEXT,
            company_domain TEXT,
            source_url TEXT,
            icp_score REAL,
            verify_status TEXT,
            verified_at TEXT
        )
        """,
    )

    # Seed a single high-ICP, valid row that should pass any reasonable export policy.
    conn.execute(
        """
        INSERT INTO v_emails_latest (
            email, first_name, last_name, title_norm,
            company_name, company_domain, source_url,
            icp_score, verify_status, verified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "diagnostic@example.com",
            "Diag",
            "Nostic",
            "CEO",
            "Diagnostic Corp",
            "diagnostic.com",
            "http://example.com",
            99.0,
            "valid",
            "2025-11-28T00:00:00Z",
        ),
    )

    # 2) Try to iterate exportable leads using the real loader + policy.
    try:
        leads = list(
            exporter_mod.iter_exportable_leads(
                conn,
                policy_name="default",
            ),
        )
    except AttributeError as exc:
        pytest.fail(
            "exporter.iter_exportable_leads(...) raised AttributeError when using the "
            "real ExportPolicy implementation.\n\n"
            "This is the same call path that scripts/export_leads.py uses.\n\n"
            f"AttributeError: {exc!r}\n\n"
            "This confirms that the real ExportPolicy object returned by "
            "exporter._load_export_policy('default') does not implement the "
            "interface that iter_exportable_leads expects. In particular, it must "
            "define:\n\n"
            "    def is_exportable_row(\n"
            "        self,\n"
            "        *,\n"
            "        email,\n"
            "        verify_status,\n"
            "        icp_score,\n"
            "        extra,\n"
            "    ) -> tuple[bool, str]\n\n"
            "and that method is currently missing.",
        )
    except TypeError as exc:
        pytest.fail(
            "exporter.iter_exportable_leads(...) raised TypeError when calling the "
            "real ExportPolicy.is_exportable_row(...).\n\n"
            "This indicates a mismatch between the expected signature:\n"
            "    is_exportable_row(self, *, email, verify_status, icp_score, extra)\n"
            "and the actual implementation.\n\n"
            f"TypeError: {exc!r}",
        )

    # If no exception, we at least ensure we got zero or more ExportLead objects.
    # We don't assert exact export policy semantics here, just that the pipeline
    # doesn't crash when using the real ExportPolicy.
    assert isinstance(leads, list)
