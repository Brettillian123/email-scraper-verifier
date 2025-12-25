# tests/test_o26_verify_labels.py
from __future__ import annotations

from src.verify.labels import (
    VerifyLabel,
    choose_primary_index,
    classify_base_label,
    compute_verify_label,
    compute_verify_label_from_row,
    is_test_send_upgrade,
)


def test_classify_base_label_valid_native_vs_catchall_tested() -> None:
    # Plain valid with no special reason → native.
    assert classify_base_label("valid", None) == VerifyLabel.VALID_NATIVE

    # O26 upgrade reason → catch-all tested.
    assert (
        classify_base_label("valid", "no_bounce_after_test_send")
        == VerifyLabel.VALID_CATCHALL_TESTED
    )

    # Non-valid statuses pass through as-is.
    assert classify_base_label("invalid", None) == VerifyLabel.INVALID
    assert classify_base_label("risky_catch_all", None) == VerifyLabel.RISKY_CATCH_ALL
    assert classify_base_label("unknown_timeout", None) == VerifyLabel.UNKNOWN_TIMEOUT


def test_compute_verify_label_primary_and_alternate() -> None:
    # Native valid, no primary flag.
    assert compute_verify_label("valid", None, is_primary=None) == VerifyLabel.VALID_NATIVE

    # Native valid, primary vs alternate.
    assert compute_verify_label("valid", None, is_primary=True) == VerifyLabel.VALID_NATIVE_PRIMARY
    assert (
        compute_verify_label("valid", None, is_primary=False) == VerifyLabel.VALID_NATIVE_ALTERNATE
    )

    # Catch-all-tested valid, primary vs alternate.
    assert (
        compute_verify_label(
            "valid",
            "no_bounce_after_test_send",
            is_primary=True,
        )
        == VerifyLabel.VALID_CATCHALL_TESTED_PRIMARY
    )
    assert (
        compute_verify_label(
            "valid",
            "no_bounce_after_test_send",
            is_primary=False,
        )
        == VerifyLabel.VALID_CATCHALL_TESTED_ALTERNATE
    )


def test_is_test_send_upgrade_helper_and_row_wrapper() -> None:
    upgraded_row = {
        "verify_status": "valid",
        "verify_reason": "no_bounce_after_test_send",
        "email": "brett.anderson@example.com",
        "source": "extracted",
    }
    native_row = {
        "verify_status": "valid",
        "verify_reason": "no_bounce_other_reason",
        "email": "brett.anderson@example.com",
        "source": "extracted",
    }

    assert is_test_send_upgrade(upgraded_row) is True
    assert is_test_send_upgrade(native_row) is False

    # compute_verify_label_from_row should automatically pick the right base.
    assert (
        compute_verify_label_from_row(upgraded_row, is_primary=True)
        == VerifyLabel.VALID_CATCHALL_TESTED_PRIMARY
    )
    assert (
        compute_verify_label_from_row(native_row, is_primary=True)
        == VerifyLabel.VALID_NATIVE_PRIMARY
    )


def test_compute_verify_label_for_non_valid_passthrough() -> None:
    # For non-valid statuses, label is just the coarse status bucket.
    for status, expected in [
        ("invalid", VerifyLabel.INVALID),
        ("risky_catch_all", VerifyLabel.RISKY_CATCH_ALL),
        ("unknown_timeout", VerifyLabel.UNKNOWN_TIMEOUT),
    ]:
        label = compute_verify_label(status, "anything", is_primary=True)
        assert label == expected


def test_choose_primary_index_returns_none_if_no_valids() -> None:
    rows = [
        {
            "verify_status": "invalid",
            "verify_reason": "hard_bounce",
            "email": "bad@example.com",
            "source": "extracted",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-01T00:00:00",
        },
        {
            "verify_status": "risky_catch_all",
            "verify_reason": "rcpt_2xx_unknown_catchall",
            "email": "maybe@example.com",
            "source": "generated",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-02T00:00:00",
        },
    ]

    primary_idx = choose_primary_index(rows)
    assert primary_idx is None


def test_choose_primary_prefers_extracted_over_generated() -> None:
    rows = [
        {
            "verify_status": "valid",
            "verify_reason": None,
            "email": "brett.anderson@example.com",
            "source": "extracted",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-01T00:00:00",
        },
        {
            "verify_status": "valid",
            "verify_reason": None,
            "email": "brett.anderson+alias@example.com",
            "source": "generated",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-02T00:00:00",
        },
    ]

    # Both are native-valid, but 'extracted' should win over 'generated'.
    primary_idx = choose_primary_index(rows)
    assert primary_idx == 0


def test_choose_primary_prefers_native_over_catchall_tested() -> None:
    rows = [
        {
            # Native valid
            "verify_status": "valid",
            "verify_reason": None,
            "email": "banderson@example.com",
            "source": "generated",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-01T00:00:00",
        },
        {
            # Catch-all-tested valid (O26 upgrade)
            "verify_status": "valid",
            "verify_reason": "no_bounce_after_test_send",
            "email": "brett.anderson@example.com",
            "source": "generated",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-02T00:00:00",
        },
    ]

    # With the same source, native valid should be preferred over catchall-tested
    # for primary selection.
    primary_idx = choose_primary_index(rows)
    assert primary_idx == 0


def test_choose_primary_penalises_role_like_localparts() -> None:
    rows = [
        {
            # Role-like address; should be strongly penalised.
            "verify_status": "valid",
            "verify_reason": None,
            "email": "info@example.com",
            "source": "extracted",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-02T00:00:00",
        },
        {
            # Name-based address; more human-looking, same source.
            "verify_status": "valid",
            "verify_reason": None,
            "email": "brett.anderson@example.com",
            "source": "extracted",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-01T00:00:00",
        },
    ]

    primary_idx = choose_primary_index(rows)
    assert primary_idx == 1


def test_full_label_codes_for_primary_and_alternate_rows() -> None:
    """
    End-to-end sanity check for label codes when we apply a primary/alternate
    split for a single person.
    """
    rows = [
        {
            "verify_status": "valid",
            "verify_reason": None,
            "email": "banderson@example.com",
            "source": "generated",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-01T00:00:00",
        },
        {
            "verify_status": "valid",
            "verify_reason": "no_bounce_after_test_send",
            "email": "abrett@example.com",
            "source": "generated",
            "first_name": "Brett",
            "last_name": "Anderson",
            "verified_at": "2025-01-02T00:00:00",
        },
    ]

    primary_idx = choose_primary_index(rows)
    assert primary_idx == 0

    labels = []
    for idx, row in enumerate(rows):
        is_primary = idx == primary_idx
        labels.append(compute_verify_label_from_row(row, is_primary=is_primary))

    # First row is native primary; second is catch-all-tested alternate.
    assert labels[0] == VerifyLabel.VALID_NATIVE_PRIMARY
    assert labels[1] == VerifyLabel.VALID_CATCHALL_TESTED_ALTERNATE
