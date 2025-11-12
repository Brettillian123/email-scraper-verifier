# tests/test_ingest_normalize.py
import json

import pytest

from src.ingest.normalize import (
    norm_company_name,
    norm_domain,
    norm_title,
    normalize_row,
)

# -----------------------------
# Domain normalization (R13)
# -----------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("acme.com", "acme.com"),
        # IDN: bücher.de -> xn--bcher-kva.de
        ("bücher.de", "xn--bcher-kva.de"),
        ("XN--BCHER-KVA.DE", "xn--bcher-kva.de"),  # already-IDNA, lowercased
        (None, None),
        ("", None),
    ],
)
def test_norm_domain_idna(raw, expected):
    assert norm_domain(raw) == expected


# -----------------------------
# Name + title normalization
# -----------------------------


@pytest.mark.parametrize(
    "full_name,exp_first,exp_last",
    [
        (" jean   de la   cruz ", "Jean", "de la Cruz"),
        ("mary-kate o’leary", "Mary-Kate", "O’Leary"),
    ],
)
def test_name_particles_and_hyphens_via_row(full_name, exp_first, exp_last):
    row, errs = normalize_row({"full_name": full_name})
    assert row["first_name"] == exp_first
    assert row["last_name"] == exp_last
    # Should not hard-error
    assert isinstance(errs, list)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("vp, sales & marketing", "VP, Sales & Marketing"),
        ("Head Of Data", "Head of Data"),
        ("cto", "CTO"),
        ("Phd researcher", "PhD Researcher"),
        ("", None),
        (None, None),
    ],
)
def test_title_normalization(raw, expected):
    got, _errs = norm_title(raw)
    assert got == expected


# -----------------------------
# Company normalization
# -----------------------------


@pytest.mark.parametrize(
    "raw,exp_name_norm,exp_key",
    [
        ("Crestwell Partners, LLC", "Crestwell Partners, LLC", "crestwell partners"),
        ("  Bücher   GmbH  ", "Bücher GmbH", "bucher"),
        ("Acme Inc", "Acme, Inc.", "acme"),
        ("Example Pty Ltd", "Example Pty Ltd", "example"),
        ("", None, None),
        (None, None, None),
    ],
)
def test_company_normalization(raw, exp_name_norm, exp_key):
    nn, key, errs = norm_company_name(raw)
    assert nn == exp_name_norm
    assert key == exp_key
    assert isinstance(errs, list)


# -----------------------------
# Provenance preservation
# -----------------------------


def test_normalize_row_preserves_source_url_and_titles():
    raw = {
        "company": "Crestwell Partners, LLC",
        "domain": "bücher.de",
        "full_name": "mary-kate o’leary",
        "title": "vp, sales & marketing",
        "source_url": "https://example.com/team",
    }
    row, errs = normalize_row(raw)

    # Provenance must be carried through untouched
    assert row["source_url"] == raw["source_url"]

    # Title fields: original is preserved; normalized computed
    assert row["title"] == "vp, sales & marketing"
    assert row["title_raw"] == "vp, sales & marketing"
    assert row["title_norm"] == "VP, Sales & Marketing"

    # Company helpers for upsert
    assert row["company_name_norm"] == "Crestwell Partners, LLC"
    assert row["company_norm_key"] == "crestwell partners"

    # Domain helper
    assert row["norm_domain"] == "xn--bcher-kva.de"

    # Errors is a JSON array string
    assert isinstance(row["errors"], str)
    json.loads(row["errors"])  # must be valid JSON
