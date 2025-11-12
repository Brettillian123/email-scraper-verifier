# tests/test_normalization.py
import pytest

# Robust import for src/ layout
try:
    from src.ingest.normalize import (
        norm_company_name,
        norm_domain,
        norm_person_name,
        norm_title,
    )
except ModuleNotFoundError:
    import os
    import sys

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from src.ingest.normalize import (
        norm_company_name,
        norm_domain,
        norm_person_name,
        norm_title,
    )


# -----------------------------
# Domain normalization (IDNA)
# -----------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("bücher.example", "xn--bcher-kva.example"),
        ("例え.テスト", "xn--r8jz45g.xn--zckzah"),
        (" EXAMPLE.com ", "example.com"),
        (None, None),
        ("", None),
    ],
)
def test_norm_domain_idna_lowercase(raw, expected):
    assert norm_domain(raw) == expected


# -----------------------------
# Person name normalization
# -----------------------------


@pytest.mark.parametrize(
    "first,last,exp_first,exp_last",
    [
        # particles kept lowercase unless leading; last word capped
        ("jean", "de la cruz", "Jean", "de la Cruz"),
        # preserve hyphens and capitalize parts around them
        ("mary-kate", "o’leary", "Mary-Kate", "O’Leary"),
        ("Jean-Luc", "Picard", "Jean-Luc", "Picard"),
        # multi-token given names
        ("  anna   maria  ", "  schmidt ", "Anna Maria", "Schmidt"),
        # empty → soft warning, empty display
        (None, None, "", ""),
    ],
)
def test_norm_person_name_cases(first, last, exp_first, exp_last):
    first_out, last_out, errs = norm_person_name(first, last)
    assert (first_out, last_out) == (exp_first, exp_last)
    assert isinstance(errs, list)


# -----------------------------
# Title normalization
# -----------------------------


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
def test_title_normalization_rules(raw, expected):
    got, errs = norm_title(raw)
    assert got == expected
    assert isinstance(errs, list)


# -----------------------------
# Company normalization
# -----------------------------


@pytest.mark.parametrize(
    "raw,exp_name_norm,exp_key",
    [
        ("Crestwell Partners, LLC", "Crestwell Partners, LLC", "crestwell partners"),
        ("  Bücher   GmbH  ", "Bücher GmbH", "bucher"),
        ("Acme inc", "Acme, Inc.", "acme"),
        ("Example Pty Ltd", "Example Pty Ltd", "example"),
        ("DataCo S.A.", "DataCo S.A.", "dataco"),
        ("", None, None),
        (None, None, None),
    ],
)
def test_company_suffix_display_and_key(raw, exp_name_norm, exp_key):
    nn, key, errs = norm_company_name(raw)
    assert nn == exp_name_norm
    assert key == exp_key
    assert isinstance(errs, list)
