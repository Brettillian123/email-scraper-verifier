# tests/test_ingest_normalize.py
import pytest

# Import after setting up any env in other tests; here we just import directly.
from src import ingest as I  # expects src/ingest.py with functions shown below


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("acme.com", "acme.com"),
        ("http://acme.com", "acme.com"),
        ("https://WWW.Acme.COM/path?q=1", "acme.com"),
        # IDN: bücher.de -> xn--bcher-kva.de
        ("bücher.de", "xn--bcher-kva.de"),
        ("https://bücher.de/team", "xn--bcher-kva.de"),
        ("http://xn--bcher-kva.de", "xn--bcher-kva.de"),
    ],
)
def test_normalize_domain(raw, expected):
    assert I.normalize_domain(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        (" Acme, Inc.  ", "Acme, Inc."),
        ("\nBücher   GmbH\t", "Bücher GmbH"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_company(raw, expected):
    assert I.normalize_company(raw) == expected


@pytest.mark.parametrize(
    "full,first,last",
    [
        ("Ada Lovelace", "Ada", "Lovelace"),
        ("Dr. John Q. Public Jr.", "John", "Public"),
        ("  Anna   Schmidt  ", "Anna", "Schmidt"),
        ("", "", ""),
        (None, "", ""),
    ],
)
def test_split_name(full, first, last):
    got_first, got_last = I.split_name(full)
    assert (got_first, got_last) == (first, last)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("cto", "engineering"),
        ("Head of Sales", "sales"),
        ("VP Marketing", "marketing"),
        ("CFO", "finance"),
        ("IT Manager", "it"),
        ("Chief Operating Officer", "operations"),
        ("Founder", "founder"),
        ("Unknown Thing", "other"),
        ("", "other"),
        (None, "other"),
    ],
)
def test_map_role(raw, expected):
    assert I.map_role(raw) == expected
