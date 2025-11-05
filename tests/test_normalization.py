import pytest

# Robust import for src/ layout
try:
    from ingest import map_role, normalize_domain, split_name
except ModuleNotFoundError:
    import os
    import sys

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
    from ingest import map_role, normalize_domain, split_name


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("bücher.example", "xn--bcher-kva.example"),
        ("http://例え.テスト", "xn--r8jz45g.xn--zckzah"),
        (" EXAMPLE.com ", "example.com"),
        (None, ""),
    ],
)
def test_normalize_domain_idn_to_ascii(raw, expected):
    assert normalize_domain(raw) == expected


@pytest.mark.parametrize(
    "role,expected",
    [
        ("CEO", "executive"),
        ("Chief Executive Officer", "executive"),
        ("Head of Sales", "sales"),
        ("Sales Manager", "sales"),
        ("Unknown-Role", "other"),
    ],
)
def test_map_role_buckets(role, expected):
    assert map_role(role) == expected


@pytest.mark.parametrize(
    "full,first,last",
    [
        ("Ada Lovelace", "Ada", "Lovelace"),
        ("  anna  maria  schmidt ", "Anna Maria", "Schmidt"),
        ("Prince", "Prince", ""),
        ("Jean-Luc Picard", "Jean-Luc", "Picard"),
    ],
)
def test_split_name(full, first, last):
    first_val, last_val = split_name(full)
    assert (first_val, last_val) == (first, last)
