# tests/test_email_pattern_vocabulary.py
"""
Guarantees for the canonical email-format vocabulary.

The round-trip invariant is the load-bearing one: if classify_pattern and
render_pattern ever stop being exact inverses, every count in
company_email_pattern silently starts describing the wrong format.
"""

from __future__ import annotations

import pytest

from src.generate.patterns import (
    PATTERN_OTHER,
    PATTERN_PRIOR_ORDER,
    PATTERN_VOCABULARY,
    RENDERABLE_PATTERNS,
    classify_pattern,
    fold_name,
    is_role_localpart,
    localpart_of,
    name_variants,
    pattern_confidence,
    render_pattern,
    render_pattern_variants,
)

DOMAIN = "crestwellpartners.com"


@pytest.mark.parametrize("pattern", RENDERABLE_PATTERNS)
@pytest.mark.parametrize(
    ("first", "last"),
    [
        ("John", "Doe"),
        ("José", "García"),
        ("Ana", "García-López"),
        ("Mary Jane", "van der Berg"),
        ("Søren", "Kierkegaard"),
    ],
)
def test_round_trip(pattern: str, first: str, last: str) -> None:
    """classify_pattern(render_pattern(p, fn, ln, d), fn, ln) == p"""
    address = render_pattern(pattern, first, last, DOMAIN)
    assert address is not None, f"{pattern} did not render for {first} {last}"
    assert classify_pattern(address, first, last) == pattern


def test_vocabulary_shape() -> None:
    assert PATTERN_VOCABULARY == (*RENDERABLE_PATTERNS, PATTERN_OTHER)
    assert set(PATTERN_PRIOR_ORDER) == set(RENDERABLE_PATTERNS)
    assert len(PATTERN_PRIOR_ORDER) == len(RENDERABLE_PATTERNS)


def test_other_is_not_renderable() -> None:
    assert render_pattern(PATTERN_OTHER, "John", "Doe", DOMAIN) is None
    assert render_pattern("not-a-pattern", "John", "Doe", DOMAIN) is None
    # Unrecognisable local-parts classify as "other" rather than raising.
    assert classify_pattern("jd2024", "John", "Doe") == PATTERN_OTHER


def test_diacritics_are_folded() -> None:
    assert fold_name("José") == "jose"
    assert fold_name("Gonçalves") == "goncalves"
    assert fold_name("Müller-Schmidt") == "mullerschmidt"
    assert fold_name("Søren") == "soren"
    assert fold_name("O'Brien") == "obrien"
    assert render_pattern("first.last", "José", "García", DOMAIN) == f"jose.garcia@{DOMAIN}"
    # The company writes the folded form; we must still recognise the person.
    assert classify_pattern("jose.garcia", "José", "García") == "first.last"


def test_compound_and_last_token_surnames_are_both_offered() -> None:
    assert name_variants("García-López") == ["garcialopez", "lopez"]
    assert name_variants("van der Berg") == ["vanderberg", "berg"]
    assert name_variants("Mary Jane", keep="first") == ["maryjane", "mary"]
    assert render_pattern_variants("flast", "José", "García-López", DOMAIN) == [
        f"jgarcialopez@{DOMAIN}",
        f"jlopez@{DOMAIN}",
    ]
    # Either form is recognised on the way back in.
    assert classify_pattern("jlopez", "José", "García-López") == "flast"
    assert classify_pattern("jgarcialopez", "José", "García-López") == "flast"


@pytest.mark.parametrize(
    "local",
    [
        # RFC 2142
        "info",
        "sales",
        "support",
        "admin",
        "postmaster",
        "abuse",
        "webmaster",
        "hostmaster",
        "noc",
        "security",
        # obvious extras
        "hello",
        "contact",
        "team",
        "careers",
        "jobs",
        "press",
        "help",
        "billing",
        # embedded as a token
        "sales-team",
        "info_us",
        "support2",
    ],
)
def test_role_localparts_are_rejected(local: str) -> None:
    assert is_role_localpart(local) is True


def test_role_addresses_are_never_generated_for_a_person() -> None:
    # A person actually named "Info Sales" must not produce info@ or sales@.
    assert render_pattern("first", "Info", "Sales", DOMAIN) is None
    assert render_pattern("last", "Info", "Sales", DOMAIN) is None
    # Steve Jobs loses jobs@ but keeps sjobs@.
    assert render_pattern("last", "Steve", "Jobs", DOMAIN) is None
    assert render_pattern("flast", "Steve", "Jobs", DOMAIN) == f"sjobs@{DOMAIN}"
    assert not is_role_localpart("sjobs")


def test_missing_name_parts_do_not_produce_addresses() -> None:
    assert render_pattern("first.last", "John", "", DOMAIN) is None
    assert render_pattern("flast", "", "Doe", DOMAIN) is None
    assert render_pattern("first", "John", "", DOMAIN) == f"john@{DOMAIN}"
    assert render_pattern("first.last", "John", "Doe", "") is None


def test_localpart_normalisation() -> None:
    assert localpart_of("John.Doe@Example.com") == "john.doe"
    assert localpart_of("jose.garcia+leads@example.com") == "jose.garcia"
    assert localpart_of("") == ""


def test_confidence_grades() -> None:
    assert pattern_confidence(3, 0)[1] == "trusted"
    assert pattern_confidence(2, 5)[1] == "trusted"
    assert pattern_confidence(1, 0)[1] == "candidate"
    assert pattern_confidence(0, 4)[1] == "refuted"
    assert pattern_confidence(0, 0)[1] == "unknown"
    # Monotone in evidence, and defined at zero evidence.
    assert pattern_confidence(0, 0)[0] == 0.5
    assert pattern_confidence(5, 0)[0] > pattern_confidence(1, 0)[0]
    assert pattern_confidence(0, 5)[0] < pattern_confidence(0, 1)[0]


def test_ambiguous_localpart_resolves_to_the_higher_yield_pattern() -> None:
    # A one-letter first name makes flast and firstlast identical; flast wins
    # 95% of decidable probes here, firstlast 65%.
    assert classify_pattern("jdoe", "J", "Doe") == "flast"
