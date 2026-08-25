# tests/test_address_harvest.py
"""
Unit tests for src/extract/address_harvest.py.

Everything here runs against inline HTML fixtures -- no database, no network.
"""

from __future__ import annotations

import pytest

from src.extract.address_harvest import (
    HarvestedAddress,
    PersonRef,
    attribute_address,
    attribute_local_part,
    build_allowed_domains,
    domain_matches,
    harvest_page,
    is_role_local_part,
    normalize_host,
)

SOURCE_URL = "https://www.acmerobotics.com/team"
ALLOWED = build_allowed_domains("acmerobotics.com", SOURCE_URL)

# One fixture covering every extraction path plus both rejection paths.
FIXTURE_HTML = """
<html>
  <head>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "Organization",
      "name": "Acme Robotics",
      "employee": [
        {
          "@type": "Person",
          "name": "Priya Raghunathan",
          "jobTitle": "Chief Technology Officer",
          "email": "mailto:priya.raghunathan@acmerobotics.com"
        }
      ]
    }
    </script>
    <script>
      // JS-assembled address: deliberately NOT decoded.
      var u = 'hidden' + '@' + 'acmerobotics.com';
    </script>
  </head>
  <body>
    <div class="card">
      <h3>Maria Okonkwo</h3>
      <p>Head of Field Operations</p>
      <a
        href="mailto:m.okonkwo@acmerobotics.com?subject=Hello%20there&amp;cc=x@y.com"
      >Email Maria</a>
    </div>

    <div class="card">
      <h3>Tomas Lindqvist</h3>
      <p>Reach him at tomas.lindqvist&#64;acmerobotics.com any time.</p>
    </div>

    <div class="card">
      <h3>Wei Zhang</h3>
      <p>wei [at] acmerobotics [dot] com</p>
    </div>

    <footer>
      <p>General enquiries: hello@acmerobotics.com</p>
      <p>Recruiting partner: sandra.melnyk@talentbridgesearch.com</p>
    </footer>
  </body>
</html>
"""


@pytest.fixture(scope="module")
def harvest():
    return harvest_page(FIXTURE_HTML, source_url=SOURCE_URL, allowed_domains=ALLOWED)


def _by_email(harvest) -> dict[str, HarvestedAddress]:
    return {a.email: a for a in harvest.personal}


# ---------------------------------------------------------------------------
# Extraction paths
# ---------------------------------------------------------------------------


def test_jsonld_person_email_is_harvested(harvest):
    addr = _by_email(harvest)["priya.raghunathan@acmerobotics.com"]
    assert addr.method == "jsonld"
    assert addr.display_name == "Priya Raghunathan"
    assert addr.local_part == "priya.raghunathan"
    assert addr.domain == "acmerobotics.com"


def test_mailto_is_harvested_with_query_string_stripped(harvest):
    addr = _by_email(harvest)["m.okonkwo@acmerobotics.com"]
    assert addr.method == "mailto"
    assert "?" not in addr.email
    assert "subject" not in addr.email


def test_mailto_query_cc_address_is_not_harvested(harvest):
    # The ?cc=x@y.com rider must not leak in as its own address.
    assert "x@y.com" not in _by_email(harvest)


def test_plain_text_with_html_entity_is_harvested(harvest):
    addr = _by_email(harvest)["tomas.lindqvist@acmerobotics.com"]
    assert addr.method == "text"


def test_bracketed_obfuscation_is_decoded(harvest):
    addr = _by_email(harvest)["wei@acmerobotics.com"]
    assert addr.method == "obfuscated"


def test_javascript_assembled_address_is_not_decoded(harvest):
    assert "hidden@acmerobotics.com" not in _by_email(harvest)


# ---------------------------------------------------------------------------
# Rejection paths
# ---------------------------------------------------------------------------


def test_foreign_domain_address_is_rejected(harvest):
    assert "sandra.melnyk@talentbridgesearch.com" not in _by_email(harvest)
    assert all(a.domain == "acmerobotics.com" for a in harvest.personal)
    assert harvest.rejected_foreign >= 1


def test_role_address_is_separated_not_treated_as_a_person(harvest):
    assert "hello@acmerobotics.com" not in _by_email(harvest)
    assert [a.email for a in harvest.role] == ["hello@acmerobotics.com"]


def test_personal_set_is_exactly_the_four_named_people(harvest):
    assert set(_by_email(harvest)) == {
        "priya.raghunathan@acmerobotics.com",
        "m.okonkwo@acmerobotics.com",
        "tomas.lindqvist@acmerobotics.com",
        "wei@acmerobotics.com",
    }


# ---------------------------------------------------------------------------
# Conservative-by-design behaviours
# ---------------------------------------------------------------------------


def test_prose_containing_at_is_not_decoded_into_an_address():
    html = "<p>Take a look at acmerobotics.com for details.</p>"
    got = harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)
    assert got.personal == []
    assert got.role == []


def test_bare_word_obfuscation_requires_a_dot_marker():
    html = "<p>gabriela at acmerobotics dot com</p>"
    got = harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)
    assert [a.email for a in got.personal] == ["gabriela@acmerobotics.com"]


def test_placeholder_localparts_are_dropped():
    html = "<p>test@acmerobotics.com and yourname@acmerobotics.com</p>"
    got = harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)
    assert got.personal == []
    assert got.rejected_placeholder == 2


def test_duplicates_are_deduplicated_case_insensitively():
    html = """
      <a href="mailto:Rita.Vargas@AcmeRobotics.com">Rita</a>
      <p>rita.vargas@acmerobotics.com</p>
    """
    got = harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)
    assert len(got.personal) == 1
    # mailto outranks plain text, so that provenance wins.
    assert got.personal[0].method == "mailto"


def test_script_and_style_contents_are_not_scanned_as_visible_text():
    # Inline JSON/JS blobs routinely carry vendor and system addresses that
    # were never published as contact details.
    html = """
      <p>real.person@acmerobotics.com</p>
      <script>var cfg = {"apiOwner": "leak@acmerobotics.com"};</script>
      <style>/* styled@acmerobotics.com */</style>
    """
    got = harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)
    assert [a.email for a in got.personal] == ["real.person@acmerobotics.com"]


def test_a_bare_tld_in_the_allow_list_cannot_admit_everything():
    got = harvest_page(FIXTURE_HTML, source_url=SOURCE_URL, allowed_domains=["com"])
    assert len(got) == 0


def test_no_allowed_domains_yields_nothing():
    got = harvest_page(FIXTURE_HTML, source_url=SOURCE_URL, allowed_domains=[])
    assert len(got) == 0


def test_empty_html_is_safe():
    assert len(harvest_page("", source_url=SOURCE_URL, allowed_domains=ALLOWED)) == 0


def test_malformed_jsonld_does_not_raise():
    html = '<script type="application/ld+json">{not json,,,</script>'
    assert len(harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)) == 0


def test_jsonld_email_on_a_foreign_domain_is_still_rejected():
    html = """
    <script type="application/ld+json">
    {"@type": "Person", "name": "Outside Recruiter",
     "email": "r@otherfirm.com"}
    </script>
    """
    got = harvest_page(html, source_url=SOURCE_URL, allowed_domains=ALLOWED)
    assert got.personal == []
    assert got.rejected_foreign == 1


# ---------------------------------------------------------------------------
# Domain / role helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("https://WWW.Acme.com:443/team", "acme.com"),
        ("www.acme.com", "acme.com"),
        ("jane@acme.com", "acme.com"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_host(value, expected):
    assert normalize_host(value) == expected


@pytest.mark.parametrize(
    "email_domain,expected",
    [
        ("acme.com", True),
        ("www.acme.com", True),
        ("mail.acme.com", True),
        ("notacme.com", False),
        ("acme.com.evil.net", False),
        ("", False),
    ],
)
def test_domain_matches(email_domain, expected):
    assert domain_matches(email_domain, {"acme.com"}) is expected


@pytest.mark.parametrize(
    "local,expected",
    [
        ("hello", True),
        ("contact", True),
        ("team", True),
        ("careers", True),
        ("jobs", True),
        ("press", True),
        ("postmaster", True),  # RFC 2142
        ("hostmaster", True),  # RFC 2142, added by this module
        ("noc", True),  # RFC 2142, added by this module
        ("sales-eu", True),
        ("bioinformatics", False),  # must not trip on "info"
        ("wholesale", False),  # must not trip on "sales"
        ("priya.raghunathan", False),
    ],
)
def test_is_role_local_part(local, expected):
    assert is_role_local_part(local) is expected


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------

PEOPLE = [
    PersonRef(1, "Priya", "Raghunathan", "Priya Raghunathan"),
    PersonRef(2, "Maria", "Okonkwo", "Maria Okonkwo"),
    PersonRef(3, "Tomas", "Lindqvist", "Tomas Lindqvist"),
]


@pytest.mark.parametrize(
    "local,person_id,pattern",
    [
        ("priya.raghunathan", 1, "first.last"),
        ("m.okonkwo", 2, "f.last"),
        ("tomaslindqvist", 3, "firstlast"),
        ("priya", 1, "first"),
        ("praghunathan", 1, "flast"),
    ],
)
def test_attribute_local_part_matches_and_names_the_pattern(local, person_id, pattern):
    got = attribute_local_part(local, PEOPLE)
    assert got is not None
    assert (got.person_id, got.pattern) == (person_id, pattern)


def test_attribute_local_part_returns_none_when_unknown():
    assert attribute_local_part("someoneelse", PEOPLE) is None


def test_attribute_local_part_refuses_ambiguous_matches():
    twins = [PersonRef(10, "Alex", "Kim"), PersonRef(11, "Alex", "Kowalski")]
    assert attribute_local_part("alex", twins) is None


def test_attribute_local_part_ignores_plus_addressing():
    got = attribute_local_part("priya.raghunathan+news", PEOPLE)
    assert got is not None and got.person_id == 1


def test_display_name_breaks_an_ambiguous_tie():
    twins = [PersonRef(10, "Alex", "Kim", "Alex Kim"), PersonRef(11, "Alex", "Kowalski")]
    addr = HarvestedAddress(
        email="alex@acmerobotics.com",
        local_part="alex",
        domain="acmerobotics.com",
        source_url=SOURCE_URL,
        method="jsonld",
        display_name="Alex Kim",
    )
    got = attribute_address(addr, twins)
    assert got is not None and got.person_id == 10


def test_harvested_fixture_attributes_to_known_people(harvest):
    matched = {
        a.email: attribute_address(a, PEOPLE)
        for a in harvest.personal
        if attribute_address(a, PEOPLE) is not None
    }
    assert matched["priya.raghunathan@acmerobotics.com"].pattern == "first.last"
    assert matched["m.okonkwo@acmerobotics.com"].pattern == "f.last"
    assert matched["tomas.lindqvist@acmerobotics.com"].pattern == "first.last"
    # wei@ belongs to nobody we extracted -> unattributed, but still harvested
    # and still usable for pattern inference.
    assert "wei@acmerobotics.com" not in matched
