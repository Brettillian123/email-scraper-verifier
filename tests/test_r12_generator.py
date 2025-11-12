from src.generate.permutations import (
    generate_permutations,
    infer_domain_pattern,
    normalize_name_parts,
)


def test_basic_patterns():
    emails = generate_permutations("John", "Doe", "example.com")
    assert "john.doe@example.com" in emails
    assert "jdoe@example.com" in emails
    assert "doej@example.com" in emails


def test_role_aliases_blocked():
    # if the normalization would yield a role alias, ensure it is filtered
    emails = generate_permutations("Info", "", "example.com")
    assert all(not e.startswith("info@") for e in emails)


def test_accented_names_normalize():
    assert normalize_name_parts("José", "Gonçalves") == ("jose", "goncalves", "j", "g")


def test_pattern_inference_limits_candidates():
    published = ["jane.doe@example.com", "alex.smith@example.com"]
    only = infer_domain_pattern(published, "John", "Doe")
    emails = generate_permutations("John", "Doe", "example.com", only_pattern=only)
    assert emails == {"john.doe@example.com"}
