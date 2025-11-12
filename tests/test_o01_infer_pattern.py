from src.generate.patterns import infer_domain_pattern


def test_infers_first_dot_last():
    examples = [
        ("John", "Doe", "john.doe"),
        ("Jane", "Doe", "jane.doe"),
        ("Jim", "Beam", "jim.beam"),
    ]
    inf = infer_domain_pattern(examples)
    assert inf.pattern == "first.last"
    assert inf.confidence >= 0.8
    assert inf.samples == 3


def test_no_decision_when_mixed():
    examples = [
        ("John", "Doe", "john.doe"),
        ("Jane", "Doe", "jdoe"),
    ]
    inf = infer_domain_pattern(examples)
    assert inf.pattern is None
