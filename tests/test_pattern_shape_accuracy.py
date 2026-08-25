"""Accuracy gate for pattern inference, measured against real-world data.

The fixture is derived from 180 addresses published on real company sites whose
true pattern was established by matching the local-part to a known employee. The
names and local-parts were then REGENERATED synthetically, preserving the feature
the classifier keys on -- separator, name lengths, hyphenation, diacritics,
trailing digits -- and the observed pattern mix (flast 42%, first.last 41%,
first 12%, firstl 4%). No real person, address or domain survives in the fixture.

Thresholds are set below what the same rules scored when this was written:
92.9% overall and 100% (83/83) on the high-confidence band, against 94.2% and
100% (92/92) on the unanonymised production data.

The point of this test is that toy examples cannot catch a regression that only
shows up on messy real local-parts. Chief among those: `firstl` is genuinely
ambiguous with `flast` for short names, which is why the low-confidence band is
allowed to be wrong and the high-confidence band is not.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "pattern_ground_truth.tsv"

# Measured 92.9% / 100.0% at the time of writing; both carry deliberate headroom.
MIN_OVERALL_ACCURACY = 0.88
MIN_HIGH_CONFIDENCE_ACCURACY = 0.97
HIGH_CONFIDENCE = 0.75


def _rows() -> list[tuple[str, str, str, str]]:
    with FIXTURE.open(encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        next(reader, None)  # header
        return [(r[0], r[1], r[2], r[3]) for r in reader if len(r) >= 4]


@pytest.fixture(scope="module")
def rows() -> list[tuple[str, str, str, str]]:
    data = _rows()
    assert len(data) > 100, f"fixture looks truncated: {len(data)} rows"
    return data


@pytest.fixture(scope="module")
def first_names(rows) -> set[str]:
    """The dictionary the shape classifier consults. In production this comes
    from the people table; here the fixture supplies its own."""
    return {first.lower() for _lp, first, _last, _pat in rows}


def test_classify_pattern_recovers_the_known_pattern(rows):
    """classify_pattern() has the person's real name, so it should be near-exact."""
    from src.generate.patterns import PATTERN_OTHER, classify_pattern

    wrong = [
        (lp, expected, classify_pattern(lp, first, last))
        for lp, first, last, expected in rows
        if classify_pattern(lp, first, last) != expected
    ]
    # Allow only the genuinely ambiguous cases (a one-letter first name makes
    # flast and firstlast identical); anything else is a real regression.
    hard_failures = [w for w in wrong if w[2] == PATTERN_OTHER]
    assert not hard_failures, f"classify_pattern returned OTHER for: {hard_failures[:5]}"
    assert len(wrong) / len(rows) < 0.10, (
        f"classify_pattern mismatched {len(wrong)}/{len(rows)}: {wrong[:5]}"
    )


def test_shape_inference_accuracy_on_real_world_shapes(rows, first_names):
    """infer_pattern_from_localpart() does NOT get the person's name -- it reads
    the pattern out of the local-part alone. That is what lets us learn a
    company's pattern from a published address whose owner we never extracted."""
    patterns = pytest.importorskip("src.generate.patterns")
    infer = getattr(patterns, "infer_pattern_from_localpart", None)
    if infer is None:
        pytest.skip("infer_pattern_from_localpart not implemented yet")

    correct = incorrect = unclassified = 0
    hi_correct = hi_total = 0
    confusions: dict[tuple[str, str], int] = {}

    for local_part, _first, _last, expected in rows:
        pattern, confidence = infer(local_part, first_names)
        if pattern is None:
            unclassified += 1
            continue
        if pattern == expected:
            correct += 1
            if confidence >= HIGH_CONFIDENCE:
                hi_correct += 1
                hi_total += 1
        else:
            incorrect += 1
            confusions[(expected, pattern)] = confusions.get((expected, pattern), 0) + 1
            if confidence >= HIGH_CONFIDENCE:
                hi_total += 1

    decided = correct + incorrect
    assert decided, "classifier decided nothing at all"

    accuracy = correct / decided
    assert accuracy >= MIN_OVERALL_ACCURACY, (
        f"shape inference accuracy {accuracy:.1%} < {MIN_OVERALL_ACCURACY:.0%}; "
        f"top confusions: {sorted(confusions.items(), key=lambda kv: -kv[1])[:5]}"
    )

    # The important guarantee: when it claims high confidence, it is right.
    # A company's whole pattern is derived from these, so a confident error
    # propagates to every colleague at that domain.
    if hi_total:
        hi_accuracy = hi_correct / hi_total
        assert hi_accuracy >= MIN_HIGH_CONFIDENCE_ACCURACY, (
            f"high-confidence accuracy {hi_accuracy:.1%} < "
            f"{MIN_HIGH_CONFIDENCE_ACCURACY:.0%} over {hi_total} predictions"
        )


def test_shape_inference_never_guesses_role_addresses(first_names):
    """Role mailboxes must never yield a person-pattern -- doing so would poison
    a company's pattern with `info`/`sales` and mis-generate every colleague."""
    patterns = pytest.importorskip("src.generate.patterns")
    infer = getattr(patterns, "infer_pattern_from_localpart", None)
    if infer is None:
        pytest.skip("infer_pattern_from_localpart not implemented yet")

    for role in ("info", "sales", "support", "careers", "no-reply", "postmaster"):
        pattern, confidence = infer(role, first_names)
        assert pattern is None or confidence < HIGH_CONFIDENCE, (
            f"role address {role!r} produced {pattern!r} at confidence {confidence}"
        )


def test_shape_inference_rejects_malformed_local_parts(first_names):
    patterns = pytest.importorskip("src.generate.patterns")
    infer = getattr(patterns, "infer_pattern_from_localpart", None)
    if infer is None:
        pytest.skip("infer_pattern_from_localpart not implemented yet")

    for bad in ("", "   ", "@@@", "a b c", "éééé!"):
        pattern, confidence = infer(bad, first_names)
        assert pattern is None, f"{bad!r} should not classify, got {pattern!r}"
        assert confidence == 0.0 or confidence is not None
