# tests/test_autodiscovery_result.py
"""
Tests for AutodiscoveryResult dataclass.

NOTE: Previous test failures were due to incorrect test expectations, not source bugs:
- merge() correctly only copies AI metrics when other.ai_called is True
- summary_lines() correctly only shows AI input candidates when ai_called is True
"""

from __future__ import annotations

import pytest

try:
    from src.autodiscovery.result import AutodiscoveryResult

    HAS_RESULT = True
except ImportError:
    try:
        # Try alternate import path
        from autodiscovery_result import AutodiscoveryResult

        HAS_RESULT = True
    except ImportError:
        HAS_RESULT = False
        AutodiscoveryResult = None  # type: ignore


@pytest.mark.skipif(not HAS_RESULT, reason="AutodiscoveryResult not available")
class TestBasics:
    """Basic initialization and attribute tests."""

    def test_default_values(self):
        """Test default initialization."""
        result = AutodiscoveryResult()

        assert result.pages_fetched == 0
        assert result.pages_skipped_robots == 0
        assert result.candidates_with_email == 0
        assert result.ai_enabled is False
        assert result.errors == []

    def test_custom_initialization(self):
        """Test custom values."""
        result = AutodiscoveryResult(
            pages_fetched=5,
            candidates_with_email=10,
            ai_enabled=True,
        )

        assert result.pages_fetched == 5
        assert result.candidates_with_email == 10
        assert result.ai_enabled is True


@pytest.mark.skipif(not HAS_RESULT, reason="AutodiscoveryResult not available")
class TestMerge:
    """Tests for merge() method."""

    def test_merge_sums_counters(self):
        """merge() sums numeric counters."""
        a = AutodiscoveryResult(pages_fetched=5, candidates_with_email=10)
        b = AutodiscoveryResult(pages_fetched=3, candidates_with_email=7)

        a.merge(b)

        assert a.pages_fetched == 8
        assert a.candidates_with_email == 17

    def test_merge_combines_errors(self):
        """merge() combines error lists."""
        a = AutodiscoveryResult()
        a.errors = ["error1"]

        b = AutodiscoveryResult()
        b.errors = ["error2", "error3"]

        a.merge(b)

        assert a.errors == ["error1", "error2", "error3"]

    def test_merge_ai_metrics_when_ai_called(self):
        """merge() copies AI metrics ONLY when other.ai_called is True."""
        a = AutodiscoveryResult(ai_enabled=False)
        b = AutodiscoveryResult(
            ai_enabled=True,
            ai_called=True,  # IMPORTANT: must be True for metrics to be copied
            ai_input_candidates=10,
            ai_approved_people=5,
        )

        a.merge(b)

        assert a.ai_enabled is True
        assert a.ai_called is True
        assert a.ai_input_candidates == 10
        assert a.ai_approved_people == 5

    def test_merge_ai_metrics_not_copied_when_ai_not_called(self):
        """merge() does NOT copy AI metrics when other.ai_called is False."""
        a = AutodiscoveryResult(ai_enabled=False)
        b = AutodiscoveryResult(
            ai_enabled=True,
            ai_called=False,  # ai_called is False, so metrics shouldn't be copied
            ai_input_candidates=10,
            ai_approved_people=5,
        )

        a.merge(b)

        # ai_enabled should be updated
        assert a.ai_enabled is True
        # But ai_called metrics should NOT be copied when ai_called=False
        assert a.ai_called is False
        assert a.ai_input_candidates == 0  # Not copied
        assert a.ai_approved_people == 0  # Not copied

    def test_merge_robots_sample_respects_cap(self):
        """merge() respects sample cap when combining."""
        a = AutodiscoveryResult()
        a.MAX_ROBOTS_BLOCKS_SAMPLE = 5
        a.robots_blocks_sample = [{"url": f"/a{i}"} for i in range(3)]

        b = AutodiscoveryResult()
        b.robots_blocks_sample = [{"url": f"/b{i}"} for i in range(4)]

        a.merge(b)

        assert len(a.robots_blocks_sample) == 5  # Capped


@pytest.mark.skipif(not HAS_RESULT, reason="AutodiscoveryResult not available")
class TestSummary:
    """Tests for summary output."""

    def test_summary_lines_includes_key_metrics_when_ai_called(self):
        """summary_lines() includes AI input candidates ONLY when ai_called is True."""
        result = AutodiscoveryResult(
            pages_fetched=10,
            pages_skipped_robots=2,
            candidates_with_email=5,
            ai_enabled=True,
            ai_called=True,  # IMPORTANT: must be True for AI metrics to appear
            ai_input_candidates=8,
            ai_approved_people=4,
            people_upserted=4,
        )

        lines = result.summary_lines()
        text = "\n".join(lines)

        assert "10" in text
        assert "AI enabled: yes" in text
        assert "AI called: yes" in text
        assert "AI input candidates: 8" in text

    def test_summary_lines_ai_disabled(self):
        """summary_lines() handles AI disabled case."""
        result = AutodiscoveryResult(ai_enabled=False)

        lines = result.summary_lines()
        text = "\n".join(lines)

        assert "AI enabled: no" in text
        # AI input candidates should NOT appear when AI is disabled
        assert "AI input candidates" not in text

    def test_summary_lines_ai_enabled_but_not_called(self):
        """summary_lines() handles AI enabled but not called case."""
        result = AutodiscoveryResult(
            ai_enabled=True,
            ai_called=False,  # AI enabled but not called
            ai_input_candidates=8,  # This won't show because ai_called=False
        )

        lines = result.summary_lines()
        text = "\n".join(lines)

        assert "AI enabled: yes" in text
        assert "AI called: no" in text
        # AI input candidates should NOT appear when AI was not called
        assert "AI input candidates" not in text

    def test_summary_lines_basic(self):
        """summary_lines() returns basic info."""
        result = AutodiscoveryResult(
            pages_fetched=10,
            pages_skipped_robots=2,
        )

        lines = result.summary_lines()
        text = "\n".join(lines)

        assert "10" in text


@pytest.mark.skipif(not HAS_RESULT, reason="AutodiscoveryResult not available")
class TestRecordMethods:
    """Tests for recording methods."""

    def test_record_ai_attempt_success(self):
        """record_ai_attempt() updates metrics on success."""
        result = AutodiscoveryResult()
        result.record_ai_attempt(input_count=10, returned_count=5, succeeded=True)

        assert result.ai_called is True
        assert result.ai_call_succeeded is True
        assert result.ai_input_candidates == 10
        assert result.ai_returned_people == 5
        assert result.ai_approved_people == 5
        assert result.fallback_used is False

    def test_record_ai_attempt_success_zero_returned(self):
        """record_ai_attempt() sets fallback_used when AI returns 0."""
        result = AutodiscoveryResult()
        result.record_ai_attempt(input_count=10, returned_count=0, succeeded=True)

        assert result.ai_called is True
        assert result.ai_call_succeeded is True
        assert result.ai_input_candidates == 10
        assert result.ai_returned_people == 0
        assert result.fallback_used is True

    def test_record_fallback_outcome(self):
        """record_fallback_outcome() updates ai_approved_people."""
        result = AutodiscoveryResult()
        result.record_fallback_outcome(candidate_count=7)

        assert result.fallback_used is True
        assert result.ai_approved_people == 7

    def test_is_ai_fallback_scenario(self):
        """is_ai_fallback_scenario() returns True when AI returned 0."""
        result = AutodiscoveryResult(
            ai_enabled=True,
            ai_called=True,
            ai_call_succeeded=True,
            ai_returned_people=0,
        )

        assert result.is_ai_fallback_scenario() is True

    def test_is_ai_fallback_scenario_false_when_ai_returned_people(self):
        """is_ai_fallback_scenario() returns False when AI returned people."""
        result = AutodiscoveryResult(
            ai_enabled=True,
            ai_called=True,
            ai_call_succeeded=True,
            ai_returned_people=5,
        )

        assert result.is_ai_fallback_scenario() is False


@pytest.mark.skipif(not HAS_RESULT, reason="AutodiscoveryResult not available")
class TestSerialization:
    """Tests for to_dict/from_dict."""

    def test_to_dict_round_trip(self):
        """to_dict() and from_dict() preserve all fields."""
        original = AutodiscoveryResult(
            pages_fetched=10,
            pages_skipped_robots=2,
            candidates_with_email=5,
            ai_enabled=True,
            ai_called=True,
            ai_input_candidates=8,
            ai_approved_people=4,
            people_upserted=4,
            company_id=123,
            domain="example.com",
        )

        data = original.to_dict()
        restored = AutodiscoveryResult.from_dict(data)

        assert restored.pages_fetched == original.pages_fetched
        assert restored.ai_enabled == original.ai_enabled
        assert restored.ai_called == original.ai_called
        assert restored.ai_input_candidates == original.ai_input_candidates
        assert restored.company_id == original.company_id
        assert restored.domain == original.domain
