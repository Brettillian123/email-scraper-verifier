# src/autodiscovery_result.py
"""
Autodiscovery result payload for queue parity and observability (Task E).

This module defines the standardized result structure that:
  - Queue tasks return and store in job.meta
  - Demo script uses for final summary
  - Enables consistent metrics across sync and async execution

Key improvement: AI metrics are now split to accurately report:
  - ai_enabled: Whether AI is configured/available
  - ai_called: Whether we actually attempted an AI call
  - ai_returned_people: How many people the AI returned (may be 0)
  - fallback_used: Whether we fell back to heuristics after AI returned 0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AutodiscoveryResult:
    """
    Standardized result payload for autodiscovery operations.

    This is returned by queue tasks and stored in RQ job.meta for observability.
    All fields are JSON-serializable for queue transport.
    """

    # Crawl metrics
    pages_fetched: int = 0
    pages_skipped_robots: int = 0
    robots_blocks_sample: list[dict[str, Any]] = field(default_factory=list)

    # Extraction metrics
    candidates_with_email: int = 0
    candidates_without_email: int = 0

    # AI refinement metrics - SPLIT for accuracy
    ai_enabled: bool = False  # Is AI configured and available?
    ai_called: bool = False  # Did we actually make an AI call?
    ai_call_succeeded: bool = False  # Did the AI call complete without error?
    ai_input_candidates: int = 0  # How many candidates did we send to AI?
    ai_returned_people: int = 0  # How many people did AI return? (may be 0)
    fallback_used: bool = False  # Did we fall back to heuristics after AI returned 0?

    # Legacy field for backward compat (computed from ai_returned_people or fallback)
    ai_approved_people: int = 0  # Final count after AI or fallback

    # Persistence metrics
    people_upserted: int = 0
    people_skipped_quality: int = 0  # NEW: Candidates rejected by quality gates
    emails_upserted: int = 0

    # Generation metrics
    permutations_generated: int = 0
    emails_generated: int = 0

    # Error tracking
    errors: list[str] = field(default_factory=list)

    # Context
    company_id: int | None = None
    domain: str | None = None

    # Cap for robots_blocks_sample to avoid bloating payloads
    MAX_ROBOTS_BLOCKS_SAMPLE: int = field(default=25, repr=False)

    def add_robots_block(self, block_info: dict[str, Any]) -> None:
        """
        Add a robots block to the sample, respecting the cap.

        Args:
            block_info: Dict from RobotsBlockInfo.to_dict() or equivalent
        """
        if len(self.robots_blocks_sample) < self.MAX_ROBOTS_BLOCKS_SAMPLE:
            self.robots_blocks_sample.append(block_info)
        self.pages_skipped_robots += 1

    def add_error(self, error: str) -> None:
        """Add an error message to the result."""
        self.errors.append(error)

    def record_ai_attempt(
        self,
        *,
        input_count: int,
        returned_count: int,
        succeeded: bool = True,
    ) -> None:
        """
        Record the outcome of an AI refinement attempt.

        Args:
            input_count: Number of candidates sent to AI
            returned_count: Number of people AI returned
            succeeded: Whether the AI call completed without error
        """
        self.ai_called = True
        self.ai_call_succeeded = succeeded
        self.ai_input_candidates = input_count
        self.ai_returned_people = returned_count

        if succeeded and returned_count > 0:
            self.ai_approved_people = returned_count
            self.fallback_used = False
        elif succeeded and returned_count == 0:
            # AI returned nothing - caller will likely use fallback
            self.fallback_used = True
            # ai_approved_people will be set by record_fallback_outcome()

    def record_fallback_outcome(self, candidate_count: int) -> None:
        """
        Record the outcome when falling back to heuristics after AI returns 0.

        Args:
            candidate_count: Number of candidates from fallback heuristics
        """
        self.fallback_used = True
        self.ai_approved_people = candidate_count

    def record_quality_rejection(self, count: int = 1) -> None:
        """Record candidates rejected by quality gates."""
        self.people_skipped_quality += count

    def to_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable dict for queue payloads.

        This is what gets stored in job.meta["autodiscovery_result"].
        """
        return {
            "pages_fetched": self.pages_fetched,
            "pages_skipped_robots": self.pages_skipped_robots,
            "robots_blocks_sample": self.robots_blocks_sample,
            "candidates_with_email": self.candidates_with_email,
            "candidates_without_email": self.candidates_without_email,
            # AI metrics - full detail
            "ai_enabled": self.ai_enabled,
            "ai_called": self.ai_called,
            "ai_call_succeeded": self.ai_call_succeeded,
            "ai_input_candidates": self.ai_input_candidates,
            "ai_returned_people": self.ai_returned_people,
            "fallback_used": self.fallback_used,
            "ai_approved_people": self.ai_approved_people,
            # Persistence metrics
            "people_upserted": self.people_upserted,
            "people_skipped_quality": self.people_skipped_quality,
            "emails_upserted": self.emails_upserted,
            # Generation metrics
            "permutations_generated": self.permutations_generated,
            "emails_generated": self.emails_generated,
            # Errors and context
            "errors": self.errors,
            "company_id": self.company_id,
            "domain": self.domain,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AutodiscoveryResult:
        """
        Reconstruct an AutodiscoveryResult from a dict.

        Useful for reading results from job.meta.
        """
        return cls(
            pages_fetched=data.get("pages_fetched", 0),
            pages_skipped_robots=data.get("pages_skipped_robots", 0),
            robots_blocks_sample=data.get("robots_blocks_sample", []),
            candidates_with_email=data.get("candidates_with_email", 0),
            candidates_without_email=data.get("candidates_without_email", 0),
            ai_enabled=data.get("ai_enabled", False),
            ai_called=data.get("ai_called", False),
            ai_call_succeeded=data.get("ai_call_succeeded", False),
            ai_input_candidates=data.get("ai_input_candidates", 0),
            ai_returned_people=data.get("ai_returned_people", 0),
            fallback_used=data.get("fallback_used", False),
            ai_approved_people=data.get("ai_approved_people", 0),
            people_upserted=data.get("people_upserted", 0),
            people_skipped_quality=data.get("people_skipped_quality", 0),
            emails_upserted=data.get("emails_upserted", 0),
            permutations_generated=data.get("permutations_generated", 0),
            emails_generated=data.get("emails_generated", 0),
            errors=data.get("errors", []),
            company_id=data.get("company_id"),
            domain=data.get("domain"),
        )

    def merge(self, other: AutodiscoveryResult) -> AutodiscoveryResult:
        """
        Merge another result into this one (for aggregating across stages).

        Returns self for chaining.
        """
        self.pages_fetched += other.pages_fetched
        self.pages_skipped_robots += other.pages_skipped_robots

        # Merge robots blocks sample up to cap
        remaining_cap = self.MAX_ROBOTS_BLOCKS_SAMPLE - len(self.robots_blocks_sample)
        if remaining_cap > 0:
            self.robots_blocks_sample.extend(other.robots_blocks_sample[:remaining_cap])

        self.candidates_with_email += other.candidates_with_email
        self.candidates_without_email += other.candidates_without_email

        # AI metrics: take the latest values from the stage that actually ran AI
        if other.ai_called:
            self.ai_enabled = other.ai_enabled
            self.ai_called = other.ai_called
            self.ai_call_succeeded = other.ai_call_succeeded
            self.ai_input_candidates = other.ai_input_candidates
            self.ai_returned_people = other.ai_returned_people
            self.fallback_used = other.fallback_used
            self.ai_approved_people = other.ai_approved_people
        elif other.ai_enabled and not self.ai_called:
            self.ai_enabled = True

        self.people_upserted += other.people_upserted
        self.people_skipped_quality += other.people_skipped_quality
        self.emails_upserted += other.emails_upserted
        self.permutations_generated += other.permutations_generated
        self.emails_generated += other.emails_generated

        self.errors.extend(other.errors)

        # Context: prefer non-None values
        if other.company_id is not None:
            self.company_id = other.company_id
        if other.domain is not None:
            self.domain = other.domain

        return self

    def summary_lines(self) -> list[str]:
        """
        Return a list of human-readable summary lines for logging/display.
        """
        lines = [
            f"Pages fetched: {self.pages_fetched}",
            f"Pages skipped (robots): {self.pages_skipped_robots}",
            f"Candidates with email: {self.candidates_with_email}",
            f"Candidates without email: {self.candidates_without_email}",
        ]

        # AI metrics - now with full detail
        lines.append(f"AI enabled: {'yes' if self.ai_enabled else 'no'}")
        if self.ai_enabled:
            lines.append(f"AI called: {'yes' if self.ai_called else 'no'}")
            if self.ai_called:
                lines.append(f"AI call succeeded: {'yes' if self.ai_call_succeeded else 'no'}")
                lines.append(f"AI input candidates: {self.ai_input_candidates}")
                lines.append(f"AI returned people: {self.ai_returned_people}")
                lines.append(f"Fallback used: {'yes' if self.fallback_used else 'no'}")
        lines.append(f"Final approved people: {self.ai_approved_people}")

        lines.extend(
            [
                f"People upserted: {self.people_upserted}",
            ]
        )

        if self.people_skipped_quality > 0:
            lines.append(f"People skipped (quality): {self.people_skipped_quality}")

        lines.append(f"Emails upserted: {self.emails_upserted}")

        if self.permutations_generated > 0:
            lines.append(f"Permutations generated: {self.permutations_generated}")

        if self.errors:
            lines.append(f"Errors: {len(self.errors)}")

        return lines

    def print_summary(self, prefix: str = "  ") -> None:
        """Print a formatted summary to stdout."""
        for line in self.summary_lines():
            print(f"{prefix}{line}")

    def is_ai_fallback_scenario(self) -> bool:
        """
        Return True if AI was called but returned 0 people (fallback scenario).

        This is the scenario where quality gates should be strictly enforced
        to prevent bad heuristic guesses from being persisted as people.
        """
        return (
            self.ai_enabled
            and self.ai_called
            and self.ai_call_succeeded
            and self.ai_returned_people == 0
        )
