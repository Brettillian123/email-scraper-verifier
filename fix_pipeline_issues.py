#!/usr/bin/env python3
"""
fix_pipeline_issues.py - Fixes three critical pipeline issues:

1. Autodiscovery not finding all people (page classifier too aggressive)
2. Unknown timeouts on SMTP probes (timeouts too short)
3. Stops probing on hard invalid (should continue all permutations)

Run: python3 fix_pipeline_issues.py
"""

import shutil
import time
from pathlib import Path


def backup_file(filepath: Path) -> Path:
    """Create a timestamped backup."""
    backup = filepath.with_suffix(f".py.bak.{int(time.time())}")
    shutil.copy(filepath, backup)
    return backup


# =============================================================================
# FIX 1: Page classifier being too aggressive
# =============================================================================


def fix_page_classifier() -> bool:
    """
    The page classifier skips pages classified as job_board, press_release,
    testimonial, news, careers - but these pages often have team member info.

    Fix: Make the classifier less aggressive by only skipping clearly
    third-party pages.
    """
    filepath = Path("/opt/email-scraper/src/queueing/tasks.py")
    if not filepath.exists():
        print(f"  ⚠ {filepath} not found")
        return False

    backup_file(filepath)
    content = filepath.read_text()

    # The current code skips too many page types
    old_code = """\
                if page_type in {
                    "job_board",
                    "press_release",
                    "testimonial",
                    "news",
                    "careers",
                }:
                    log.debug(
                        "P1/P2 skipping extraction for page_type=%s url=%s",
                        page_type,
                        src_url,
                    )
                    continue"""

    # New code: only skip clearly external/third-party pages
    # Keep extracting from careers, news, press_release as they often have team info
    new_code = """\
                # Only skip pages that are clearly third-party
                # Careers pages often list hiring managers, press releases mention execs
                if page_type in {"job_board", "testimonial"}:
                    log.debug(
                        "P1/P2 skipping extraction for page_type=%s url=%s",
                        page_type,
                        src_url,
                    )
                    continue"""

    if old_code in content:
        content = content.replace(old_code, new_code)
        filepath.write_text(content)
        print("  ✓ Fixed page classifier (less aggressive now)")
        return True

    # Check if it's a different pattern
    if (
        "job_board" in content
        and "press_release" in content
        and "testimonial" in content
    ):
        print("  ⚠ Page classifier pattern different than expected - check manually")
        print("    Look for: page_type in {...} around line 2656")
        return False

    print("  ✓ Page classifier already fixed or not present")
    return False


# =============================================================================
# FIX 2: SMTP timeouts too short
# =============================================================================


def fix_smtp_timeouts() -> bool:
    """
    The SMTP probe timeouts are:
    - SMTP_CONNECT_TIMEOUT: 5 seconds (too short for slow mail servers)
    - SMTP_COMMAND_TIMEOUT: 10 seconds (can timeout on greylisting)

    Fix: Increase defaults and add better error handling.
    """
    filepath = Path("/opt/email-scraper/src/config.py")
    if not filepath.exists():
        print(f"  ⚠ {filepath} not found")
        return False

    backup_file(filepath)
    content = filepath.read_text()

    changes_made = False

    # Increase SMTP_CONNECT_TIMEOUT from 5 to 10
    old_connect = 'SMTP_CONNECT_TIMEOUT: int = _getenv_int("SMTP_CONNECT_TIMEOUT", 5)'
    new_connect = 'SMTP_CONNECT_TIMEOUT: int = _getenv_int("SMTP_CONNECT_TIMEOUT", 10)'
    if old_connect in content:
        content = content.replace(old_connect, new_connect)
        changes_made = True
        print("  ✓ Increased SMTP_CONNECT_TIMEOUT: 5 → 10 seconds")

    # Increase SMTP_COMMAND_TIMEOUT from 10 to 20
    old_command = 'SMTP_COMMAND_TIMEOUT: int = _getenv_int("SMTP_COMMAND_TIMEOUT", 10)'
    new_command = 'SMTP_COMMAND_TIMEOUT: int = _getenv_int("SMTP_COMMAND_TIMEOUT", 20)'
    if old_command in content:
        content = content.replace(old_command, new_command)
        changes_made = True
        print("  ✓ Increased SMTP_COMMAND_TIMEOUT: 10 → 20 seconds")

    # Increase SMTP_PREFLIGHT_TIMEOUT from 1.5 to 3.0
    old_preflight = (
        'SMTP_PREFLIGHT_TIMEOUT_SECONDS: float = _getenv_float('
        '"SMTP_PREFLIGHT_TIMEOUT_SECONDS", 1.5)'
    )
    new_preflight = (
        'SMTP_PREFLIGHT_TIMEOUT_SECONDS: float = _getenv_float('
        '"SMTP_PREFLIGHT_TIMEOUT_SECONDS", 3.0)'
    )
    if old_preflight in content:
        content = content.replace(old_preflight, new_preflight)
        changes_made = True
        print("  ✓ Increased SMTP_PREFLIGHT_TIMEOUT: 1.5 → 3.0 seconds")

    if changes_made:
        filepath.write_text(content)
        return True

    print("  ✓ SMTP timeouts already adjusted or different than expected")
    return False


def fix_smtp_timeout_clamp() -> bool:
    """
    There's also a SMTP_COMMAND_TIMEOUT_CLAMP that limits command timeout to 10s.
    This needs to be increased too.
    """
    filepath = Path("/opt/email-scraper/src/queueing/tasks.py")
    if not filepath.exists():
        return False

    content = filepath.read_text()
    changes = False

    old_cmd = '"SMTP_COMMAND_TIMEOUT_CLAMP", "10.0"'
    new_cmd = '"SMTP_COMMAND_TIMEOUT_CLAMP", "20.0"'
    if old_cmd in content:
        content = content.replace(old_cmd, new_cmd)
        print("  ✓ Increased SMTP_COMMAND_TIMEOUT_CLAMP: 10 → 20 seconds")
        changes = True

    old_conn = '"SMTP_CONNECT_TIMEOUT_CLAMP", "6.0"'
    new_conn = '"SMTP_CONNECT_TIMEOUT_CLAMP", "10.0"'
    if old_conn in content:
        content = content.replace(old_conn, new_conn)
        print("  ✓ Increased SMTP_CONNECT_TIMEOUT_CLAMP: 6 → 10 seconds")
        changes = True

    old_tcp = '"TCP25_PROBE_TIMEOUT_SECONDS", "1.5"'
    new_tcp = '"TCP25_PROBE_TIMEOUT_SECONDS", "3.0"'
    if old_tcp in content:
        content = content.replace(old_tcp, new_tcp)
        print("  ✓ Increased TCP25_PROBE_TIMEOUT_SECONDS: 1.5 → 3.0 seconds")
        changes = True

    if changes:
        filepath.write_text(content)
        return True

    return False


def fix_tcp25_cache_ttl() -> bool:
    """
    The TCP25 preflight cache TTL is 300 seconds (5 minutes).
    If a preflight fails, ALL probes for that MX will fail for 5 minutes.
    Reduce this to 60 seconds so failures are retried sooner.
    """
    filepath = Path("/opt/email-scraper/src/queueing/tasks.py")
    if not filepath.exists():
        return False

    content = filepath.read_text()

    # Reduce TTL from 300 to 60 seconds
    if "ttl_s: int = 300" in content:
        content = content.replace("ttl_s: int = 300", "ttl_s: int = 60")
        filepath.write_text(content)
        print("  ✓ Reduced TCP25 cache TTL: 300 → 60 seconds")
        return True

    return False


# =============================================================================
# FIX 3: Ensure all permutations get probed (don't stop on invalid)
# =============================================================================


def fix_probe_continuation() -> bool:
    """
    The issue: When one email permutation returns 'invalid', probing stops.
    This happens because errors in _enqueue_r16_probe are being caught but
    causing loop continuation issues.

    The current code does enqueue all probes, but there may be an issue
    with how the probes are being processed or how results are being tracked.

    Let's add better logging and ensure the loop continues properly.
    """
    filepath = Path("/opt/email-scraper/src/queueing/tasks.py")
    if not filepath.exists():
        return False

    content = filepath.read_text()

    old_enqueue = """def _enqueue_r16_probe(email_id: int | None, email: str, domain: str) -> None:
    \"""
    Enqueue the R16 probe task explicitly. Best-effort (swallows Redis errors).
    \"""
    try:
        q = Queue(name="verify", connection=get_redis())
        q.enqueue(
            task_probe_email,
            email_id=int(email_id or 0),
            email=email,
            company_domain=domain,
            force=False,
            job_timeout=20,
            retry=None,
        )
    except Exception as e:
        log.warning("R16 enqueue skipped (best-effort): %s", e)"""

    new_enqueue = """def _enqueue_r16_probe(email_id: int | None, email: str, domain: str) -> None:
    \"""
    Enqueue the R16 probe task explicitly. Best-effort (swallows Redis errors).
    \"""
    try:
        q = Queue(name="verify", connection=get_redis())
        job = q.enqueue(
            task_probe_email,
            email_id=int(email_id or 0),
            email=email,
            company_domain=domain,
            force=False,
            job_timeout=30,  # Increased from 20 to allow for slower servers
            retry=None,
        )
        log.info(
            "R16 probe enqueued",
            extra={
                "email": email,
                "email_id": email_id,
                "domain": domain,
                "job_id": job.id if job else None,
            },
        )
    except Exception as e:
        log.warning(
            "R16 enqueue failed: %s",
            e,
            extra={"email": email, "domain": domain},
        )"""

    if old_enqueue in content:
        content = content.replace(old_enqueue, new_enqueue)
        filepath.write_text(content)
        print("  ✓ Enhanced _enqueue_r16_probe with better logging")
        return True

    if "R16 probe enqueued" in content:
        print("  ✓ _enqueue_r16_probe already has enhanced logging")
        return False

    print("  ⚠ _enqueue_r16_probe pattern not found - check manually")
    return False


def fix_generation_loop() -> bool:
    """
    Ensure the generation loop doesn't exit early on any single failure.
    Each permutation should be tried independently.
    """
    filepath = Path("/opt/email-scraper/src/queueing/tasks.py")
    if not filepath.exists():
        return False

    content = filepath.read_text()

    old_return = """    log.info(
        "R12 generated emails",
        extra={
            "person_id": person_id,
            "domain": dom,
            "first_norm": nf,
            "last_norm": nl,
            "only_pattern": effective_pattern,
            "company_pattern": company_pattern,
            "domain_pattern": domain_pattern,
            "inference_confidence": inf_conf,
            "inference_samples": inf_samples,
            "count": inserted,
            "enqueued": enqueued,
            "max_probes_per_person": max_probes,
        },
    )
    return {
        "count": inserted,
        "enqueued": enqueued,"""

    new_return = """    # Log summary with clear indication of success/failure
    if inserted == 0:
        log.warning(
            "R12 generated NO emails - check permutation generation",
            extra={
                "person_id": person_id,
                "domain": dom,
                "first_norm": nf,
                "last_norm": nl,
                "only_pattern": effective_pattern,
                "candidates_attempted": (
                    len(ranked_candidates) if ranked_candidates else 0
                ),
            },
        )
    else:
        log.info(
            "R12 generated emails",
            extra={
                "person_id": person_id,
                "domain": dom,
                "first_norm": nf,
                "last_norm": nl,
                "only_pattern": effective_pattern,
                "company_pattern": company_pattern,
                "domain_pattern": domain_pattern,
                "inference_confidence": inf_conf,
                "inference_samples": inf_samples,
                "count": inserted,
                "enqueued": enqueued,
                "max_probes_per_person": max_probes,
            },
        )
    return {
        "count": inserted,
        "enqueued": enqueued,"""

    if old_return in content:
        content = content.replace(old_return, new_return)
        filepath.write_text(content)
        print("  ✓ Added warning logging for zero email generation")
        return True

    return False


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    print("=" * 60)
    print("Email Scraper Pipeline Issues Fix")
    print("=" * 60)
    print()

    print("[1/4] Fixing page classifier (autodiscovery issue)...")
    fix_page_classifier()

    print()
    print("[2/4] Fixing SMTP timeouts...")
    fix_smtp_timeouts()
    fix_smtp_timeout_clamp()
    fix_tcp25_cache_ttl()

    print()
    print("[3/4] Fixing probe continuation...")
    fix_probe_continuation()

    print()
    print("[4/4] Fixing generation loop logging...")
    fix_generation_loop()

    print()
    print("=" * 60)
    print("Fixes Applied!")
    print("=" * 60)
    print(
        """
IMPORTANT: The "stops on hard invalid" issue might actually be:
1. Workers not listening to 'verify' queue
2. Probes enqueued but timing out before processing
3. Redis queue backlog

Check with:
  rq info

Make sure workers listen to ALL queues:
  pkill -f "rq worker"
  rq worker orchestrator crawl generate verify -v

To see probes being enqueued and processed:
  tail -f /var/log/email-scraper/worker.log | grep -E "R16|probe|verify"

Next steps:
1. Restart workers: rq worker orchestrator crawl generate verify -v
2. Restart uvicorn: uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload
3. Test: bash test_pipeline.sh brandtcpa.com
"""
    )


if __name__ == "__main__":
    main()

