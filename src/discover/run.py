"""Discovery orchestration: find candidates, score them, promote the best.

    sources -> dedupe -> profile -> score -> promote (or park)

WHY THE CAPS ARE THE IMPORTANT PART. Discovery feeds a pipeline that sends
outbound SMTP, and on 2026-08-25 an unbounded run of that pipeline got the host's
port 25 blocked by the provider. Auto-queueing companies is only safe because
three ceilings sit between a bad source and a large bill:

  * MAX_CANDIDATES_PER_RUN  -- how many domains are even looked at
  * MAX_PROMOTE_PER_DAY     -- how many can enter the pipeline
  * the existing daily probe caps, which bound what the pipeline then does

A source that suddenly yields 10,000 domains gets truncated and logged, not
obeyed. Silent truncation would read as "we covered everything", so it is
reported both ways.

Nothing is ever re-profiled: every domain seen lands in `discovered_company`
with a decision, including the rejects. Re-crawling known rejects nightly is the
same shape of waste that had the probe pass grinding tarpit hosts forever.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass

from src.db import get_conn
from src.discover.profile import build_profile, score_profile
from src.discover.sources import ADAPTERS, Candidate

log = logging.getLogger(__name__)

#: Score at or above which a company is promoted without a human looking.
#: Deliberately high: a false positive costs crawl time, probe budget and a
#: company in the book that should not be there.
AUTO_QUEUE_THRESHOLD = int(os.getenv("DISCOVERY_AUTO_QUEUE_THRESHOLD", "60"))
MAX_CANDIDATES_PER_RUN = int(os.getenv("DISCOVERY_MAX_CANDIDATES", "300"))
MAX_PROMOTE_PER_DAY = int(os.getenv("DISCOVERY_MAX_PROMOTE_PER_DAY", "25"))


@dataclass
class DiscoveryResult:
    seen: int = 0
    already_known: int = 0
    profiled: int = 0
    promoted: int = 0
    parked: int = 0
    rejected: int = 0
    truncated: int = 0
    promoted_domains: list[str] | None = None


def _known_domains(con) -> set[str]:
    """Everything already in the pipeline, plus everything already judged."""
    known: set[str] = set()
    for sql in (
        "SELECT lower(domain) FROM companies WHERE domain IS NOT NULL AND domain <> ''",
        "SELECT lower(domain) FROM discovered_company",
    ):
        try:
            known.update(r[0] for r in con.execute(sql).fetchall() if r and r[0])
        except Exception:
            log.debug("could not read known domains from: %s", sql, exc_info=True)
    return known


def _promoted_today(con) -> int:
    try:
        row = con.execute(
            "SELECT count(*) FROM discovered_company"
            " WHERE decision = 'queued' AND decided_at >= now() - interval '24 hours'"
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def gather(con, *, limit: int) -> tuple[list[Candidate], int]:
    """Run every enabled source. Returns (candidates, dropped_to_cap)."""
    try:
        rows = con.execute(
            "SELECT id, kind, target FROM discovery_source WHERE enabled = TRUE ORDER BY id"
        ).fetchall()
    except Exception:
        log.warning("discovery_source unreadable; nothing to gather", exc_info=True)
        return [], 0

    found: list[Candidate] = []
    for src_id, kind, target in rows:
        fn = ADAPTERS.get(str(kind))
        if not fn:
            log.warning("unknown discovery source kind", extra={"kind": kind})
            continue
        try:
            got = fn(target)
        except Exception:
            log.warning("source failed", exc_info=True, extra={"kind": kind, "target": target})
            got = []
        found.extend(got)
        try:
            con.execute(
                "UPDATE discovery_source SET last_run_at = now(), last_found = ? WHERE id = ?",
                (len(got), int(src_id)),
            )
            con.commit()
        except Exception:
            log.debug("could not stamp discovery_source", exc_info=True)

    # Dedupe within the batch, preserving first provenance.
    seen: set[str] = set()
    uniq: list[Candidate] = []
    for c in found:
        if c.domain in seen:
            continue
        seen.add(c.domain)
        uniq.append(c)

    dropped = max(0, len(uniq) - limit)
    if dropped:
        log.warning(
            "candidate cap hit: %s of %s considered this run, %s dropped",
            limit, len(uniq), dropped,
        )
    return uniq[:limit], dropped


def _record(con, cand: Candidate, score: int, reasons: list[str], signals: dict, decision: str):
    con.execute(
        "INSERT INTO discovered_company"
        " (domain, source, source_detail, icp_score, icp_reasons, signals_json,"
        "  decision, decided_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, now())"
        " ON CONFLICT (domain) DO UPDATE SET"
        "  last_seen_at = now(), icp_score = EXCLUDED.icp_score,"
        "  icp_reasons = EXCLUDED.icp_reasons, signals_json = EXCLUDED.signals_json,"
        "  decision = EXCLUDED.decision, decided_at = now()",
        (
            cand.domain, cand.source, cand.source_detail, int(score),
            ",".join(reasons)[:1000], json.dumps(signals)[:4000], decision,
        ),
    )


def discover(*, dry_run: bool = False) -> DiscoveryResult:
    """One full discovery pass."""
    res = DiscoveryResult(promoted_domains=[])
    con = get_conn()
    try:
        candidates, res.truncated = gather(con, limit=MAX_CANDIDATES_PER_RUN)
        res.seen = len(candidates)
        if not candidates:
            return res

        known = _known_domains(con)
        promote_budget = max(0, MAX_PROMOTE_PER_DAY - _promoted_today(con))
        to_promote: list[str] = []

        for cand in candidates:
            if cand.domain in known:
                res.already_known += 1
                continue

            prof = build_profile(cand.domain)
            res.profiled += 1
            score, reasons = score_profile(prof)

            if not prof.reachable:
                decision = "rejected"
                res.rejected += 1
            elif score >= AUTO_QUEUE_THRESHOLD and prof.stage_hits and promote_budget > 0:
                # A STAGE SIGNAL IS REQUIRED, not merely a high score.
                #
                # careers(20) + team(15) + sector(25) is exactly 60, so every
                # competent SaaS homepage lands on the threshold by construction.
                # The first live run promoted anthropic.com, auth0.com,
                # betterment.com and allscripts.com on precisely that pattern --
                # all real companies, none of them anywhere near the seed-to-
                # Series-A stage that buys fractional executives.
                #
                # Stage ("Series A", "backed by", "raised") is the only signal on
                # a homepage that separates a 12-person company from a public
                # one. Without it there is no evidence of stage at all, so the
                # candidate is parked for a human rather than auto-promoted.
                decision = "queued"
                promote_budget -= 1
                to_promote.append(cand.domain)
                res.promoted += 1
            else:
                # Below threshold, no stage evidence, or the daily promotion
                # budget is spent. Parked either way -- and a parked domain keeps
                # its score, so it is not re-profiled tomorrow just to be
                # re-parked.
                decision = "parked"
                res.parked += 1

            if not dry_run:
                try:
                    _record(con, cand, score, reasons, prof.as_dict(), decision)
                    con.commit()
                except Exception:
                    log.warning("could not record candidate", exc_info=True,
                                extra={"domain": cand.domain})
                    try:
                        con.rollback()
                    except Exception:
                        pass

            log.info(
                "candidate %s score=%s -> %s (%s)",
                cand.domain, score, decision, ",".join(reasons[:4]),
            )

        res.promoted_domains = to_promote
        if to_promote and not dry_run:
            _enqueue(to_promote)
        return res
    finally:
        try:
            con.close()
        except Exception:
            pass


def _enqueue(domains: list[str]) -> None:
    """Hand promoted companies to the normal pipeline.

    The pipeline creates the `companies` rows itself, so nothing needs inserting
    here -- and the run is subject to every ceiling the probing side already has.
    """
    try:
        from src.api.app import _db_insert_run, _enqueue_pipeline_start

        run_id = _db_insert_run(
            tenant_id="dev",
            user_id="user_dev",
            user_email=None,
            domains=domains,
            options={},
            label=f"discovered-{len(domains)}",
        )
        _enqueue_pipeline_start(run_id=run_id, tenant_id="dev")
        log.info("enqueued %s discovered companies as run %s", len(domains), run_id)
    except Exception:
        log.error("failed to enqueue discovered companies", exc_info=True)
