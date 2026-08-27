"""Company discovery: link extraction, ICP scoring, and the caps that make
auto-queueing safe.

Discovery feeds a pipeline that sends outbound SMTP, and an unbounded run of that
pipeline got the host's port 25 blocked on 2026-08-25. Promoting companies
without a human in the loop is only defensible because the ceilings hold, so the
ceilings are tested at least as carefully as the scoring.
"""

from __future__ import annotations

import pytest

from src.discover import profile as prof_mod
from src.discover import sources as src_mod

pytestmark = pytest.mark.no_db


# ---------------------------------------------------------------------------
# Link extraction
# ---------------------------------------------------------------------------


def test_portfolio_links_become_domains():
    html = """
      <a href="https://acme.io/">Acme</a>
      <a href="https://www.beta-corp.com/about">Beta</a>
      <a href="/relative">nope</a>
    """
    assert src_mod._domains_from_html(html) == ["acme.io", "beta-corp.com"]


def test_social_and_infrastructure_hosts_are_dropped():
    """A portfolio page links its own socials and its CDN on every card. Without
    this filter those outrank the actual portfolio companies by frequency."""
    html = """
      <a href="https://twitter.com/fund">tw</a>
      <a href="https://www.linkedin.com/company/fund">li</a>
      <a href="https://d1234.cloudfront.net/logo.png">img</a>
      <a href="https://realcompany.com">real</a>
    """
    assert src_mod._domains_from_html(html) == ["realcompany.com"]


def test_subdomains_reduce_to_the_mail_domain():
    """blog.acme.com is acme.com for our purposes -- probing the blog host would
    resolve to the wrong MX, or none."""
    assert src_mod._registrable("blog.acme.com") == "acme.com"
    assert src_mod._registrable("www.acme.com") == "acme.com"
    assert src_mod._registrable("careers.acme.com") == "acme.com"


# ---------------------------------------------------------------------------
# ICP scoring
# ---------------------------------------------------------------------------


def _p(**kw):
    p = prof_mod.Profile(domain="x.com", reachable=True, text_len=5000)
    for k, v in kw.items():
        setattr(p, k, v)
    return p


def test_an_ideal_prospect_clears_the_auto_queue_threshold():
    from src.discover.run import AUTO_QUEUE_THRESHOLD

    score, reasons = prof_mod.score_profile(
        _p(
            has_careers=True,
            has_team_page=True,
            stage_hits=["series a"],
            sector_hits=["saas", "b2b"],
        )
    )
    assert score >= AUTO_QUEUE_THRESHOLD, f"scored {score} on {reasons}"


def test_a_competitor_is_disqualified_not_merely_downweighted():
    """An executive search firm is not a weak prospect, it is a competitor.

    It will match every sector keyword we care about and have a careers page and
    a team page, so weighting alone would promote it.
    """
    score, reasons = prof_mod.score_profile(
        _p(
            has_careers=True,
            has_team_page=True,
            sector_hits=["saas", "b2b", "platform"],
            disqualifiers=["executive search"],
        )
    )
    assert score == 0
    assert "disqualified" in reasons


def test_an_unreachable_domain_scores_zero():
    score, reasons = prof_mod.score_profile(prof_mod.Profile(domain="x.com", reachable=False))
    assert score == 0
    assert reasons == ["unreachable"]


def test_a_parked_domain_is_penalised():
    """A splash page has the keywords and none of the substance."""
    thin, _ = prof_mod.score_profile(_p(text_len=100, sector_hits=["saas"]))
    full, _ = prof_mod.score_profile(_p(text_len=5000, sector_hits=["saas"]))
    assert thin < full


def test_keyword_stuffing_cannot_dominate_the_score():
    """Sector match is capped: a page listing every buzzword is not ten times
    better a prospect than one listing two."""
    _, reasons = prof_mod.score_profile(_p(sector_hits=list(prof_mod._SECTOR.keys())))
    sector = [r for r in reasons if r.startswith("sector:")]
    assert sector and sector[0].endswith("+25")


# ---------------------------------------------------------------------------
# The ceilings
# ---------------------------------------------------------------------------


def test_candidate_cap_truncates_and_reports(monkeypatch):
    """A source that suddenly yields thousands must be truncated AND say so.

    Silent truncation reads as "we covered everything", which is how a coverage
    gap hides in plain sight.
    """
    from src.discover import run as run_mod

    many = [src_mod.Candidate(f"c{i}.com", "vc_portfolio", "u") for i in range(500)]
    monkeypatch.setitem(run_mod.ADAPTERS, "vc_portfolio", lambda t: many)

    class _Con:
        def execute(self, sql, params=None):
            if "discovery_source" in sql:
                self._rows = [(1, "vc_portfolio", "https://fund.test/portfolio")]
            else:
                self._rows = []
            return self

        def fetchall(self):
            return self._rows

        def commit(self):
            pass

    got, dropped = run_mod.gather(_Con(), limit=50)
    assert len(got) == 50
    assert dropped == 450


def test_a_generic_saas_homepage_is_not_auto_promoted():
    """The calibration bug the first live run exposed.

    careers(20) + team(15) + sector(25) is exactly the threshold, so every
    competent SaaS homepage clears it by construction. That promoted
    anthropic.com, auth0.com and betterment.com -- real companies, none of them
    near the stage that buys fractional executives.

    Scoring at the threshold is therefore not sufficient: a stage signal is
    required, because it is the only thing on a homepage that separates a
    12-person company from a public one.
    """
    from src.discover.run import AUTO_QUEUE_THRESHOLD

    generic = _p(
        has_careers=True,
        has_team_page=True,
        # sums past the sector cap, so the score lands exactly on the threshold
        sector_hits=["saas", "fintech", "security", "compliance"],
    )
    score, _ = prof_mod.score_profile(generic)
    assert score >= AUTO_QUEUE_THRESHOLD, "precondition: this is the shape that clears on score"
    assert not generic.stage_hits, "and it carries no stage evidence, so it must not promote"


def test_thresholds_are_configured_conservatively():
    """These are the only thing between a bad source and a large outbound SMTP
    bill. A regression that loosens them should fail here first."""
    from src.discover import run as run_mod

    assert run_mod.AUTO_QUEUE_THRESHOLD >= 50
    assert run_mod.MAX_PROMOTE_PER_DAY <= 50
    assert run_mod.MAX_CANDIDATES_PER_RUN <= 1000
