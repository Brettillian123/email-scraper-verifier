# The 2026-08 accuracy build

What changed, what it measured, and how to read a run without being misled.
This is the code running on Contabo (`157.173.206.82`, image `crestwell-iq:local`).

---

## 1. Three silent failures came first

None of these threw an exception. Each made part of the system a no-op while runs
kept reporting success. They are listed first because finding them was worth more
than everything built afterwards.

| What was broken | How long | Why nobody saw it |
|---|---|---|
| `POST /runs` returned **501** | since April | `src/api/app.py` imported `pipeline_start` from `src.queueing.tasks`; it lives in `src.queueing.pipeline_v2`. Nothing could start a run. |
| **SMTP probing was entirely off** | since the Contabo migration | `assert_smtp_probing_allowed()` gates on the machine's own hostname. A container gets a random hex id, so every probe raised `SmtpProbingDisabledError` — and runs still finished `succeeded`. |
| **`company_email_pattern` did not exist** | always | Pattern learning wrote to a table that had never been created, so nothing was ever learned. |

The shared shape: **a component that is disabled reports the same thing as a
component that had nothing to do.** When a stage reports success, check that it
did work, not merely that it did not error.

---

## 2. What the build actually does

### Learn the pattern, then probe the likely address

Previously the pipeline generated every permutation for a person and probed them
in arbitrary order. Now:

1. **`src/extract/address_harvest.py`** reads addresses a company already
   publishes on its own site and infers the pattern by matching the local part
   against a known employee name. No third-party source, no scraping of anyone
   else's service.
2. **`src/generate/patterns.py`** holds the ranking. Vocabulary widened to
   `flast`, `first.last`, `first`, `firstlast`, `f.last`, `firstl`, `last.first`,
   `first_last`. Ranking is Laplace-smoothed over confirmed/refuted evidence in
   `company_email_pattern`, so a hard-failed probe **refutes** a pattern rather
   than merely failing to confirm it.
3. **`src/verify/candidates.py`** orders candidates by that confidence, so the
   likely address goes first and the rest are dropped once one accepts.

Accuracy of the inference step, gated in CI by
`tests/test_pattern_shape_accuracy.py` against a 162-row fixture:

| band | result |
|---|---|
| overall | **94.4%** (153/162) |
| confidence >= 0.75 | **100%** (83/83) |
| role addresses (`info`, `sales`, `support`, ...) | correctly rejected |

`tests/fixtures/pattern_ground_truth.tsv` is **fully synthetic**. Names were
regenerated while preserving separator, length, hyphenation, diacritics, trailing
digits and the observed pattern mix. No real person, address or domain is in it.

### Probe more cheaply

`src/verify/smtp.py` reuses one SMTP session across all candidates for a domain,
enforces a per-domain time budget, and bounds reads (`SMTP_SESSION_MAX_BYTES`,
including the banner, via `_BoundedSMTP`).

### Only retry what the server actually deferred

`_classify_exc()` buckets socket timeouts and unexpected local exceptions as
`temp_fail` with `code=None`. The greylist retry therefore requires a
**server-issued 4xx**, not merely the `temp_fail` label. Without that gate a
`TypeError` in our own code would sit on a 15-minute retry loop, and the timeout
probes that already consume 16.5% of SMTP wall-time would each be retried three
more times. Locked by `tests/test_greylist_deferral_gate.py`.

### Do not skip healthy hosts

`src/resolve/mx.py` requires 30 samples and a >30% temp-fail rate before calling
a host a tarpit. The previous threshold mislabelled healthy providers: flagged
hosts fell 103 to 62, and Proofpoint and Google cleared.

### Keep the run alive while it works

`runs.updated_at` was written only during fan-out, which completes in seconds.
The reaper judged liveness from it, so a legitimately-running multi-hour run
would be marked failed and **re-enqueued while still running**, duplicating all
crawling and outbound SMTP. The worker path now bumps the heartbeat.

### Egress hardening

`src/fetch/ssrf.py` is the single guard. `validate_url()` refuses non-public
hosts; `safe_get()` streams and truncates at `max_bytes` so a gzip bomb cannot be
buffered whole. The resolver's HEAD probe goes through it too. The local part is
bounded to RFC 5321's 64 octets, which is what actually drove the regex blowup —
restricting domain labels alone measured as only a constant-factor win.

---

## 3. Measured results

Two sweeps, and the difference between them is the important part.

| | batch of 25 (exec-sorted head) | run `a661ca92` (the remaining 256) |
|---|---:|---:|
| companies enqueued | 25 | 256 |
| companies crawled | — | 191 |
| reached verification | 18 | 87 |
| **gained a valid address** | **17 (94%)** | **19 (7.4%)** |
| probes | 896 | 2,711 |
| **probes per valid** | **3.66** | **25.8** |
| temp-fail rate | 0% | **22%** |

Historical baseline before this build was ~44 probes per valid.

**The 3.66 was never the steady-state number.** The first batch was the
favourable head, sorted so companies with a known decision-maker went first. The
tail is 7x more expensive per result, and 22% of its probes hit greylisting or
tarpitting — which is what makes the deferral work above load-bearing rather than
theoretical.

Company coverage moved 328 to 346 of 965. Valid addresses total 1,807.

### Where the loss actually is

191 companies were crawled; only 87 produced an address to test. **104 companies
were fetched successfully and yielded nothing generatable.** Verification is
doing its job on what reaches it — the bottleneck is extraction and pattern
availability, upstream. Probing harder will not move the number.

---

## 4. Reading a run without being misled

Three reporting defects are known and unfixed. They do not affect the
correctness of addresses that are found.

1. **`status='succeeded'` fires before the verify tail drains.** Run `a661ca92`
   was marked succeeded at 21:33; verification wrote rows until roughly 22:30.
   Query `verification_results` by time window, not by run status — measuring
   this run by its terminal status undercounted it as 20 companies against a true
   87.
2. **`progress_json` is never revised.** Every domain still reads
   `state: "enqueued"` and `metrics.total_companies: 0` on a completed run. A
   stalled domain looks exactly like a finished one.
3. **A company that yields nothing records no reason** — no note of whether it
   was no people extracted, no pattern, or no MX. This is why section 3 can
   locate the largest loss in the funnel but not attribute it.

Fixing (3) is the highest-value next task: it turns the 104-company gap from a
number into a diagnosis.

---

## 5. Deliberately not done

- **Test-send / bounce oracle** — stays shelved. Mailgun's AUP bans it by name;
  reputation and ESP sending limits make it the wrong instrument.
- **GitHub / npm / PyPI as address sources** — prohibited. GitHub's Acceptable
  Use Policy forbids using information from the service for spamming purposes,
  including selling personal information to recruiters, headhunters and job
  boards.
- **Gravatar, provider-priors, headless browser** — all tested, all rejected on
  measurement (2% recall, flat signal, and cost respectively).
- **Chasing >90% coverage.** Catch-all domains are definitionally undecidable by
  RCPT probing. The realistic ceiling is materially lower, and run `a661ca92`
  suggests the earlier ~74% estimate is itself optimistic.

---

## 6. Deletions

The lead-search stack was never wired to a UI, `lead_search_docs` held 0 rows,
and its tests were the bulk of the failing suite. Removed: `search/backend.py`,
`search/indexing.py`, `search/cache.py`, the facets materialized view,
`ingest/company_enrich.py`, `crawl/targets.py`, `resolve/behavior.py`,
`db_ingest.py`, `api/runs_v2.py`, `api/run_finalize.py`, and the
`activity_logging` middleware.

`GET /leads/search` remains as a stub returning the original response shape with
an empty result set, because `CrestwellIQ.tsx` and `PipelineTracker.tsx` still
call it. **Change those callers rather than resurrecting the backend.**

Net across the build: 11,595 insertions, 9,827 deletions, 80 files.
