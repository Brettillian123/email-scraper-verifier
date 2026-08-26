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

### Where the loss actually was — and the bug behind it

An earlier version of this section read the funnel wrong, and the correction is
the most important thing in this document.

191 companies were crawled and only 87 retained an address, which looked like an
extraction failure. It was not. `CLEANUP_INVALID_GENERATED` **deletes** the email
row for every address that hard-fails, so a company that was probed and missed is
indistinguishable in the `emails` table from one that was never probed at all.
Those companies were probed.

The real cause was in how probe failures were recorded. See
[pattern evidence is person-scoped](#7-pattern-evidence-is-person-scoped).

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
   was no people extracted, no pattern, no MX, or every candidate bounced.
   `CLEANUP_INVALID_GENERATED` then deletes the failed rows, so the outcome is
   unrecoverable after the fact. This is what made the diagnosis in section 7
   take a full round of wrong conclusions to reach, and it is the highest-value
   observability work outstanding.

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

---

## 7. Pattern evidence is person-scoped

The single most damaging defect found in this cycle, and the reason the sweep in
section 3 returned 7.4% instead of something much higher.

### The conflation

`_teach_unprocessed()` banked every 550 from a probe session as evidence against
the **format**. But a 550 for `kellyleveque@magicspoon.com` proves exactly one
thing: Kelly LeVeque has no mailbox at that domain. It says nothing about whether
the company uses `firstlast`.

Those are different claims, and the code treated them as the same claim.

### Why that was catastrophic rather than merely imprecise

Extraction legitimately picks up people who are not employees — advisors, brand
ambassadors, testimonial names, board members, people who have left. Magic Spoon's
`/pages/people` route lists athletes. Every one of them bounces on **every**
format we try.

So one absent person banked a failure against the entire vocabulary at once. With
`REFUTED_MIN_FAILURES = 2`, it took just **two** such people to refute every
pattern a company had. `rank_patterns()` then filtered out the refuted patterns,
every remaining employee ranked to an empty candidate plan, and nobody at that
company was ever probed again.

The damage compounds in the worst possible direction: the more people a company
has listed, the more likely two of them are non-employees, and the whole company
is written off — including the real employees further down the list.

### Measured, in production

| | |
|---|---:|
| companies with every pattern refuted and none confirmed | **73** |
| people at those companies | 521 |
| **people never probed as a result** | **460** |
| pattern rows holding failure counts that came only from absent people | 622 |
| companies affected once partial refutations are counted | **120** |
| **people never probed, full set** | **535** |

`caremessage.org` is the clean illustration: **26 people**, refuted after 3 of
them bounced, ~23 never probed — and `ccorral@caremessage.org` (`flast`) resolves
perfectly well. `elemenohealth.com`: 5 people, all patterns refuted, and
`steve.mundro@elemenohealth.com` (`first.last`) resolves. Both formats were rank
1 and 2 in the base-rate order. Their own evidence had declared them dead.

This is also why the exec-sorted batch of 25 scored 94%: a real CEO gets probed
first, **confirms** a pattern, and the company is never poisoned.

### The fix

Failures are held until the person is shown to have a mailbox at all:

- If some candidate for a person **accepts** and others are refused, the person
  exists, the MX demonstrably distinguishes local-parts, and the refusals are
  real format evidence — recorded, exactly as before.
- If **nothing** accepts, the probe set is the signature of an absent person and
  teaches nothing. The held failures are dropped.

That is the whole change: `pending_failures` in `_generate_emails_sequential()`,
flushed by `_flush_failures()` only on an accept. Locked by
`tests/test_pattern_evidence_person_scope.py`, which fails against the old code
with `[('flast', False), ('first.last', False), ('first', False)]` — one absent
person refuting three formats.

Note that no negative evidence is lost. The accept-alongside-rejection case is
strictly *better* evidence than what was being collected before, because it is
the only case where a rejection is attributable to format at all.

### Repairing what was already written

`scripts/backfill_clear_person_scoped_refutations.py` resets `failed_count` to 0
**only** at companies with no confirmed pattern anywhere — those never saw an
accept, so every failure they hold came from the absent-person path. Companies
that do have a confirmed pattern keep their failures, because those sat beside an
accept. `inferred_count` is never touched; it comes from the harvester, not from
probing.

Applied 2026-08-25: 622 rows across 120 companies, 1,261 failure counts dropped.
A pre-repair dump of the table is at `/root/company_email_pattern.pre-repair.sql`
on the VPS.

### Measured after the fix

Run `3c392669`, 2026-08-25: the 115 companies the bug had written off, re-run with
the evidence cleared and the corrected teaching logic.

| | |
|---|---:|
| companies re-run | 115 |
| **companies that recovered a valid address** | **12** |
| **valid addresses recovered** | **98** |
| companies recording their first-ever valid address | 11 |
| company coverage | 346 → **355** |
| valid addresses total | 1,807 → **1,847** |
| **companies re-poisoned (all patterns refuted, none confirmed)** | **0** |

Every one of those 115 companies had produced **zero** before: all patterns
refuted, ~535 people never probed. `caremessage.org` — the 26-person company used
as the illustration above — returned 6 valid addresses.
`uplandspecialty.com` returned 39 and `mhubchicago.com` 16.

The zero in the last row is the load-bearing number. Before the fix, 73 companies
sat in that state. This run created none, because every company that recorded
failures also recorded a confirmation — which is precisely the condition the fix
requires.

End-to-end check in production: re-probing Kelly LeVeque at `magicspoon.com`
returns `all_invalid` at 3 probes and writes **nothing** to
`company_email_pattern` — all six patterns still read `failed_count = 0` with
`last_seen_at` untouched. Under the old code that single person would have
refuted three formats.

---

## 8. Queue priority: the stages were running serially

RQ dequeues from queues in **strict priority order**, not round-robin. Both
workers ran `orchestrator,crawl,generate,verify`, so every crawl job in a run had
to finish before either worker would look at generate.

Measured on run `3c392669`: the crawl queue drained for over an hour while the
generate queue grew to 655 and produced **not one verification**. Only when crawl
hit zero did generation begin. The two stages ran end to end when they could have
overlapped.

Worker 2 now runs `orchestrator,generate,verify,crawl` — generate first, falling
back to crawl when there is nothing to generate, so neither queue starves the
other and neither worker idles. Set in
`docker-compose.crestwelliq.yml`; worker 1 keeps the original order.

---

## 9. The 2026-08-25 outage — probe volume, and what it cost

**Read this before running any backfill.**

### What happened

The backfill of never-probed people (run `c19269bd`, 455 companies) issued
roughly **8,700 RCPT probes in a single run, ~6,400 of them at one provider**.

- The provider began throttling: **921 × `450`**.
- **The host blocked outbound port 25 entirely** — 674 probes recorded
  `port25_unreachable`. All verification stopped for a day and the block had to
  be lifted by hand.

Nothing in the system objected, because nothing was watching the right thing.
The RPS limiter caps **burst**, and every individual second was inside its
budget. Sustained volume had no ceiling at all.

### What it cost

The run did find **863 valid addresses**. It also did damage:

| | before | after |
|---|---:|---:|
| confirmed addresses | 1,847 | **1,564** |
| companies with a valid address | 355 | **286** |

`CLEANUP_INVALID_GENERATED` deletes an address, and its whole verification
history, the moment a probe returns `invalid`. A throttling provider **rejects
rather than defers**, and a policy rejection under directory-harvest suspicion is
indistinguishable from "no such user" at the SMTP layer. Some of that 283 is
genuine churn. **How much cannot be established, because the rows were deleted.**

Note what did *not* cause damage: the host block itself surfaced as
`port25_unreachable`, which classifies as `temp_fail` and correctly deletes
nothing. The earlier work on that classification held up.

### Two guards, both now in place

**1. A rolling daily ceiling on probe volume** (`src/queueing/rate_limit.py`),
global and per MX host, checked before every probe:

| | default | env |
|---|---:|---|
| global | 3,000 / day | `SMTP_DAILY_PROBE_CAP` |
| per MX host | 750 / day | `SMTP_DAILY_PROBE_CAP_PER_MX` |

The per-MX cap is the one that matters here — it would have stopped that run at
roughly an eighth of the volume it reached. A refused probe returns **`unknown`,
never `invalid`**: an address we declined to test is untested, not dead, and
grading it otherwise would turn a rate-limit event into permanent data loss. The
budget fails **open** on a Redis error, because halting all verification is worse
than losing the ceiling, and the host block is the backstop regardless.

**2. A confirmed address is never deleted by a later 550.**
`emails.first_valid_at` is stamped on first confirmation and never cleared;
both deletion paths refuse when it is set, and the address keeps its row while
recording the new status, so a genuinely dead mailbox is still visible as invalid
rather than vanishing. The guard fails **closed** — a missing column or a query
error means "keep it". Migration
`2026_08_26_protect_confirmed_addresses.sql` stamps the 1,561 addresses already
in the table, so existing data is covered rather than only future confirmations.

Both locked by `tests/test_probe_volume_and_confirmed_addresses.py`.

### The principle, for the third time

This is the same error as the person-scoped pattern bug in section 7, in a
different costume: **a single observation was allowed to erase accumulated
evidence.** One bounce refuted a company's whole vocabulary; one 550 deleted a
confirmed address. Both are now required to survive corroboration before they
destroy anything.

### Operational rule

Do not run an unbounded backfill. Size it against the per-MX ceiling first:

```sql
SELECT count(*) AS people, count(DISTINCT company_id) AS companies
  FROM people p WHERE p.generation_at IS NULL AND p.first_name <> '';
```

At ~3 probes per person, 750 probes per MX per day is roughly **250 people per
provider per day**. A run spanning thousands of people belongs spread across days,
not compressed into one evening.

---

## 10. The outage loss was mostly correction, not damage

Section 9 says the ~1,148 deleted addresses could not be assessed because the
rows were gone. They can be assessed — by asking whether those people have a
findable address *now*, on healthy SMTP. Measured 2026-08-26:

| test | result |
|---|---|
| 25 people from the deleted cohort, probed across the **full ten-pattern vocabulary** | **0 recovered** |
| 40 surviving **pre-August** confirmations, re-probed | **90% still resolve** |
| 40 surviving **August** confirmations, re-probed | **100% still resolve** |

Zero recovery across the entire vocabulary is not a ranking failure or a probe
budget failure. Those mailboxes do not exist.

**So the run retired stale confirmations rather than destroying good ones.** The
1,847 figure included roughly 1,148 Feb–April guesses that had since died; the
1,564 that remain resolve at 90–100% on re-test. The address book got smaller and
substantially more accurate in the same movement.

This corrects the earlier claim that "most of that loss is probably recoverable".
It was not recoverable, and it did not need to be.

What remains true from section 9: the volume was reckless, the host block was
real, and the guards belong there regardless. A run that deletes 1,148 rows
should not be able to happen by accident even when the deletions turn out to be
right — the outcome was luck, not design.

### The addressable pool is finished

| `people.generation_status` | count |
|---|---:|
| never asked | **0** |
| `deferred` | 5 |
| `exhausted` | 469 |
| `all_invalid` | 1,319 |

Nobody with a name and a domain is unasked. `all_invalid` is final — that is the
cohort tested above. The genuinely open queue is 474 people, roughly one night
under the daily cap, after which `nightly_recovery.py` logs "no unprobed people
remain" and no-ops.

**Coverage now grows by finding new PEOPLE, not by probing harder.** Discovery and
extraction are the remaining levers; the SMTP layer has been taken as far as this
company/people set allows.
