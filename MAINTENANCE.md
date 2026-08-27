# Things that will need a decision

Two halves. `scripts/ops_audit.py` runs nightly and **tells you** when a
condition below has come true — check `GET /api/browser/ops-audit`, or
`/opt/crestwell/crestwelliq/logs/ops-audit.log`. This file holds the reasoning
the audit cannot fit into one line, plus the items no check can detect.

Nothing here is broken today. These are decisions with a trigger, written down
so they are not rediscovered as incidents.

---

## Watched automatically

| Trigger | Check | What to do |
|---|---|---|
| Outbound port 25 blocked | `port25` | Verification is dead until it clears. The nightly probe pass self-disables. Do **not** remove that preflight to "get things moving". |
| A run `running` > 6h | `stuck_runs` | Confirm queues are empty, then mark it terminal. If this fires repeatedly, the completion fix has regressed. |
| Nightly probe pass has nothing left | `recovery_backlog` | **Remove the cron.** It wakes five times a night for nothing. |
| >50% of addresses over 120 days old | `address_freshness` | Re-verify in **paced** batches. See below. |
| A schema object missing | `migrations` | Apply from `db/migrations/`. Code depending on these fails *silently*. |
| `run_reaper_state` exists | `reaper` | The reaper migration has been applied — see below before leaving it on. |
| Catch-all share ≥45% over 30 days | `catch_all_share` | The ceiling moved, not the code. |
| People behind tarpit hosts | `tarpit_blocked` | Informational. They are retried nightly at lowest priority and the flag self-expires — act only if the count grows. |

---

## The reasoning behind the triggers

### Address decay is the recurring one

Measured 2026-08-26: **~64% of Feb–April confirmations were dead by late
August**, while addresses confirmed that same month resolved at 100%. Nobody was
tracking that, which is why it surfaced as an outage rather than a maintenance
task — a re-verification pass rediscovered it all at once, at full speed, and got
outbound port 25 blocked.

Re-verification is *routine*. It is only dangerous when unpaced. At ~3 probes per
person, `SMTP_DAILY_PROBE_CAP_PER_MX=750` is roughly **250 people per provider
per day**, and most prospects sit on Google or Outlook. A few thousand addresses
is a week of nights, not one evening.

### The reaper migration is deliberately unapplied

`db/migrations/2026_08_20_run_reaper.sql` has never been applied. The reaper
defaults to `requeue=True`, so a false positive does not merely mislabel a run —
it **re-enqueues live work**, duplicating all crawling and outbound SMTP. Given
that excessive outbound SMTP is what got port 25 blocked, that is a real
operational risk.

Run completion was fixed on 2026-08-26 (the last person job at a company now
declares it done, not the fan-out) and is covered by
`tests/test_run_completion_waits_for_people.py`. Before applying the migration,
watch a few real runs reach `succeeded` at the right moment — the counter is
best-effort by design, and a missing key delays completion rather than faking it.

### Tarpit hosts, and why the verdict expires

Mimecast and Zoho genuinely defer us — verified on fresh evidence 2026-08-27:
given a clean window they still deferred 70% and 87% of probes respectively, and
re-earned the flag on present-day data.

What changed is that the verdict is now *falsifiable*. It used to be computed over
180 days, and since a flagged host is skipped before a socket opens, no new
evidence could ever overturn it — Mimecast was being condemned on probes that
predated the greylist deferral built to handle it. `SMTP_TARPIT_WINDOW_DAYS`
(14) means a host that stops deferring clears itself, with no one intervening.

### Growth no longer comes from probing

**Nobody with a name and a domain is unasked.** The SMTP layer has been taken as
far as this company/people set allows. Effort aimed at "hit rate" in verification
is spending in the wrong place; coverage now grows through discovery and
extraction — more people, not more probes.

---

## Not detectable by any check

- **`MAX_PROBES_PER_PERSON = 3`** caps candidates at three of ten renderable
  patterns, covering ~95% of the measured frequency distribution. Widening to
  four reaches ~99% for one extra RCPT on an already-open session. Untested
  against provider tolerance — worth measuring **after** address decay is handled,
  not before, and never during a backfill.

- **36 tests still skip** under `scripts/run_tests.sh` (759 pass). Worth a look
  when someone next touches those areas; not urgent.

- **`lead_search_docs`** is an orphan table at 0 rows. `GET /leads/search` is a
  stub kept alive only because `CrestwellIQ.tsx` and `PipelineTracker.tsx` still
  call it. Change the callers, then drop both.

- **Legacy outreach paths.** `outreach-pipeline.py` and
  `Automation-Additions/Email Automation/` are superseded and marked. The live
  path is `crestwell-autonomy/worker-api/`. No automated sending exists at any
  stage — every message stops at a draft and a human presses Send (charter D-61).

---

## How to run things

```bash
# the full suite, against a disposable Postgres + Redis
scripts/run_tests.sh

# the audit, by hand
/usr/local/bin/cw-iq-ops-audit.sh

# what the nightly pass would do, without doing it
docker exec -w /app -e PYTHONPATH=/app cw-iq-worker-1 \
  python scripts/nightly_recovery.py --dry-run

# probe budget consumed today
docker exec cw-iq-worker-1 python -c "from src.queueing.rate_limit import daily_probe_usage; from src.queueing.redis_conn import get_redis; print(daily_probe_usage(get_redis(), 'aspmx.l.google.com'))"
```

Rollback images, newest first: `crestwell-iq:pre-tarpit-window`,
`:pre-tarpit-precheck`, `:pre-run-completion`, `:pre-probe-caps`,
`:pre-observability`, `:pre-evidence-fix`, `:prev`.
