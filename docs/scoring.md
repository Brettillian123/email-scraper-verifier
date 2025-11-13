# Scoring (R14) — Ideal Customer Profile (ICP)

This document explains how R14 lead scoring works: which inputs are used, how the score
is computed, and how to tune it safely.

# Overview

- R14 adds a simple, interpretable ICP score for each person row:

  - people.icp_score — integer, 0–100 (clamped)

  - people.icp_reasons — JSON list of human-readable reason strings

  - people.last_scored_at — ISO8601 UTC timestamp (e.g., 2025-11-12T15:23:01Z)

- The scorer is additive:

  - Each matched signal (role, seniority, company size, etc.) adds points.

  - Scores are clamped into [0, cap] (typically 0–100).

  - Missing fields are not penalized; they just don’t contribute points.

- Configuration lives in:

  - docs/icp-schema.yaml

- The engine lives in:

  - src/scoring/icp.py

  - src/config.load_icp_config() loads the YAML at process start.

  - Inputs used

- For each person/company pair we use the following normalized inputs:

  - From people:

    - domain — email/domain, e.g., acme.com

    - role_family — coarse role family from O02 (e.g., Sales, Marketing)

    - seniority — ladder level from O02 (C, VP, Director, Manager, IC)

  - From companies:

    - size — discrete bucket (e.g., "51-200", "201-1000")

    - industry — coarse industry tag (e.g., "b2b_saas", "services")

  - attrs.tech_keywords — list of tech stack hints, e.g.:
    - ["salesforce", "hubspot", "aws"]

Note: tech_keywords may be populated via O03 (enrichment) and/or O08
(tech-stack detection).

# Config schema (docs/icp-schema.yaml)

R14 uses the top-level keys in docs/icp-schema.yaml:

min_required:
  - domain
  - role_family

weights:
  role_family:
    Sales: 30
    Marketing: 25
    Engineering: 10
    Operations: 10
  seniority:
    C: 30
    VP: 25
    Director: 15
    Manager: 10
    IC: 5
  company_size:
    "1-10": 2
    "11-50": 5
    "51-200": 10
    "201-1000": 15
    "1001-5000": 12
    "5001+": 8
  industry_bonus:
    b2b_saas: 10
    services: 3
  tech_keywords:
    salesforce: 6
    hubspot: 4
    snowflake: 4
    aws: 3

thresholds:
  good: 70
  stretch: 55
  reject: 30

null_penalty: 0
cap: 100

# Key points

- min_required — minimum fields required to score:

  - If any are missing, the score is forced to 0 and the reason
    list includes "missing_min_required".

- weights — additive weights for each signal class.

- thresholds — suggested cutoffs for downstream usage:

  - good / stretch / reject are labels only; the scorer
    itself returns just a number.

- null_penalty — kept at 0 by default; nulls do not subtract points.

- cap — maximum score; final value is clamped to [0, cap].

- You can also keep legacy sections (e.g., fields, scoring, normalization_rules)
  in the same YAML; the scorer ignores them.

# Scoring algorithm

- Implementation: src/scoring/icp.py, function compute_icp.

Signature:

ScoreResult = dataclass(
    score: int,
    reasons: list[str],
)

def compute_icp(
    person: dict[str, Any],
    company: dict[str, Any] | None,
    cfg: dict[str, Any],
) -> ScoreResult:
    ...

1. Gate on required fields

We collect the set of “have” fields:

"domain", "role_family", "seniority"

from either the person dict or the company dict:

have = {
    k for k in ["domain", "role_family", "seniority"]
    if (person.get(k) or _get(company, k))
}


If min_required is not a subset of have, we return:

ScoreResult(score=0, reasons=["missing_min_required"])


No exceptions are raised; missing fields are treated as “not scoreable”.

2. Role family

If we have person["role_family"] = rf and
weights.role_family[rf] = w, we add w:

score += w

reasons.append(f"role_family:{rf}+{w}")
e.g., "role_family:Sales+30"

3. Seniority

Similarly, for person["seniority"] = sr and
weights.seniority[sr] = w:

score += w

reasons.append(f"seniority:{sr}+{w}")

4. Company size and industry

From the company dict:

size = company["size"]

industry = company["industry"]

If weights.company_size[str(size)] = w, we add it with

"company_size:201-1000+15"

If weights.industry_bonus[str(industry)] = w, we add:

"industry:b2b_saas+10"

5. Tech keywords

We look for a list of tech hints in either:

company["attrs"]["tech_keywords"] (preferred), or

company["tech_keywords"] (fallback).

For each keyword kw:

Look up weights.tech_keywords[kw.lower()] = w.

If present, score += w and append:

"tech:salesforce+6"

6. Clamp and return

Finally:

score = max(0, min(cap, score))
return ScoreResult(score=score, reasons=reasons)

Where scores are stored

Database columns (added by scripts/migrate_r14_add_icp.py):

people.icp_score — integer, nullable.

people.icp_reasons — TEXT, JSON-encoded list of strings.

people.last_scored_at — TEXT, ISO8601.

For newly ingested rows:

src/ingest/persist._insert_person:

Builds a person_for_icp dict:

domain, role_family, seniority

Calls compute_icp(person_for_icp, company=None, cfg=ICPCFG) if:

ICP config loaded, and

columns icp_score, icp_reasons, last_scored_at exist.

Writes score, reasons, and timestamp into the people insert payload.

For existing rows:

scripts/backfill_r14_icp.py:

Joins people and companies on domain.

Reconstructs person and company dicts from the DB.

Calls compute_icp(...).

Updates icp_score, icp_reasons, last_scored_at in-place.

# Tuning the scorer

You can safely tune the model by editing docs/icp-schema.yaml and
re-running the backfill script.

- Steps

  - Edit weights and/or thresholds:

  - Increase weights.role_family.Sales to prioritize Sales roles.

  - Add new industries under weights.industry_bonus.

  - Extend weights.tech_keywords with your stack.

  - Re-run backfill:

  - python scripts/backfill_r14_icp.py --db data/dev.db


  - Re-ingest new data if needed (via scripts/ingest_csv.py) — online
    scoring uses the same ICPCFG load.

# Tips

Prefer small integer weights that sum to at most cap for your
“ideal” profile.

Avoid negative weights; use lack of points as implicit down-weighting.

Keep weights interpretable; for example, “VP Sales at 201–1000 B2B SaaS
using Salesforce” scores by:

+30 (Sales)

+25 (VP)

+15 (201–1000)

+10 (b2b_saas)

+6 (salesforce)

Total: 86/100.

# Observability (optional, recommended)

For quick inspection of score distribution:

After running scripts/backfill_r14_icp.py, you can run a simple SQL query:

SELECT
    icp_score / 10 * 10 AS bucket,
    COUNT(*) AS n
FROM people
WHERE icp_score IS NOT NULL
GROUP BY bucket
ORDER BY bucket;


Or add lightweight logging/printing to your backfill script to show a
histogram or a few top leads by score.

These tools make it easy to see whether your weights are too aggressive
(everyone ≥80) or too conservative (everyone ≤40).

# FAQ

Q: What happens if docs/icp-schema.yaml is missing or invalid?
A: load_icp_config() returns {}. In that case, online scoring is skipped
and ICP columns remain NULL unless you provide a valid config and run
the backfill.

Q: Does changing weights retroactively affect existing scores?
A: Not automatically. You must re-run scripts/backfill_r14_icp.py to
recompute scores for historical rows.

Q: Do we ever overwrite raw titles/roles?
A: No. Titles and roles are normalized (O02) into title_norm,
role_family, and seniority, but raw fields remain stored separately
(e.g., title_raw, role).
