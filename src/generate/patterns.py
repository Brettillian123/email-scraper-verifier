from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

# Local-part builder type
LPFn = Callable[[str, str], str]

# Canonical pattern set (ASCII, lowercase, separators normalized)
PATTERNS: dict[str, LPFn] = {
    "first.last": lambda fn, ln: f"{fn}.{ln}",
    "f.last": lambda fn, ln: f"{fn[:1]}.{ln}",
    "firstl": lambda fn, ln: f"{fn}{ln[:1]}",
    "flast": lambda fn, ln: f"{fn[:1]}{ln}",
    "first": lambda fn, ln: fn,
    "last": lambda fn, ln: ln,
    "first_last": lambda fn, ln: f"{fn}_{ln}",
    "first-last": lambda fn, ln: f"{fn}-{ln}",
    "firstlast": lambda fn, ln: f"{fn}{ln}",
}

ROLE_ALIASES = {"info", "sales", "support", "hello", "marketing", "press", "admin"}


def _safe(s: str) -> str:
    # Keep only [a-z0-9], collapse runs
    out = []
    for ch in s.lower():
        out.append(ch if ("a" <= ch <= "z") or ("0" <= ch <= "9") else " ")
    return "".join(out).split()


def norm_name(first: str, last: str) -> tuple[str, str]:
    fn = "".join(_safe(first))
    ln = "".join(_safe(last))
    return fn, ln


def apply_pattern(first: str, last: str, key: str) -> str:
    fn, ln = norm_name(first, last)
    return PATTERNS[key](fn, ln)


@dataclass(frozen=True)
class Inference:
    pattern: str | None
    confidence: float
    samples: int


def infer_domain_pattern(examples: Iterable[tuple[str, str, str]]) -> Inference:
    """
    examples: iterable of (first, last, email_localpart)
    Returns best-fitting pattern if it clearly dominates, else pattern=None.
    Rule: ≥2 hits AND ≥0.8 of non-role examples must match the same pattern.
    """
    # Filter out role aliases
    ex = [(fn, ln, lp) for fn, ln, lp in examples if lp not in ROLE_ALIASES]
    n = len(ex)
    if n < 2:
        return Inference(None, 0.0, n)

    scores: dict[str, int] = {k: 0 for k in PATTERNS}
    for first, last, lp in ex:
        fnorm, lnorm = norm_name(first, last)
        for k, builder in PATTERNS.items():
            if builder(fnorm, lnorm) == lp:
                scores[k] += 1

    best, hits = max(scores.items(), key=lambda kv: kv[1])
    conf = (hits / n) if n else 0.0
    if hits >= 2 and conf >= 0.80:
        return Inference(best, conf, n)
    return Inference(None, conf, n)
