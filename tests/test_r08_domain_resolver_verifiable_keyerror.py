"""
Diagnostic regression for:
  KeyError: 'atom.com' inside src.resolve.domain.decide()

Observed in batch run:
  Testing: Verifiable (verifiable.com)
  -> resolver probes https://verifiable.ai (302)
  -> crash in decide(): KeyError 'atom.com'

Purpose of this test:
  1) Reproduce the crash deterministically (network-on, opt-in).
  2) Wrap decide() to dump the full decision inputs on KeyError:
     - how domains appear across item fields (domain, final_domain, url_host, etc.)
     - which groupings include/exclude the KeyError key
     - which items reference the missing key ('atom.com') and in what fields

How to run locally (PowerShell):
  $env:RUN_NETWORK_TESTS="1"
  pytest -k verifiable_keyerror -s

Tip:
  You can also set log verbosity:
  $env:LOG_LEVEL="DEBUG"
"""

from __future__ import annotations

import dataclasses
import logging
import os
from collections import Counter, defaultdict
from typing import Any

import pytest


def _safe_getattr(obj: Any, name: str) -> Any:
    try:
        return getattr(obj, name)
    except Exception as e:  # pragma: no cover
        return f"<getattr-error {name}: {type(e).__name__}: {e}>"


def _iter_public_attrs(obj: Any) -> list[str]:
    """
    Return a stable list of "interesting" public attributes likely to exist on
    resolver item objects, focusing on domain/url/score signals.
    """
    names: set[str] = set()

    if dataclasses.is_dataclass(obj):
        for f in dataclasses.fields(obj):
            names.add(f.name)
    else:
        # Heuristic: public non-callable attributes
        for n in dir(obj):
            if n.startswith("_"):
                continue
            names.add(n)

    def interesting(n: str) -> bool:
        nl = n.lower()
        return any(
            k in nl
            for k in (
                "domain",
                "host",
                "url",
                "href",
                "redirect",
                "final",
                "effective",
                "target",
                "score",
                "reason",
                "source",
                "kind",
                "label",
                "tld",
                "netloc",
                "scheme",
                "path",
            )
        )

    # Sort for stable output
    return sorted([n for n in names if interesting(n)])


def _stringify_simple(v: Any) -> str:
    if v is None:
        return "None"
    if isinstance(v, (str, int, float, bool)):
        return repr(v)
    # Keep it compact for lists/dicts/etc.
    try:
        s = repr(v)
    except Exception:  # pragma: no cover
        s = f"<unrepr {type(v).__name__}>"
    if len(s) > 300:
        s = s[:300] + "...(truncated)"
    return s


def _extract_domainish_values(item: Any) -> dict[str, str]:
    """
    Pull out any attribute values that look like domains/hosts/urls.
    This is intentionally redundant: we want to see where 'atom.com' appears.
    """
    out: dict[str, str] = {}
    for name in _iter_public_attrs(item):
        val = _safe_getattr(item, name)
        # Only record simple-ish things
        if isinstance(val, (str, int, float, bool)) or val is None:
            out[name] = _stringify_simple(val)
    return out


def _collect_possible_domain_keys(items: list[Any]) -> dict[str, Counter]:
    """
    Build counters by attribute name for anything that looks like a domain key.
    Helps identify mismatches like:
      - best_domain derived from final_domain, but grouping uses domain
      - normalization mismatch (www., trailing dot, punycode, etc.)
    """
    counters: dict[str, Counter] = {}
    for it in items:
        vals = _extract_domainish_values(it)
        for k, v in vals.items():
            kl = k.lower()
            if "domain" in kl or "host" in kl:
                counters.setdefault(k, Counter())[v] += 1
    return counters


def _group_by_attr(items: list[Any], attr: str) -> dict[str, list[Any]]:
    groups: dict[str, list[Any]] = defaultdict(list)
    for it in items:
        v = _safe_getattr(it, attr)
        if v is None:
            key = "None"
        else:
            key = str(v)
        groups[key].append(it)
    return dict(groups)


def _print_items_referencing_key(items: list[Any], key: str, *, limit: int = 25) -> None:
    printed = 0
    key_l = key.lower()

    for idx, it in enumerate(items):
        vals = _extract_domainish_values(it)
        hit_fields: dict[str, str] = {}
        for k, v in vals.items():
            if key_l in v.lower():
                hit_fields[k] = v

        if hit_fields:
            print(f"\n--- ITEM #{idx} references {key!r} in fields: {sorted(hit_fields.keys())}")
            print(f"type={type(it).__name__}")
            # Show the matching fields first
            for fk in sorted(hit_fields.keys()):
                print(f"  {fk} = {hit_fields[fk]}")
            # Then show a compact set of all domainish fields
            for fk in sorted(vals.keys()):
                if fk in hit_fields:
                    continue
                print(f"  {fk} = {vals[fk]}")
            printed += 1
            if printed >= limit:
                print(f"\n(Stopped after {limit} matching items.)")
                break

    if printed == 0:
        print(f"\nNo items contained {key!r} in any domainish field.")


@pytest.mark.network
def test_verifiable_keyerror_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Opt-in network test that reproduces and diagnoses the KeyError.

    This is intentionally NOT a CI default. It runs only when:
      RUN_NETWORK_TESTS=1
    """
    if os.getenv("RUN_NETWORK_TESTS", "").strip() != "1":
        pytest.skip("Set RUN_NETWORK_TESTS=1 to run this network diagnostic test.")

    # Import inside test so local edits to resolver are picked up.
    from src.resolve import domain as domain_mod  # type: ignore[import]

    # Make logs easier to see when running `pytest -s`
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

    orig_decide = domain_mod.decide

    def wrapped_decide(items: Any) -> Any:
        # Some code may pass tuples/iterables; normalize once for analysis.
        items_list = list(items)

        try:
            return orig_decide(items_list)
        except KeyError as e:
            missing = str(e.args[0]) if e.args else repr(e)

            print("\n" + "=" * 90)
            print("DECIDE() DIAGNOSTIC DUMP (KeyError)")
            print("=" * 90)
            print(f"KeyError missing key: {missing!r}")
            print(f"items count: {len(items_list)}")

            # Print available grouping candidates
            attr_counters = _collect_possible_domain_keys(items_list)
            if not attr_counters:
                print("\nNo domain/host-like attributes detected on items.")
            else:
                print("\n--- DOMAIN/HOST COUNTS BY ATTRIBUTE (top 20 each) ---")
                for attr in sorted(attr_counters.keys()):
                    c = attr_counters[attr]
                    print(f"\n[{attr}] unique={len(c)}")
                    for val, n in c.most_common(20):
                        print(f"  {n:>3}  {val}")

            # Show group keys for the most likely grouping attributes, if present
            likely_group_attrs = [
                "domain",
                "domain_norm",
                "dom",
                "host",
                "host_norm",
                "final_domain",
                "effective_domain",
                "redirect_domain",
                "target_domain",
            ]
            present_group_attrs = [
                a for a in likely_group_attrs if any(hasattr(it, a) for it in items_list)
            ]
            if present_group_attrs:
                print("\n--- GROUP KEYS (by candidate attribute) ---")
                for a in present_group_attrs:
                    groups = _group_by_attr(items_list, a)
                    keys_sorted = sorted(groups.keys())
                    print(f"\nGroup attr={a!r} keys({len(keys_sorted)}):")
                    # Keep keys compact
                    for k in keys_sorted[:60]:
                        print(f"  - {k!r} (n={len(groups[k])})")
                    if len(keys_sorted) > 60:
                        print(f"  ...(and {len(keys_sorted) - 60} more keys)")
            else:
                print(
                    "\nNo likely grouping attributes found (domain/domain_norm/final_domain/etc.)."
                )

            # Print the subset of items that reference the missing key anywhere
            _print_items_referencing_key(items_list, missing, limit=25)

            print("\n" + "=" * 90)
            print("END DIAGNOSTIC DUMP")
            print("=" * 90 + "\n")

            raise

    # Monkeypatch the decide() function so we can dump the inputs when it crashes.
    monkeypatch.setattr(domain_mod, "decide", wrapped_decide)

    # This call should reproduce the observed failure mode.
    # If it does NOT crash, you’ll see a successful decision.
    decision = domain_mod.resolve("Verifiable", "verifiable.com")

    # Minimal assertion: just ensure we returned something coherent.
    # (The whole point is “do not crash”.)
    assert decision is not None
