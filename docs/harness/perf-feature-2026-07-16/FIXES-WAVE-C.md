# Perf-Feature Scan — Wave C tail: Hot-path waste (Theme C)

> 2 commits, **2 High findings** closed — the actionable remainder of Theme C
> (its Mediums shipped in the med-tail batches). Pure hot-path cost, low risk.
> Baseline preserved: build clean, tests **267 → 268** (+1 test, 0 regressions).
> Branch `vibeman/wave-c-hotpath-2026-07-17` (off master after PR #13).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `9dcc30a` | czech-mpsv #1 | replace the per-key trends + ARES store reads with two bulk reads |
| 2 | (this) | engine-traits #1 | compute search facets only when requested + drop the to_json round-trip |

## What was fixed

1. **~4–6k sequential store round-trips per run (czech-mpsv #1).** Every daily
   `mpsv-vpm` run walked each national `|ALL|` agg cell awaiting
   `history(role_region_agg, key, 10)` — one SQLite round-trip per cell — then
   walked each distinct employer IČO awaiting `get(employers, ico)` purely to skip
   already-enriched ones, all serialized behind `await`. Collapsed each to one
   query: a single `changes_since(role_region_agg, None, N)` (newest-first, grouped
   by key, each key's window truncated to 10 to preserve the prior
   `history(key, 10)` semantics exactly — `history` orders `revision DESC`,
   `changes_since` `created_at DESC`, per-key monotonic so the same newest-first
   order), and a single `list(employers, N)` into a `HashSet` skip-set. Both bulk
   reads are capped and log when the cap is hit (a silent truncation would
   mis-classify cells as `new` or re-fetch known IČOs).

2. **~50× stored-doc overread on every search (engine-traits #1).**
   `SearchResponse` always carried facets, so every query ranked `max(limit, 1000)`
   docs and, for each, ran `searcher.doc()` (decompressing the full body) +
   `to_json` + `from_str` — ~980 of 1000 retrievals pure waste on a `limit=20`
   query, to `+= 1` two counters. The saved-search runner paid it per enabled
   search **per job completion** (≈20k facet-discarding queries/day). Added
   `SearchRequest.facets` (default off; the `/search` route opts in, the
   saved-search runner leaves it off): with facets off, `sample_size` collapses to
   `offset+limit`, so only the page window is decoded. Also replaced the per-doc
   `to_json`→`from_str` round-trip with direct `doc.get_first(field)` reads (drops
   the whole-body serialize just to read a few short fields); removed the now-dead
   `schema` field.

## Deferred (within engine-traits #1)

Layer 2 — `FAST` flags on the `app`/`dataset` schema fields + columnar facet
counting so the facets-**on** path (`/search` only) also avoids per-doc decodes.
It needs a schema migration/rebuild (the `schema_is_current` detect-and-rebuild
path is the precedent), and `/search` is the low-frequency path — unchanged here,
no regression. The dominant win (saved-search + default queries) is delivered by
layer 1.

## Gate

```
cargo build --workspace   # clean (0 warnings)
cargo test --workspace    # 268 passed / 0 failed  (was 267)
```

## Open Highs after this wave — the campaign's actionable Highs are done

3 of the original 36 remain, **all gated on live research / architecture**:
engine-traits #2 (binary/streaming HTTP body — architectural, own design pass),
trades #2 phase c (per-state OEWS wages — needs new metered research), grants-gov
#1 (federal money enrichment — needs a live upstream response shape). Themes
A/B/C/D/F/G/H/I/K now have zero actionable open Highs.
