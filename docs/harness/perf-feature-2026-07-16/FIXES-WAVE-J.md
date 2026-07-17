# Perf-Feature Scan — Wave J: Extraction & crawl power (Theme J)

> 4 commits, **4 High findings** closed; 1 (engine-traits #2) deliberately
> deferred with rationale below.
> Baseline preserved: build clean, tests **253 → 261** (+8 tests, 0 regressions).
> Branch `vibeman/wave-j-extraction-2026-07-17` (off master after PR #10).

## Commits

| # | Commit | Finding | What |
|---|--------|---------|------|
| 1 | `3d5d93c` | declarative #2 | `each` repeating-container rule so list pages yield one object per item |
| 2 | `a17fc3b` | broad-crawler #2 | host-fair round-robin frontier + per-host page budget |
| 3 | `f0bdba2` | fetch-engines #3 | scripted page actions (scroll/click/wait) for infinite-scroll |
| 4 | (this) | wasm #3 | plugin params envelope + self-describing manifest |

## What was fixed

1. **List pages were impossible or silently wrong (declarative #2).** The only
   multi-value path was `css` + `all: true`, which returns independent parallel
   arrays — if card #12 lacks a `.price`, `price` mis-zips against `name` for every
   later item. Added `Rule::Each { selector, fields }`: for each matched element it
   runs `fields` **scoped to that element**, emitting one object per match, so a
   missing field is a `null` on its own item. Inner rules may be css/regex/const or
   a nested `each`; json/xpath are rejected at compile.

2. **Multi-seed crawls degraded to single-site (broad-crawler #2).** A plain-FIFO
   frontier had no host notion, so one large seed's depth-1 links consumed the whole
   `max_pages` budget and the other seeds were never expanded — reported as a healthy
   `kept:500/hosts:20`. Rewrote `Frontier` to bucket per host and hand out
   **round-robin**, with an optional `max_pages_per_host` cap and an honest
   `skipped_host_budget` stat. Checkpoint on-disk shape kept unchanged (flat queue;
   buckets rederived on load), so existing checkpoints resume without a reset.

3. **The browser tier couldn't reach infinite-scroll (fetch-engines #3).** A render
   captured one viewport of the first paint and returned — silently truncating the
   listings the browser tier exists for (a short listing isn't thin content, so it
   won the tier and never escalated). Added `RenderRequest.actions: Vec<PageAction>`
   (also on `FetchRequest`), run after settle and before `evaluate` under a
   one-nav-timeout budget: `scroll_bottom`, `scroll_by`, `click`, `type`,
   `wait_for_selector`, `wait_ms`, and `repeat {…, until_selector_count_stable}` (the
   scroll-until-no-new-rows loop). `RenderedPage.actions_completed` makes truncation
   visible.

4. **Plugins couldn't be configured per job (wasm #3).** The ABI passed only the
   document, so any variation meant recompiling a wasm module. Added a params
   envelope: `Plugins::run` gains `params`; the host prefers an `extract_v2` export
   (input = `{doc, params}` envelope) and falls back to legacy `extract`, so old
   plugins keep working. Added an optional `describe()` manifest export, read once at
   load and surfaced by `GET /plugins`. Reference plugin `title-extractor` rebuilt to
   export both; verified end-to-end against the real wasm.

## Deferred — engine-traits #2 (binary/streaming HTTP body)

**Not done this wave, by design.** Both the scan INDEX ("a structural ceiling …
deserves its own design pass, not a wave slot") and the open-Highs inventory flagged
this as **architectural**. Its (additive) sketch still rewrites `read_body_capped`
— the path every HTTP fetch takes — adds a streaming-to-file download mode and a
behavior change (typed error on a non-text body instead of lossy-decoding), and
ripples through 6+ `HttpResponse` construction sites and the fetcher passthrough.
Most importantly it needs **live verification** this environment can't provide:
actually landing the CMS RVU ZIP as an artifact and measuring the `mpsv-vpm` peak-RSS
drop. Shipping a change to the hottest path blind, as the fifth commit of an
already-large wave, is the wrong trade. It should be a focused session of its own
(content_type + download_to streaming, verified against a real binary fetch).

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 261 passed / 0 failed  (was 253)
```

## Open Highs after this wave

10 of the original 36 remain: engine-traits #2 (deferred, above), theme F caching
(3), theme I domain model (3), theme C tail (2), and E grants-gov #1 (deferred).
Themes A/B/D/G/H/K have zero open Highs; theme J has one deferred.
