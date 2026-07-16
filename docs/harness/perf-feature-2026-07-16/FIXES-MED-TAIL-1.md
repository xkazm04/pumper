# Perf-Feature Scan — Medium Tail, Batch 1: wasted work / unbounded allocation

> 5 commits, 5 Medium findings closed (perf-tail: work computed then discarded,
> or copies/buffers that should be bounded).
> Baseline preserved: build clean, tests **224 → 226** (0 regressions).
> Branch `vibeman/perf-feature-2026-07-16`. Not pushed.

## Theme

The remaining tail is mostly Mediums scattered one-or-two per report. This batch
takes the cohesive **"hot-path waste"** cluster — each is a local, well-pinned
change that removes an avoidable allocation, copy, or CPU spike.

## Commits

| # | Commit | Finding | What |
|---|---|---|---|
| 1 | `133f313` | wasm #2 | compile plugins off the async runtime in `reload()` |
| 2 | `985a3ef` | extraction-crawl-api-watch #2 | stop cloning every doc + DocReport in `extract_and_upsert` |
| 3 | `1875b76` | census #2 | filter the market-blend read to state rows in SQL |
| 4 | `92d318b` | tiered-fetcher #3 | count text with an early-exit counter, don't build Markdown to measure it |
| 5 | `b097cd8` | fetch-engines #2 | cap captured browser HTML like the HTTP tier caps bodies |

## What was fixed

1. **wasm `reload()` off the runtime.** `reload()` is `async` but its body was
   synchronous fs + a full Cranelift compile per module, parking a tokio *worker*
   thread for ~0.2–2s on a 10–20 module dir and stalling unrelated requests. `run()`
   already used `spawn_blocking` for the same reason; `reload()` now does too.

2. **Extractor clone elimination.** The shared extract tail deep-cloned every HTML
   body (`keyed.iter().map(|(_,d)| d.clone())`) just to split keys from docs — ~2×
   peak RSS on a 10k-page batch — and deep-cloned every `DocReport` into a throwaway
   Vec for `summarize_reports`. Now `keyed.into_iter().unzip()` (zero copies) and
   `summarize_reports` takes `impl IntoIterator<Item = &DocReport>`.

3. **Census blend filtered in SQL.** `sync_market_blend` read the *entire*
   `establishments` dataset and parsed every row's JSON, then discarded every county
   row in Rust (~98% waste on a nationwide county run) — and the
   `ORDER BY updated_at DESC LIMIT 50000` was a silent truncation cliff past the cap.
   Now reads `geo = state` via `list_filtered`, cutting to the ~208 rows that matter
   and removing the cliff.

4. **Fetcher: predicate, not product.** The escalation decision built a full-page
   Markdown document just to count its chars, then discarded it whenever
   `to_markdown` was false (the extractor/plugin hot paths). Now Markdown is built
   only for the caller; the "≥ min_chars of text?" decision uses new
   `markdown::text_len_capped(html, min)` — same SKIP/whitespace rules, a counter
   that early-exits at the cap, no output String. Applied to both tiers.

5. **Browser HTML cap.** The HTTP tier caps its body at `max_body_bytes` (16 MiB);
   the browser tier buffered the entire serialized DOM into an unbounded String, so
   a JS-heavy page bypassed the memory guard on the *more expensive* tier. Added
   `[browser] max_html_bytes` (+ per-render `RenderRequest.max_body_bytes` override);
   over-cap renders return `Error::Browser` naming cap + URL, symmetric with
   `Error::Http`. Decision is a pure, Chrome-free `over_html_cap` helper.

## Verification

| Gate | Before | After |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 224 / 0 | 226 / 0 |

New tests: `text_len_capped` saturates/exact/skips-boilerplate and agrees with
`html_to_markdown` on the threshold; `over_html_cap` strict/zero-disabled. The
extractor, census, and wasm fixes preserve behaviour and are covered by their
existing tests.

## Remaining Medium/Low tail (open)

Still open after this batch (roughly, by area):
- **app-registry #2** (declare app preconditions), **#3** (Low, honest ZST negative — likely no action)
- **broad-crawler #3** (sitemap `<lastmod>` for revisit prioritization)
- **config-catalog #2** (per-request timeout override), **#3** (freshness monitor)
- **czech-labour #2** (drop parsed feed before ARES phase)
- **declarative-extraction #3** (HTML→Markdown as a scoped rule)
- **engine-capability-traits #3** (search offset + true total)
- **eu-funding #3** (CMS self-baselining watcher)
- **extraction-crawl-api-watch #3** (plugin `source` mode)
- **http-api-routes #3** (response CompressionLayer)
- **job-worker #2** (scheduler misfire re-enumeration)
- **live-events-webhooks #3** (delivery id/ts header + timestamped signature)
- **census #3** (employer-side wage/revenue benchmark)
- **trades #3** (vintage-keyed datasets)
- **web-research #2** (`salvage_json` for research), **#3** (readable double-store)

Plus the larger deferred items (a genuinely open set, not tail): several **Highs**
the themed waves didn't reach (extraction repeating-container, engine binary body,
fetch scripted actions, wasm concurrency cap, various domain-app feature gaps), the
crawl delta-journal checkpoint, and grants money-enrichment (needs a live
`fetchOpportunity` response to verify).
