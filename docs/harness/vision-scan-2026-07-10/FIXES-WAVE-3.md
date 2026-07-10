# Vision Scan Fix Wave 3 — Crawler & Extraction Maturity

> 6 commits, 6 ideas closed + 8 duplicates absorbed (themes T6 crawler + T5 extraction).
> Baseline preserved: build clean → build clean; tests 31 → 37 (+6, 0 failed).

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | canonical-url | 89f4ad25 | Canonical URL normalization (absorbs aeef0195 half) |
| 2 | url-filters | 64cdbc45 | Allow/deny URL pattern filters (absorbs aeef0195, eb7abeb5 halves) |
| 3 | sitemap-delay | f6edbeb7 | Sitemap discovery + crawl-delay from robots.txt (absorbs 84178e68, b83add58, 9943f303) |
| 4 | checkpoint | ebc44974 | Resumable crawl w/ persistent frontier checkpoint (absorbs 8dcb39cc) |
| 5 | transforms | e6f1b844 | Field post-processing transform pipeline (absorbs 0044d1cc typed coercion) |
| 6 | xpath | 1bfb19ba | XPath rule type in extraction engine (absorbs 395b44dd) |

## What was built

**Crawler (`crates/core/src/crawl.rs` + crawl app):**
- URLs canonicalized before the frontier (tracking params dropped, query sorted, trailing slash trimmed) — kills `?utm_*` duplicate crawling.
- `include_patterns`/`exclude_patterns` regex filters (seeds exempt), counted as `skipped_filtered`.
- `RobotRules` parses `Crawl-delay` (star group) and `Sitemap:` directives; `sitemap_seeds=true` expands seeds from declared sitemaps (fallback `/sitemap.xml`, index followed one level, caps 2000 URLs / 10 maps/host); crawl-delay honored via per-host next-allowed gate with rotation-capped requeue + idle sleep, delays capped at 30s.
- `checkpoint` (crawl-app param → named JSON beside per-job artifact dirs): queue + seen-set + kept SimHash fingerprints saved every 25 kept pages (write-then-rename) and at end; loaded before seeding. `stats.resumed` reports restoration.

**Extraction (`crates/core/src/extract.rs`):**
- Fields take an optional `transforms` chain: trim/lowercase/uppercase, to_number (currency/thousands tolerant), to_int, to_bool, regex_replace, split(+index), default-on-null; element-wise over arrays; backward compatible via serde(flatten).
- New `xpath` rule type via the pure-Rust `skyscraper` crate (new dep): attribute → value, text node → content, element → recursive text; parse-once-per-doc; invalid expressions fail at compile.

## Patterns established

7. **Canonicalize at the frontier boundary** — dedup keys must be normalized where URLs enter the queue, not at fetch time.
8. **Rotation-capped requeue for politeness gates** — when delaying a queue item, cap rotations at queue length and sleep when nothing dispatchable; avoids both spinning and stalling.
9. **serde(flatten) wrapper for extending tagged rule enums** — `FieldRule { #[serde(flatten)] rule, transforms }` adds orthogonal config to every variant without touching the enum, fully backward compatible.

## What remains (INDEX themes)

T7 API surface hardening, T4 search activation, T9 domain data products, T10 platform plays.

## Follow-ups from this wave

- Crawl-delay gate applies per dispatch, but concurrent in-flight fetches to the SAME host dispatched before the first completes can still cluster; the per-domain Governor (http engine) softens this. Tighten if a target site complains.
- Sitemap seeding fetches sequentially; parallelize per host if seeding large hosts becomes slow.
- `skyscraper` adds ~1min to cold workspace builds (grammar-heavy crate) — acceptable; revisit if CI time matters.
