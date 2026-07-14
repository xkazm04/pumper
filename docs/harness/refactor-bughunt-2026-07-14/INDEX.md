# Code-Refactor + Bug-Hunter Scan — pumper, 2026-07-14

> Dual-lens audit (🐛 bug-hunter + 🧹 code-refactor combined, ~5 findings/context) over the full 21-context map.
> 21 parallel subagent runs, batched in waves of 8. Read-only scan; no code changed.
> Baseline at scan time: `cargo build` clean, **177 tests passing / 0 failing**, 0 warnings.

---

## Totals

| | Critical | High | Medium | Low | **Total** |
|---|---:|---:|---:|---:|---:|
| Across 21 contexts | 4 | 24 | 59 | 15 | **102** |
| Share | 4% | 24% | 58% | 15% | 100% |

Lens split: **77 bug-hunter · 25 code-refactor.** Counts verified two ways (per-report `Total:` headers sum = per-finding `Severity:` bullets = 102).

---

## Per-context breakdown

(Sorted by criticals desc, then highs, then total.)

| # | Context | C | H | M | L | Total | Report |
|---|---|---:|---:|---:|---:|---:|---|
| 1 | US Grant Opportunities | 1 | 2 | 2 | 0 | 5 | `us-grant-opportunities.md` |
| 2 | Extraction, Crawl & API Watch | 1 | 1 | 2 | 1 | 5 | `extraction-crawl-api-watch.md` |
| 3 | Czech Labour Market (MPSV) | 1 | 1 | 2 | 1 | 5 | `czech-labour-market-mpsv.md` |
| 4 | WASM Plugin Sandbox | 1 | 1 | 1 | 2 | 5 | `wasm-plugin-sandbox.md` |
| 5 | Broad Crawler | 0 | 2 | 3 | 0 | 5 | `broad-crawler.md` |
| 6 | US Trades Wages, Tax & Valuation | 0 | 2 | 3 | 0 | 5 | `us-trades-wages-tax-valuation.md` |
| 7 | US Trades Business Density (Census) | 0 | 2 | 3 | 0 | 5 | `us-trades-business-density-census.md` |
| 8 | Web Research & Readable Content | 0 | 2 | 3 | 0 | 5 | `web-research-readable-content.md` |
| 9 | Tiered Fetcher & Politeness | 0 | 2 | 2 | 1 | 5 | `tiered-fetcher-politeness.md` |
| 10 | App & Job Model | 0 | 1 | 3 | 1 | 5 | `app-job-model.md` |
| 11 | Engine Capability Traits | 0 | 1 | 2 | 2 | 5 | `engine-capability-traits.md` |
| 12 | Declarative Extraction Engine | 0 | 1 | 4 | 0 | 5 | `declarative-extraction-engine.md` |
| 13 | Dataset Store & Change Detection | 0 | 1 | 4 | 0 | 5 | `dataset-store-change-detection.md` |
| 14 | Full-Text Search Index | 0 | 1 | 4 | 0 | 5 | `full-text-search-index.md` |
| 15 | Fetch Engines (HTTP/Browser/Claude) | 0 | 1 | 4 | 0 | 5 | `fetch-engines.md` |
| 16 | Live Events & Webhooks | 0 | 1 | 3 | 1 | 5 | `live-events-webhooks.md` |
| 17 | HTTP API & Routes | 0 | 1 | 3 | 1 | 5 | `http-api-routes.md` |
| 18 | EU & Regulatory Funding Watchers | 0 | 1 | 4 | 0 | 5 | `eu-regulatory-funding-watchers.md` |
| 19 | Job Worker & Cron Scheduler | 0 | 0 | 4 | 1 | 5 | `job-worker-cron-scheduler.md` |
| 20 | Configuration & Data Source Catalog | 0 | 0 | 2 | 2 | 4 | `configuration-data-source-catalog.md` |
| 21 | App Registry | 0 | 0 | 2 | 1 | 3 | `app-registry.md` |

---

## The 4 Critical findings

1. **WASM sandbox — guest-controlled output length drives an unbounded host allocation.** Low 32 bits of `extract`'s return drive `vec![0u8; out_len]` (up to 4.29 GB) on the host heap *before* any bounds check, unaffected by the linear-memory cap; alloc failure aborts the whole process and `spawn_blocking` can't catch it → one crafted plugin return DoSes the server. `crates/engine-wasm/src/lib.rs:147-156`
2. **Extractor — path traversal / arbitrary file read.** `artifact_path` + `job_id` + free-form `source.app` are joined into a filesystem path with zero sanitization, so a crafted dataset record (`artifact_path: "../../../etc/passwd"` or an absolute path) yields arbitrary server-file read returned in job output. The sibling crawl app already sanitizes its checkpoint name — guard pattern known but omitted. `crates/apps/extractor/src/lib.rs:117-142`
3. **grants-common — `parse_date` panics on a non-ASCII date value.** `&s[..s.len().min(10)]` is a raw **byte** slice; a non-ASCII char (em-dash in a "Deadline—see website" cell) straddling byte 10 panics on a non-char-boundary and hard-fails the entire scrape run. Reached from grants-gov's digest and via `norm_date` in both normalizers. `crates/apps/grants-common/src/lib.rs:346`
4. **mpsv-vpm — monthly salaries silently discarded.** `is_monthly()` gates every monthly salary on `typMzdy.id.contains("mesic")`, but that field uses the codebook-URI `"Name/id"` form (like its siblings `CzIsco/93291`, `Kraj/108`), so the substring never matches — the entire salary distribution and posted-vs-official gap silently empty while the run reports success on healthy posting counts. `crates/apps/mpsv-vpm/src/lib.rs:806-827`

---

## The 24 High findings — grouped by theme

### Security / trust-boundary / DoS
- **WASM store limiter caps memory but not tables/instances** — a malicious module OOMs at instantiation, bypassing the 64 MB cap. `engine-wasm/src/lib.rs:115,122-125`
- **Fully-permissive CORS over an unauthenticated, mutating, data-bearing API** — any site the operator visits can cross-origin read all scraped data and trigger deletes/enqueues (DNS-rebinding defeats the localhost assumption). `server/src/routes.rs:110-117`

### Cost / metering governance
- **Metering + `budget_usd` seam is bypassable** — `ctx.engines` is `pub` and the `extractor` app spends paid Claude through the raw fetcher (`AutoWithResearch`), so cost ledger + budget are silently defeated. `core/src/app.rs` + `apps/extractor/src/lib.rs:206-228`

### Cache correctness
- **HTTP cache identity ignores `headers`/`proxy`** — cacheable GETs differing only in `Accept`/`Accept-Language` (content negotiation) or proxy (geo egress) collide and serve the wrong body. `core/src/engine.rs:88-89,128-129` → `core/src/cache.rs:40-49`
- **`ttl_override` never caps read staleness** — `HttpCache::get` takes no freshness bound, so a long-TTL writer silently defeats a short-TTL reader (the exact two-watches-on-one-endpoint scenario in `watch`'s own docs), returning ~50-min-stale content as fresh and defeating change detection. `core/src/cache.rs:52-73`

### Concurrency & atomicity
- **`Datasets::upsert` is a non-atomic read-modify-write** — SELECT → UPDATE/INSERT → `add_revision` in separate autocommit statements; concurrent same-key writers (per-app concurrency is configurable >1) either abort the whole `upsert_many` batch or diff against stale bases and corrupt the revision/diff chain the change-feed relies on. `core/src/datasets.rs:132-203`
- **`EventBus::emit` assigns the seq id outside the ring lock** and broadcasts outside it — concurrent emits (worker per-job tasks + HTTP handlers) corrupt ring order and wire order, dropping events for live subscribers and firing false `reset` storms. `server/src/events.rs:89-100`

### Data-truth (money/number/sentinel)
- **`money_range` sweeps *all* numbers in `EstAmounts`** — "Up to $500k" → floor $500k; "5 awards" → floor $5, corrupting CA award floor/ceiling. `apps/ca-grants` → `grants-common/src/lib.rs:313-331`
- **Negative Census annotation/jam sentinels (`-666666666`) summed raw into CBP/NES totals** — guarded in `fetch_denominator` but forgotten in the two primary metric parsers → corrupt national totals + market blend. `apps/census-density/src/lib.rs:241-252`, `apps/census-nonemp/src/lib.rs:195-197`
- **Census disclosure-suppressed cells silently become 0** and count as genuinely-reported places → fabricated $0 markets. `apps/census-nonemp/src/lib.rs:194-219`
- **homewyse priced values stored raw (`j.get("low")`) instead of the validated number** — string-quoted prices silently dropped from the unified rollup. `apps/homewyse-pricing/src/lib.rs:148-172`

### Change-detection integrity (sync/upsert misuse)
- **homewyse pricing keyed on model free-text `job` + `upsert_many`** — drifting phrasing accumulates stale/duplicate rows unboundedly, steadily corrupting `summarize_pricing`'s operator-facing low/median/high envelope. `apps/homewyse-pricing/src/lib.rs:147-186`

### Silent-failure / empty-as-success (scrapers report broken as OK)
- **eu-sedia has no "positive total, zero parsed rows" drift guard** — a renamed/nested `results` array → `[]`, loop breaks after page 1, run returns Ok `fetched: 0`, masking an upstream schema break (grants-gov guards this exact case). `apps/eu-sedia/src/lib.rs:99-129`
- **JSON-pointer extraction rules are never validated at `compile()`** (CSS/regex/xpath fail-closed) — a malformed pointer returns `Null` → classified `Empty` (a real miss) instead of `Error`, defeating the whole `DocReport`/`FieldStatus` purpose. `core/src/extract.rs:116,407-410`
- **Research app accepts any LLM JSON shape as `structured: true`** and stores hallucinated/wrong-shape output verbatim; the `json_schema` guardrail is never set. `apps/research/src/lib.rs:42-69`
- **Readable returns empty extraction as HTTP-200 success** (`unwrap_or_default`; fetcher returns Ok on thin content). `apps/readable/src/lib.rs:46-62`
- **Plugin app persists fetch/plugin *error* records into the output dataset** as if they were data. `apps/plugin/src/lib.rs:56-82`
- **Malformed search query returns HTTP 500 (not 400)** and silently kills saved-search alerts (`parse_query` error → `Error::App` → blanket 500; alert path fails open with only a warn). `engine-search/src/lib.rs:217-219`

### Crawler correctness
- **Near-duplicate pages' outbound links are never followed** (link-following nested inside the "kept" branch) — pagination/faceted-nav subtrees reachable only via near-dup pages are silently under-crawled. `core/src/crawl.rs:580-640`
- **robots.txt fetch is awaited inside the scheduling loop** — every new host stalls all in-flight fetches. `core/src/crawl.rs:489-531`

### Scraper regional/aggregate bias
- **mpsv-vpm region roll-ups drop every posting lacking a CZ-ISCO code** — biasing the "true regional distribution." `apps/mpsv-vpm/src/lib.rs:192-222`

### Resource / throughput
- **Chrome launch runs while the global `holders` mutex is held** — a cold start, crash-relaunch, or recycle serializes every render across every profile; one crash-looping Chrome wedges the whole render pool. `engine-browser/src/lib.rs:214-243`

### Duplication (code-refactor lens)
- **Near-total duplication of the two grant `run()` bodies + HTTP builders** — the cross-source finalize belongs in `grants-common`. `apps/grants-gov/src/lib.rs:82-219` vs `apps/ca-grants/src/lib.rs:67-177`

### Fetcher tier logic
- **`AutoWithResearch` discards an already-fetched lower-tier body when the Claude tier errors** — wasted fetch + no fallback content. `core/src/fetcher.rs:420-461`

---

## Triage themes

| # | Theme | ~Count | Why it's a wave, not scattered fixes |
|---|---|---:|---|
| T1 | **Silent-failure / empty-as-success drift** | ~16 | Scrapers return Ok on broken/empty/garbage upstream — one shared "honest result" contract (drift guard + empty-is-error) fixes the class. |
| T2 | **Data-truth parsing** (money/number/date/sentinel) | ~9 | Locale commas, byte-slicing, disclosure sentinels, unvalidated model numbers — all corrupt stored values; fix the shared parsers once. |
| T3 | **Concurrency & atomicity** | ~7 | Non-atomic RMW, seq-outside-lock, index-before-fence, check-then-spend — same "wrap in a transaction / hold the lock" mental model. |
| T4 | **Security / trust-boundary / DoS** | ~9 | Path traversal, guest-controlled alloc, permissive CORS, unbounded query/retry — the untrusted-input boundary. |
| T5 | **Cache correctness** | 2 | Cache identity + freshness bound — two tightly-coupled fixes to `cache.rs`. |
| T6 | **Cost / metering governance** | ~4 | Unmetered spend paths + budget-bypass — close the `ctx.engines` seam and meter timeout/failure paths. |
| T7 | **Resource leaks / unbounded growth** | ~7 | Leaked tabs, unbounded delivery log, phantom governor slots, double-held content. |
| T8 | **Change-detection integrity** (sync/upsert misuse) | ~5 | `upsert_many` where `sync_many` belongs, empty-set tombstoning — the record-lifecycle contract. |
| T9 | **Crawler correctness** | ~6 | Link-following, robots semantics, frontier caps, revisit — all `crawl.rs`. |
| T10 | **Duplication / consolidation** (refactor lens) | ~18 | Grants/census/trades apps + list-handler + LRU + jitter duplication — divergence already caused bugs (census sentinels). |
| T11 | **Config / catalog / registry drift** | ~5 | Catalog vs. crate truth, duplicate ids/docs_urls, silent app overwrite. |

---

## Suggested next-phase split (fix waves)

Ordered by severity-then-coherence; each wave is one focused session (~5–7 fixes, one mental model). Waves 1–3 carry all criticals + the highest-value highs.

- **Wave 1 — Crash & sandbox safety (all 4 Criticals + WASM hardening).** WASM out_len alloc (C) + tables/instances cap (H) + wall-clock/epoch deadline (M); extractor path traversal (C) + `save_artifact` sanitization (M); `parse_date` byte-slice panic (C); mpsv `is_monthly` (C). *Nothing crashes the process or escapes the sandbox.*
- **Wave 2 — Cost truth + cache identity.** Metering seam bypass (H); Claude-spend-on-timeout unmetered (M); fail-all-tiers records no cost/strike (M); budget check-then-spend overshoot (M); cache key ignores headers/proxy (H); `ttl_override` staleness cap (H).
- **Wave 3 — Concurrency & atomicity.** `upsert` non-atomic RMW (H); `emit` seq outside lock (H); search-index-before-fence (M); enqueue+touch non-atomic dup run (M); governor phantom-slot on cancel (M); reaper staleness-vs-heartbeat invariant (M).
- **Wave 4 — Scraper data-truth (numbers must be true).** money_range greedy sweep (H); census negative sentinels (H); census suppressed→0 (H); homewyse raw priced values (H); `parse_first_number` EU decimal comma 100× (M); mpsv `official_wage_index` as_f64-only (M).
- **Wave 5 — Honest results (empty/garbage ≠ success).** eu-sedia drift guard (H); JSON-pointer rule validation (H); research accepts-any-JSON (H); readable empty-as-success (H); plugin persists error records (H); search bad-query 500→400 + saved-search silent (H); HN empty-parse-as-success (M).
- **Wave 6 — Crawler correctness.** near-dup links never followed (H); robots await stalls loop (H); robots ignores Allow/wildcards (M); frontier seen-cap silent drop (M); crawl-delay re-churns frontier (M); revisit 404 permanent-gone (M).
- **Wave 7 — API hardening + resource bounds.** permissive CORS (H); Chrome launch holds global mutex (H); non-cursor list ignores `limit` DoS (M); `max_attempts` unbounded (M); `Last-Event-ID` overflow (M); `webhook_deliveries` unbounded (M); failed page.content leaks tab (M).
- **Wave 8 — Change-detection integrity + remaining scraper mediums.** homewyse free-text key + accumulation (H, if not folded into W4); HN upsert delisted (M); state-tax upsert snapshot (M); `detect_removed` empty→tombstone-all (M); `JsonFilter` Gte/Lte numeric (M); SimHash `DefaultHasher` instability (M); `duplicate_pairs` unbounded scan (M).
- **Wave 9+ — Duplication/consolidation tail (T10, ~18 refactor findings) + config/catalog/registry drift (T11) + Lows.** Lower risk, high maintainability payoff; run as dedicated refactor sessions (grants/census/trades shared helpers, list-handler macro, LRU/jitter/since-parser dedup, catalog cron/docs_url fixes, registry duplicate-id assertion).

---

## How this scan was run

- **Scanners:** `code_refactor` + `bug_hunter` role prompts (from Vibeman `src/lib/prompts/registry/agents/`), applied as a combined dual-lens per context.
- **Date:** 2026-07-14. **Scope:** all 21 contexts / 7 groups of the pumper context map (project `51ed5294-…`, branch `master`).
- **Method:** one `general-purpose` subagent per context (read-only), each read its scoped files in full + closely-related files to confirm, targeted 5 combined findings, wrote one structured report. Dispatched in waves of 8. Orchestrator read only the terse replies during scanning.
- **Convention-primed:** each subagent was given pumper's intentional patterns (fail-open workers, runtime sqlx, `ts()` ordering, attempt-fenced writes, metering seam, `sync_many` vs `upsert_many`, profiled-request cache bypass) to suppress false positives. Several agents explicitly dropped candidate findings after confirming they were intentional or unreachable.
- **Verification:** findings counted two ways (report `Total:` headers sum = per-finding `Severity:` bullets = 102). Baseline `cargo build` clean, 177 tests passing.
