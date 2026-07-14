# Refactor + Bug-Hunt — Fix Waves 1–7 (pumper, 2026-07-14)

> **29 findings closed in 22 atomic commits** across 7 themed waves + the deferred dataset-upsert atomicity fix.
> Severity closed: **all 4 Criticals + 17 Highs + 8 Mediums.** 73 findings remain open.
> Baseline preserved: `cargo build` clean, tests **177 → 184** (7 regression tests added), 0 warnings, 0 regressions throughout.
> Branch: `vibeman/refactor-bughunt-2026-07-14` (off `master`, not pushed).

## Wave 7 — API hardening + resource bounds: 2 Highs + 2 Mediums
- **CORS off by default** (`aa84250`, High) — was `CorsLayer::permissive()` (allow-all) over a mutating, unauthenticated API; now same-origin only, with a `[server] cors_allowed_origins` opt-in.
- **Chrome launch outside the global lock** (`b4526d0`, High) — `acquire()` no longer holds the holders mutex across `launch().await`, so one profile's cold start / crash-relaunch can't stall the whole render pool.
- **max_attempts clamped** (`aa84250`, Medium) — client-supplied `max_attempts` is bounded to [1, 20] on jobs/schedules/triggers (was unbounded → non-terminating retries).
- **Browser tab leak on content error** (`b4526d0`, Medium) — a failed `page.content()` now still aborts the drainer + closes the tab instead of leaking both.

**Deferred with reason (API):** non-cursor list branches ignore `limit` (Medium — a wide mechanical change across ~11 endpoints; better as a focused pass so each legacy branch converts to a capped paged read); `webhook_deliveries` unbounded growth (Medium — needs a purge method + a scheduler-tick caller, like `HttpCache::purge_expired`).

## Wave 6 — Crawler correctness: 1 High + 2 Mediums (`f4c6b42`)
- **Near-dup links** (High) — outbound links are now followed from near-duplicate pages too, not just kept ones; previously subtrees reachable only via a near-dup page (pagination/faceted nav) were silently under-crawled.
- **Frontier cap counter** (Medium) — URLs refused at the `MAX_FRONTIER` cap are now counted and surfaced as `stats.frontier_dropped` instead of being silently dropped.
- **robots Allow/wildcards** (Medium) — `RobotRules` now honors `Allow` directives and `*`/`$` wildcards with longest-match precedence (Google robots spec), matching path+query; was Disallow-prefix-only. Tests added.

**Deferred with reason (crawler):** robots.txt fetch awaited in the scheduling loop (High — needs concurrent robots prefetch / a core-loop restructure; the stall is bounded to once per new host and HTTP-timeout-capped); crawl-delay 200 ms re-churn (Medium — an optimization; correct today); revisit treats a single 404/410 as permanently gone (Medium — needs a gone-count/gone-since schema field to tell transient from dead).

## Wave 5 — Honest results (empty/garbage ≠ success): 6 Highs + 1 Medium
The dominant open theme. A scrape/query that fails or returns nothing must not report success.
- **eu-sedia drift guard** (`2a80c0c`) — positive `totalResults` with zero parsed rows now errors instead of returning `fetched: 0` as success.
- **hackernews** (`1742ce3`) — a 200 that parses to zero stories (drift/soft rate-limit) now errors.
- **readable** (`cce1ced`) — empty extraction now errors instead of returning an empty-but-ok 200.
- **plugin** (`daa0556`) — fetch/plugin `{error}` records are no longer upserted into the output dataset.
- **research** (`f64e5b1`) — the `json_schema` guardrail is now set, and `structured` only holds when the returned object matches the promised shape (else falls back to text).
- **extract json-pointer** (`ec71f42`) — a malformed pointer now fails `compile()` (like css/regex/xpath) instead of becoming a silent `Empty` miss.
- **search 400** (`eff7fcf`) — a malformed query returns HTTP 400 via a new `Error::BadRequest` variant, not a 500.

## Commits

| # | Commit | Findings closed | Severity | Files |
|---|---|---|---|---|
| 1 | `f9fd14b` | wasm-plugin-sandbox #1, #2 | **C** + H | `crates/engine-wasm/src/lib.rs` |
| 2 | `5158d60` | extraction-crawl-api-watch #1 | **C** | `crates/apps/extractor/src/lib.rs` |
| 3 | `db41471` | app-job-model #3 | M | `crates/core/src/app.rs` |
| 4 | `fe162a2` | us-grant-opportunities #1 | **C** | `crates/apps/grants-common/src/lib.rs` |
| 5 | `469c8ab` | czech-labour-market-mpsv #1 | **C** | `crates/apps/mpsv-vpm/src/lib.rs` |
| 6 | `c7f1d11` | engine-capability-traits #1, tiered-fetcher-politeness #1 | H + H | `cache.rs`, `engine.rs`, `engine-http/lib.rs`, `tests/cache.rs` |
| 7 | `50578b3` | live-events-webhooks #1, #2 | H + M | `crates/server/src/events.rs` |
| 8 | `573aa0c` | dataset-store-change-detection #1 | H | `crates/core/src/datasets.rs`, `tests/datasets.rs` |
| 9 | `93d0969` | us-trades-business-density-census #1, #2 | H + H | census-density, census-nonemp |
| 10 | `12bf8e3` | us-trades-wages-tax-valuation #2 | H | `crates/apps/homewyse-pricing/src/lib.rs` |
| 11 | `725e218` | czech-labour-market-mpsv #4 | M | `crates/apps/mpsv-vpm/src/lib.rs` |

Result: **all 4 Criticals + 8 Highs + 3 Mediums closed** (15 findings). 87 findings remain open.

## Deferred item resolved — dataset upsert atomicity (`573aa0c`, High)
The top follow-up from the Waves 1–3 checkpoint. `Datasets::upsert` ran its SELECT, record write, and revision append as three separate autocommit statements, so concurrent same-key writers corrupted the revision/diff chain or aborted the batch. The sequence now runs inside a `BEGIN IMMEDIATE` transaction on one pooled connection (writers serialize via the up-front write lock; `IMMEDIATE` avoids the `SQLITE_BUSY_SNAPSHOT` that a deferred read-then-write upgrade hits under WAL). `add_revision` is now generic over the executor. Regression test: 20 concurrent same-key upserts → a contiguous 1..=20 revision chain.

## Wave 4 — Scraper data-truth (3 Highs + 1 Medium)
- **Census jam/suppression sentinels** (`93d0969`). Negative jam sentinels (`-666666666`) were parsed as valid negatives and summed into national totals; suppressed cells became `0` yet counted as reported places (fabricated $0 markets). A shared `census_num` helper treats missing/non-numeric/negative as suppressed; rows with a suppressed primary metric are skipped, not fabricated.
- **homewyse validated prices** (`12bf8e3`). Records stored the raw `j.get("low")` after validating parsed copies, so string-quoted prices were dropped by the unified rollup. Now the validated numbers are persisted.
- **mpsv string-encoded stats** (`725e218`). `official_wage_index` used `as_f64()` only, dropping any benchmark row whose stat arrived as a string; a `wage_num` helper accepts numbers and Czech-formatted strings (whitespace thousands, decimal comma).

## Wave 1 — Crash & sandbox safety (all 4 Criticals)

1. **WASM guest-output DoS + resource caps** (`f9fd14b`). The plugin ABI packs `(out_ptr<<32)|out_len`; `out_len` is fully guest-controlled and the host allocated `vec![0u8; out_len]` (up to ~4 GiB) *before* any bounds check, aborting the process on OOM. The `[ptr, ptr+len)` range is now validated against the plugin's own linear-memory size first. The `StoreLimits` also only capped `memory_size`, letting a module exhaust host RAM at instantiation via huge tables/instances — now `memories`/`tables`/`table_elements`/`instances` are all bounded.
2. **Extractor path traversal** (`5158d60`). `read_source_body` joined `source.app` + `job_id` + `artifact_path` (all untrusted) into a path with no sanitization; `Path::join` lets an absolute/`..` segment escape the artifacts root → arbitrary server-file read into job output. Each component is now validated as a single safe path segment. The sibling `save_artifact` (`db41471`) got the same guard for param-composed names.
3. **grants `parse_date` panic** (`fe162a2`). The datetime fallback sliced `&s[..min(10)]` on **bytes**; a non-ASCII char (em-dash close-date cell) straddling byte 10 panicked and hard-failed the whole scrape. Now the date prefix is taken by splitting on the first space/`T` (char-safe). Regression test added.
4. **mpsv silent salary loss** (`469c8ab`). `is_monthly()` string-matched `"mesic"` against `typMzdy.id`, a codebook URI (`"TypMzdy/N"`) that never contains it — silently emptying the entire salary distribution while reporting success. The gate is removed; the monthly-wage fields within the sane band are the signal (no hourly fields exist). Regression test added; dead `typMzdy` field dropped.

## Wave 2 — Cache correctness (2 Highs)

6. **Cache identity + read staleness** (`c7f1d11`). `HttpCache::key` hashed only method+url+body, so requests differing in `headers` (Accept-Language content negotiation) or `proxy` (geo egress) collided and served the wrong body — key now includes sorted headers + proxy. And `HttpCache::get` returned any live entry regardless of the reader's freshness need, so a long-TTL writer defeated a short-TTL reader (stale content served as fresh); `get` now takes a `max_age` bound fed from `ttl_override`. Unit + integration tests added.

## Wave 3 — Concurrency & atomicity (1 High + 1 Medium)

7. **Event-bus emit ordering + replay overflow** (`50578b3`). `EventBus::emit` assigned the seq id and broadcast *outside* the ring lock, so concurrent emitters could buffer/send a higher id ahead of a lower one — corrupting order and firing false `reset` storms. Seq assignment, ring insert, and broadcast now all happen under the ring lock. Also `replay`'s `after + 1` overflowed on an adversarial `Last-Event-ID: u64::MAX` → `saturating_add`.

## Patterns established (for future audits)

1. **Byte-slicing UTF-8 by index panics.** `&s[..n]` is a landmine anywhere upstream text can be non-ASCII. Prefer `split`, `char_indices`, or `chars().take(n)`.
2. **Untrusted path components need per-segment validation.** `Path::join` silently escapes on an absolute or `..` component. Any path built from params/records must reject non-single-segments (guard once, reuse — crawl did, extractor/save_artifact didn't).
3. **Guest/host ABI must bound guest-supplied lengths before allocating.** Validate against the guest's own memory size, and cap *every* store-growable resource, not just linear memory.
4. **A cache key must include every response-varying input** (headers, proxy), and a cache read must honor the reader's freshness bound, not just the writer's TTL. Sort map-typed key inputs — `HashMap` order is nondeterministic.
5. **Assign-then-lock is a reorder race.** An id/seq assigned outside the lock that guards its buffer can be published out of order. Assign inside the lock.
6. **Silent-success on empty/garbage upstream** (the dominant open theme): a scrape that parses 0 rows from a positive-total response, or gates real data behind a never-matching string, reports success while emptying the dataset. Guard drift explicitly.

## What remains (see INDEX.md)

92 findings still open. Highest-value next waves:
- **Dataset `upsert` non-atomic RMW** (High, `datasets.rs`) — the top deferred item: SELECT→UPDATE/INSERT→add_revision run as separate autocommit statements; concurrent same-key writers corrupt the revision/diff chain or abort the batch. Needs a `BEGIN IMMEDIATE` transaction threaded through `add_revision` (executor refactor) + a concurrency test. Deferred deliberately — a rushed change to the core change-detection path is higher-risk than the bug.
- **Wave 4 (data-truth):** money_range greedy sweep, census negative sentinels + suppressed→0, homewyse raw priced values, EU decimal-comma 100× (`parse_first_number`), mpsv `official_wage_index` as_f64.
- **Wave 5 (honest results):** eu-sedia drift guard, JSON-pointer rule validation, research accepts-any-JSON, readable empty-as-success, plugin persists error records, search bad-query 500→400.
- **Wave 6 (crawler), Wave 7 (API hardening: CORS, unbounded list `limit`, Chrome-launch mutex), Wave 8 (change-detection integrity), Wave 9+ (duplication tail, ~18 refactor findings + config/registry drift).**
