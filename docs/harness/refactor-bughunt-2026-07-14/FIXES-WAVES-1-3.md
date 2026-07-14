# Refactor + Bug-Hunt — Fix Waves 1–3 (pumper, 2026-07-14)

> 8 findings closed in 7 atomic commits across 3 themed waves.
> Baseline preserved: `cargo build` clean, tests **177 → 179** (2 regression tests added), 0 warnings throughout.
> Branch: `vibeman/refactor-bughunt-2026-07-14` (off `master`, not pushed).

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

Result: **all 4 Criticals + 4 Highs + 2 Mediums closed** (10 findings).

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
