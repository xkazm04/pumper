# Perf-Feature Scan — Fix Wave 3: Metering & Politeness Integrity

> 4 commits, 4 findings closed (theme A: the platform's own controls silently
> not applying on its highest-volume paths).
> Baseline preserved: build clean, tests **192 → 211** (+19, all new; 0 regressions).
> Branch `vibeman/perf-feature-2026-07-16`, off `master` @ `8753dd3`. Not pushed.

## Why this wave first

The user picked Wave 3. Its four findings share one shape: a guard or accounting
seam wired at the wrong layer, so the platform's own controls (budget ceilings,
cost ledger, tier learning, politeness) silently don't apply on the paths that
generate the most traffic. Fixing them is correctness of the system's own
promises, not new capability.

## Commits

| # | Commit | Finding | Severity | Files |
|---|---|---|---|---|
| 1 | `bf56ebc` | app-job-model #3 — unenforced config invariants | Medium | `core/src/config.rs` |
| 2 | `7a66236` | app-job-model #2 — O(n²) budget re-aggregate | Medium | `core/src/{costs,app,lib}.rs`, `server/src/worker.rs`, extractor test |
| 3 | `8ba3d7d` | app-job-model #1 — crawler bypasses metering | High | `apps/crawl/src/lib.rs` |
| 4 | `11288e6` | tiered-fetcher #1 — governor skips the browser tier | High | `core/src/{fetcher,governor}.rs`, `server/src/state.rs`, extractor test |

## What was fixed

1. **Config invariants enforced (`Config::validate`).** `stale_after_secs` must
   exceed `heartbeat_secs` was a doc-comment, not a check — so `heartbeat_secs=300`
   with the default `stale_after_secs=120` made the reaper treat *every healthy
   job* as hung: re-queued, restarted, reaped, until permanent failure, with
   nothing in the logs pointing at the config. Now rejected at load, along with
   `job_timeout_secs<=stale_after_secs`, `concurrency==0`, and
   `governor.penalty_cap<base`. `0` stays a disable switch, so each rule only
   binds when its features are on. A test guards the repo's own `config.toml`.

2. **Budget check backed by a running total, not a per-call `SELECT SUM`.**
   `remaining_budget_usd` ran on every metered call and re-summed the job's whole
   cost history — O(n) per call, O(n²) per job. New `costs::SpentTotal`
   (`AtomicU64`-backed `f64`) is seeded once from the ledger at context
   construction (so retried jobs count prior spend) and advanced per metered
   write; the ledger stays authoritative. `add()` drops non-finite/≤0 deltas so a
   bad engine cost can't NaN-poison the ceiling. Also extracted the cost/tier
   side-effects out of `fetch` into public `AppContext::meter` / `::learn_tier`
   seams (one implementation, reused by `fetch`/`research`, called by commit 3).

3. **Crawler fetches are now metered and train the tier router.** The crawl app
   drove `ctx.engines.http` raw (it owns its own concurrency/robots/frontier), so
   the platform's highest-volume path recorded zero cost events and taught the
   tier router nothing. A `MeteringHttpClient` decorator tallies per-host outcomes;
   the app flushes them through `meter`/`learn_tier` after the crawl in O(hosts)
   writes — deliberately not per-fetch, which would re-create the write contention
   commit 2 just removed. The high-value half is tier learning: the crawl's
   per-host bot-wall/failure signal now trains the router other apps consult.

4. **The browser tier is now governed.** Politeness `acquire`/`penalize`/`reward`
   lived only inside `HttpEngine::send`, so browser renders had no per-host
   spacing and no adaptive backoff — and the tier router pins repeatedly-blocked
   hosts *to* the browser tier, so the hosts already hostile to us got unlimited
   renders. The Fetcher now shares the HTTP engine's `Arc<Governor>` and governs
   the browser tier at the escalation seam: `acquire` before each render,
   `penalize` on a browser Blocked verdict, `reward` on a healthy render. (Option
   (a) — move the governor to the Fetcher and drop it from HttpEngine — was
   rejected: it would strip politeness from the crawler's raw-HTTP path.)

## Verification

| Gate | Before wave | After wave |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 192 / 0 | 211 / 0 |

New tests: 8 config-validate rules + disable paths + repo-config guard; 5
`SpentTotal` (seed/accumulate/concurrent-CAS/NaN-guard); 3 `host_of`; 2 browser-
tier governor (penalize-on-wall, reward-on-success).

## Patterns established (catalogue additions)

1. **Guard-at-the-wrong-seam.** A control (budget, cost ledger, politeness, tier
   learning) wired inside one engine/method silently exempts every caller that
   reaches the resource another way. When auditing a "shipped" control, grep for
   *all* call sites of the underlying resource, not just the blessed wrapper — the
   raw path is where the control leaks. (Governor-in-HttpEngine, metering-in-fetch
   both leaked to the crawler; the ledger leaked to the crawler too.)
2. **Doc-comment invariants rot into outages.** "Must exceed X" in a doc-comment
   with no `validate()` is a latent config footgun. Promote load-bearing
   invariants to a checked `validate()` that names the offending keys.
3. **Aggregate-after, don't write-per-item, on hot paths.** When a raw high-volume
   path needs accounting, tally in memory and flush O(distinct) after, rather than
   one DB write per event — otherwise you re-introduce the single-writer contention
   you were trying to measure.
4. **Bit-cast f64 in an AtomicU64** is the lock-free running-total pattern; guard
   `add()` against non-finite/≤0 so a bad input can't poison a comparison.

## What remains (per the INDEX)

Wave 3 is complete. Highest-value next waves per the suggested split:
- **Wave 1 — Grants coverage & truth** (5, incl. the one Critical: eu-sedia→unified;
  user pre-decided EUR budgets map to `Null`). The `closing-soon` wrong-column
  ordering (currently filed under Wave 7/G) is a truth bug worth promoting here.
- **Wave 2 — Write amplification** (5): full-dataset reindex per job grows forever;
  `upsert_many`/`detect_removed` per-record transactions; quadratic crawl checkpoint.

Also standalone (out-of-lens, bug-hunter shape): the crawler's `artifact_name`
uses `stats.kept`, which restarts at 0 on checkpoint resume — a resumed crawl
overwrites prior `page-NNNN.html` files. Data-integrity; fix regardless of wave.
