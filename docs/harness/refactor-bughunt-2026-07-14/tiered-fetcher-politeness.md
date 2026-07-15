# Tiered Fetcher & Politeness — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 2, Medium: 2, Low: 1)
> Files scanned: `crates/core/src/fetcher.rs`, `crates/core/src/governor.rs`, `crates/core/src/cache.rs` (scoped); confirmed against `crates/engine-http/src/lib.rs`, `crates/core/src/config.rs`, `crates/core/src/tiers.rs`, `crates/apps/watch/src/lib.rs`, `crates/server/src/state.rs`, `crates/core/tests/cache.rs`, `.perfect/Perfect/directions/fetch-no-cache-ttl.md`

## 1. `ttl_override` never caps read staleness — a long-TTL entry defeats a short-TTL reader
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / edge-case
- **File**: `crates/core/src/cache.rs:52-73` (read path); trigger at `crates/engine-http/src/lib.rs:545-566`; contract at `crates/core/src/fetcher.rs:82-86`
- **Scenario**: `HttpCache::get(key)` selects `WHERE key = ?1 AND expires_at > now` and takes **no** max-age / freshness argument — the caller's `ttl_override` is applied only on `put` (engine-http `fetch()` line 562-566), never on `get`. Concrete repro from the `watch` app's own docs ("useful when several watches share one hot endpoint", `apps/watch/src/lib.rs:33-35,63-66`): watch A fetches `https://host/x` with `cache_ttl_secs=3600` → stores an entry with `expires_at = now+3600`. 50 minutes later watch B fetches the same URL with `cache_ttl_secs=60` (it wants content no older than 60 s). B's `get` finds A's entry (`expires_at` still in the future) and returns a **~50-minute-stale body** as if current.
- **Root cause**: `ttl_override` was implemented as "how long this write stays fresh for *future* readers" (see shipped direction `fetch-no-cache-ttl.md` acceptance: "write behaves per ttl_override"), but the feature's own motivation is "caps staleness." Freshness is a property of the *read*, yet `get` has no way to express "reject anything older than N seconds," so a pre-existing longer-lived entry silently wins.
- **Impact**: wrong result — a monitor that asked to cap staleness silently consumes far-staler content, so a change that happened >TTL-ago but <stored-expiry-ago is never detected (the entire point of `watch` is change detection). Only `no_cache` actually guarantees freshness today; `cache_ttl_secs` does not when the URL is shared.
- **Fix sketch**: give `get` a `max_age`/`fresh_after` bound derived from the request's `ttl_override` (e.g. `SELECT ... AND expires_at > now AND created_at >= (now - ttl_override)`), threading the override through `HttpEngine::fetch`'s read side, not just the write. `created_at` is already stored.

## 2. `AutoWithResearch` discards an already-fetched lower-tier body when the Claude tier errors
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / no-graceful-degradation
- **File**: `crates/core/src/fetcher.rs:420-461` (Claude tier), with the dropped body at `379-401` (browser thin branch)
- **Scenario**: strategy `AutoWithResearch`. HTTP comes back thin, browser renders a **real but sub-threshold** page (e.g. 240 chars, `min_content_chars` default 250) → browser thin branch pushes an escalation line and lets `page.html` drop out of scope. The Claude tier then runs `let out = self.claude.research(research).await?;` — the `?` propagates any error (budget ceiling hit via `max_budget_usd`, Claude CLI crash, rate limit, timeout). The whole `fetch` returns `Err`, and the 240-char browser body that was already in hand is gone.
- **Root cause**: the escalation pipeline treats each lower tier's output as disposable once it decides to escalate, and the top tier has no fallback: a transient failure of the *most expensive, least reliable* tier is allowed to nuke a fetch that already produced usable (if thin) content. There's no "return the best result so far" path.
- **Impact**: reliability failure / content loss — a recoverable partial success is converted into a hard error on a plausible, intermittent condition (Claude budget/CLI failures are routine), so a research-augmented monitor gets nothing instead of the thin body it could have fingerprinted or alerted on.
- **Fix sketch**: on Claude-tier error, instead of `?`, push an `Error` `TierTrace` and fall back to the best lower-tier body captured so far (retain the browser/HTTP `html`+`markdown` in locals rather than dropping them), returning that with the escalation trail; only return the exhausted-tiers error when no tier produced any body.

## 3. Governor `acquire` advances `next_slot` before awaiting — a cancelled fetch leaks a phantom reservation
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: race-condition / cancellation
- **File**: `crates/core/src/governor.rs:105-126`
- **Scenario**: `acquire` reserves its slot **synchronously** (`entry.next_slot = start + interval`) and only then `sleep_until(wake).await`s. In this platform fetch futures are routinely cancelled: the worker's per-job `job_timeout_secs`, the reaper re-queuing a stale job, and `shutdown_drain_secs` all drop in-flight job futures. If a job is dropped while parked in `acquire`'s `sleep_until`, the reservation stays committed: `next_slot` has been pushed forward by one `interval` for a request that never went out.
- **Root cause**: the slot is claimed at reservation time, not at send time, and there is no drop-guard/rollback. The bucket accounts for intended requests, not completed ones, so cancellations accumulate as spacing debt on that host.
- **Impact**: over-throttling — after a burst of cancellations against one host, subsequent real requests wait behind `next_slot` values that correspond to requests that never happened. Self-healing (drains as real requests catch up) and bounded per cancellation, so not a hang, but it silently slows a host under timeout/shutdown churn.
- **Fix sketch**: acceptable to document as a known minor cost; if worth fixing, wrap the reservation in a drop guard that rolls `next_slot` back toward `now` when the future is dropped before the sleep completes, or reserve lazily. Low urgency given the self-healing bound.

## 4. Near-duplicate ~80-line tier-attempt blocks (HTTP vs browser)
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/core/src/fetcher.rs:240-334` (HTTP tier) and `336-418` (browser tier)
- **Scenario**: the two tier arms share almost the entire shape: start an `Instant`, `match` engine `Ok`/`Err`, compute `needs_count` → optional `wall` → optional `markdown` → `text_len` → `enough`, then either push a `TierVerdict::Ok` `TierTrace` and `return outcome(...)`, or push an escalation string + a non-winner `TierTrace`, plus a strategy-specific `Err` early-return and an `Error`-verdict trace. Six `TierTrace { ... }` struct literals are spelled out across the file, most fields defaulted. They differ only in: the engine call, the `needs_count` predicate, the wall detector (`http_bot_wall(status, body)` vs `challenge_marker(html)`), whether `http_status`/`cache_hit` are `Some`, and the "return anyway" condition.
- **Root cause**: escalation logic was written inline per tier rather than factored into a shared "evaluate one tier attempt → (winner? trace + body)" helper.
- **Impact**: wasted maintenance / drift risk — any change to trace semantics, thinness rules, or escalation-string format must be made twice and kept in sync (the two arms already diverge subtly, e.g. the return condition polarity), which is exactly where a future bug hides.
- **Fix sketch**: extract a `push_trace(&mut trace, tier, verdict, status, chars, cache_hit, latency, cost, detail)` helper and a small `evaluate(tier, body, status, needs_count, min_chars)` returning `(enough, wall, markdown, text_len)`, so each arm shrinks to: call engine → evaluate → branch. No behavior change.

## 5. Duplicated deterministic-LCG jitter (governor vs HTTP retry backoff)
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/core/src/governor.rs:229-238` and `crates/engine-http/src/lib.rs:494-504`
- **Scenario**: both compute jitter with the identical LCG scramble — same magic constants `6364136223846793005` and `1442695040888963407`, the same `(scrambled >> 33) as f64 / (1u64 << 31) as f64` fraction, and the same `frac.min(1.0)`. The engine-http comment even notes it "mirrors the governor's approach." Two independent copies of the same numeric recipe.
- **Root cause**: the "no-`rand`, deterministic fraction from a seed" idiom was copy-pasted into a second crate instead of shared.
- **Impact**: minor maintenance drift — a fix to the distribution (the `>> 33 / 2^31` only yields 31 bits of a 64-bit scramble) or the constants would need to be applied in two places.
- **Fix sketch**: hoist a `pub(crate) fn lcg_frac(seed: u64) -> f64` (or a `deterministic_jitter(seed, span) -> Duration`) into `pumper_core` and call it from both; keeps the deterministic/resume-safe property while single-sourcing the recipe.
