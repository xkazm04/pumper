---
subject: software-engineering/convergence-loop-and-requeue
project: pumper
raised_by: intake intake-kube-0903 (peer comparison)
source: librarian/sources/2026-09-03-kube-rs.md
stage: the retry decision inside Storage::fail (crates/core/src/storage.rs:466-517) and its two callers in crates/server/src/worker.rs
size: 3 files / ~110 lines / M
status: accepted
---

## Why the scope implies it

`scope.does` names a *"durable SQLite job queue behind an HTTP API"*. The queue's single most consequential decision is not whether to retry — pumper answers that well, with a typed, exhaustive, test-pinned predicate — but **when**, and that decision is currently made by a function that has been given nothing to make it with.

`Storage::fail` computes `10 * 2^attempts` capped at 3600 seconds, plus up to 25% deterministic jitter (`crates\core\src\storage.rs:475-492`). Its inputs are the job id and the attempt number. The error arrives as `&str` (`:466`), for the row's `error` column, and is never consulted. Both callers pass it a rendered string: `worker.rs:1020` for a terminal failure through `fail_permanently`, `worker.rs:1027` and `:1044` for a retryable one through `fail`.

That means the ladder is identical for every retryable failure the runtime can produce. A `503` from a host that is genuinely down, a `429` that the HTTP engine has already read a `Retry-After: 600` from, a locked SQLite file, a panicking app and a browser that could not find Chrome all wait exactly 10, 20, 40 seconds. The 429 case is the expensive one and it is not hypothetical: `crates\engine-http\src\lib.rs:984-991` reads the server's stated delay properly, honours both RFC 7231 forms, treats it as a floor rather than a replacement, and then `capped_retry_sleep` (`:925-930`) correctly *gives up* when the stated wait will not fit the fetch budget — minting a `budget_exhausted` that is deliberately retryable because *"this host was slow this time"* (`:945-949`). The job level then re-queues it in 10 seconds, at which point the whole fetch runs again into the same rate limit the server had asked us to wait ten minutes for. The stated delay is read, respected, and then thrown away one layer up.

There is a second, quieter cost. `Error::Config` and `Error::Plugin{Unknown|Disabled|MissingExport}` are now correctly identified as *ours* by `is_router_failure()` (`crates\core\src\error.rs:453-493`, landed yesterday) and the fetch ladder stops on them (`crates\core\src\fetcher.rs:1174`). But the job ladder does not: an ours-error is retryable and rides 10/20/40 producing the identical sentence three times, which is precisely the amplification the router-failure axis was introduced to end — ended at the tier ladder and not at the job ladder.

The peer's answer is structural and it is the one the corpus is forging right now. In `kube-runtime`, the requeue decision is a **parameter of the loop, not a behaviour of it**: `error_policy` is an argument to `applier` (`C:/t/kube/kube-runtime/src/controller/mod.rs:401`), it receives the object, the reconciler's *typed* error and the context (`:466-479`), and it returns an `Action` — `requeue(duration)` or `await_change()` (`:85-120`) — which the runtime then obeys. The subject being forged for this in the registry, `operations/control-plane-operations/convergence-loop-and-requeue`, carries it as the technique `error-policy-as-a-separate-function`; the subject was written in the same intake run as this proposal, so the golden path and this direction are two views of one finding rather than an import.

## What the first context contains

A **requeue policy function** that sees the error, and the one place the store calls it. One concept, one new pure function, no new module.

**The function.** A pure `fn requeue_after(error: &Error, attempts: i64, stated: Option<Duration>) -> Requeue` in `crates/core`, where `Requeue` is a small enum — `After(Duration)` / `Never` — and the default arm is byte-identical to today's `10 * 2^attempts` capped at 3600 with the existing deterministic jitter, so a failure the policy has nothing to say about behaves exactly as it does now. Pure, `now`-free and jitter-seeded from its arguments, so it is unit-testable against a table the way `decide` is (`crates\server\src\scheduler.rs:800-852` is the model, including its stated reason for being pure).

**What the policy may say.** Three things, and no more in the first context:
- A **stated wait outranks the ladder.** When the failure carries a server-stated delay, `After(max(ladder, stated))` — the same floor rule `retry_delay` already applies inside the engine (`crates\engine-http\src\lib.rs:984-991`), lifted one layer so the job honours what the fetch already learned.
- An **ours-error does not ride the ladder.** `Requeue::Never` when `error.is_router_failure()`, which routes it to `fail_permanently` — closing the amplification at the job level that `fetcher.rs:1174` closed at the tier level, using the predicate that already exists.
- **Store contention is not an engine failure.** `is_store_contention()` (`error.rs:518-526`) already separates a lock wait from a defect; a busy store deserves a short bounded wait, not the engine ladder, because the remedy is a different one and the recovery is typically sub-second.

**Carrying the stated wait.** The one piece of plumbing: `budget_exhausted` (`crates\engine-http\src\lib.rs:945-968`) knows the stated delay it could not fit and discards it into prose. It becomes the carrier — a field on the error, in the shape `Error::Claude` already uses to carry `ClaudeSpend` past a failure (`error.rs:159-164`), for exactly the same reason: *"money the CLI reports on its way out is real money, and a `String`-only variant had nowhere to put it."* A server-stated wait the ladder is about to override is the same kind of fact.

**The call site.** `Storage::fail` (`storage.rs:466-517`) takes the `Error` rather than a `&str` — it already receives one at both callers and renders it there (`worker.rs:1027`, `:1044`) — calls `requeue_after`, and writes `available_at` from the answer. The `(status, attempts)` fence, the stale-write discard and the `attempts < max_attempts` threshold are untouched.

**What it must NOT absorb.** Not `is_terminal_for_job` — that axis decides *whether*, this one decides *when*, and collapsing them loses the cell that matters (`Error::Config` is retryable-in-principle and pointless-to-retry-here, which is two facts). Not kube's shape wholesale: an `Action` returned by user code is wrong here, because the queue is durable and the store must own the write; the policy is a pure function the store calls, not a callback an app supplies. Not the fetch ladder's own retry loop (`crates\engine-http\src\lib.rs:557-624`), which stays exactly as it is — this is strictly the job level. Not the webhook delivery ladder in `fail_delivery` (`storage.rs:1879-1925`), which has its own configured `backoff_secs` array and a different consumer contract; the reset-after-sustained-health finding there is a separate, later direction. Not `max_attempts`, priority, or the aging coefficient.

## The measurable

**Job retries that re-fail with the same server-stated rate limit: today the full ladder, target 0.**

Measured in `crates\server\src\e2e\` on the trait-level fake seam (`crates\core\src\testing.rs`). Wire an engine that returns a `429` with `Retry-After: 600` on every call; enqueue a job with `max_attempts = 3`. Today the job runs the fetch ladder, exhausts its fetch budget, re-queues at 10 s, and repeats — three full engine invocations for one rate limit. After: one, with `available_at` at least 600 seconds out. Assert the fake's call count and the row's `available_at` delta.

**Second number — the ours-error at the job level.** Same harness, an engine returning `Error::Config("missing key")`, `max_attempts = 3`. Assert `attempts` on the finished row: today 3, after 1. This is the direct continuation of the multi-provider-gateway-plane direction accepted on 2026-09-03, which closed the same amplification one layer down; the pair of numbers together is the whole argument for the axis.

**Third number — the default arm did not move.** A table test over one instance of every variant the policy has no opinion about, asserting the delay is bit-identical to `10 * 2^attempts` plus the existing seeded jitter. If this test is hard to write, the policy has absorbed the ladder instead of wrapping it.

**Before acceptance:** the base rate. Count failed job rows in the retained history whose `error` text contains a `429`, a `503` with a stated wait, or a `config:`/`plugin:` prefix, and divide by all failed rows. If that share is under a few percent, the honest verdict is `deferred` with "a stated-wait or ours-error failure is observed re-riding the job ladder" as the return condition.

## What would make this wrong

**If the stated wait cannot be carried without a wide change.** The plumbing assumes `budget_exhausted` is the only site that knows a stated delay it could not honour. If the delay is also learned in the browser engine, the archive engine and the governor's 429 penalty path (`crates\engine-http\src\lib.rs:618`, `:1151`), then either three carriers appear — which is exactly the "one carrier, never two" rule `is_router_failure`'s doc states at `error.rs:434-438` — or the field has to live on a variant all three can raise. If neither is clean, ship the ours-error arm alone: it needs no carrier at all, since the predicate already exists.

**If `Requeue::Never` on an ours-error takes retries from a transient.** `Error::Config` reaching a *running* ladder is meant to be impossible — `Config::validate()` catches 29 misconfigurations at boot (`crates\core\src\config.rs:742`) — so the variants that reach here are the ones it cannot catch, and at least one of them may be genuinely transient (a config file being rewritten under a running process). The failure direction is the bad one: a transient marked `Never` loses a job silently. The mitigation is that `is_router_failure` is already exhaustive with a stated safe default, but if any of its `true` arms turns out to have a transient producer, the correct fix is at the variant (retype at the seam, as `Error::Profile` already does — `error.rs:397-405`) and not by weakening the predicate.

**If it makes the ladder unpredictable to an operator.** Today the answer to "when will this retry" is one formula anyone can compute. After, it is a function of the error. If the job row does not say *why* it got the delay it got, the change trades a predictable system for a smarter opaque one, which is a bad trade for a service one person operates. The row must record the policy's reason alongside `available_at`, in the same spirit as `RECOVERY_REASON_LEASE` (`storage.rs:36-37`) — and if that field is dropped for expedience during implementation, the direction should be re-reviewed rather than shipped.

**If the store gains a dependency it should not have.** `requeue_after` reads `Error`, which `crates/core` owns, so there is no cycle. But if the policy is tempted to consult config (`[worker]` knobs), the store's `fail` would need a config handle it does not have today. Keep the policy's inputs to the three named arguments; a configurable ladder is a separate decision and a bigger one.
