---
subject: software-engineering/concurrency-guards
project: pumper
raised_by: intake intake-kube-0903 (peer comparison)
source: librarian/sources/2026-09-03-kube-rs.md
stage: the claim statement in crates/core/src/storage.rs and the worker's exclusion list in crates/server/src/worker.rs
size: 3 files / ~90 lines / M
status: accepted
---

## Why the scope implies it

`scope.does` opens with *"durable SQLite job queue behind an HTTP API"*. A durable queue's contract is not only that work is not lost; it is that the queue decides **what may run at the same time**, because the caller cannot. pumper decides that today at exactly one granularity — the app — and the decision is made for a different reason than exclusion.

`blocked_apps` (`crates\server\src\worker.rs:517-530`) builds the list of apps at or above their concurrency limit, and `claim_next` renders it as `AND app NOT IN (…)` inside the claim statement (`crates\core\src\storage.rs:393-414`). The doc says what it is for, and it is not exclusion: *"Apps listed in `blocked` are skipped, which is how the worker enforces per-app concurrency limits (fairness across many apps' queues)."* Fairness across queues is the right reason for that list. It means that within one app's budget, two jobs that write the same dataset rows claim two slots and run at once, and nothing anywhere refuses them.

pumper already refuses the *enqueue* half of this, and refuses it well. `enqueue_dedup` short-circuits on an `idempotency_key` and closes the concurrent-insert race by re-reading the winner after a unique-key violation (`storage.rs:310-315`, `:358-366`), and the two trigger fan-in paths do not wait for a caller to supply a key — they synthesise one: `idempotency_key(trigger_id, source_job_id)` → `trig:{trigger}:{source}` (`crates\server\src\triggers.rs:552-554`), consumed at `:1249,1259` and `:1387,1395`, with a redelivery reported as `Ok((_, false))` rather than becoming a second job. What dedup cannot do is anything about two rows that are **already in the table**: a scheduled run and a manual re-run of the same app, a hop from one trigger and a hop from another onto the same target, a job re-queued by the lease reaper while its predecessor's task is still finishing.

The peer draws the two limits apart deliberately, and states that they are independent. `Scheduler` parks a message whose equal is currently executing in a `pending` set instead of running it (`C:/t/kube/kube-runtime/src/scheduler.rs:114-139`), while `Runner` caps total parallel executions with a separate number (`kube-runtime/src/controller/runner.rs:20-25,44-57`) — and the builder's doc says the guarantee out loud: *"Note that despite concurrency, a controller never schedules concurrent reconciles on the same object"* (`controller/mod.rs:605-617`). That guarantee is what lets a kube reconciler be written without a lock: no reconciler has to remember to take one, because the queue took it.

Two things in pumper already almost say this. The scheduler's overlap guard refuses to stack a second run of a schedule while the previous one is queued or running (`crates\server\src\scheduler.rs:381-384`, `run_holds_slot` at `:697-699`) — per-key exclusion, for exactly one key shape, implemented once. And the `(status, attempts)` fence on `complete`/`fail`/`heartbeat` (`storage.rs:456-460`, `:472-474`, `:1015-1027`) already proves the store can hold a per-row ownership claim; it just holds it for the *attempt*, not for the target.

## What the first context contains

A **target key on the job row**, and the two places the claim statement must consult it. One column, one predicate, no new module.

**The key.** A nullable `target_key TEXT` on `jobs`, written at enqueue by the doors that know what a job acts on. It is not a hash of `params` — that is the wrong granularity in both directions (two jobs with different pagination params act on one dataset; two jobs with identical params under different `budget_usd` do not conflict). It is what the app declares: a `ScrapeApp::target_key(&params) -> Option<String>` default method returning `None`, so an app opts in the way `fetch_bytes` and `transact` opt in on the engine traits (`crates\core\src\engine.rs:1191-1196`, `:1232-1234`) — by overriding a default that refuses. An app that returns `None` behaves exactly as today.

**Consumer one — the claim.** `claim_next` (`storage.rs:393-414`) gains a second exclusion clause alongside the app one: `AND (target_key IS NULL OR target_key NOT IN (SELECT target_key FROM jobs WHERE status = 'running' AND target_key IS NOT NULL))`. The whole guard lives inside the one atomic `UPDATE … WHERE id = (SELECT …) RETURNING` that already makes a claim exclusive, so there is no window between the check and the claim, and no second process can disagree with the first. This is the load-bearing sentence of the proposal: the exclusion must be in the claim's `SELECT`, not in the worker, or it is advisory.

**Consumer two — the worker's non-starvation.** A job held out by the target exclusion is not blocked forever: it is simply not the row the claim picked, and it will be picked on the tick after the holder finishes, which the worker already wakes for (`worker.rs:97-98`, *"A finished job may unblock a previously-capped app"* — the same `notify_one` covers this case unchanged).

**Consumer three — the operator's answer.** `GET /jobs` and the schedule health derivation gain the same vocabulary the overlap guard already has: a queued job whose target is held reads as *held*, not as *late*. `scheduler.rs`'s `StepOutcome::Held` (`:176-178`) is the existing precedent for the word.

**What it must NOT absorb.** Not the per-app cap — that is fairness and stays exactly as it is, with its own list and its own reason. Not `idempotency_key`, which answers a different question (*should this request create work at all*) and must keep answering it: the two coexist, and the cells prove it — a trigger redelivery is deduped and never becomes a row, while a scheduled run and a manual re-run are both legitimate rows that must not overlap. Not the schedule overlap guard, which is a *pre-enqueue* refusal keyed on `schedule_id` and should stay; if this lands, the guard's relationship to it is worth one comment, not a merge. Not a lease or an election — the exclusion is a property of the claim, not of the process, which is precisely why a second `pumper` process would inherit it for free while inheriting nothing from a worker-side check. Not `crates/apps/**` beyond a `target_key` override per app that wants one; the first context ships the mechanism plus overrides for the two apps where a double-write is already plausible.

## The measurable

**Concurrent runs against one target: today unbounded within the app's cap, target 1.**

Measured by a new case under `crates\server\src\e2e\`, built on the trait-level fake seam in `crates\core\src\testing.rs`. Enqueue two jobs of one app with a shared `target_key` through two doors that legitimately skip dedup — the cron fire path (`scheduler.rs:412`, plain `enqueue`) and `POST /apps/{name}/jobs` with no `idempotency_key` (`crates\server\src\routes\jobs.rs:167`) — with `[worker] concurrency = 4` and no per-app limit, then drive both through `worker::run_one` (`worker.rs:195-201`). Assert the counting fake's concurrent-call high-water mark and the overlap of `started_at`/`finished_at` in the two job rows. Today: 2. After: 1.

**The paired assertion, which is what keeps the fix from becoming the per-app cap in disguise:** two jobs of the same app with *different* `target_key`s must still run concurrently, and one job with `target_key IS NULL` must never be held by anything. If either fails, the exclusion has widened into a serializer and the throughput cost is real rather than notional.

**Second number, for the operator:** the count of queued jobs whose `available_at` has passed and that are not running. Today that number means "the worker is behind"; after, it separates into *behind* and *held*, and only the first is a capacity finding.

**Third number, before acceptance:** how often the situation actually occurs. `SELECT COUNT(*) FROM jobs a JOIN jobs b ON a.app = b.app AND a.id < b.id WHERE a.started_at < b.finished_at AND b.started_at < a.finished_at` over the retained history, grouped by app, is the base rate of *any* same-app overlap; the share of those whose params name the same source is the base rate of the defect. **This should be checked before the proposal is accepted.**

## What would make this wrong

**If no app can name a target.** The whole design assumes the unit of conflict is expressible as a short string from `params`. For the grants apps and the market-data apps it plainly is (a source id, a dataset scope). For `web-crawler` or `agentic-research` it may not be — a crawl's conflict surface is the set of URLs it will discover, which is not knowable at enqueue. If more than half the apps cannot implement `target_key` honestly, the column is a partial guard that reads as a total one, which is worse than no guard, and the correct outcome is `deferred` with "two apps have implemented `target_key` in a branch" as the return condition.

**If the overlap has never happened.** This is amplification-free machinery: unlike the router-failure axis, it does not save any work, it only prevents a class of wrong write. If the third number above returns zero same-source overlaps across the retained history, the honest verdict is `deferred`, and the return condition is one observed double-write — a dataset row whose provenance shows two job ids at the same instant.

**If the exclusion serializes a legitimate parallel shape.** A dataset that is written by page, by many jobs, on purpose, would be serialized to one writer by a key naming the dataset. The failure direction matters and it is the bad one: an over-broad key costs throughput silently and continuously, while an absent key costs a rare double-write. The mitigation is that `target_key` defaults to `None` and every override is a deliberate per-app decision with the parallel shape in front of the author — but if the first two overrides turn out to need per-page keys to stay parallel, the granularity is wrong and the design should be reconsidered before spreading.

**If the claim statement gets slower.** The added `NOT IN (SELECT …)` runs on every claim, on a table that also holds every finished job. Without an index on `(status, target_key)` this turns an indexed point claim into a scan under load, and the queue's own throughput is the thing being protected. If the claim's p95 in `crates\core\tests\store_instrument_chokepoint.rs`'s harness moves materially with the index in place, the predicate belongs in a small in-memory set maintained beside `blocked_apps` — accepting that it is then per-process and stops working the day a second process exists, which is a trade to make explicitly rather than to discover.
