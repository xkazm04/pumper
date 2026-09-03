# pumper vs. `kube-rs/kube` — structural peer comparison (Rust craft and control-loop design)

- **Source**: `kube-rs/kube`, clone `C:/t/kube`, pinned `7a4641d4cc2f693b2dee97b9fc15fadb96d7f62e`
- **Design record**: `librarian/sources/2026-09-03-kube-rs.md` (intake run `intake-kube-0903`)
- **Dimension**: (a) Rust coding expertise and control-loop design. pumper is **not** a peer on (b) cluster operations — it ships no deployment artifact of any kind.
- **Why this peer**: `kube-runtime` is a triggers → scheduler → runner → reconciler pipeline with per-key exclusion, a global concurrency cap, delayed requeue and an error policy. pumper is a triggers → claim → worker → app pipeline with per-app exclusion, a global concurrency cap, delayed requeue and a terminal/retryable fork. **Same machine, opposite substrate**: kube's queue is a `DelayQueue` in memory and its source of truth is somebody else's server; pumper's queue is a SQLite table and its store *is* the truth. Almost every verdict below turns on that one difference, and it is why `keep ours` is the largest class.
- **Verdicts** come from the closed set `adopt` / `adapt` / `keep ours` / `different forces`. Every one carries its reason.

**Verdict tally: 34 points — 7 `adopt`, 8 `adapt`, 15 `keep ours`, 4 `different forces`.**

**Headline: the design record's Study-1 seeds are directionally right and wrong in five places, all corrected below** — dedup is not purely client-supplied, because the trigger fan-in synthesises its own key, and the real gap is *in-flight exclusion* rather than enqueue dedup (§2.1, §2.3); the scheduler's outcome enum has six variants, not four (§4.5); the overlap guard is at `scheduler.rs:381-384`, not `:39-40` (§4.6); the terminal classification is test-pinned at `error.rs:716-933`, not `:637-837`, and the load-bearing part is the *exhaustive inventory guard* at `:868-933` (§1.3); and pumper's `#[ignore]` discipline is not a gap to adopt but the strongest artifact either tree has on that axis (§7.2).

---

## 1. Error design

**1.1 — One enum per subsystem vs. one enum per workspace.**
kube: five error enums, each scoped to the layer that raises it — `watcher::Error` (`C:/t/kube/kube-runtime/src/watcher.rs:21-48`), `controller::Error` (`controller/mod.rs:62-83`), `finalizer::Error` (`finalizer.rs:14-54`), `runner::Error` (`controller/runner.rs:14-18`), `kube_client::Error`. A caller at any layer matches only on variants that layer can produce.
pumper: one `Error` with 18 variants for the whole workspace (`C:\Users\kazda\kiro\pumper\crates\core\src\error.rs:153-270`), shared by `core`, `server`, the six engine crates and every app.
**Verdict: `adapt`.** The split kube buys with five enums, pumper buys differently and mostly gets: the classification questions a caller actually asks are answered by predicates on the whole enum (§1.3, §1.4) rather than by which enum arrived. What is genuinely lost is narrowing — `crates/engine-http` can return `Error::Plugin`, and nothing in the type system says it cannot. The cheap half is a per-crate `Result` alias with a `#[non_exhaustive]` sub-enum for the engines; the expensive half (five enums, five `From` impls) buys less here than it does in a library with external implementors.

**1.2 — `#[source]` on every variant vs. a `format!` at the raise site.**
kube: every non-leaf variant carries `#[source]` — `watcher::Error`'s five (`watcher.rs:23,27,31,35`), `controller::Error`'s three (`controller/mod.rs:74,79,83`), `finalizer::Error`'s four (`finalizer.rs:28,32,36,40`). The chain is the taxonomy: a consumer can walk to the root cause without parsing text.
pumper: `#[from]` exists on exactly four variants — `Storage` (`error.rs:167`), `Io` (`:265`), `Json` (`:267`), `Other` (`:269`). The other fourteen carry `String`, and **186 construction sites across `crates/` build one with `format!`**, flattening the cause at the raise site (`crates\core\src\catalog.rs:330`, `crates\core\src\config.rs:713,716` are typical: `format!("{}: {e}", path.display())`).
**Verdict: `adopt`.** This is the single largest craft gap in the study and it is the pumper proposal (`2026-09-03-error-source-chains.md`). Note what it is *not*: it is not an argument against the String variants' human messages, which are good. It is that `e` is consumed into text at the moment it is most structured, so no downstream consumer can ever ask what it was.

**1.3 — Retryability as a property of the type.**
kube: stated once, in prose, for a whole enum — *"These are all considered retryable from a watcher's point of view, even though they may require patching of rbac/netpols in the background to fix"* (`watcher.rs:21-24`). There is no predicate and no test; a new variant inherits the sentence silently.
pumper: `Error::is_terminal_for_job()` (`error.rs:412-421`), five variants, each with a written justification for why attempt #2 re-fails in the identical place (`:327-411`) — and the classification is **held by an exhaustive match in a test** (`expected_terminal`, `:876-900`) driven over one instance of every variant (`:904-933`), so adding a variant stops the test compiling until someone decides.
**Verdict: `keep ours` — inverse list.** kube's is a doc comment; pumper's is a compiled decision with an inventory guard and thirteen behavioural tests (`:716-866`). The seed's line reference (`:637-837`) is wrong; the load-bearing artifact is `:868-933`.

**1.4 — A second, orthogonal axis: whose failure is it.**
kube: absent. A `kube_client::Error` from a misconfigured kubeconfig and one from a 503 are the same variant to every consumer.
pumper: `Error::is_router_failure()` (`error.rs:453-493`) — exhaustive, no `_` arm, with the reason the default is *theirs* written out (`:446-452`): *"a theirs-error misfiled as ours loses a document the ladder would have fetched, while an ours-error misfiled as theirs costs only what happens today."* Read by the fetch ladder at `crates\core\src\fetcher.rs:1174`.
**Verdict: `keep ours` — inverse list.** Two independent axes over one enum, with the cells that prove they are different questions named in the doc (`:430-432`). kube has one axis and does not have this one.

**1.5 — Classification by result code, never by prose.**
kube: `RetryPolicy` branches on `StatusCode` (`kube-client/src/client/retry.rs:52-56`) — 429/503/504 and nothing else.
pumper: `Error::is_store_contention()` (`error.rs:518-526`) reads SQLite's **extended** result code through `sqlite_primary_code` (`:543-545`, low byte = primary code), with the anti-pattern named — *"The messages SQLite attaches … are famously ambiguous prose that the driver itself apologises for"* (`:508-511`) — and both directions tested (`:990-1006` and `:1011-1027`).
**Verdict: `keep ours`.** Same rule, and pumper states it better and tests the over-classification direction, which kube does not.

**1.6 — The framework's error generic over the caller's error.**
kube: `Error<ReconcilerErr, QueueErr>` (`controller/mod.rs:62-83`) — the user's typed error survives the runtime instead of being flattened, and `finalizer.rs:16-20` warns that this deliberately makes `anyhow` awkward.
pumper: `ScrapeApp::run` returns `pumper_core::Result`, so an app author's own taxonomy lands in `Error::App(String)` (`error.rs:183-184`) and is gone.
**Verdict: `adapt`, narrowly.** kube's force is external implementors it cannot see; every pumper app is in-tree, in `crates/apps/`, and can raise `Error::SourceDrift` or `Error::BadRequest` directly — which is exactly what the grants apps do. The transferable half is not the generic; it is that an app must have a route to the typed variants and not only to `App`.

**1.7 — An error boundary that keeps the failed item's identity.**
kube: `DeserializeGuard<K>(pub Result<K, InvalidObject>)` buffers into `serde_value::Value`, tries `K`, and on failure re-parses **only `metadata`** so the broken object still implements `Resource` and can be named, logged and reconciled (`kube-core/src/error_boundary.rs:12-59`; the `meta()` fallback at `:81-84`; the test named after the failure, `should_parse_meta_of_invalid_objects`, at `:97-110`).
pumper: `crates\core\src\json_salvage.rs` recovers a valid document from fenced or embedded model output, and `Error::Parse` stays retryable while `Error::SourceDrift` is terminal (`error.rs:236-263`) — but there is no per-item boundary. One malformed record in a listing fails the batch or is dropped without an identity.
**Verdict: `adopt`.** Different forces at the transport layer (pumper salvages malformed *carriage*; kube isolates well-formed-but-schema-invalid *items*) but complementary, and the gap is real for every app in `crates/apps/` that ingests an externally-owned collection.

---

## 2. The work queue

**2.1 — What a duplicate is, and who decides.**
kube: the system decides. `ReconcileRequest`'s `reason` is excluded from `Hash`/`PartialEq` (`controller/mod.rs:298-302`), so an object occupies exactly one scheduler slot however many triggers fired for it.
pumper: **both**, and the design record's seed missed the half that already works. `enqueue_dedup` short-circuits on an `idempotency_key` (`crates\core\src\storage.rs:310-315`) and closes the concurrent-insert race by re-reading the winner after a unique-key violation (`:358-366`) — and the two trigger fan-in paths **synthesise their own key** rather than waiting for a caller to supply one: `idempotency_key(trigger_id, source_job_id)` = `trig:{trigger}:{source}` (`crates\server\src\triggers.rs:552-554`), used at `:1249,1259` and `:1387,1395`, with a redelivery reported as `Ok((_, false))` rather than as a second job.
**Verdict: `keep ours` for the trigger fan-in — with three enqueue sites that opted out.** `crates\server\src\scheduler.rs:400-412` (the cron fire path, which has `schedule_id` and a firing instant in hand), `crates\server\src\routes\triggers.rs:488` (the `?fire=true` preview) and `crates\server\src\datahub.rs:1845` all call plain `enqueue`. The scheduler's is defensible — the overlap guard (§4.6) covers the same ground for that path — but the rule that "the system synthesises the key when it knows the target" is stated nowhere, so each new work-creating door decides again.

**2.2 — Earliest-wins on re-schedule.**
kube: re-scheduling a key already queued **mutates the existing entry to the earlier time** (`reset_at`) rather than enqueueing a copy (`kube-runtime/src/scheduler.rs:76-112`), with the priority rule stated: a user-triggered request should beat the reconciler's own retry.
pumper: `available_at` is written once at insert (`storage.rs:318`) and moved only by `fail`'s backoff (`:492-496`) or a reset. A second enqueue of the same target cannot pull the first one forward; it makes a second row.
**Verdict: `adapt`.** The mechanism does not port — a durable row is addressed by `id`, not by an equality on its content — but the *rule* does, and the place it is missing is the trigger fan-in, where two triggers for the same target both enqueue.

**2.3 — In-flight exclusion, at which granularity.**
kube: per **key**. A message whose equal is executing is parked in `pending` rather than run (`scheduler.rs:114-139`; `controller/runner.rs:20-25`), and the guarantee is stated in the builder doc: *"despite concurrency, a controller never schedules concurrent reconciles on the same object"* (`controller/mod.rs:605-617`).
pumper: per **app**. `blocked_apps` builds an exclusion list the claim SQL applies as `app NOT IN (…)` (`crates\server\src\worker.rs:40`, `storage.rs:393-414`). Two jobs of the same app against the same target run concurrently whenever the app's cap allows.
**Verdict: `adapt`.** The per-app cap is a *fairness* device (one busy app must not starve others) and is right for that; it is not a mutual-exclusion device, and nothing in the tree provides one. Note that §2.1's dedup does **not** cover this: dedup refuses a second *enqueue*, and says nothing about two jobs already in the table — a scheduled run and a manual re-run of the same app against the same source, or two hops from different triggers onto one target — claiming slots at the same instant and writing the same dataset rows. This is the pumper proposal (`2026-09-03-target-key-exclusion.md`).

**2.4 — The global cap, and whether it is one number or two.**
kube: one. `max_concurrent_executions: u16`, `0` = unbounded (`controller/runner.rs:36,44-57`).
pumper: two, deliberately independent — a process-wide `Semaphore::new(concurrency)` (`worker.rs:20-22`) and a per-app limit through the claim exclusion (`:40`, `app_limit` at `:532`).
**Verdict: `keep ours`.** kube's single cap is enough because per-key exclusion already spreads the work; pumper's unit is an app, so without the second number one app's backlog owns every slot.

**2.5 — Debounce.**
kube: a configurable `debounce` is *added* to a request's expiry so a churning key coalesces, with the intent written on the field — *"allows for a request to be emitted, if the scheduler is 'uninterrupted' for the configured debounce period"* (`scheduler.rs:52-58`, applied at `:82-88`).
pumper: absent. A trigger firing five times in a second enqueues five jobs.
**Verdict: `adapt`, with a named trigger.** Debounce over a durable queue is a different object — it is a delay on *insert*, not on a queue entry — and it is not worth building until the trigger pipeline shows coalescible bursts. The trigger to watch is duplicate `trigger_id` rows within one `available_at` window.

**2.6 — Durable vs. in-memory.**
kube: everything is in memory. A restart discards the scheduler, the runner's slots and the store, and correctness is restored by a full relist — safe **only** because the apiserver is the source of truth and republishes everything.
pumper: the queue is a table. `available_at`, `attempts`, `heartbeat_at` and the `(status, attempts)` fence are columns (`storage.rs:19-21`), a boot sweep verdicts every orphaned `running` row (`:941-959`), and every recovery path goes through the same `fail()` door so the attempt ladder applies identically no matter what noticed (`:964-1009`, with the anti-pattern it replaces stated at `:932-940`).
**Verdict: `keep ours` — the strongest inverse-list entry in this study.** kube answers "what if the process dies" with "ask the server again". pumper has no server to ask, and its answer — one verdict function with two callers — is a design kube never had to make.

**2.7 — Heartbeat leases and a reaper.**
kube: absent, and the absence is load-bearing: an uncapped grep for leader election / `coordination.k8s.io` over the whole tree returns zero code hits. A controller's correctness argument is idempotence, not exclusivity.
pumper: `heartbeat()` guarded on `(status, attempts)` (`storage.rs:1015-1027`), `reap_stale()` measuring from `COALESCE(heartbeat_at, started_at, created_at)` and routing every stale row through `fail()` (`:1029-1053`).
**Verdict: `keep ours` — inverse list.** kube needs no lease because a dead controller loses nothing; pumper's store holds the only copy of a claimed job, so the lease is what makes a claim revocable.

**2.8 — Priority, and starvation.**
kube: no priority. Every key is equal; ordering is by expiry.
pumper: `claim_next` orders by *effective* priority — `priority + waited_secs / aging_coeff` — with `created_at` as a FIFO tiebreak and `0.0` restoring plain priority order (`storage.rs:383-409`).
**Verdict: `keep ours` — inverse list.** Priority only means something when work is heterogeneous and expensive; kube's units are uniform. Aging as a starvation guard, expressed inside the claim's `ORDER BY` rather than as a separate sweeper, is a technique kube has no need for and no equivalent of.

**2.9 — A clamp learned from a panic.**
kube: `time.min(max_schedule_time())` exists because an unclamped `Instant` panicked `DelayQueue` in the field, with the issue linked inline (`scheduler.rs:85-88`).
pumper: the same reflex in two places — the job ladder clamps at 3600s (`storage.rs:475-478`) and `retry_after_value` clamps a server-stated wait to 600s with a past or malformed date yielding `None` rather than zero (`crates\engine-http\src\lib.rs:1005-1016`).
**Verdict: `keep ours`.** Same discipline, arrived at independently, and pumper's is the harder case (an adversarial header rather than an internal overflow).

**2.10 — Backpressure that is never a drop.**
kube: the reflector's dispatcher applies backpressure to senders and retains an inactive receiver so the channel survives zero subscribers (`kube-runtime/src/reflector/dispatcher.rs:40-54`).
pumper: `fanout.rs` states the rule as a constraint — *"At the ceiling a unit runs inline on the caller… Inline is slow; dropped is a silently-unsent webhook, so the backpressure is never a drop"* (`crates\server\src\fanout.rs:16-19`) — and the placement decision is extracted as a pure function so it is testable without a runtime (`:52-60`).
**Verdict: `keep ours`.** Same rule; pumper names the alternative it refused, which kube does not.

---

## 3. Backoff

**3.1 — Backoff as a stream adapter vs. three call-site ladders.**
kube: one composable adapter. `StreamBackoff<S, B>` pauses a stream after any `Err`, resets `B` on any `Ok`, and **closes the stream** when `next_backoff()` returns `None` (`kube-runtime/src/utils/stream_backoff.rs:9-42`).
pumper: three ladders in three modules — the HTTP fetch ladder (`crates\engine-http\src\lib.rs:557-624`), the job ladder inside `Storage::fail` (`storage.rs:466-517`), and the webhook-delivery ladder inside `fail_delivery` (`:1879-1925`).
**Verdict: `adapt`.** kube also has three (`RetryPolicy`, `StreamBackoff`, `ResetTimerBackoff`); the difference is that kube's are *layered* — one wraps the next — while pumper's are parallel and share only `lcg_fraction`. The transferable half is not a `Stream` adapter (pumper's units are rows, not stream items) but a shared `Ladder` type the three call sites parameterise, so that a policy change lands once.

**3.2 — Reset after sustained health, not after the first success.**
kube: `ResetTimerBackoff` resets the inner policy only once `reset_duration` has elapsed since the last backoff (`kube-runtime/src/utils/backoff_reset_timer.rs:34-49`), and the default watcher strategy is 800 ms → 30 s with a 120-second reset window (`watcher.rs:979-987`). A stream that fails every 30 minutes therefore does not restart at rung 0 each time.
pumper: `fail_delivery` indexes `backoff_secs` by the row's persisted `retry_count` (`storage.rs:1908-1917`), and `retry_count` is cleared when a delivery succeeds — so a receiver that flaps (succeed, fail, succeed, fail) re-enters at rung 0 on every failure, forever, and never reaches the top of its own ladder.
**Verdict: `adopt`.** This is the one place where kube's mechanism ports almost verbatim: the reset needs a clock, not just a success. It applies to `fail_delivery` and to the `[resilience]` source-health ladder, and it is measurable (§ Tests, T3).

**3.3 — A server-stated delay as a floor, not a replacement.**
kube: `server_aware` honours `Retry-After` **over** the computed delay (`kube-client/src/client/retry.rs:63,80-88`) — the stated wait can shorten the ladder as well as lengthen it.
pumper: `retry_delay` takes `max(backoff, retry_after)` (`crates\engine-http\src\lib.rs:984-991`), so a stated wait is a **floor** and can never shorten the ladder, and both RFC 7231 forms are parsed with the HTTP-date converted from a passed-in `now` for determinism (`:998-1016`).
**Verdict: `keep ours` — inverse list.** pumper's direction is the safe one: honouring a server's request must not become a mechanism for retrying sooner than the ladder would have.

**3.4 — The stated wait against a total-time budget.**
kube: no end-to-end budget. A `Retry-After: 600` sleeps 600 seconds and the request has no clock of its own.
pumper: `capped_retry_sleep` returns `None` — stop now — when the sleep plus a minimum attempt will not fit the remaining budget, with the rule written out: *"Truncating the sleep instead would be worse than failing — it would retry earlier than the server asked, which is the one thing politeness must never do"* (`crates\engine-http\src\lib.rs:917-930`). `attempt_timeout` clamps each attempt to the remainder (`:935-940`), and the failure is minted as a distinct, still-retryable `budget_exhausted` naming the knob that stopped it (`:945-968`).
**Verdict: `keep ours` — inverse list.** kube does not have this decision, and pumper's is already a corroborated second sighting from the portkey run.

**3.5 — Jitter.**
kube: `ExponentialBackoffMaker` with `HasherRng` (`retry.rs:41,86-87`) — real randomness, not reproducible.
pumper: deterministic jitter from a seed and no `rand` dependency, in both ladders — job id + attempt (`storage.rs:483-492`) and URL + attempt (`crates\engine-http\src\lib.rs:971-991`) — so a replayed run picks the same delay and a test can assert it.
**Verdict: `keep ours` — inverse list.** Determinism costs nothing here and buys a testable ladder; kube's herd protection is identical either way.

**3.6 — The retryable set: closed in the type, or open in the config.**
kube: closed. 429 / 503 / 504, in the policy's own `match`, with the intent in the type's doc (`retry.rs:52-56`). A 4xx that is not a rate limit can never be retried, whatever an operator writes.
pumper: `HttpConfig::retryable_statuses`, defaulting to `[429, 502, 503, 504]` (`crates\core\src\config.rs:1346,1368`), read at `crates\engine-http\src\lib.rs:624`. An operator can add `400` and pumper will amplify its own bad requests.
**Verdict: `adapt`.** The default is right and better than kube's (502 belongs in the set). The gap is that nothing refuses a nonsensical entry: `Config::validate()` already refuses 29 misconfigurations, and a check that the set contains no non-idempotent-safe 4xx other than 429 belongs beside them.

---

## 4. The reconcile / converge loop

**4.1 — Told *that*, never *why*.**
kube: the trigger reason is carried for tracing and excluded from equality, and the doc states the consequence — *"This mapping mechanism ultimately hides the reason for the reconciliation request, and forces you to write an idempotent reconciler"* (`controller/mod.rs:631-634`; the `NOTE` at `:298-302`; `ReconcileReason` kept for spans only at `:334-366`).
pumper: the scheduler's unit is a cron firing, and the job it enqueues carries `params` merged from the app's defaults and the schedule's own keys (`scheduler.rs:871-883`) — not a reason. An app never learns whether it was fired by cron, by the API or by a trigger.
**Verdict: `keep ours`.** pumper arrived at the same property by a different route (params are the whole input; provenance rides in `schedule_id`/`trigger_id` columns for the operator, not for the app), and `merge_params` being shared by three doors is the guarantee that the input is identical whoever fired it.

**4.2 — The requeue policy as a function separate from the work.**
kube: `error_policy` is a **parameter** of `applier` (`controller/mod.rs:401`), returns an `Action`, and is invoked on the reconciler's typed error with the object and the context in hand (`:466-479`). Requeue-after is data the policy returns, not behaviour the runtime owns.
pumper: the requeue decision is inside the store. `Storage::fail` computes `10 * 2^attempts` capped at 3600s (`storage.rs:475-478`) knowing nothing but the attempt number; the only fork on the error is `is_terminal_for_job()` at `crates\server\src\worker.rs:1003` choosing `fail_permanently` over `fail`. A `429` with a `Retry-After: 600` that the HTTP ladder already read and gave up on re-queues at 10 seconds.
**Verdict: `adapt`.** This is the pumper proposal (`2026-09-03-requeue-policy-function.md`) and it maps directly to `convergence-loop-and-requeue`'s `error-policy-as-a-separate-function` technique. What must **not** be adopted is kube's shape wholesale — an `Action` returned from user code is wrong here, because the queue is durable and the store must own the write.

**4.3 — Idempotence.**
kube: forced structurally, by erasing the reason.
pumper: asserted where it matters and stated — the catalog reconcile reports a partial apply honestly *"since re-running reconcile is idempotent and finishes the remainder"* (`scheduler.rs:487-490`), and the recovery sweep is bounded twice precisely because *"it is idempotent, and the next sweep picks up the remainder"* (`storage.rs:980-985`).
**Verdict: `keep ours`.** Same property, reached by making every sweep resumable rather than by hiding information from the worker.

**4.4 — The decision extracted as a pure function.**
kube: `schedule_message` decides *and* mutates the `DelayQueue` in one method (`scheduler.rs:76-112`); the decision cannot be exercised without the queue.
pumper: `decide()` is pure, takes `now` and `cutoff` as parameters, and says why — *"pure (no I/O), so it is unit-testable against simulated downtime and DST boundaries"* (`scheduler.rs:800-852`). So are `misfire_cutoff` (`:730-736`), `still_firable` (`:448-450`), `firing_budget` (`:468-470`), `run_holds_slot` (`:697-699`) and `collapse_count` (`:739-755`) — each extracted with the anti-pattern it replaces written above it.
**Verdict: `keep ours` — inverse list.** This is pumper's strongest control-loop craft and kube has no equivalent. `misfire_cutoff`'s reasoning in particular (`:715-729`: deriving "missed" from the previous pass rather than from a constant, because a long tick would otherwise reclassify an on-time firing) is a decision kube's scheduler never has to make and would not survive making badly.

**4.5 — What one pass reports.**
kube: the applier yields `Result<(ObjectRef, Action), Error<…>>` per reconcile; there is no per-pass aggregate.
pumper: `StepOutcome` has **six** variants — `Idle | Fired | Skipped | SkippedAndFired | Held | Refused` (`scheduler.rs:162-180`) — folded into a `PassTally` (`:188-199`) whose `absorb` **cannot propagate an error by signature**, with the isolation guarantee stated as such: *"This function cannot propagate — an `Err` is logged with the schedule's id and counted, and the caller keeps looping — which is the whole isolation guarantee, expressed as a signature"* (`:203-228`).
**Verdict: `keep ours` — inverse list, and a correction.** The design record's seed said four variants; there are six, and the two it missed (`SkippedAndFired`, `Refused`) are the two that encode real decisions — per-firing rather than per-batch misfire classification (`:611-620`) and refusing without recording so that fixing a row makes it fire (`:326-334`).

**4.6 — The overlap guard.**
kube: per-key exclusion is the guard, and it is in the runner.
pumper: `latest_run(…).holds_slot` (`scheduler.rs:381-384`), keyed on the **newest** job for the schedule (`run_holds_slot`, `:697-699`) with the anti-pattern it replaces written out at `:683-696` — an existential guard over all of a schedule's jobs stayed true forever once an old failed job was retried, so the schedule silently stopped firing while `GET /schedules` answered `ok`.
**Verdict: `keep ours`.** Correcting the seed, which located this at `scheduler.rs:39-40` (a constant). And note the guarantee is the same one kube states at `controller/mod.rs:605-617`, reached through a different observable.

**4.7 — Panic containment.**
kube: none. A panicking reconciler unwinds the runner's task; there is no `catch_unwind` anywhere in `kube-runtime`.
pumper: two nested layers with the reason for each placement. Per-tick containment sits inside `run` rather than at the spawn boundary *"on purpose: respawning `run` would re-run `boot_reconcile` and throw away the cross-tick `cron_cache`, i.e. a panic would silently change scheduling behaviour"* (`crates\server\src\scheduler.rs:66-105`), and per-schedule containment sits inside the loop because *"an app whose `default_params()` unwinds is reached INLINE from here … and one such app must not cost every other schedule its pass"* (`:280-295`). A panicking job is finalised through the same attempt-fenced path as an app error, with a `panicked: ` marker (`worker.rs:552-580`, `:1039-1045`).
**Verdict: `keep ours` — inverse list.** kube can afford to let a task die because the process is disposable and the server republishes. pumper's scheduler tick *is* the process heartbeat — cron, the reaper, the dead-letter drain, the refresher and the DataHub poll all ride it (`scheduler.rs:96-104`) — so an unwind used to kill five subsystems while the HTTP server kept answering.

**4.8 — Graceful drain.**
kube: the queue terminating fires a oneshot that shuts the scheduler down and lets in-flight reconciles complete (`controller/mod.rs:416-441`), with a debug line per stage.
pumper: two phases with a stated split (`worker.rs:243-258`) — a clean-finish window, then a **cooperative suspend** in which every running job's cancel token fires and `execute` treats the cancellation as *re-queue with checkpoint*, not as failure; anything still running at the true deadline goes through `recover_stuck`. Cancel and suspend are distinguished by an explicit intent handshake (`CancelKind`, `:373-388`; `claim_user_cancel`, `:427-452`) because *"an operator's explicit cancel that landed inside the drain window was silently converted into 'run again later'"*. The fan-out pools drain after, in dependency order, and **say what they abandoned** (`:319-372`).
**Verdict: `keep ours` — inverse list.** kube's drain has nothing to preserve. pumper's has a durable row per in-flight unit and answers the question kube never asks: what does a half-done unit become.

**4.9 — A readiness barrier before any work runs.**
kube: `Runner::delay_tasks_until(store.wait_until_ready())` (`controller/mod.rs:485-490`; `controller/runner.rs:59-70`) — the scheduler keeps accepting and coalescing while the cache fills, and no reconcile runs against a partial cache, because a reconciler reading a half-filled cache concludes its children are missing and recreates them.
pumper: the worker claims from the first tick. There is no partial-cache hazard (the store is the truth and is complete when it opens), and boot ordering is instead handled by `recover_stuck` verdicting orphans before the loop starts.
**Verdict: `different forces`.** The hazard kube's barrier exists for cannot occur here. Recorded so the decision is inherited rather than rediscovered if a warm in-memory index (the catalog registry, Tantivy) ever gates correctness rather than latency.

---

## 5. Typestate and marker traits

**5.1 — Making the illegal call unrepresentable.**
kube: private marker traits `ClusterScope` / `NamespaceScope`, implemented for the two `k8s_openapi` scopes, so a namespaced call on a cluster-scoped type is a **compile error, not a 404** (`kube-client/src/client/client_ext.rs:15-25`).
pumper: no typestate. The nearest and strongest analogue is `RemovalGuard` (`crates\core\src\datasets.rs:343-367`) — a zero-sized token with a private field, mintable only by asking a source's health state, required by `detect_removed`. The reasoning is the same one, written out: *"Requiring this token turns the check from a convention into a precondition: there is no way to reach removal detection without having asked the health state, because there is no other way to obtain the token. It carries no data — its whole value is that it cannot be forged"* (`:334-342`).
**Verdict: `keep ours`.** Same force, different mechanism, and pumper's carries the incident that motivated it (a peer app that hand-rolled `upsert_many` + `detect_removed` and bypassed the check silently).

**5.2 — The escape hatch, and where it lives.**
kube: `DynamicResourceScope` implements **both** markers, so a runtime-discovered type opts out of the check at exactly one line rather than by weakening the API (`client_ext.rs:23-25`).
pumper: `RemovalGuard::for_self_derived_snapshot()` is the escape hatch, is `pub(crate)`, and says why — *"the capped search result set IS the complete snapshot of the view, and there is no external source whose health could be degrading. Crate-private on purpose — nothing outside the store may mint a guard without asking a health state"* (`datasets.rs:359-364`).
**Verdict: `keep ours`.** kube's escape hatch is public because its callers are external; pumper's is crate-private because they are not. Both put the opt-out on one line with its reason, which is the transferable rule.

---

## 6. Generic over typed and dynamic

**6.1 — One API serving compile-time-known and runtime-discovered types.**
kube: `trait Resource { type DynamicType; }` — static types set `DynamicType = ()` and pay nothing, runtime-discovered types carry `ApiResource`, and **one `Api<K>` serves both**, so the dynamic path is not a parallel API with its own bugs (`kube-core/src/resource.rs:27-40`).
pumper: the dynamic surface is `serde_json::Value` params against a registry of `Arc<dyn ScrapeApp>` keyed by name (`crates\core\src\app.rs:919`); the typed surface is each app's own structs behind that boundary. There is no static path to unify with — every job's params cross an HTTP door as JSON.
**Verdict: `different forces`.** kube's `DynamicType` exists because the *same* client must serve both a compiled-in `Pod` and a CRD discovered at runtime. pumper has one shape of caller. The lesson that does transfer is the zero-cost-when-static discipline, and there is nothing here to apply it to.

**6.2 — The dynamic input validated once, by a schema, at every door.**
kube: `PatchParams::validate` refuses `force` without an apply patch and a manager name over 128 characters **before a request is sent** (`kube-core/src/params.rs:678-690`).
pumper: `validate_app_params` is shared by the jobs door (`crates\server\src\routes\jobs.rs:147-149`), the schedule create door, and the scheduler's own fire path (`scheduler.rs:891-899`), with the reason for the third stated — rows that predate the create-time check, or were edited in SQL, are caught on the way out. A schedule whose params fail is `Refused` without recording, so fixing the row makes it fire (`:341-357`).
**Verdict: `keep ours`.** Same rule — the illegal combination fails before work starts — and pumper applies it at three doors with an explicit statement that they cannot disagree, which kube's single door does not have to say.

---

## 7. The test-power ladder

**7.1 — The ladder written as MUST / MUST NOT with a stated default.**
kube: four classes with prohibitions — *"Unit tests MUST NOT try to contact a Kubernetes cluster"*, *"E2E tests MUST NOT be used where an integration test is sufficient"* — closing on **"use the least powerful method of testing available to you"**, then a per-crate assignment (`C:/t/kube/CONTRIBUTING.md:96-110`).
pumper: the *apparatus* is richer than kube's (§7.2, §7.3) but the **doctrine is unwritten**. `CLAUDE.md`'s command table distinguishes `just test` / `just test-ignored` / `just lanes` and says which rungs `just ci` blocks on, but nowhere states which class of test a new case belongs in, or which classes are forbidden from touching a network.
**Verdict: `adopt`.** Twelve lines in `CLAUDE.md`, no code. The ranked feature list places it third precisely because it is that cheap.

**7.2 — `#[ignore]` for live dependencies.**
kube: 33 sites, each with a human reason string (`kube/src/lib.rs:253` `"needs kubeconfig"`, `:272` `"needs cluster (creates + patches foo crd)"`; `kube-client/src/api/entry.rs:333`), so the default `cargo test` is hermetic. Nothing checks the reasons.
pumper: 29 sites, and the reason string is a **machine-checked record**. A quarantined flake carries owner, entry date, expiry and cause and names the command that audits it (`crates\core\src\governor.rs:347`); a long lane names its criteria file and the leg that runs it (`crates\core\tests\datasets_bulk_perf.rs:127`). `just flake-check` reconciles the register against the tree's `#[ignore]`s **in both directions** with three exit codes, and a `3` (cannot check) is explicitly not a pass (`.ai/manifest.yaml`, `capabilities.flake-check`).
**Verdict: `keep ours` — inverse list, and a correction.** The design record listed the `#[ignore]` convention as the cheapest fleet-wide adoption from kube. pumper is two levels past it: kube's ignore reasons cannot expire and cannot be wrong.

**7.3 — Proving the gate can go red.**
kube: coverage via `tarpaulin.toml` and a live-cluster e2e job that is deliberately trivial because its job is to prove in-cluster auth works, not to test logic.
pumper: `just harness-test` — *"the fixture suites that prove `flake-check` and `lane-certify` can still go red"* — is one of the nine rungs `just ci` blocks on, and `just lane-health` publishes each lane's pass-rate history with **never green as its own category** (`justfile:133,173`; `CLAUDE.md` command table).
**Verdict: `keep ours` — inverse list.** A gate never observed failing is indistinguishable from a gate that cannot fail. Neither tree's e2e story is complete, but only one of them tests its own gates.

---

## 8. Lint discipline

**8.1 — The strictness level, and where it is declared.**
kube: in the source. `#![deny(clippy::all)]` **and** `#![deny(clippy::pedantic)]` at the top of `kube-runtime/src/lib.rs:12-13`, so the level travels with the crate and a local `cargo clippy` reproduces CI exactly.
pumper: nowhere in the source. No crate carries a `#![deny]` or `#![warn]` attribute; the gate is `cargo clippy --workspace --all-targets -- -D warnings` in CI and in `just lint` — default level, denied. A contributor running bare `cargo clippy` sees warnings; CI sees errors.
**Verdict: `adopt`.** Not pedantic — that is a large, separate decision — but moving `-D warnings` into the crates as `#![deny(warnings)]` (or at minimum `#![deny(clippy::all)]`) makes the local and the remote run the same run.

**8.2 — Justified exemptions.**
kube: seven `#![allow(...)]` lines, **each with the reason it exists** on the line above — `// Triggered by educe derives on enums`, `// Triggered by nightly clippy on idiomatic code` (`kube-runtime/src/lib.rs:14-22`) — so a future reader can test whether the exemption is still earned.
pumper: 27 `#[allow]` sites, of which roughly twenty are bare `#[allow(clippy::too_many_arguments)]` with no comment (`crates\core\src\datasets.rs:572,683,1212,1297,1397,1990,2520,2802,2880`; `storage.rs:1385`; `fetcher.rs:1298`). Four carry a reason; the rest are unexamined.
**Verdict: `adopt`.** One comment per allow, and the ones that cannot be justified are a list of functions to convert to parameter structs.

**8.3 — Supply-chain gating.**
kube: `deny.toml` plus a dedicated `audit.yml` workflow.
pumper: `deny.toml` plus `cargo deny check advisories bans sources` as a named capability and a `just ci` rung (`justfile:81,193`).
**Verdict: `keep ours`** — parity, and pumper's is inside the one command a contributor runs rather than in a separate workflow.

---

## 9. Module composition

**9.1 — À la carte components vs. one seam per loop.**
kube: *"all components are designed to be usable á la carte if your operator doesn't quite fit that mold"* (`kube-runtime/src/lib.rs:7-9`), delivered by making every layer a `Stream`: `watcher` is a `Stream` over an explicit FSM, `reflector` is a pass-through combinator, `applier` is `stream::select` into a scheduler into a runner (`controller/mod.rs:422-503`).
pumper: the composition is not by combinator but by **extracting a one-shot seam per loop**. `run_one` is *"one claim→execute→finalize pass with no loop, semaphore, or per-app caps — the deterministic seam the e2e tests drive"* (`crates\server\src\worker.rs:195-201`), and `reconcile_one` is extracted *"so the failure of this schedule is a value the caller absorbs … and so the step is drivable directly from a test"* (`scheduler.rs:301-312`).
**Verdict: `keep ours`.** Both trees solved the same problem — a long-lived loop is untestable — and pumper's answer costs one function per loop instead of restructuring everything as a `Stream`. kube needs the combinators because its consumers assemble the pipeline; pumper's consumers call an HTTP API.

**9.2 — Zero streams, on purpose.**
kube: combinators end to end.
pumper: no `futures::Stream` in the control path; the loops are `loop { tokio::select! { … } }` over a claim (`worker.rs:27-121`) and a tick (`scheduler.rs:61-111`).
**Verdict: `different forces`.** A `Stream` over `claim_next` would add a layer and lose the two things the plain loop buys: the `?`-free `PassTally::absorb` isolation (§4.5), and the ability to put `catch_unwind` around exactly one tick body while keeping cross-tick state (§4.7). Recorded so a future refactor inherits the reasoning.

**9.3 — The dependency rule, stated where a machine reads it.**
kube: layering is implicit in the workspace graph — `kube-core` → `kube-client` → `kube-runtime` → `kube` — and stated only in prose in an off-tree architecture document.
pumper: the rule is a field in the agent contract: *"apps depend only on core (plus parsing libs); engines depend only on core; the server wires everything. Never make an app depend on another app or on an engine crate"* (`.ai/manifest.yaml`, `boundaries.dependencyRule`), alongside `neverTouch` and `secretsFrom`.
**Verdict: `keep ours`.** kube's graph enforces itself through Cargo; pumper's has one shape Cargo cannot enforce (app → app), and writing it where a tool reads it is the cheapest thing available short of a gate.

---

## 10. Repository maintenance

**10.1 — A changelog that carries rationale.**
kube: `CHANGELOG.md` is 16,411 words, generated from releases, with the convention stated in the file's own header — *"NOTICE this is mostly generated from releases - ONLY ADD NEWS here now"*, *"Anything added in the UNRELEASE'D section will be prepended to the autogenerated release"* (`C:/t/kube/CHANGELOG.md:1-5`) — and the 4.0.0 entry spends paragraphs on *why* (the CEL crate, the retry default, the timeout removal) rather than listing PRs.
pumper: **no changelog of any kind.** The rationale that would live there is instead distributed across doc comments, which is why this study can cite so many of them — but there is no per-release record, so "what changed and why" is answerable only by reading the tree.
**Verdict: `adopt`, narrowly.** Not a generated PR list, which pumper has no releases to generate from. The transferable half is the header convention: one file, one UNRELEASED section, and the rule that only rationale goes in by hand.

**10.2 — Release tooling.**
kube: `release.toml` (cargo-release) plus a `release.yml` workflow, alongside eleven other workflows.
pumper: no release artifact and no deployment artifact of any kind — confirmed by the design record's fleet pass and unchanged here. The distribution story is `cargo run -p pumper-server --bin pumper`.
**Verdict: `adapt`.** The absence is a scope decision, not an oversight — pumper is a local-first service with one operator. What is worth taking is smaller: `CLAUDE.md` already documents that `--bin pumper` is required because the package ships three binaries with no `default-run`, and a `default-run` key plus a version-stamped `--version` would remove the sentence.

---

## Tests to initiate

Paired, with the instrument and the number that would move.

**T1 — Can two jobs for one target run at the same instant?** (§2.3)
Enqueue two jobs of the same app with identical `params` through two *different* doors that each legitimately skip dedup — the cron fire path (`scheduler.rs:412`) and `POST /apps/{name}/jobs` with no `idempotency_key` — with `[worker] concurrency = 4`, then drive both through `worker::run_one` (`crates\server\src\worker.rs:195-201`) against a counting fake from `crates\core\src\testing.rs`. *Instrument*: overlapping `started_at`/`finished_at` windows in `jobs`, and the fake engine's concurrent-call high-water mark. *Number that would move*: concurrent runs against one target — **today 2, target 1** under a per-key exclusion. *Paired arm*: two jobs of the same app against *different* targets must still run concurrently, or the exclusion has become the per-app cap it must not duplicate.

**T2 — Does anything stop a second scheduler?** (§2.7, and the single-writer assumption)
Run two `pumper` processes against one SQLite file for one hour with a `* * * * *` schedule. *Instrument*: rows in `jobs` carrying that `schedule_id`, and the schedule's `last_run` progression. *Number that would move*: duplicate firings per minute — **today unbounded** (the overlap guard reads `latest_run` outside any transaction and both processes read `holds_slot = false`), **target 0**. This is the test that decides whether the single-scheduler assumption at `scheduler.rs:381-384` needs a lease or only a comment.

**T3 — Does a flapping receiver ever climb its own ladder?** (§3.2)
Drive `fail_delivery` against a receiver that fails, succeeds, fails, succeeds on a 30-minute period for six hours. *Instrument*: `webhook_deliveries.retry_count` at each failure and the computed `next` from `storage.rs:1908-1917`. *Number that would move*: the delay applied at hour six — **today `backoff_secs[0]` on every failure**, because `retry_count` clears on success; under a reset-after-sustained-health rule it would be the rung the failure rate earns. *Paired arm*: a receiver that is genuinely healthy for the full reset window must still return to rung 0, or the fix has turned a transient into a permanent penalty.

**T4 — What does an error lose at the boundary?** (§1.2)
A test over `crates/` asserting that no `Error` variant is constructed with a `format!` that interpolates a value implementing `std::error::Error`. *Instrument*: the count of such sites — **today 186**. *Number that would move*: sites that discard a typed cause, target 0 for the four variants the proposal covers.

---

## Features, ranked, with why the scope admits each

`scope.does`: *"durable SQLite job queue behind an HTTP API; pluggable scrape and fetch engines"*, *"research cache, remote fetch fabric, cost ledger, tier memory"*.

1. **In-flight exclusion by target key** → proposal `2026-09-03-target-key-exclusion.md`. The queue **is** the product; `scope.does` names it first. §2.1 shows pumper already has the *enqueue* half of kube's rule and synthesises its own keys where it knows the target; §2.3 shows the *execution* half — one runner per key — is absent, and that the per-app cap is a fairness device being asked to do a correctness job. Highest ranked because a double-write is a data defect, not a latency one.
2. **Source chains on the error enum** → proposal `2026-09-03-error-source-chains.md`. `scope.does` names *"pluggable … engines"* — plural, behind one interface — and every engine's failure crosses a crate boundary as text. §1.2's 186 sites are the measure. Second because it is the cheapest large improvement in the tree and every later classification improvement depends on it.
3. **Requeue policy as a function the error can inform** → proposal `2026-09-03-requeue-policy-function.md`. The durable queue's retry ladder currently knows only the attempt number. Third because §4.2's cost is money and politeness rather than correctness, and because the first two make it easier.
4. *(not proposed — recorded)* **The test-power ladder as written doctrine** (§7.1). Twelve lines in `CLAUDE.md`. Not proposed because it is documentation, not a direction, and belongs in the next doc-sync pass.
5. *(not proposed — recorded)* **Per-allow justification and `#![deny]` in the crates** (§8.1, §8.2). One comment per allow plus one attribute per crate.

---

## The inverse list — where pumper is ahead

Ordered by how much of it the corpus should carry.

1. **A durable queue with one verdict function and two callers** (§2.6, §2.7). kube's whole recovery story is "relist from the server". pumper has no server, and `issue_recovery_verdicts` (`storage.rs:964-1009`) with its stated anti-pattern — a boot sweep and a lease reaper applying *different policies to the same class of row* — is the design kube never had to make. Already lands in `job-coordination/terminal-state-recovery`, which the code cites by name.
2. **Two independent classification axes over one error enum, both exhaustive** (§1.3, §1.4). `is_terminal_for_job` and `is_router_failure`, each with a no-`_`-arm match, per-variant written reasons, a default that is the safe direction, and an inventory guard that breaks the build on a new variant. kube has one axis, in prose, untested.
3. **A pure decision function per loop, with the anti-pattern above it** (§4.4, §4.5). `decide`, `misfire_cutoff`, `still_firable`, `firing_budget`, `run_holds_slot`, `PassTally::absorb`. Six extractions, each carrying the failure that motivated it. kube's scheduler decides and mutates in one method.
4. **A two-phase drain that turns cancellation into a checkpoint, and distinguishes it from an operator's cancel** (§4.8). `CancelKind` plus the intent handshake under the same mutex. kube's drain has nothing durable to preserve and so has no equivalent decision.
5. **`#[ignore]` reasons that expire and are reconciled in both directions** (§7.2), plus a gate that proves it can go red (§7.3). Two rungs past the convention the design record wanted pumper to adopt.
6. **A stated wait bounded by a total-time budget, refused rather than truncated** (§3.4), and **a stated wait as a floor rather than a replacement** (§3.3). Both stronger than kube's `server_aware`, and the first is a second sighting of a technique already in flight.
7. **Deterministic jitter with no `rand` dependency** (§3.5), shared by two ladders and asserted in tests.
8. **Priority with an aging starvation guard inside the claim's `ORDER BY`** (§2.8).
9. **Panic containment placed where cross-tick state survives, with the placement argued** (§4.7).
10. **An unforgeable capability token as the precondition a convention could not be** (§5.1) — `RemovalGuard`, reaching kube's typestate force without typestate.
