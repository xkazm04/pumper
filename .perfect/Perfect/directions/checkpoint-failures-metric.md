---
slug: checkpoint-failures-metric
type: perfect/direction
context: "[[api-surface]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## The banked claim — CONFIRMED absent, but the "one-file change" framing is REFUTED

r22's late Lot B report banked: *"a `pumper_checkpoint_failures_total{app,reason}` Prometheus series
— `/metrics` is rendered in `routes/meta.rs`, which was Class C and outside its write set, so this is
a genuine one-file follow-up rather than an omission."*

**The series is absent — confirmed.** Complete inventory of `/metrics`
(`crates/server/src/routes/meta.rs:53-159`): `pumper_jobs{status}`,
`pumper_job_failures_total{app}`, `pumper_job_duration_seconds_{sum,count,max}`,
`pumper_job_queue_wait_seconds_{sum,count,max}`, `pumper_cost_usd{app,engine}`,
`pumper_apps{ready}`, `pumper_schedules{enabled}`, `pumper_webhook_deliveries{status}`,
`pumper_webhook_oldest_undelivered_seconds`, `pumper_webhook_delivery_attempts_total`,
`pumper_webhook_deliveries_succeeded_total`, `pumper_remote_egress_fetches{served_by}`. No checkpoint
series.

**The data exists and the labels are derivable.** Two `AtomicU64`s on `JobCheckpointer`
(`crates/server/src/progress.rs:112-113`), read via `failures()` (`:211-216`), stamped into the result
JSON under `"checkpoint_failures"` (`:126`) by `checkpoint_failure_stamp` (`:178-186`) as
`{stale_lineage, storage_error, total}`, carried by `carry_checkpoint_failures`
(`worker.rs:1821-1833`) at `:822`. Reason vocabulary is exactly `stale_lineage | storage_error`
(`progress.rs:140-145`); `app` is a first-class `jobs` column.

## The correction that matters — and it is why this is rejected in its banked form

**The stamp lands only on the SUCCESS path.** `worker.rs:822` sits inside
`Outcome::Finished(Ok(mut result))` (`:804`). Failed, cancelled, timed-out and panicked runs carry no
block at all. And when an app's result is not a JSON object, `carry_checkpoint_failures` returns it
unstamped and it is dropped to a `warn!` (`worker.rs:823-827`, `:1826-1832`).

**So a `jobs.result`-derived series systematically undercounts exactly the runs an operator most
wants counted** — and being DB-derived it also shrinks under retention pruning (the same caveat
`pumper_job_failures_total` documents at `meta.rs:76-77`).

**Cost is not the blocker; the framing is.** A `jobs.result` scan needs `json_extract` per row with no
covering index (`crates/core/migrations/0001_init.sql:16-17` — none covers `result`), but that is
already the house standard: `status_counts` is an unindexed `GROUP BY` full scan and
`job_timing_stats` computes six `julianday` deltas per row, both on every uncached scrape, with the
whole body cached 5s (`METRICS_TTL`, `meta.rs:38`).

**It is not a one-file change either way.** DB-derived needs a new aggregate in
`crates/core/src/storage.rs` + the render in `meta.rs`. Process-counter needs
`crates/server/src/progress.rs` + `crates/server/src/state.rs` + `meta.rs`. Two-to-three files.

## The design a builder should actually ship

**A process-lifetime `AtomicU64` pair incremented in `JobCheckpointer::record_failure`
(`progress.rs:219-236`), surfaced through `AppState`.** Genuinely monotonic, complete across *all*
outcomes, zero query cost, immune to retention pruning. Same file count as the DB version and
strictly better data. **Do not build the `jobs.result` scan.**

## Why REJECTED this round

**On the 6-direction cap.** Observability-only, and the gap is already partly covered: the
`checkpoint_failed` job event fires (`progress.rs:229`) and the stored block is visible on
`GET /jobs/{id}`. It lost the slot to six directions closing live defects rather than adding a view
of them.

**Banked with the design correction above** — which is worth more than the original claim, since the
banked framing would have shipped a metric that lies about the runs that matter most.

---

## r24 re-verification (2026-08-14) — CONFIRMED, sizing question ANSWERED, and a live defect found that changes the verdict

**Absence confirmed.** Full `/metrics` inventory at HEAD is 14 series (`meta.rs:68-146`, `:186-203`,
`:210-252`); no checkpoint series. **Stamp confirmed success-path-only — 1 of 7 arms.**
`carry_checkpoint_failures` is called exactly once, `worker.rs:822`, inside `Outcome::Finished(Ok)`
(`:804`). The other six arms carry nothing: `Cancelled if ShutdownSuspend` (`:769`), `Cancelled`
(`:786`), `Finished(Err)` terminal (`:885`), `Finished(Err)` retryable (`:903`), `Panicked` (`:921`),
`TimedOut` (`:937`).

### The finding that flips this from observability-only to a real defect

**The shutdown-suspend arm logs a durability assertion it knows may be false.** `worker.rs:769-784`
calls `state.storage.reset(job.id)` and logs *"job suspended for shutdown; re-queued to resume from
checkpoint"* (`:778`) — **which is false exactly when `storage_error > 0`**, and the sink holding
that count is alive and in scope at `:776`. A green log line asserting a resume the process already
knows may not exist. r23 rejected this direction as *"a view of defects rather than closing one"*;
that reasoning no longer holds.

### The sizing question, answered decisively — 3-4 files, not 6

`AppState` is **already in scope at the construction site**, `worker.rs:676-679`:
`JobCheckpointer::new(job.id, job.attempts, state.storage.clone()).announcing(job.app.clone(),
state.events.clone())`. **`.announcing` (`progress.rs:204-208`) is a `#[must_use]` builder that
exists solely to hand an `Arc` from `AppState` into this sink** — a process counter copies that exact
pattern: field on `AppState` (`state.rs`), sibling builder on `JobCheckpointer` (`progress.rs`), one
chained call at `worker.rs:678`, one render block in `meta.rs`. `record_failure` (`progress.rs:219-236`)
is the single increment point. **No new dependency threading.**

**Precedent to copy for the rendering half:** `pumper_remote_egress_fetches{served_by}` — counter type
`EgressCounters`, read `meta.rs:155`, rendered by `egress_metrics` `meta.rs:186-203`, with
`meta.rs:182-185` documenting the process-lifetime/reset-on-restart contract. And **`meta.rs:168-171`
already states the repo's own rule that DB-derived `_total` series are dishonest because retention can
lower them** — which is the argument against the `jobs.result` design, in the repo's own words.

Vocabulary: `CheckpointFailure::{StaleLineage, StorageError}` (`progress.rs:133-137`), strings
`"stale_lineage"`/`"storage_error"` (`:141-143`), stamp shape `:178-186`,
`CHECKPOINT_FAILURES_KEY` `:126`, `CHECKPOINT_FAILED_STATUS` `:123`.

## Acceptance criteria (r24)

1. A checkpoint failure is counted **on every job outcome**, not only on success — process-lifetime
   `AtomicU64` per reason, surfaced through `AppState`. **Do NOT build the `jobs.result` scan**; it
   undercounts exactly the runs that matter and shrinks under retention pruning.
2. `pumper_checkpoint_failures_total{reason}` renders on `/metrics`, following the `egress_metrics`
   shape including its process-lifetime/reset-on-restart doc contract.
3. **The shutdown-suspend log at `worker.rs:769-784` stops asserting a resume it cannot promise** when
   `storage_error > 0`. The sink is in scope at `:776` — the information is already there. Choose the
   lever (downgrade the claim, name the failure count in the line, or both) and say why in the diff.
4. A test proving the counter moves on a non-success outcome, named after the anti-pattern.
5. `docs/features/runtime.md` (+ `http-api.md` if the `/metrics` surface list lives there) documents
   the new series and its process-lifetime semantics.

**ACCEPTED r24.** Gate: director-self-gated (autonomous, Athena-dispatched).
