---
slug: checkpoint-failures-metric
type: perfect/direction
context: "[[api-surface]]"
lens: robustness
status: rejected
size: S
proposed: 2026-08-14
accepted: —
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
