---
slug: checkpoint-failure-is-visible
type: perfect/direction
context: "[[job-worker]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: 2026-08-14
commit: 28fb46a
---

## What & why

**A job can lose its durability entirely and nothing anywhere says so.**

`JobCheckpointer::save` returns `false` for two real, non-hypothetical reasons — the job's attempt
lineage went stale (it was reset, reaped, or re-claimed mid-run, which is *the* scenario
checkpoints exist for) and a storage error (blob over the 8 MB cap, disk full, a locked DB). Each
emits a `tracing::warn!` and returns `false`.

**All 11 app call sites discard that bool.** Every one is `ctx.checkpoint(..).await;` or
`ctx.checkpoint_now(..).await;` as a statement. The trait's own doc says the return value is there
"so apps can count it" — a documented contract with **zero** implementors, which is exactly the
docs-describing-behavior-the-code-does-not-have class this repo flags.

The consequence for the operator: a long federal-grants or CORDIS harvest whose checkpoints stopped
landing at minute 3 will, on its next reap or restart, resume from nothing and redo hours of
governor-paced fetching — and `GET /jobs/{id}` looked perfectly healthy the entire time. The only
trace is a `warn` line in a log nobody is tailing.

**Fix it at the sink, not at the 11 call sites.** Asking nine apps to each write the same three
lines is the churn version of this fix; it also spreads the write set across two lots. The sink
already knows every failure — make *it* count and surface them.

## Evidence

- `crates/core/src/app.rs:40-49` — the trait doc: returns `false` "so apps can count it".
- `crates/server/src/progress.rs:136-153` — the two genuine `false` paths, each `warn!`-only.
- The 11 discarding call sites, every one a bare statement:
  `connector-api-watch:342` · `cordis:389,402` · `extractor:1863` · `grants-gov:514,530` ·
  `mpsv-vpm:1063,1073` · `plugin:1417` · `provisioner:852` · `research:562,612` ·
  `state-licensing:368`.
- `crates/core/src/storage.rs:2092-2118` — `save_checkpoint`'s lineage fence (`WHERE … attempts = ?3`)
  returning `Ok(false)`, and its `MAX_CHECKPOINT_BYTES` rejection returning `Err`.
- `crates/server/src/routes/jobs.rs:246-250` — `GET /jobs/{id}` merges a live progress snapshot and
  nothing else; there is no checkpoint-health field anywhere on the job surface.
- `crates/server/src/progress.rs:53-56` — the progress store is **cleared on finalize**, so a
  live-only surface cannot answer this question after the fact. Whatever you add must survive the
  run.

## Acceptance criteria

1. `JobCheckpointer` counts its own failed saves, distinguishing **stale lineage** from
   **storage error** — they mean different things to an operator (the first says another attempt
   owns this job; the second says the disk or the blob is the problem).
2. The count is **visible on a surface that outlives the run**, not only in the live progress
   snapshot (which `finalize` clears). Name the surface you chose and why in your report; a
   `/metrics` counter plus a job-event emission is the expected shape, following the r18 egress
   series precedent. **Do not add a schema migration for this.**
3. A throttle-skipped save is **not** a failure and must never be counted as one — `save` already
   returns `true` for it deliberately (`:129-130`), and conflating the two would manufacture the
   alarm it is meant to report. Pin that with a test.
4. Tests cover both failure kinds and the throttle-skip, driving `JobCheckpointer` directly (it
   takes a `Storage`, so `TempStore` is enough — this direction does **not** depend on
   [[harness-expresses-the-run]]).
5. `docs/features/runtime.md` states the checkpoint seam's failure contract and where a dropped
   checkpoint now shows up. Today the doc mentions checkpoints only in passing (`:27`, `:109-112`).
6. **No `crates/apps/**` file is edited.** If you conclude the fix genuinely requires touching an
   app, stop and return `DECISION NEEDED` — those files belong to the sibling lot this round.

## Risks / non-goals

- **Non-goal:** making apps react to a failed checkpoint (abort, retry, degrade). That is a
  behavior change across nine apps and needs its own round; this direction makes the failure
  *observable*, which is the precondition for ever deciding what to do about it.
- **Non-goal:** `#[must_use]` on `AppContext::checkpoint`/`checkpoint_now`. It would force an edit
  at all 11 sites — including files owned by the other lot this round — and lint-driven `let _ =`
  at every site buys visibility for nobody.
- **Risk:** the counter must not become a hot lock on the checkpoint path. It is already behind a
  `Mutex` for the throttle; an atomic is the cheap answer.

## Build record

(filled during build)
