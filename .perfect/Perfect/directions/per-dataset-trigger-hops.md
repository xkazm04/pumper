---
slug: per-dataset-trigger-hops
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 48e7ade
---

## What & why
Multi-dataset runs fire exactly ONE trigger hop for a nondeterministically-chosen dataset;
the rest silently dedup away. The idempotency key `trig:{trigger}:{source_job_id}`
(`triggers.rs:573`) omits the dataset, while `fire_dataset_triggers` (`triggers.rs:388`)
iterates a `HashMap<&str, Vec<&Revision>>` per dataset — iterations 2..n hit `Ok((_, false))`
at `:587` and vanish without a log. Saved-search view materializations (`worker.rs:1320-1324`,
same `job.id`) collide the same way, including with the fanout hop when a view targets the
job's own app. Grants — the repo's highest-value app — is a multi-dataset writer. Docs claim
"one target job per trigger per source run, carrying the whole capped batch"; reality is one
arbitrary dataset's slice.

## Evidence
- `crates/server/src/triggers.rs:573` — key omits dataset
- `crates/server/src/triggers.rs:388` — per-dataset HashMap loop (RandomState order)
- `crates/server/src/triggers.rs:587` — silent dedup branch
- `crates/server/src/worker.rs:1320-1324` — view hops share source job id
- `docs/features/triggers.md` — "whole capped batch" claim

## Acceptance criteria
- Idempotency key includes the dataset (and disambiguates saved-search view hops vs fanout
  hops for the same source job).
- Test: a run writing >1 dataset under a `source_dataset='*'` trigger enqueues one hop PER
  dataset, deterministically.
- Test: a saved-search view materialization no longer collides with the fanout hop.
- Dedup suppression logs (at least debug-level) with trigger id + key.
- Safe against already-fired old-format keys (no re-fire storm on upgrade; state why).
- `docs/features/triggers.md` corrected to describe actual per-dataset semantics.

## Risks / non-goals
- Non-goal: fan-in/join semantics (documented non-goal stays).
- Risk: changing key format re-fires triggers for in-flight source jobs at upgrade — builder
  must reason about the window and document it.

## Build record
- Builder T1 (opus), wave 1 → master `48e7ade` (+ style `b635d14`). Extracted
  `DatasetBatch{Run,View}` + `dataset_idempotency_key()`: run keys
  `trig:{t}:{job}:ds:{dataset}`, view keys `...:view:{search}:ds:{dataset}`, terminal
  unchanged. Datasets walked sorted (was RandomState). Dedup suppression now logs at debug.
  Upgrade safety reasoned in the commit: old keys never collide with new; only window is a
  pre-upgrade-fanned-out job re-executed post-upgrade (fires once more, bounded, no storm).
  Tests: unit key tests + e2e `multi_dataset_run_fires_one_hop_per_dataset_not_one_per_run`,
  `view_materialization_hop_does_not_dedup_against_the_fanout_hop` (real store, literal keys
  read via SQL — Job doesn't expose idempotency_key). docs/features/triggers.md corrected.
- Gates: worktree 1050/0; master full-workspace green post-pick.
