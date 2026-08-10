---
slug: search-ghost-doc-gc
type: perfect/direction
context: "[[search-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: dc03bd0
---

## What & why
Every job run mints a whole-result search doc id'd `{app}:{job_id}` — unique per run,
indexed forever, deleted by nothing. Largest unbounded-growth source in the index; no merge
tuning, no GC, no size telemetry (`/search/status` = doc_count only). These docs also stamp
`dataset = app`, injecting a fictitious dataset into facets and `?dataset=` filters.

## Evidence
- `worker.rs:1471-1493, 1586-1612` (ids `{app}:{job_id}`/`{app}:{job_id}:{i}`; url-keyed
  variant upserts, job-keyed never dies)
- `worker.rs:1484/1605` — `dataset: app.to_string()`
- engine-search/lib.rs:238 default merge policy, no GC; status route = doc_count only

## Acceptance criteria
- Job-result docs get a bounded lifecycle — pruned alongside job retention or capped
  per app (builder proposes mechanism with rationale; Director decides at review).
- `dataset` field on job-result docs made honest (not the app name masquerading as a
  dataset); facet consumers verified.
- `/search/status` gains size/segment telemetry (bytes on disk, segment count).
- Test proves ghosts die under the chosen lifecycle; test proves facets no longer report
  the fictitious dataset.

## Risks / non-goals
- Saved searches may match job-result docs — pruning must not re-alert or break seen-state.
- Non-goal: changing record-doc (url-keyed) semantics.

## Build record
- Builder SE1 (opus), wave 1 → master `dc03bd0`. Mechanism: per-run ids KEPT + pre-add
  sweep of the app's prior `_job` snapshot (delete-then-add in opstamp order; test proves
  a run survives its own sweep). Builder REFUTED the stable-id option as harmful:
  claim_unseen is id-keyed, a stable id alerts once ever. Reserved namespaces `_job`
  (swept) / `_records` (url-keyed, durable) — the fictitious dataset stamp hit url-keyed
  docs too. Sweep conditional via `sweeps_prior_job_snapshot` (delete_dataset commits —
  unconditional would fsync per job). `index_stats()` default-impl trait method (core scope
  deviation, endorsed) → /search/status disk_bytes + segment_count.
- Honest: sweep call site not driven by a job-level e2e; no-re-alert reasoned from
  INSERT OR IGNORE, not integration-tested.
- Gates: worktree 1107/0; wave-1 integration gate green.
