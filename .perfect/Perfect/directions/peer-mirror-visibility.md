---
slug: peer-mirror-visibility
type: perfect/direction
context: "[[dataset-peering]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-10
commit: 4ea11b7
---

## What & why
The point of mirroring into a local pumper is that the mirror behaves like local data —
watches fire, triggers chain, search finds it. Today none of that happens: mirrored
records are invisible to every downstream mechanism because the change batch is scoped
to `job.app` (`"peer"`) while writes land under the namespace (`peer_<x>`). The module
doc at `lib.rs:36` claims downstream triggers see mirrored removals — false as built.
Round 6 fixed exactly this class for saved searches (`run_indexed_apps`, 21c838d);
watches and dataset triggers never got the same widening. User moment: "I set a watch on
my mirror and it fired when the origin's change arrived."

## Evidence
- `crates/server/src/worker.rs:1025` — `changes_since(&job.app, None, ...)` builds the
  run's change batch; `notify_watches` (`worker.rs:839`) and
  `fire_dataset_triggers` (`worker.rs:840`, `EvalScope::Dataset(&job.app)` at
  `triggers.rs:665`) both consume it → zero coverage of `peer_<x>` writes.
- `crates/apps/peer/src/lib.rs:234-242` — the result declares no `index_datasets`, so
  `dataset_search_docs` (`worker.rs:1540-1602`) indexes nothing and
  `run_indexed_apps` (`worker.rs:1202`) never widens to the mirror namespaces.
- `crates/server/src/worker.rs:1192-1210` — the widening seam exists and is tested for
  saved searches (`job_without_index_datasets_scopes_to_its_own_app_only`,
  `worker.rs:2009`); the watch/trigger path is the one layer that still scopes by
  `job.app` alone.
- `crates/apps/peer/src/lib.rs:36` — the false claim to make true.

## Acceptance criteria
- [ ] The peer's result declares `index_datasets` for every mirrored
      `(namespace, dataset)` it wrote this run — search indexing and saved-search
      alerts light up through the EXISTING seams with no worker special-casing for
      `peer`.
- [ ] The run change batch (and therefore watches + dataset triggers) covers the run's
      virtual apps: `load_run_changes` (or its callers) unions changes across
      `run_indexed_apps(job.app, index_specs)`. The batch keying must not conflate
      identically-named datasets from different apps — key by (app, dataset) or
      equivalent.
- [ ] This widening is a deliberate semantic change for ALL virtual-app runs (a watch
      on `grants` starts firing on `ca-grants` runs that write `grants/unified`) —
      mirror the round-6 reasoning in a doc comment, and cover the grants-shaped case
      with a test proving a watch scoped to the virtual app fires exactly once per run.
- [ ] Peer-level test: a mirrored write + a mirrored tombstone both surface in the
      change batch under the namespace app; `lib.rs:36`'s claim is now true (update the
      module doc to say precisely what propagates).
- [ ] Docs: the peering feature doc (created by this wave — see
      [[peer-two-node-proof]]) documents which downstream mechanisms see mirrored data.

## Risks / non-goals
- Non-goal: trust propagation, reconcile, scheduling — separate banked seeds.
- Risk: widening watches to virtual apps could double-fire when a watch matches BOTH
  the job app and a virtual app naming the same dataset — dedupe the union by
  (app, dataset, key) before dispatch; test it.
- Risk: `changes_since` per virtual app multiplies queries per run — bounded by the
  result's declared spec count; keep the 1000-row cap per app and say so.
- Sequencing: shares `worker.rs`/`lib.rs` with the rest of the lot — build after
  [[peer-feed-loss-windows]], before [[peer-two-node-proof]].

## Build record
load_run_changes unions changes_since across run_indexed_apps (1000-row cap PER app); batch keyed (app, dataset) — group_by_app_dataset; notify_watches loads watches per app of the batch, watch_covers_entry prevents wildcard double-fire; dataset_idempotency_key + dataset_trigger_obj gain the app; suppress_unhealthy/enforce_contracts judge each pair on its own health/contract; saved-search re-badging hack deleted (real job flows through). Peer declares index_datasets via mirrored_specs — zero worker special-casing. Semantic widening for ALL virtual-app runs documented in-code and in peering.md. Review: keep.
