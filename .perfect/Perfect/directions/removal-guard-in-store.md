---
slug: removal-guard-in-store
type: perfect/direction
context: "[[dataset-storage]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: c21f630
---
## What & why
The guard that stops a degrading source from tombstoning a whole dataset off a short snapshot lives
in `AppContext::sync_many_with_provenance` — one layer above the store. Any caller that hand-rolls
upsert + `detect_removed` bypasses it entirely, and the peer app already does exactly that. Push the
guard down to the store API so the unsafe path stops existing, and enforce the convention with an
inventory test rather than a doc sentence (repo doctrine, `.claude/CLAUDE.md`).

## Evidence
- Guard location: `crates/core/src/app.rs:613-623` (`enforced_state(...).suppresses_removals()`).
- Empty-snapshot-only protection in the store: `crates/core/src/datasets.rs:662-664`.
- Bypassing caller: `crates/apps/peer/src/lib.rs:38-44`.

## Acceptance criteria
- Removal detection is only reachable through a guarded seam (health-aware / snapshot-token API).
- Inventory test (EXPECTED-diff idiom) proving no crate calls raw `detect_removed` outside it.
- Peer app migrated to the guarded seam with its mirror semantics preserved.
- Test: a suppressed-removal run leaves existing tombstones and live records untouched.

## Risks / non-goals
Not a change to what "degrading" means — the resilience verdict logic stays as-is. Keep the
empty-snapshot guard as defence in depth.

## Build record
`Datasets::detect_removed` now requires a `RemovalGuard`, mintable ONLY via
`RemovalGuard::for_source_state(state)` (None when degrading) — the check became a **precondition of
the operation instead of a convention around it**; bypassing it is now impossible rather than merely
discouraged. Peer app migrated to a new `Datasets::tombstone_keys` (removal by NAME, which is what it
actually needed — it was only doing snapshot inference to reach the tombstone writer). Both paths
share `apply_tombstones`, so de9f0a0's atomicity/chunking holds for both. `artifact_sha` omission
preserved; the empty-mirror refusal extracted as `tombstones_would_empty_the_mirror` with its own
test (it used to be an accidental side effect of the store's empty-present guard).
Inventory test `no_crate_calls_detect_removed_outside_the_guarded_seam` walks every
crates/**/src/**.rs in EXPECTED-diff form, **mutation-checked**, and asserts it scanned >50 files so
a broken walk cannot pass vacuously.
Director review: read the diff; the capability-token design is stronger than the brief asked for.
Cherry-picked as c21f630. Gates: 938 workspace tests pass on master.
