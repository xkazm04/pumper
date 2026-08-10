---
slug: enforcement-preview
type: perfect/direction
context: "[[source-resilience]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 413bc6f
---
## What & why
Enforcement ships off and there is no way to answer the only question that matters before turning it
on: *what would this have done to my fleet?* The evidence is already stored — every run's verdict,
score and self-explaining `reasons` are written regardless of enforcement. Replay that history and
report, per source: when it would have entered each state, which datasets would have been diverted
to `@q`, which pushes suppressed, which removals withheld, which index writes skipped. Same dry-run
pattern accepted for retention in round 4; it is what converts a decorative safety system into one
an operator would trust with writes.

## Evidence
- Verdicts/sketches/fingerprints written unconditionally in soak: `crates/core/src/resilience/store.rs:686-762`.
- Soak is a no-op only downstream: `store.rs:673-678`; default `enforce = false` `config.rs:370`.
- Self-explaining reasons already persisted: `detect.rs:164-182,315-321`; read at `routes/health.rs:164-184`.
- The four consequences to simulate: `app.rs:642-648`, `app.rs:613-626`, `worker.rs:970-992`,
  `worker.rs:1450-1459`.
- Precedent to mirror: `GET /retention/preview` (round 4, commit `92129d1`).

## Acceptance criteria
- Read-only endpoint + a `just` recipe (keep CLAUDE.md's task-runner table in sync).
- Per-source timeline of would-be state transitions, each with the reason that triggered it.
- Counts of would-be suppressed pushes, withheld removals, diverted writes, skipped index writes.
- Provably zero side effects — a test asserting the store is unchanged after a preview.
- Answers "is my fleet ready for `enforce = true`" in one call, naming the sources that are not.

## Risks / non-goals
Read-only; never writes a verdict or moves a state. Replay uses the STORED verdicts, not a re-run of
detection against a newer rule set — the point is fidelity to what actually happened.

## Build record
(pending)
