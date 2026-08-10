---
slug: datahub-config-honesty
type: perfect/direction
context: "[[datahub-bridge]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 175ce65
---

## What & why
The shipped config.toml has `[datahub] enabled = true` against localhost:8080 — a default
checkout with no GMS spawns a doomed emission (with its DB reads) after EVERY successful
job, warning each time. And docs/features/datahub.md predates M25/M26: the entire
governance actuator (schedule disabling, budget zeroing, job enqueueing) is undocumented
in features docs, while the known-gap "trigger edges not emitted" is now false.

## Evidence
- config.toml:242-243 (enabled = true); `datahub.rs:559` (spawn per success)
- docs/features/datahub.md:7,11-23,32-35 (pre-M25/M26; false known-gap)
- routes/query.rs:532,546 (OpenAPI descriptions omit govern/flows)

## Acceptance criteria
- Shipped config.toml sets `enabled = false` with a comment on how to opt in (behavior
  change for anyone relying on the accidental default — documented in the commit).
- datahub.md rewritten to the real surface: emit set (incl. flows/fine-grained lineage),
  the governance actuator + its blast radius, status shape, honest known-gaps.
- OpenAPI route descriptions corrected.

## Risks / non-goals
- Non-goal: any behavior change beyond the config default.

## Build record
- Builder DH1 (opus), wave 1 → master `175ce65` (gate in flight at write). config.toml
  ships `enabled = false` (behavior change flagged in commit body); datahub.md rewritten
  (config table, real emit set, governance actuator + blast-radius table, today's
  freeze-on-outage truth — DH2 changes it, doc states today's); OpenAPI corrected.
- DH1 flagged: repo-wide fmt drift now ~190 sites (growing; round-6 seed (c) — decide
  toolchain pin vs one fmt commit at wrap).
- Gates: worktree 1098/0.
