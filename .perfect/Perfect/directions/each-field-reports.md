---
slug: each-field-reports
type: perfect/direction
context: "[[extraction-core]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
Inside an `Each` listing rule, per-inner-field extraction outcomes are invisible: the whole
array gets ONE FieldStatus, so a listing whose `price` selector silently dies (site drops a
class) still reports Matched as long as the container matched. Every downstream honesty
surface — worst_fields, replay deltas, resilience health sketches, provisioner gates,
datahub lineage — is blind inside listings, which is exactly where most real datasets live.
After this ships, listing rot is visible the run it starts.

## Evidence
- crates/core/src/extract.rs:659-671 — `CompiledRule::Each` returns `(Value::Array(items),
  true, "", container_matched)` → one `FieldStatus::classify` for the whole rule.
- each_extract/extract_scoped (~:827-876) — inner fields extracted with no report plumbing.
- Verified live 2026-08-12 (Director re-read; round 10 did not touch extract.rs).

## Acceptance criteria
- Each rules report per-inner-field outcomes aggregated across items (per field: matched /
  empty / error counts out of item count), ADDITIVELY on the existing report shape — existing
  consumers must keep compiling and their current semantics must not silently change.
- worst_fields (or its equivalent quality roll-up) surfaces an inner field that died across
  the listing; a wholly-dead inner field is distinguishable from a sparse one.
- extract_scoped emits inner reports too, or the builder records exactly why it cannot.
- Test named after the anti-pattern (e.g. `listing_rot_not_invisible`): an Each corpus where
  one inner selector stops matching → the report shows it; the old behavior (Matched, no
  signal) is pinned as the refuted case.
- Builder verifies which consumers read `report.fields` (resilience sketch, replay,
  provisioner, datahub lineage) and states per consumer whether it now sees inner misses or
  is a recorded follow-up.

## Risks / non-goals
- Non-goal: changing FieldStatus for non-Each rules or breaking the report JSON shape.
- Risk: report size on wide listings — cap or aggregate (counts, not per-item lists).

## Build record
(pending)
