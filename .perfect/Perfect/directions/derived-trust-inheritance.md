---
slug: derived-trust-inheritance
type: perfect/direction
context: "[[dataset-api]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: e8ed5e9
---

## What & why
Derived rows are written with `trust=None, provenance=None` — and NULL means stable, so a
quarantined/provisional source row derives into a stable-looking derived row. This is the
same laundering hole round 5 closed for `grants/unified` (`916a38e`), alive in every derived
dataset. There is also no lineage: a derived record has no link to its source record or the
spec that produced it. And an unparseable `lookup` column silently degrades a spec into a
plain passthrough via `.ok()` on the untagged parse — wrong shape written, no error, no log.

## Evidence
- `core/datasets.rs:2033` (`apply_derived` → `upsert_many_at_depth(..., None, None, ...)`),
  `:2105-2112` (backfill, same)
- `core/datasets.rs:2772-2786` — untagged StoredJoin parse; `:2781-2785` silent `(None,None)`
- Provenance stamps exist on revisions (`0030_provenance.sql`) but derived writes never set
  them

## Acceptance criteria
- Derived rows inherit trust: the minimum trust across the source row and any lookup-joined
  rows (a provisional input can never yield a stable-labeled derived row). Test proves it.
- Derived revisions carry provenance identifying the derived spec (id/hash) and the source
  revision that produced them.
- Unparseable `lookup` column is LOUD: the spec errors (or is marked broken), never silently
  degrades to passthrough. Test covers the degradation path.
- Group/aggregate path handled honestly (trust = min over group members, or documented
  explicitly if different).
- Backfill path gets the same inheritance as the live recompute path.

## Risks / non-goals
- Trust propagation on group aggregates has judgment in it — spec above decides (min over
  members); builder flags DECISION NEEDED if that proves wrong.
- Non-goal: retroactive restamping of existing derived rows (backfill re-run achieves it).

## Build record
- Builder D2 (opus), wave 2 → master `e8ed5e9` (gate in flight at write time). `weakest_trust`
  floor semantics (unknown labels rank WEAKEST — conservative); batches partitioned per
  inherited stamp (`partition_by_trust` + `upsert_derived_rows`) so a strong stamp never
  covers a weak row; group path inherits weakest scanned member, live + backfill. Provenance
  per 0030 idiom: `derived_spec_fingerprint` registered in rules_versions → rules_hash;
  source write's job_id inherited; source_url/artifact_sha stay Null so derived rows never
  claim replayable. `parse_stored_join` now ERRORS on corrupt lookup column — spec skipped
  loudly, omitted from GET /derived, errors on GET /derived/{id}.
- Refuted: the live ROW path also re-parsed filters per record (brief said only backfill);
  doctor signal skipped honestly (doctor.rs outside scope).
- Gates: worktree full workspace 1072/0.
