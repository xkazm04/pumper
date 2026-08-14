---
slug: retention-pin-is-dead-code
type: perfect/direction
context: "[[dataset-storage]]"
lens: robustness
status: rejected
size: —
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## The banked claim, and why it is REFUTED — decisively

Carried in the queue since an earlier round as *"a retention pin mechanism exists but nothing reads
it"*, with the implied consequence *"a pinned dataset would be deleted by retention."*

**The premise fails at every link.** Re-scouted 2026-08-14.

### The pin is the FIRST branch of the sweep

`crates/core/src/retention.rs:160-173`:
```rust
pub fn keep_reason(...) -> Option<KeepReason> {
    if pinned.contains(&file.reference) {
        return Some(KeepReason::Pinned);
    }
    if protect_cassettes && file.reference.name == CASSETTE_FILE {
        return Some(KeepReason::Cassette);
    }
    (file.modified >= older_than).then_some(KeepReason::WithinWindow)
}
```
Ahead of both the cassette rule and the age cutoff. `artifact_is_reclaimable` (`:148-155`) is
*defined in terms of* `keep_reason` specifically so the two cannot disagree, and
`plan_artifact_retention` (`:177-221`) accounts `pinned_files` / `pinned_bytes` separately.

### Both consumers pass real pins, from the same builder

- `crates/server/src/routes/retention.rs:36` loads `state.datasets.pinned_artifact_refs()`, `:44`
  passes it into `plan_artifact_retention`.
- The janitor calls **that same function** — `crates/server/src/main.rs:539`
  `crate::routes::artifact_retention_plan(&state, artifact_days)`, deletes at `:541-545`, logs
  `pinned = plan.pinned_files` at `:550`.

Preview and janitor **cannot diverge by construction.**

### `/retention/preview` IS pin-aware

Reports `pinned_files` / `pinned_bytes` globally (`routes/retention.rs:90-91`) and per-app inside
`per_app` (`:92`, from `AppReclaim.pinned_files/pinned_bytes`, `retention.rs:109-111`).

### The pin source is a real, non-trivial query

`crates/core/src/datasets.rs:1976-2002` — a two-armed `UNION`: the revision's own snapshot, plus the
*current* record for any key with a replayable revision (so a crawl revisit that moved the body to a
new `job_id` does not un-pin it), both gated on `artifact_sha IS NOT NULL AND rules_hash IS NOT NULL`.

### There is a test written specifically to catch the death of this pin

`retention.rs:352-370`, `a_pinned_body_is_not_reclaimed_by_age_alone`, whose doc reads: *"Delete the
`pinned.contains` branch from `keep_reason` and this test fails — which is the point: the pin is not
a comment."* Plus `pinning_is_per_body_not_per_job_directory` (`:376-384`).

**Answer to "would a pinned dataset be deleted by retention": no. Verdict: REJECTED, premise refuted.**
Remove it from the queue; do not re-bank in this form.

## The one genuine narrow gap worth banking in its place

The module doc (`retention.rs:10-20`) names **three** live readers of archived bodies: `rederive`,
`AppContext::read_source_artifact` (the cross-job crawl→extract/plugin seam), and VCR cassettes.
**Only #1 pins.** `read_source_artifact` is not a pin source — a body is pinned only if some revision
stamped *both* `artifact_sha` and `rules_hash` (`datasets.rs:1982`, `:1994`). A crawl body not yet
extracted from, or written by a non-stamping path, is reclaimable by age alone while that seam still
addresses it. `retention.rs:55-58` states outright that nested-name bodies *"can never be the target
of a provenance claim — it is therefore never pinned, and is governed by age alone."*

**Latent, not live:** `artifact_retention_days` defaults to 0 (off), and the revision-vs-artifact
ordering hazard is already caught by config validation (`config.rs:1755-1774`). Size **M** — a real
query widening with real correctness stakes. Worth a slate slot in a future dataset-storage round;
worth nothing as "the pin is dead code."
