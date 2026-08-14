---
slug: crawl-corpus-stays-addressable
type: perfect/direction
context: "[[dataset-storage]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: 2026-08-14
commit: 0b6be6f
---

## What & why

**No crawl body is ever pinned against artifact retention — not "not yet extracted from", but
*every* crawl body, *always*, by construction. And when one is reclaimed while a later job still
addresses it, the job goes green with zero records.**

The crawl→extract/plugin `source` mode is this repo's documented cross-job workflow, and
`AppContext::read_source_artifact` (`crates/core/src/app.rs:163-191`) is its seam — **11 callers
across 4 apps** (`extractor` ×6 incl. `induce.rs`/`replay.rs`, `plugin` ×4 incl. `observatory.rs`,
`mpsv-vpm` ×1, whose own comment reads *"Every day not captured is lost forever"*).

`pinned_artifact_refs` (`crates/core/src/datasets.rs:1976-2002`) gates **both** UNION arms on
`artifact_sha IS NOT NULL AND rules_hash IS NOT NULL`. The crawl stamps:

| dataset | `artifact_sha` | `rules_hash` | pinned? |
|---|---|---|---|
| `pages` (`crawl/src/lib.rs:583-587`, `job_prov()`) | None | None | no |
| `page_versions` (`crawl/src/lib.rs:669-678`) | **Some** | **None** — deliberate, commented | no |

Arm 2 cannot rescue them either: it requires a revision on the *same* `(app, dataset, key)` carrying
both halves, and nothing writes into the crawl's keys but the crawl.

**The gate conflates two different questions.** `rules_hash` marks *"a RuleSet made a provenance
claim about this record"*. The pin needs to answer *"is this artifact still addressable by a live
record"* — and that is answered by `artifact_path` + `job_id`, **which crawl `pages` records do
carry** (`crawl/src/lib.rs:760`), and which are exactly the triple `read_source_artifact` resolves
(`app.rs:168-187`) and exactly what `ArtifactRef` is made of. Director-verified in source: the data
needed for the pin is already present; only the gate excludes it.

**The failure surface is silent green.** `extractor/src/lib.rs:1656-1667` pushes an unreadable body
onto `missing` and `continue`s; `:1675-1684` then runs the extraction with `FetchHealth::default()`
(comment: an unreadable stored body "is a corpus problem, not a fetch problem"); `:1689-1709`
returns `Ok`. Because the run is zero-doc with clean fetch health, the extraction-health detector
moves neither state nor baseline — so **`/sources` keeps reporting the source `healthy`**. Job green,
source green, zero records, and the only trace is a `missing_keys` array truncated at
`MISSING_ECHO_LIMIT`.

## Evidence

- `crates/core/src/datasets.rs:1976-2002` — the two-arm pin query and its gate.
- `crates/apps/crawl/src/lib.rs:575-587` (`job_prov`), `:669-678` (`page_versions` prov), `:760`
  (`pages` records carry `artifact_path`), `:649-651` (same `{artifact_path, job_id}` contract).
- `crates/core/src/app.rs:163-191` — `read_source_artifact` resolves `{app}/{job_id}/{artifact_path}`.
- `crates/core/src/retention.rs:10-20` names three readers of archived bodies; the pin serves only
  `rederive`. `:55-58` is a footnote beside this, not the main story.
- `crates/core/src/config.rs:1119` — `artifact_retention_days: 0`; `config.rs:857-867`'s interlock
  rationale (*"artifact pins are held by replayable revisions"*) is **vacuously satisfied** for crawl
  bodies: there are no pins to lose early.
- `crates/core/src/doctor.rs:171-187` — `half_stamped_provenance`.

## Acceptance criteria

1. A body that a live record still addresses via `read_source_artifact` is **not reclaimable by age
   alone**, whether or not any RuleSet ever made a provenance claim about it.
2. The widening is a **named, tested function or a documented query change**, not an inline tweak —
   with a test named after the anti-pattern proving a crawl-written body survives a retention sweep
   that would otherwise delete it, and a **counter-test** proving a body nothing addresses is still
   reclaimable. The guard must not turn retention into a no-op.
3. **Verify before you widen:** confirm what `pinned_artifact_refs` returns for crawl `pages` and
   `page_versions` rows today by test, not by reading. If arm 2 already covers some case I have
   called uncovered, say so and narrow the change — a silently-no-op widening is the exact failure
   class this direction exists to kill.
4. **Rider — the doctor tells the operator to break a deliberate design.**
   `doctor.rs:171-187` raises `half_stamped_provenance` on `page_versions` (sha without rules_hash)
   and its remediation text says to stamp both or neither, contradicting the commented decision at
   `crawl/src/lib.rs:675-677` (*"`None` = unknown, never a fabricated pin"*). Every healthy crawl run
   grows a finding the operator is told to fix and shouldn't. Make the check or its remediation text
   honest about the archives-whole-bodies case.
5. **Rider — `pinned_bytes: 0` currently reads as "nothing needs protecting" when the truth is
   "nothing is protectable".** `AppReclaim` computes `cassette_files`/`cassette_bytes`
   (`retention.rs:113-114`, filled `:199-202`) but `RetentionPlan` (`:120-132`) declares no cassette
   totals and the rollup (`:212-219`) skips them — so `retention.rs:100-101`'s claim that "the three
   `*_bytes` breakdowns partition it exactly" is true per-app and **false at the plan level**, leaving
   an unlabelled residue. Emit the missing plan-level total so the preview partitions.
6. `docs/features/datasets.md` says which readers the pin protects and which it does not.

## Risks / non-goals

- **Non-goal: changing the crawl's write path.** `crawl/src/lib.rs:575-587` gives an explicit,
  measured rationale (per-record stamping = one write transaction per crawled page on the platform's
  highest-volume write path). Fix the *pin*, not the writer.
- **Non-goal: `crates/apps/**` entirely.** This is a core-side fix; the app callers are read-only
  evidence.
- **Latent, and say so honestly:** `artifact_retention_days` defaults to 0, so nothing is being lost
  today. The defect is that turning on the documented retention knob silently destroys the documented
  crawl→extract pipeline. Do not overstate it as live data loss in docs or commit message.
- Widening a pin makes retention reclaim *less*. State the new worst-case retention behaviour
  explicitly so an operator is not surprised by a sweep that frees less than before.

## Build record (r24)

**SHIPPED `0b6be6f`** — KEEP. Criterion 3 was honoured literally and it mattered: **the new test
failed first with `pinned = {}`** for crawl-shaped `pages` + `page_versions` rows, so the claim was
*measured* rather than inferred before anything was widened.

Arm 1 (rederive's snapshot) is unchanged and still requires both halves. Arm 2 now asks only "does a
**live** record address this body" — `job_id` + `artifact_path`, non-empty, `removed_at IS NULL`.
**One tightening beyond the brief, and correct:** `records` retains tombstoned rows, so `removed_at
IS NULL` was added — a tombstoned record is returned by no read surface and must not pin.
The `<> ''` guard mirrors `read_source_artifact`'s own empty-check, so the two agree by construction.

**Replacing an existing test was legitimate and I checked it.** `unstamped_records_pin_nothing`
asserted that a record carrying `job_id` + `artifact_path` pins nothing — i.e. **it encoded the
defect as the contract.** Its rationale ("retention must not become decorative") is preserved by the
counter-test `a_body_no_live_record_addresses_is_still_reclaimable`.

Both riders shipped: the doctor's `half_stamped_provenance` remediation now names
`artifact_sha`-without-`rules_hash` as the legitimate archives-whole-bodies shape (so a healthy crawl
run stops growing a finding the operator is told to fix); `/retention/preview` gained plan-level
`cassette_*` **and** `within_window_*` so four classes partition `total_bytes` exactly.
Tests: retention 9/9, doctor 10/10, core 600/600.

**Builder refutations, both sharpening the note:**
1. "11 `read_source_artifact` callers across 4 apps" is understated — it is **17 call sites across 6
   modules** in 4 app crates. Larger blast radius than either scout or Director measured.
2. My rider claimed the `*_bytes` breakdowns partition "true per-app, false at plan level". **False
   at BOTH levels:** `WithinWindow` incremented nothing anywhere, so any body younger than the cutoff
   was unaccounted for per-app too. A cassette-only fix would have left the bigger residue.

**New worst case, stated in the docs rather than discovered by an operator:** on a crawl-heavy store
the current body of every live `pages` record and every archived `page_versions` body now reports as
`pinned`. What stays reclaimable: superseded copies in abandoned job dirs (the growth driver this
module was written for), tombstoned records' bodies, and orphans.

**Director-applied Class C follow-up (`303dbd5`):** `docs/features/extraction.md` still described the
old pin rule — the doc a source-mode user actually reads. Lot B reported it from outside its doc
write set rather than reaching in. Now names both readers.
**Blocked, not skipped:** splitting `half_stamped_provenance` by half needs
`Datasets::half_stamped_revisions` to return *which* half, forcing an edit to
`routes/doctor.rs` outside the write set. The builder took the direction's stated alternative (fix
the summary + remediation text) and explicitly refused the fake fix of adding a `StoreFacts` field
the live endpoint would never populate. Banked for a future dataset-storage round.
