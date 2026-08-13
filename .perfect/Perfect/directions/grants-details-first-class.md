---
slug: grants-details-first-class
type: perfect/direction
context: "[[us-federal-grants]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 3b8d994+6489ec2+0154932
---

## What & why

`grants/opportunity_details` is the only source of federal award amounts in the whole product —
the catalog says so in prose — and it is the least governed dataset the repo writes.

1. **It can never report `unchanged`.** `detail_record` writes
   `"harvested_at": ts(chrono::Utc::now())` into every record, and `flush_details` upserts with
   plain `upsert_many_stamped` and no `DerivedPaths`. Change detection hashes the whole value, so
   a re-harvest of a byte-identical `fetchOpportunity` body writes a **new revision** and reads
   `changed`. `grep -rn "DerivedPaths" crates/apps/grants-gov/` → **0 hits** — while `eu-sedia`
   (`:279`) and the plugin observatory (`observatory.rs:737`) both adopted that exact seam, which
   r14 built for this. The dataset is genuinely watchable (`registry.rs:82-89` registers `grants`
   as a virtual namespace with grants-gov as a publisher), so `POST /watches {app:"grants",
   dataset:"opportunity_details"}` is accepted and rides `load_run_changes` → `notify_watches` /
   `fire_dataset_triggers`. Every notification it will ever send is noise.
2. **It has no catalog row at all.** `grep -n "opportunity_details" catalog/data-sources.toml` →
   one hit, inside a free-text `notes` field. No `[[source]]`, no `[source.contract]`, no
   `max_staleness_hours`. `/catalog/health` cannot see it and `enforce_contracts` finds no
   contract for the pair. This is the same omission `topic-stats-honesty` (r14) fixed for cordis
   by adding the `cordis-topic-stats` row.
3. **The one contract that does exist is structurally inert.** `max_row_delta_pct = 50.0` is
   declared over the listing as "the floor every publish must clear", but `Contract::evaluate`
   computes row delta **only when `removed > 0`**, and grants-gov writes with
   `upsert_many_with_provenance`, which never emits a `removed` revision. The declared mass-delete
   guard over the largest grant source cannot fire, ever.
4. **The join reads the whole corpus to extract three numbers.** `enrich_with_detail_amounts`
   calls `ctx.datasets.list(UNIFIED_APP, DETAILS_DATASET, 1_000_000)` and deserializes every
   record — including the verbatim `synopsis` object with full NOFO announcement HTML, tens of KB
   each — to read `requirements.{award_floor, award_ceiling, estimated_total_funding}`. No
   tripwire at the limit (cordis got `aggregate_truncated` + a warning for exactly this) and
   `Datasets::list` has no `removed_at IS NULL` filter, so tombstoned details join too.

## Evidence

- `crates/apps/grants-gov/src/lib.rs:910` — `harvested_at: ts(chrono::Utc::now())` per record.
- `crates/apps/grants-gov/src/lib.rs:783-793` — `upsert_many_stamped`, no `DerivedPaths`.
- Adopters of the seam: `crates/apps/eu-sedia/src/lib.rs:279`,
  `crates/apps/plugin/src/observatory.rs:737`. Seam itself: `crates/core/src/datasets.rs:3751`.
- `crates/server/src/registry.rs:82-89` — `grants` virtual namespace, grants-gov as publisher.
- `catalog/data-sources.toml:48` — the sole `opportunity_details` mention, in `notes`.
- `catalog/data-sources.toml:50-56` — `max_row_delta_pct = 50.0`;
  `crates/core/src/catalog.rs:223-233` — row delta computed only when `removed > 0`;
  `crates/apps/grants-gov/src/lib.rs:342` — `upsert_many_with_provenance`.
- `crates/apps/grants-common/src/lib.rs:654-656` — the 1,000,000-row list;
  `:596-607` — the three fields actually read.
- `crates/core/src/datasets.rs:1622-1633` — `list` has no tombstone filter.
- Precedent for the tripwire: `crates/apps/cordis/src/lib.rs:431`, `:482`.

## Acceptance criteria

1. A re-harvest that fetches a byte-identical detail body reads **`unchanged`**, with no new
   revision. Adopt the existing `DerivedPaths` seam — do not build a second mechanism.
   **Verify the exact path spelling against the implementation and its adopters before writing
   it** (r19 shipped a `DerivedPaths` entry that silently did nothing because the leading-slash
   convention was guessed from prose); a test that would still pass with the seam removed has not
   proved anything.
2. A behavioural test: same input twice ⇒ second run reports `unchanged`, zero new revisions.
   Pair it with a counter-test that a genuine body change still reads `changed`, so the fix cannot
   be "stop detecting changes".
3. `grants/opportunity_details` gets a real `[[source]]` row with a contract and a staleness
   bound, modelled on `cordis-topic-stats`, and `/catalog/health` can see it.
4. The inert `max_row_delta_pct` is resolved honestly — either made able to fire, or replaced with
   a guard that matches how this app actually writes, or removed with the reason recorded. A
   declared safety net that cannot fire is worse than no declaration; **do not leave it as-is with
   a comment.**
5. The detail-corpus read stops being an unbounded full deserialization: at minimum the limit
   grows a tripwire that surfaces (`aggregate_truncated`-style) rather than silently windowing,
   and the tombstone question is answered explicitly. If a projection/narrower read is cheap here,
   take it; if it is not, say why in the diff and ship the tripwire.

## Risks / non-goals

- **Non-goal**: making `grants/opportunity_details` searchable. It is deliberately absent from
  `index_datasets` (`grants-common:204-209`); NOFO HTML in the search corpus is a separate call.
- Hazard: excluding `harvested_at` from the change hash must not make it *unreadable* — consumers
  that want "when did we last touch this" should still get it. Verify what `DerivedPaths` excludes
  (hash vs storage) before assuming.
- Hazard: adding a `[[source]]` row can trip the catalog coverage test, which requires a
  registered app (`catalog/data-sources.toml:507-516` documents the blocker and names two other
  instances). Check that test before adding the row; if it blocks you, report it rather than
  weakening the test.

## Build record

**Verdict: KEEP.** `3b8d994` (app half) + `6489ec2` (the inert contract) + `0154932` (the catalog row).
All four sub-items of the direction, and the hardest one was refused-then-solved rather than faked.

1. **`DerivedPaths` adopted, spelling verified.** `derived_paths()` = `DerivedPaths::new(["harvested_at"])`
   — dot-separated names, **not** the leading-slash JSON-pointer form r19 shipped as a silent no-op;
   the criterion demanded the check and it was made. `flush_details` moved
   `upsert_many_stamped` → `upsert_many_derived`. The one-time cost is stated honestly in the doc:
   the first run after deploy re-hashes every stored detail record, so up to the whole corpus reports
   `changed` once and then settles — *"given they reported `changed` on every harvest before, that is
   a strict improvement from run two onwards."* A declaration-equality test pins the paths.
2. **The inert contract removed, not commented** (`6489ec2`). `max_row_delta_pct = 50.0` had been
   declared since M20 and could never fire: `Contract::evaluate` computes row delta only when
   `removed > 0`, and `removed` comes only from `sync_many`, which this upsert-only app never calls.
   Removed **with the guards that DO cover the class named in its place** (`SweepEnd`, the per-page
   `oppHits` refusal, the `hitCount:0`-over-a-stored-corpus refusal), and `cordis-topic-stats` was
   correctly left alone because it writes with `sync_many` and the tripwire is live there.
   `docs/features/catalog.md` gained the applicability rule so a third inert one is not added.
3. **The catalog row, after the blocker was found and reported rather than worked around**
   (`0154932`). The harvest writes the pair `("grants", "opportunity_details")` and `grants` is a
   VIRTUAL NAMESPACE, so `live_catalog_entries_map_to_registered_apps_with_matching_cron` panicked —
   and filing it under `grants-gov` **would have passed every test while being a lie** (that pair is
   never written, so `/catalog/health` would report it permanently stale and `contract_for` would
   never match). That is the same class `6489ec2` had just removed one instance of, so shipping it
   would have contradicted its own sibling commit. The guard now accepts a namespace whose publishers
   are all registered — widening what may be NAMED, not whether it is checked — plus the rule that
   makes that safe: such a row **must** carry an empty cron, *asserted* rather than trusted, because
   `reconcile` derives desired schedules from `is_scheduled()`. Non-vacuity of the new assertion was
   probe-verified and the probe reverted.
4. **The 1M-row join bounded and reportable.** `list` → `list_filtered` (live-only: a tombstoned
   detail record used to keep joining its award amounts onto a live unified row) with
   `DETAIL_JOIN_LIMIT = 200_000` and a `DetailJoin{filled, read, truncated}` result surfaced as
   `detailCorpus` + a `warnings[]` entry. The limit's doc is honest about what it is *not*: ~146x the
   live corpus, so *"the point of the number is that reaching it is reportable, not that it is
   tight"*, and it names the better fix it could not make from an app crate (a projected-read seam in
   `Datasets` is a `crates/core` change) instead of pretending the cap is the design.

**Ledger note (process).** The app half sits under `wip(grants-gov): Lot G builder-death snapshot —
D5 in flight`; the Lot G builder died after finishing it. Content reviewed and complete; message
undersells it. Carried to the skill log.
