---
slug: grants-details-first-class
type: perfect/direction
context: "[[us-federal-grants]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
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

(filled during build)
