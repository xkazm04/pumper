---
slug: derived-change-honesty
type: perfect/direction
context: "[[eu-grants]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 45dd84b
---

## What & why
eu-sedia writes the CORDIS join block INTO each opportunities record before upsert,
and change detection hashes the whole value — so every weekly cordis run marks every
joined Horizon topic "changed" in the next daily eu-sedia run. Watches, triggers,
webhooks, revision history and the yield ledger all conflate derived-join churn with
real SEDIA publications: a consumer watching EU opportunities gets a false "changed"
wave weekly. The fix is a narrow core seam — a producer can declare derived paths
that are excluded from the change-detection hash (revisions still store the full
value) — adopted by eu-sedia for the history block. The seam is a platform primitive
(the census blend has the same disease, banked in its context note).

## Evidence
- History written into the record pre-upsert: crates/apps/eu-sedia/src/lib.rs:250-256,
  upsert at :263-272.
- Change detection hashes the whole value: crates/core/src/datasets.rs:434.
- Consequences ride the change feed: worker load_run_changes / watches / triggers /
  yield ledger (core/src/costs.rs:211 counts "changed").
- Unified is NOT polluted (normalize_eu_sedia maps explicit fields; history not among
  them — grants-common:436 area), so scope is opportunities only.

## Acceptance criteria
1. Core seam: a producer can exclude declared derived paths from change-detection
   hashing at upsert (narrowly scoped — an explicit opt-in per write, not a global
   config). Revisions/reads still carry the full value. Core tests: real field change
   fires; derived-only change does not; removal semantics untouched.
2. eu-sedia adopts it for `history`: a weekly cordis stats movement no longer marks
   joined topics changed in eu-sedia/opportunities. Test named for the anti-pattern
   (derived_churn_does_not_mark_source_changed or similar).
3. One-time transition churn (first post-upgrade run re-hashes) is reasoned about,
   bounded, and stated in the doc/commit — not discovered by users.
4. docs/features/datasets.md documents the seam; docs/features/apps.md gains the
   missing truth about this pipeline: the history join, `historyJoined`/`truncated`,
   and cordis's existence (it is absent from feature docs entirely). Fix the
   datahub.md:85-86 example that names a nonexistent eu-sedia "calls" dataset.
5. The seam's surface is documented as producer-facing (feature-doc-map: datasets doc
   owns it); no HTTP API change required.

## Risks / non-goals
- Risk: touching the change-detection hash path is blast-radius-heavy — the seam must
  be opt-in and default-off so every existing producer's behavior is byte-identical.
  An inventory-style test or assertion that no other call site changed behavior is
  worth its cost.
- Risk: derived-path removal from the hash must handle the path being absent
  gracefully (most records have no history block).
- Non-goal: adopting the seam for census (banked in us-business-census); read-side
  joins; any change to revision storage shape.

## Build record
Shipped 45dd84b (Lot E, opus). DerivedPaths opt-in per write, NONE default;
hash_input returns Cow::Borrowed when nothing declared/matched — the no-op case
is provably byte-identical, and the 4-entry-point hash-identity test asserts it
against raw SQL. refresh write: derived-only movement rewrites the body (reads
never stale), appends NO revision, verdict Unchanged — only reachable with
declared paths. SimHash deliberately full-value (/duplicates compares stored
records); derived-spec recompute passes NONE with reasoning. Transition pinned:
exactly one changed run then settles. eu-sedia adopts for history via shared
HISTORY_FIELD const (test pins join+declaration agree); e2e proves weekly cordis
churn → changed:0, no second revision, fresh read. Docs: datasets.md seam table;
apps.md gains cordis + the join; datahub.md fake-"calls" example fixed. Declared
exceptions accepted: core/lib.rs barrel export, Cargo.lock. Review: KEEP.
