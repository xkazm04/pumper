---
slug: artifact-retention-provenance-aware
type: perfect/direction
context: "[[dataset-storage]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 92129d1
---
## What & why
Artifact bodies at `data/artifacts/<app>/<job_id>/<name>` are never deleted by anything in the
workspace, so a long-running local node silently fills its disk. They are not disposable, though:
`POST /provenance/{app}/{dataset}/{key}/rederive` replays the archived body through its pinned
ruleset, stored-pages extraction reads them cross-job, and VCR cassettes live in the same tree. The
policy must therefore be provenance-aware — pin bodies still referenced by a replayable revision,
age out the rest — and it should also close the other tables that have no prune path at all
(`cost_events`, `webhook_deliveries`, `job_yield`, `saved_search_seen`).

## Evidence
- Writes: `crates/core/src/app.rs:133-150`; cross-job reads `app.rs:163-191`; VCR `core/src/vcr.rs:351`.
- Crawl revisit writes a NEW job_id copy and abandons the old: `crates/apps/crawl/src/lib.rs:568`.
- Rederive depends on the body: `crates/server/src/routes/provenance.rs:166`.
- Janitor exists but covers only revisions + health sketches: `crates/server/src/main.rs:301-338`;
  `revision_retention_days: 0` by default `crates/core/src/config.rs:907`.
- Docs admit the gap: `docs/features/extraction.md:59`, `crawling.md:56`, `resilient-extraction.md:90`.

## Acceptance criteria
- Config keys (+ validation) for artifact retention and the newly-pruned tables; off-by-default only
  where deletion is data loss, with the reason in the code comment.
- Pruning wired into the existing `retention_janitor`, not a second loop.
- A body referenced by a replayable revision inside the window SURVIVES past the age cutoff — test.
- Dry-run mode reports reclaimable bytes per app without deleting.
- Mapped `docs/features/*` updated in the same change (the three docs that admit the gap).

## Risks / non-goals
Not a general blob store. Do not delete anything a `replayable()` revision still points at. No
retroactive rewriting of provenance stamps.

## Build record
New `core::retention`: `keep_reason` orders pinned → cassette → age, and `artifact_is_reclaimable` is
DEFINED in terms of it so the two cannot disagree. `Datasets::pinned_artifact_refs` builds the veto
list from BOTH halves — the snapshot a replayable revision carries AND the current record of any key
with a replayable revision. **That second half came from a builder discovery the brief did not
contain**: `rederive` locates the body from the RECORD's current data, not the revision's, which is
where it actually looks after a crawl revisit moves the body. `Storage::prune_ledgers` bounds the
four unbounded tables, each SCOPED rather than blanket-aged: a running job's cost events survive
(they back its budget ceiling), pending/failed deliveries survive (live retry queue + replayable
DLQ), dead has its own knob. Config validation rejects three settings that parse fine and lie,
including `revision_retention_days < artifact_retention_days` (pins are held by revisions, so
pruning history first un-pins bodies early). Wired into the EXISTING janitor; `GET
/retention/preview` shares the same `artifact_retention_plan`. The pinning test uses a cutoff in the
FUTURE, so anything surviving survives because it was pinned, not because it was young.
Director review: read the diff; the pin-before-age ordering is right and the scoping choices are
each justified in code. **Accepted trade-off**: every knob ships at 0/off (follows the
`revision_retention_days` precedent — never delete user data on an assumption), which means nothing
is actually bounded until an operator opts in. Cherry-picked as 92129d1 (conflict in
`core/src/lib.rs` re-export list + `runtime.md` resolved by hand — a blind union would have
duplicated the shutdown bullet).
