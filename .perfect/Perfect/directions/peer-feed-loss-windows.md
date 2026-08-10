---
slug: peer-feed-loss-windows
type: perfect/direction
context: "[[dataset-peering]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-10
commit: 54bf16a
---

## What & why
A mirror's one promise is convergence: everything the origin has, the mirror eventually
has. Today the walk can silently and permanently lose data five different ways — a
same-timestamp revision race, a malformed cursor that silently restarts (and livelocks),
schema drift that discards items while advancing the resume point, a tombstone guard
that drops removals forever, and runs that fail every dataset while reporting a green
job. This direction closes the loss windows and makes failure visible. User moment: "my
mirror is either identical to the origin or telling me loudly why not."

## Evidence
- `crates/core/src/datasets.rs:788-794` — `changes_page` predicate `created_at > ?3`;
  `crates/core/src/datasets.rs:699-701` documents that a whole upsert-chunk shares one
  stamp. Peer advances `since` to page-1-newest (`lib.rs:334-341,421-424`): same-stamp
  revisions committed after the page was served are excluded forever.
- `crates/server/src/routes/datasets.rs:717-728` + `routes/error.rs:77-85` +
  `datasets.rs:18-20` — unparseable cursor → `after=None` → page 1 with 200, no signal.
  Peer resumes at top, everything dedupes via `seen`, `pulled` still counts
  (`lib.rs:343`) → budget burns, walk re-suspends near top: `status:"ok", capped:true,
  new:0` forever.
- `crates/apps/peer/src/lib.rs:566-586,607` — malformed items are counted
  (`skipped_malformed`) but the walk completes and `since` advances past the discarded
  revisions; a `key`→`record_key` rename on the origin is permanent silent data loss.
- `crates/apps/peer/src/lib.rs:405-410` — `tombstones_would_empty_the_mirror` refusal
  adds a note but `completed` stays true and `since` advances: those removals are never
  retried.
- `crates/apps/peer/src/lib.rs:224-242` — per-dataset errors become
  `{"status":"error"}` objects inside an `Ok` result: a week of total failure is a green
  job history.

## Acceptance criteria
- [ ] **Resume boundary is loss-free.** Either the peer resumes with an inclusive
      boundary and relies on idempotent re-apply (re-upserts of already-seen revisions
      are `Unchanged`), or the feed gains a keyset `since` (timestamp+rowid, matching
      the existing cursor format). These are OPTIONS — pick with the tradeoff stated
      (re-fetch cost vs API surface). A test proves: revisions stamped equal to the
      resume point but committed after page 1 was served are picked up on the next run
      (`equal_stamp_revisions_not_lost` style).
- [ ] **Malformed cursor is loud.** `GET .../changes?cursor=<garbage>` returns 400
      naming the expected format (parse failure distinguished from absent cursor);
      the peer surfaces the 400 as a run error instead of walking from the top.
      Inventory/OpenAPI updated; `bad_cursor_400_not_page_one` test.
- [ ] **Schema drift halts, not discards.** When `skipped_malformed > 0`, the walk does
      NOT advance `since` and does NOT complete; the dataset's result status reflects
      the drift with the count. Named predicate + test
      (`drift_freezes_cursor_not_advances` style).
- [ ] **Tombstone refusal defers, never drops.** Refused tombstones persist in
      `PeerState` and are retried on subsequent runs; the run's status/note says
      removals are pending. Test: refusal → next run with a live feed key set that no
      longer empties the mirror → tombstones applied.
- [ ] **Run honesty.** All-datasets-errored ⇒ the job itself fails (`Err`); any-dataset
      -errored ⇒ result `status:"partial"`, never `"ok"`. Test each.
- [ ] Repo law: every fix is an extracted named function with an anti-pattern-named
      test, wired after tests exist.

## Risks / non-goals
- Non-goal: ETag/304 revival, trust propagation, reconcile/ghost-records (banked).
- Risk: inclusive-resume re-applies the boundary chunk every run — bounded by one
  chunk; state the bound in a comment. Keyset-since instead changes a public route —
  if chosen, it must stay backward-compatible (plain timestamps keep working).
- Risk: 400-on-bad-cursor is a behavior change on a public route; the SDK only replays
  server-issued cursors, so only corruption is affected — note in doc changelog.
- Blast radius: `routes/datasets.rs` cursor handling is shared with `/history` — keep
  the 400 shape consistent across both if both parse cursors.

## Build record
All five windows closed as extracted named functions with anti-pattern-named tests: inclusive_since (1-microsecond wire rewind = exact >= boundary; option A chosen — re-fetch cost bounded to one chunk, no API surface change), parse_cursor_arg/bad_cursor_message (400 on corrupt cursor, scoped to /changes + /history; blank still = first page), walk_may_advance (drift freezes the resume point, status "drift"), merge_deferred_tombstones + pending_tombstones in PeerState (refusal defers, capped 10k oldest-first), run_outcome (all-errored fails the job, any-degraded = partial). OpenAPI 400s documented; datasets.md behavior-change note. Review: keep. Director added the missing refusal->retry->apply e2e as 06b1deb.
