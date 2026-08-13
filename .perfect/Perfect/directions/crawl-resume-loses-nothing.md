---
slug: crawl-resume-loses-nothing
type: perfect/direction
context: "[[crawler-core]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-13
commit: 92621fe
---
## What & why
A crawl that is killed, reaped, or simply hits `max_pages` **permanently loses fetched
pages**, and nothing anywhere says so. Three independent windows, all confirmed by two
scouts reading the file end to end:

- **Buffered-but-checkpointed.** Kept pages go into `sink_buf` and only reach the `pages`
  dataset every 50 (`PAGE_SINK_STRIDE`). The checkpoint saves on a 5s wall clock and
  serializes `frontier.seen` **and** `dedup_index.hashes()` — both of which already contain
  the buffered page. On resume `seen` is authoritative and `push` early-returns, so those
  URLs are never re-fetched; even if they were, their simhash is already in the restored dup
  index, so they would be dropped as duplicates. Their bodies sit **orphaned on disk** with
  no record pointing at them, and the restored fingerprints keep suppressing near-dups of
  pages that no longer exist in the dataset.
- **In-flight, popped, never returned.** `pop` removes a URL from the queue and inserts it
  into `seen`. On the `max_pages` break, up to `concurrency - 1` unresolved fetches are
  dropped and the final save persists them as seen-but-not-queued: unreachable forever. The
  incremental `max_pages: 50` walk — run it five times to sweep a site — leaks up to 75 URLs
  of coverage that no counter mentions.
- **Checkpoints only fire on the kept-page path.** The interval block sits inside the `else`
  of `if duplicate`; Failed, BotWall, NotModified, Gone and duplicate all `continue` before
  reaching it. A revisit sweep over 10,000 known pages that are almost all `304` produces
  **zero** intermediate checkpoints — kill it at 95% and you lose 95%.

The same window also swallows **gone markers** and **304 cadence markers** after
`stats.gone` / `stats.unchanged_304` have already been incremented, so a run can report
`gone: 40` with zero `gone: true` rows written — a result field contradicting the dataset.

The user moment: *"I resumed my reaped 100k crawl, it says `resumed: true`, and forty pages
are missing from `pages` forever — and I have no way to know which."*

## Evidence
- Order: body→disk `crawl.rs:1019-1024` → `sink_buf` push `:1039` → flush only at
  `PAGE_SINK_STRIDE = 50` (`:39`, `:1059-1063`) → `save_checkpoint` `:1076-1083`
  (`CHECKPOINT_MIN_INTERVAL = 5s`, `:1149`).
- Checkpoint content: `:1233-1238` serializes `frontier.seen` + `dedup_index.hashes()`;
  URL entered `seen` at `:562`, hash at `:1009`.
- Resume: `Frontier::restore` `:654-659` ("`seen` is authoritative"); `push` early-return
  `:555-557`. Revisit cannot recover it — it seeds only from stored `pages`
  (`apps/crawl/src/lib.rs:503-544`).
- In-flight: `pop` `:892`; `max_pages` break `:1114-1116`; final save `:1132-1136`;
  `queued()` `:644-649`; concurrency clamp `:753`.
- Checkpoint block placement: `:1076-1083` inside the `else` of `:1006`; the `continue`s at
  `:950` (Failed), `:955` (BotWall), `:976` (NotModified), `:993` (Gone), `:1006-1007` (dup).
  Contrast the correctly-placed `PROGRESS_STRIDE` emit at `:1110`.
- Marker flushes: `:969-973` (304), `:987-991` (gone); counters already bumped `:957-993`.
- **Why it survived:** the only resume test (`:2247`) runs `concurrency = 1` with **no sink**
  (`crawl(http, cfg, None, None, ...)`, `:2281`) — structurally incapable of catching either
  loss window.

## Acceptance criteria
1. No page that has been fetched and counted can be absent from the sink after a checkpoint:
   `sink_buf` is drained **before** any `save_checkpoint`, or the seen/fingerprint commit is
   deferred until after the flush. Builder picks and records the reasoning; either way the
   invariant to state in a comment is "the checkpoint never claims a page the sink has not
   been handed."
2. The in-flight set is not silently discarded: URLs popped but unresolved at loop exit are
   returned to the frontier (or tracked and merged into `queued()`) before the final save, so
   a resumed crawl re-fetches them. Test proves it at `max_pages` with `concurrency > 1`.
3. Intermediate checkpoints fire on **every** loop outcome, not only kept pages — hoist the
   interval block beside the progress emit. Test: a run of mostly-304 / mostly-duplicate
   outcomes produces at least one intermediate checkpoint.
4. Marker honesty: `gone` / `unchanged_304` / `revisited` counters cannot exceed the rows
   actually handed to the sink. If the builder finds the cheapest fix is the same flush
   ordering as AC1, say so rather than inventing a second mechanism.
5. **The test shape that could not catch this is replaced**: at least one resume test drives
   `crawl()` WITH a sink and `concurrency > 1`, killing mid-run, and asserts
   `records emitted before the kill ∪ records emitted after resume == every page fetched`.
   That assertion is the direction; the rest is implementation.

## Risks / non-goals
- **Checkpoint format**: if AC2 needs a new field, bump `CHECKPOINT_VERSION` — the existing
  version guard (`:1216`) already discards incompatible blobs cleanly, so do NOT invent a
  migration path.
- Do NOT redesign `MAX_FRONTIER` here (banked separately, deliberately, so two builders are
  not reshaping `Checkpoint` in one wave).
- `crates/core/src/crawl.rs` ONLY. No app-crate edits — the result-surfacing half is a
  different lot this same wave.

## Build record
(to fill during build)
